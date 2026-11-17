//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/outbox"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/queue"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
)

var (
	pool  *pgxpool.Pool
	store *postgres.Store
	rdb   *goredis.Client
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx)
	if err == nil {
		err = pgtest.MigrateUp(pg.DSN)
	}
	if err == nil {
		pool, err = pgxpool.New(ctx, pg.DSN)
	}
	var rc *tcredis.RedisContainer
	if err == nil {
		rc, err = tcredis.Run(ctx, "redis:7-alpine")
	}
	var url string
	if err == nil {
		url, err = rc.ConnectionString(ctx)
	}
	if err == nil {
		var opts *goredis.Options
		if opts, err = goredis.ParseURL(url); err == nil {
			rdb = goredis.NewClient(opts)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	store = postgres.NewStore(pool)
	code := m.Run()
	_ = rdb.Close()
	pool.Close()
	_ = rc.Terminate(ctx)
	_ = pg.Terminate()
	os.Exit(code)
}

func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, notification_attempts, outbox_events CASCADE`)
	require.NoError(t, err)
	require.NoError(t, rdb.FlushAll(t.Context()).Err())
}

// newBatch stores a batch of n SMS notifications the way the API does.
func newBatch(t *testing.T, n int) (uuid.UUID, []*domain.Notification) {
	t.Helper()
	b := &domain.Batch{ID: uuid.Must(uuid.NewV7()), Total: n, Metadata: map[string]any{}}
	ns := make([]*domain.Notification, n)
	for i := range ns {
		var err error
		ns[i], err = domain.NewNotification(domain.NewNotificationParams{
			Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", BatchID: &b.ID,
		}, time.Now())
		require.NoError(t, err)
	}
	require.NoError(t, store.InTx(t.Context(), func(r *postgres.Repo) error {
		if err := r.InsertBatch(t.Context(), b); err != nil {
			return err
		}
		_, err := r.InsertNotifications(t.Context(), ns)
		return err
	}))
	return b.ID, ns
}

type row struct {
	Type        string
	AggregateID uuid.UUID
	Payload     map[string]any
}

func outboxRows(t *testing.T) []row {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT event_type, aggregate_id, payload FROM outbox_events ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var out []row
	for rows.Next() {
		var r row
		var raw []byte
		require.NoError(t, rows.Scan(&r.Type, &r.AggregateID, &raw))
		require.NoError(t, json.Unmarshal(raw, &r.Payload))
		out = append(out, r)
	}
	return out
}

// statuses lists the status sequence each notification's events show.
func statuses(rows []row) map[uuid.UUID][]string {
	out := map[uuid.UUID][]string{}
	for _, r := range rows {
		if r.Type != events.TypeBatchCompleted {
			out[r.AggregateID] = append(out[r.AggregateID], r.Payload["status"].(string))
		}
	}
	return out
}

func ofType(rows []row, typ string) []row {
	var out []row
	for _, r := range rows {
		if r.Type == typ {
			out = append(out, r)
		}
	}
	return out
}

func claim(t *testing.T, limit int) []*domain.Notification {
	t.Helper()
	ns, err := store.Queue().Claim(t.Context(), queue.ClaimRequest{
		Channel: domain.ChannelSMS, WorkerID: "w1", Limit: limit, Now: time.Now(), Lease: time.Minute, Aging: time.Minute,
	})
	require.NoError(t, err)
	return ns
}

func complete(t *testing.T, n *domain.Notification, status domain.Status) {
	t.Helper()
	c := &queue.Completion{
		NotificationID: n.ID, WorkerID: "w1", Now: time.Now(), Status: status,
		Attempt: domain.Attempt{NotificationID: n.ID, AttemptNo: n.Attempts, Outcome: domain.OutcomeSuccess, Duration: time.Millisecond},
	}
	if status == domain.StatusFailed {
		c.Attempt.Outcome, c.Attempt.ErrorCode, c.Attempt.Error = domain.OutcomePermanentError, "http_400", "bad recipient"
	} else {
		c.Attempt.ProviderMessageID = "msg-1"
	}
	applied, err := store.Queue().Complete(t.Context(), c)
	require.NoError(t, err)
	require.True(t, applied)
}

// ---- triggers --------------------------------------------------------------

func TestTriggers_EveryStatusChangeIsAnEvent(t *testing.T) {
	reset(t)
	batchID, ns := newBatch(t, 3)
	rows := outboxRows(t)
	require.Len(t, rows, 3, "one notification.created per inserted row")
	for _, r := range rows {
		assert.Equal(t, events.TypeNotificationCreated, r.Type)
		assert.Equal(t, "pending", r.Payload["status"])
		assert.Nil(t, r.Payload["previous_status"])
		assert.Equal(t, batchID.String(), r.Payload["batch_id"])
		assert.NotContains(t, r.Payload, "recipient", "no personal data in events")
		assert.NotContains(t, r.Payload, "content")
	}

	claimed := claim(t, 2)
	require.Len(t, claimed, 2)
	_, err := store.Queue().ExtendLeases(t.Context(), "w1", []uuid.UUID{claimed[0].ID, claimed[1].ID}, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, outboxRows(t), 5, "claims are status changes; heartbeats are not")

	complete(t, claimed[0], domain.StatusSent)
	complete(t, claimed[1], domain.StatusFailed)
	var third uuid.UUID
	for _, n := range ns {
		if n.ID != claimed[0].ID && n.ID != claimed[1].ID {
			third = n.ID
		}
	}
	_, err = store.CancelNotification(t.Context(), third, time.Now())
	require.NoError(t, err)

	rows = outboxRows(t)
	seq := statuses(rows)
	assert.Equal(t, []string{"pending", "processing", "sent"}, seq[claimed[0].ID])
	assert.Equal(t, []string{"pending", "processing", "failed"}, seq[claimed[1].ID])
	assert.Equal(t, []string{"pending", "cancelled"}, seq[third])

	last := ofType(rows, events.TypeNotificationStatusChanged)
	failed := last[len(last)-2].Payload
	assert.Equal(t, "processing", failed["previous_status"])
	assert.Equal(t, "http_400", failed["error_code"])

	done := ofType(rows, events.TypeBatchCompleted)
	require.Len(t, done, 1, "the batch completed exactly once")
	assert.Equal(t, rows[len(rows)-1], done[0], "after the change that completed it")
	assert.Equal(t, map[string]any{"sent": 1.0, "failed": 1.0, "cancelled": 1.0}, done[0].Payload["counts"])
	b, err := store.GetBatchSummary(t.Context(), batchID)
	require.NoError(t, err)
	assert.NotNil(t, b.CompletedAt)
}

// TestTriggers_BatchCompletesOnceUnderConcurrency finishes all items of a
// batch at the same moment from separate transactions. Without the batch
// row lock, the last two would each see the other as unfinished and no
// batch.completed would be emitted, or both would emit it.
func TestTriggers_BatchCompletesOnceUnderConcurrency(t *testing.T) {
	for round := range 5 {
		reset(t)
		_, _ = newBatch(t, 20)
		claimed := claim(t, 20)
		require.Len(t, claimed, 20)

		var wg sync.WaitGroup
		for _, n := range claimed {
			wg.Go(func() { complete(t, n, domain.StatusSent) })
		}
		wg.Wait()
		done := ofType(outboxRows(t), events.TypeBatchCompleted)
		require.Len(t, done, 1, "round %d", round)
		assert.Equal(t, map[string]any{"sent": 20.0}, done[0].Payload["counts"])
	}
}

func TestTriggers_RedriveReopensTheBatch(t *testing.T) {
	reset(t)
	batchID, _ := newBatch(t, 2)
	claimed := claim(t, 2)
	complete(t, claimed[0], domain.StatusSent)
	complete(t, claimed[1], domain.StatusFailed)
	require.Len(t, ofType(outboxRows(t), events.TypeBatchCompleted), 1)

	_, err := store.RetryNotification(t.Context(), claimed[1].ID, time.Now())
	require.NoError(t, err)
	b, err := store.GetBatchSummary(t.Context(), batchID)
	require.NoError(t, err)
	assert.Nil(t, b.CompletedAt, "a redriven item reopens its batch")

	again := claim(t, 1)
	complete(t, again[0], domain.StatusSent)
	done := ofType(outboxRows(t), events.TypeBatchCompleted)
	require.Len(t, done, 2, "completed again")
	assert.Equal(t, map[string]any{"sent": 2.0}, done[1].Payload["counts"])
}

func TestTriggers_DuplicatesDoNotCountTowardsTheBatch(t *testing.T) {
	reset(t)
	key := "order-42"
	first, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", IdempotencyKey: key,
	}, time.Now())
	require.NoError(t, err)
	_, err = store.InsertNotifications(t.Context(), []*domain.Notification{first})
	require.NoError(t, err)

	// A batch of two, one of which already exists: only one is created.
	b := &domain.Batch{ID: uuid.Must(uuid.NewV7()), Total: 2, Metadata: map[string]any{}}
	ns := make([]*domain.Notification, 2)
	for i, k := range []string{key, "order-43"} {
		ns[i], err = domain.NewNotification(domain.NewNotificationParams{
			Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", IdempotencyKey: k, BatchID: &b.ID,
		}, time.Now())
		require.NoError(t, err)
	}
	require.NoError(t, store.InTx(t.Context(), func(r *postgres.Repo) error {
		if err := r.InsertBatch(t.Context(), b); err != nil {
			return err
		}
		_, err := r.InsertNotifications(t.Context(), ns)
		return err
	}))
	_, err = pool.Exec(t.Context(), `UPDATE notifications SET status='cancelled', cancelled_at=now() WHERE batch_id=$1`, b.ID)
	require.NoError(t, err)
	assert.Len(t, ofType(outboxRows(t), events.TypeBatchCompleted), 1, "1 of 1 created items finished")
}

// ---- relay -----------------------------------------------------------------

func relayConfig() outbox.Config {
	return outbox.Config{BatchSize: 100, PollInterval: 10 * time.Millisecond, RetryEvery: 10 * time.Millisecond, Retention: time.Hour, PruneInterval: time.Hour}
}

func streamEventIDs(t *testing.T) []int64 {
	t.Helper()
	msgs, err := rdb.XRange(t.Context(), events.DefaultStream, "-", "+").Result()
	require.NoError(t, err)
	ids := make([]int64, len(msgs))
	for i, m := range msgs {
		e, err := events.Decode(m)
		require.NoError(t, err)
		ids[i] = e.ID
	}
	return ids
}

// TestRelay_TwoRelaysPublishEverythingOnceInOrder runs two relays at once
// over a backlog of several rounds.
func TestRelay_TwoRelaysPublishEverythingOnceInOrder(t *testing.T) {
	reset(t)
	newBatch(t, 1000)
	newBatch(t, 250)

	pub := events.NewPublisher(rdb, events.DefaultStream, 100000)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for range 2 {
		go outbox.New(store, pub, relayConfig(), quiet).Run(ctx)
	}
	require.Eventually(t, func() bool { return len(streamEventIDs(t)) >= 1250 }, 10*time.Second, 20*time.Millisecond)
	cancel()

	ids := streamEventIDs(t)
	require.Len(t, ids, 1250, "no duplicates")
	for i := 1; i < len(ids); i++ {
		require.Greater(t, ids[i], ids[i-1], "in id order")
	}
	count, _, err := store.OutboxBacklog(t.Context())
	require.NoError(t, err)
	assert.Zero(t, count, "all marked published")
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, []events.Event) error {
	return errors.New("redis down")
}

func TestRelay_PublisherFailureKeepsEventsQueued(t *testing.T) {
	reset(t)
	newBatch(t, 5)

	_, err := outbox.New(store, failingPublisher{}, relayConfig(), quiet).RelayOnce(t.Context())
	require.Error(t, err)
	count, oldest, err := store.OutboxBacklog(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(5), count, "nothing lost, nothing marked")
	assert.Less(t, time.Since(oldest), time.Minute)

	n, err := outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 1000), relayConfig(), quiet).RelayOnce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 5, n, "published once Redis is back")
	assert.Len(t, streamEventIDs(t), 5)
}

func TestRelay_PrunesOldPublishedEvents(t *testing.T) {
	reset(t)
	newBatch(t, 3)
	_, err := outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 1000), relayConfig(), quiet).RelayOnce(t.Context())
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE outbox_events SET published_at = now() - interval '2 hours'
		WHERE id = (SELECT min(id) FROM outbox_events)`)
	require.NoError(t, err)

	n, err := store.PrunePublishedEvents(t.Context(), time.Now().Add(-time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Len(t, outboxRows(t), 2)
}

// ---- consumers -------------------------------------------------------------

func publish(t *testing.T, n int) {
	t.Helper()
	evs := make([]events.Event, n)
	for i := range evs {
		evs[i] = events.Event{
			ID: int64(i + 1), Type: events.TypeNotificationCreated, AggregateType: "notification",
			AggregateID: uuid.New(), Payload: json.RawMessage(`{}`), CreatedAt: time.Now(),
		}
	}
	require.NoError(t, events.NewPublisher(rdb, events.DefaultStream, 10000).Publish(t.Context(), evs))
}

// TestGroup_SharesWorkAndRetriesFailures runs two members of one consumer
// group. Each event must be handled successfully exactly once, including
// one whose first handling failed (it comes back after MinIdle).
func TestGroup_SharesWorkAndRetriesFailures(t *testing.T) {
	reset(t)
	var (
		mu      sync.Mutex
		handled = map[int64]int{}
		by      = map[string]int{}
		failed  bool
	)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, name := range []string{"c1", "c2"} {
		g := events.NewGroup(rdb, events.GroupConfig{
			Stream: events.DefaultStream, Group: "callbacks", Consumer: name, Count: 10,
			Block: 50 * time.Millisecond, MinIdle: 300 * time.Millisecond, RetryEvery: 50 * time.Millisecond, Log: quiet,
		})
		require.NoError(t, g.Ensure(ctx))
		go g.Run(ctx, func(_ context.Context, e events.Event) error {
			time.Sleep(time.Millisecond) // let the other member get a share
			mu.Lock()
			defer mu.Unlock()
			if e.ID == 7 && !failed {
				failed = true
				return errors.New("transient")
			}
			handled[e.ID]++
			by[name]++
			return nil
		})
	}
	publish(t, 100)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 100
	}, 10*time.Second, 20*time.Millisecond)
	time.Sleep(400 * time.Millisecond) // a wrong redelivery would show up now

	mu.Lock()
	defer mu.Unlock()
	for id, n := range handled {
		assert.Equal(t, 1, n, "event %d handled once", id)
	}
	assert.Positive(t, by["c1"])
	assert.Positive(t, by["c2"])
	pending, err := rdb.XPending(t.Context(), events.DefaultStream, "callbacks").Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count, "everything acknowledged")
}

func TestGroup_ConsumerCrashIsTakenOver(t *testing.T) {
	reset(t)
	publish(t, 5)
	// A consumer reads the events and dies without acknowledging them.
	_, err := rdb.XGroupCreateMkStream(t.Context(), events.DefaultStream, "callbacks", "0").Result()
	require.NoError(t, err)
	res, err := rdb.XReadGroup(t.Context(), &goredis.XReadGroupArgs{
		Group: "callbacks", Consumer: "dead", Streams: []string{events.DefaultStream, ">"},
	}).Result()
	require.NoError(t, err)
	require.Len(t, res[0].Messages, 5)

	var mu sync.Mutex
	got := map[int64]bool{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go events.NewGroup(rdb, events.GroupConfig{
		Stream: events.DefaultStream, Group: "callbacks", Consumer: "alive", Count: 10,
		Block: 50 * time.Millisecond, MinIdle: 200 * time.Millisecond, RetryEvery: 50 * time.Millisecond, Log: quiet,
	}).Run(ctx, func(_ context.Context, e events.Event) error {
		mu.Lock()
		got[e.ID] = true
		mu.Unlock()
		return nil
	})
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 5
	}, 5*time.Second, 20*time.Millisecond)
}

func TestTail_FansOutToEveryReader(t *testing.T) {
	reset(t)
	publish(t, 3) // before the readers start: not delivered
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var mu sync.Mutex
	seen := map[string][]int64{}
	for _, name := range []string{"api-1", "api-2"} {
		go events.Tail(ctx, rdb, events.TailConfig{
			Stream: events.DefaultStream, Count: 100, Block: 50 * time.Millisecond, RetryEvery: 50 * time.Millisecond, Log: quiet,
		}, func(e events.Event) {
			mu.Lock()
			seen[name] = append(seen[name], e.ID)
			mu.Unlock()
		})
	}
	time.Sleep(200 * time.Millisecond) // readers are blocked on "$"
	evs := make([]events.Event, 4)
	for i := range evs {
		evs[i] = events.Event{
			ID: int64(100 + i), Type: events.TypeNotificationStatusChanged, AggregateType: "notification",
			AggregateID: uuid.New(), Payload: json.RawMessage(`{"status":"sent"}`), CreatedAt: time.Now(),
		}
	}
	require.NoError(t, events.NewPublisher(rdb, events.DefaultStream, 1000).Publish(t.Context(), evs))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen["api-1"]) == 4 && len(seen["api-2"]) == 4
	}, 5*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int64{100, 101, 102, 103}, seen["api-1"])
	assert.Equal(t, seen["api-1"], seen["api-2"])
}
