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
		Attempt: domain.Attempt{NotificationID: n.ID, AttemptNo: n.TotalAttempts, Outcome: domain.OutcomeSuccess, Duration: time.Millisecond},
	}
	if status == domain.StatusFailed {
		c.Attempt.Outcome, c.Attempt.ErrorCode, c.Attempt.Error = domain.OutcomePermanentError, "http_400", "bad recipient"
	} else {
		c.Attempt.ProviderMessageID = "msg-1"
	}
	done, err := store.Queue().Complete(t.Context(), c)
	require.NoError(t, err)
	require.True(t, done.Applied)
}

// relayAll runs relay rounds until nothing is left to publish. Batch
// completion is counted by the relay, and the batch.completed event it
// emits goes out in the following round.
func relayAll(t *testing.T) {
	t.Helper()
	r := outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 100000), relayConfig(), quiet)
	for range 20 {
		n, err := r.RelayOnce(t.Context())
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
	t.Fatal("relay did not settle")
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

	relayAll(t) // batch completion is counted by the relay
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
// batch at the same moment from separate transactions. Counted per
// transaction, the last two could each see the other as unfinished (no
// batch.completed) or both finish it (two). The relay counts them in one
// place, in order, so the batch completes exactly once.
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
		relayAll(t)
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
	relayAll(t)
	require.Len(t, ofType(outboxRows(t), events.TypeBatchCompleted), 1)

	_, err := store.RetryNotification(t.Context(), claimed[1].ID, time.Now())
	require.NoError(t, err)
	relayAll(t)
	b, err := store.GetBatchSummary(t.Context(), batchID)
	require.NoError(t, err)
	assert.Nil(t, b.CompletedAt, "a redriven item reopens its batch")

	again := claim(t, 1)
	complete(t, again[0], domain.StatusSent)
	relayAll(t)
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
	relayAll(t)
	assert.Len(t, ofType(outboxRows(t), events.TypeBatchCompleted), 1, "1 of 1 created items finished")
}

// ---- callbacks (queued by the trigger) --------------------------------------

type callbackRow struct {
	EventID   int64
	EventType string
	URL       string
	Body      map[string]any
}

func callbacks(t *testing.T, id uuid.UUID) []callbackRow {
	t.Helper()
	rows, err := pool.Query(t.Context(),
		`SELECT event_id, event_type, url, payload FROM callback_deliveries WHERE notification_id = $1 ORDER BY event_id`, id)
	require.NoError(t, err)
	var out []callbackRow
	for rows.Next() {
		var c callbackRow
		require.NoError(t, rows.Scan(&c.EventID, &c.EventType, &c.URL, &c.Body))
		out = append(out, c)
	}
	require.NoError(t, rows.Err())
	return out
}

func withCallback(t *testing.T, url string) *domain.Notification {
	t.Helper()
	n, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi", CallbackURL: url,
	}, time.Now())
	require.NoError(t, err)
	_, err = store.InsertNotifications(t.Context(), []*domain.Notification{n})
	require.NoError(t, err)
	return n
}

// TestTriggers_FinalStatusQueuesItsCallback: the callback row is written by
// the status change itself, carrying that change's event, and only for
// final statuses of notifications that asked for one. Nothing depends on
// the event stream, so trimming it or losing Redis cannot lose a callback.
func TestTriggers_FinalStatusQueuesItsCallback(t *testing.T) {
	reset(t)
	hooked := withCallback(t, "https://hooks.example.com/x")
	plain := withCallback(t, "")

	claimed := claim(t, 2) // pending → processing: not final, no callback
	assert.Empty(t, callbacks(t, hooked.ID))
	for _, n := range claimed {
		complete(t, n, domain.StatusSent)
	}

	assert.Empty(t, callbacks(t, plain.ID), "no callback_url, no callback")
	cbs := callbacks(t, hooked.ID)
	require.Len(t, cbs, 1)
	cb := cbs[0]
	assert.Equal(t, "notification.sent", cb.EventType)
	assert.Equal(t, "https://hooks.example.com/x", cb.URL)

	var eventID int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT id FROM outbox_events
		WHERE aggregate_id = $1 AND payload->>'status' = 'sent'`, hooked.ID).Scan(&eventID))
	assert.Equal(t, eventID, cb.EventID, "the callback is the event of the change, in the same transaction")
	assert.EqualValues(t, eventID, cb.Body["event_id"])
	assert.Equal(t, "notification.sent", cb.Body["type"])
	data, _ := cb.Body["data"].(map[string]any)
	assert.Equal(t, "sent", data["status"])
	assert.Equal(t, "processing", data["previous_status"])
	assert.NotContains(t, data, "recipient", "callbacks carry state, never personal data")
}

// TestTriggers_CallbacksNeverGoBackwards: failed, then a redrive, then
// sent. The client must receive failed before sent, never sent and then
// failed, even when the failed callback is still waiting for a retry, and
// a stale callback is never sent after a newer one was delivered.
func TestTriggers_CallbacksNeverGoBackwards(t *testing.T) {
	reset(t)
	n := withCallback(t, "https://hooks.example.com/x")
	complete(t, claim(t, 1)[0], domain.StatusFailed)
	_, err := store.RetryNotification(t.Context(), n.ID, time.Now())
	require.NoError(t, err)
	complete(t, claim(t, 1)[0], domain.StatusSent)

	cbs := callbacks(t, n.ID)
	require.Len(t, cbs, 2)
	assert.Equal(t, []string{"notification.failed", "notification.sent"}, []string{cbs[0].EventType, cbs[1].EventType},
		"created in the order the changes committed")

	due := time.Now().Add(time.Minute) // rows are due from their insert (database clock)
	first, err := store.ClaimCallbacks(t.Context(), due, time.Minute, 10, 5)
	require.NoError(t, err)
	require.Len(t, first, 1, "the sent callback waits for the failed one")
	assert.Equal(t, cbs[0].EventID, first[0].EventID)

	// Suppose the newer one was delivered anyway (an older version, manual
	// SQL): the older one must then never go out.
	_, err = pool.Exec(t.Context(), `UPDATE callback_deliveries SET status = 'delivered' WHERE event_id = $1`, cbs[1].EventID)
	require.NoError(t, err)
	ok, err := store.FinishCallback(t.Context(), first[0], &postgres.CallbackOutcome{
		Status: postgres.CallbackPending, NextAttemptAt: time.Now(), Error: "503", Now: time.Now(),
	})
	require.NoError(t, err)
	require.True(t, ok)

	again, err := store.ClaimCallbacks(t.Context(), due.Add(time.Hour), time.Minute, 10, 5)
	require.NoError(t, err)
	assert.Empty(t, again, "a callback older than a delivered one is never claimed")
	closed, err := store.SupersedeCallbacks(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), closed)
	var status string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT status FROM callback_deliveries WHERE event_id = $1`, cbs[0].EventID).Scan(&status))
	assert.Equal(t, postgres.CallbackSuperseded, status)
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

// TestRelay_PrunesUnderSustainedLoad: with a backlog that keeps every
// round full, the relay never reaches an idle round. Pruning used to run
// only there, so under a long campaign the table grew without bound.
func TestRelay_PrunesUnderSustainedLoad(t *testing.T) {
	reset(t)
	newBatch(t, 3)
	_, err := outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 100000), relayConfig(), quiet).RelayOnce(t.Context())
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE outbox_events SET published_at = now() - interval '2 hours'`)
	require.NoError(t, err)
	newBatch(t, 1000) // far more than one round

	cfg := relayConfig()
	cfg.BatchSize, cfg.PruneInterval = 1, 20*time.Millisecond // every round is full
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 100000), cfg, quiet).Run(ctx)

	require.Eventually(t, func() bool {
		var old int
		require.NoError(t, pool.QueryRow(t.Context(),
			`SELECT count(*) FROM outbox_events WHERE published_at < now() - interval '1 hour'`).Scan(&old))
		return old == 0
	}, 5*time.Second, 20*time.Millisecond, "old events pruned while the backlog is still draining")
	var waiting int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&waiting))
	assert.Positive(t, waiting, "the backlog was not drained first")
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

// TestRelay_WakesWorkersForDueWork: the relay replaces the old NOTIFY
// trigger. One round over new notifications on two channels sends one
// wake-up per channel; scheduled ones send none.
func TestRelay_WakesWorkersForDueWork(t *testing.T) {
	reset(t)
	var mu sync.Mutex
	woke := map[domain.Channel]int{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go store.Queue().ListenDue(ctx, func(ch domain.Channel) {
		mu.Lock()
		woke[ch]++
		mu.Unlock()
	}, func(error) {})
	time.Sleep(300 * time.Millisecond) // let LISTEN take effect

	later := time.Now().Add(time.Hour)
	var ns []*domain.Notification
	for i, p := range []domain.NewNotificationParams{
		{Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "a"},
		{Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "b"},
		{Channel: domain.ChannelEmail, Recipient: "a@example.com", Subject: "s", Content: "c"},
		{Channel: domain.ChannelPush, Recipient: "token", Subject: "s", Content: "d", ScheduledAt: &later},
	} {
		n, err := domain.NewNotification(p, time.Now())
		require.NoError(t, err, i)
		ns = append(ns, n)
	}
	_, err := store.InsertNotifications(t.Context(), ns)
	require.NoError(t, err)

	_, err = outbox.New(store, events.NewPublisher(rdb, events.DefaultStream, 1000), relayConfig(), quiet).RelayOnce(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return woke[domain.ChannelSMS] == 1 && woke[domain.ChannelEmail] == 1
	}, 3*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, woke[domain.ChannelPush], "scheduled for later: nothing to wake for")
}
