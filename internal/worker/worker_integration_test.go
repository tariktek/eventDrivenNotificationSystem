//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/worker"
)

var (
	pool  *pgxpool.Pool
	store *postgres.Store
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
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	store = postgres.NewStore(pool)
	code := m.Run()
	pool.Close()
	_ = pg.Terminate()
	os.Exit(code)
}

func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, notification_attempts CASCADE`)
	require.NoError(t, err)
}

// fakeProvider records every delivery and answers with respond(key, n),
// where n counts the deliveries of that notification so far (1-based).
type fakeProvider struct {
	mu       sync.Mutex
	order    []string // notification ids in arrival order
	times    []time.Time
	perKey   map[string]int
	respond  func(key string, n int) int
	srv      *httptest.Server
	latency  time.Duration
	contents map[string]string
}

func newFakeProvider(t *testing.T, respond func(key string, n int) int) *fakeProvider {
	t.Helper()
	p := &fakeProvider{perKey: map[string]int{}, respond: respond, contents: map[string]string{}}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Content string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		key := r.Header.Get("X-Idempotency-Key")
		p.mu.Lock()
		p.perKey[key]++
		n := p.perKey[key]
		p.order = append(p.order, key)
		p.times = append(p.times, time.Now())
		p.contents[key] = body.Content
		status := p.respond(key, n)
		p.mu.Unlock()

		time.Sleep(p.latency)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == http.StatusAccepted {
			_, _ = fmt.Fprintf(w, `{"messageId":"msg-%s-%d","status":"accepted"}`, key[:8], n)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func alwaysAccept(string, int) int { return http.StatusAccepted }

type opts struct {
	rate        int
	concurrency int
}

// startWorker runs a worker until the test ends; stop() shuts it down and
// waits for Run to return.
func startWorker(t *testing.T, p *fakeProvider, o opts) (stop func()) {
	t.Helper()
	if o.rate == 0 {
		o.rate = 200
	}
	if o.concurrency == 0 {
		o.concurrency = 50
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := worker.New(worker.Config{
		ID:            "test-" + uuid.NewString()[:8],
		Channels:      domain.Channels,
		Concurrency:   o.concurrency,
		BatchSize:     25,
		PollInterval:  50 * time.Millisecond,
		Lease:         10 * time.Second,
		ReapInterval:  time.Second,
		Aging:         5 * time.Minute,
		ShutdownGrace: 2 * time.Second,
	}, worker.Deps{
		Queue:    store.Queue(),
		Limiter:  ratelimit.NewLocal(ratelimit.Config{Rate: o.rate, Horizon: 500 * time.Millisecond}, nil),
		Sender:   provider.NewHTTPClient(provider.HTTPConfig{URLs: map[domain.Channel]string{domain.ChannelSMS: p.srv.URL, domain.ChannelEmail: p.srv.URL, domain.ChannelPush: p.srv.URL}, Timeout: 2 * time.Second}),
		Policy:   retry.Policy{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond},
		Notifier: store.Queue(),
		Log:      log,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("worker did not stop within 10s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func create(t *testing.T, n int, mutate func(i int, p *domain.NewNotificationParams)) []*domain.Notification {
	t.Helper()
	ns := make([]*domain.Notification, n)
	for i := range ns {
		p := domain.NewNotificationParams{Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: fmt.Sprintf("msg %d", i)}
		if mutate != nil {
			mutate(i, &p)
		}
		var err error
		ns[i], err = domain.NewNotification(p, time.Now())
		require.NoError(t, err)
	}
	_, err := store.InsertNotifications(t.Context(), ns)
	require.NoError(t, err)
	return ns
}

func statusCounts(t *testing.T) map[domain.Status]int {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT status, count(*) FROM notifications GROUP BY status`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[domain.Status]int{}
	for rows.Next() {
		var s string
		var c int
		require.NoError(t, rows.Scan(&s, &c))
		out[domain.Status(s)] = c
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s; statuses: %v", timeout, statusCounts(t))
}

// ---- tests -------------------------------------------------------------------

func TestWorker_DeliversEverything(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, alwaysAccept)
	startWorker(t, p, opts{})

	ns := create(t, 40, func(i int, p *domain.NewNotificationParams) {
		p.Channel = domain.Channels[i%3]
		if p.Channel == domain.ChannelEmail {
			p.Subject = "Hello"
		}
	})
	waitFor(t, 10*time.Second, func() bool { return statusCounts(t)[domain.StatusSent] == len(ns) })

	n, attempts, err := svcGet(t, ns[0].ID)
	require.NoError(t, err)
	assert.Equal(t, 1, n.Attempts)
	assert.NotEmpty(t, n.ProviderMessageID)
	assert.NotNil(t, n.SentAt)
	assert.Empty(t, n.LockedBy, "lease released")
	require.Len(t, attempts, 1)
	assert.Equal(t, domain.OutcomeSuccess, attempts[0].Outcome)
	assert.Equal(t, http.StatusAccepted, attempts[0].StatusCode)
	assert.Equal(t, "msg 0", p.contents[ns[0].ID.String()], "the provider got the content")
}

func svcGet(t *testing.T, id uuid.UUID) (*domain.Notification, []domain.Attempt, error) {
	t.Helper()
	n, err := store.GetNotification(t.Context(), id)
	if err != nil {
		return nil, nil, err
	}
	a, err := store.ListAttempts(t.Context(), id)
	return n, a, err
}

// TestWorker_SendsInPriorityOrder queues low-priority notifications first
// and high-priority ones after, then starts a slow (20/s) worker: every
// high must reach the provider before any low.
func TestWorker_SendsInPriorityOrder(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, alwaysAccept)
	lows := create(t, 10, func(_ int, p *domain.NewNotificationParams) { p.Priority = domain.PriorityLow })
	highs := create(t, 10, func(_ int, p *domain.NewNotificationParams) { p.Priority = domain.PriorityHigh })
	startWorker(t, p, opts{rate: 20})

	waitFor(t, 10*time.Second, func() bool { return statusCounts(t)[domain.StatusSent] == 20 })

	p.mu.Lock()
	defer p.mu.Unlock()
	pos := map[string]int{}
	for i, k := range p.order {
		pos[k] = i
	}
	lastHigh, firstLow := 0, len(p.order)
	for _, n := range highs {
		lastHigh = max(lastHigh, pos[n.ID.String()])
	}
	for _, n := range lows {
		firstLow = min(firstLow, pos[n.ID.String()])
	}
	assert.Less(t, lastHigh, firstLow, "all high before any low; order: %v", p.order)
}

func TestWorker_RetriesTransientFailures(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, func(_ string, n int) int {
		if n == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusAccepted
	})
	startWorker(t, p, opts{})
	ns := create(t, 5, nil)

	waitFor(t, 10*time.Second, func() bool { return statusCounts(t)[domain.StatusSent] == 5 })
	n, attempts, err := svcGet(t, ns[0].ID)
	require.NoError(t, err)
	assert.Equal(t, 2, n.Attempts)
	assert.Empty(t, n.LastError, "cleared once sent")
	require.Len(t, attempts, 2)
	assert.Equal(t, domain.OutcomeTransientError, attempts[0].Outcome)
	assert.Equal(t, "http_503", attempts[0].ErrorCode)
	assert.Equal(t, domain.OutcomeSuccess, attempts[1].Outcome)
}

func TestWorker_PermanentFailureIsNotRetried(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, func(string, int) int { return http.StatusBadRequest })
	startWorker(t, p, opts{})
	ns := create(t, 3, nil)

	waitFor(t, 10*time.Second, func() bool { return statusCounts(t)[domain.StatusFailed] == 3 })
	time.Sleep(300 * time.Millisecond) // give a wrong retry the chance to happen
	n, attempts, err := svcGet(t, ns[0].ID)
	require.NoError(t, err)
	assert.Equal(t, 1, n.Attempts)
	assert.Equal(t, "http_400", n.LastErrorCode)
	assert.NotNil(t, n.FailedAt)
	assert.Len(t, attempts, 1)
}

func TestWorker_GivesUpAfterMaxAttempts(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, func(string, int) int { return http.StatusServiceUnavailable })
	startWorker(t, p, opts{})
	ns := create(t, 2, func(_ int, p *domain.NewNotificationParams) { p.MaxAttempts = 3 })

	waitFor(t, 10*time.Second, func() bool { return statusCounts(t)[domain.StatusFailed] == 2 })
	n, attempts, err := svcGet(t, ns[0].ID)
	require.NoError(t, err)
	assert.Equal(t, 3, n.Attempts)
	assert.Len(t, attempts, 3)
	assert.Equal(t, "http_503", n.LastErrorCode)
}

// TestWorker_RespectsRateLimitEndToEnd measures the arrival rate at the
// provider, independently of the limiter's own bookkeeping.
func TestWorker_RespectsRateLimitEndToEnd(t *testing.T) {
	reset(t)
	const rate = 50
	p := newFakeProvider(t, alwaysAccept)
	create(t, 125, nil) // a backlog worth 2.5 seconds at the limit
	start := time.Now()
	startWorker(t, p, opts{rate: rate})

	waitFor(t, 15*time.Second, func() bool { return statusCounts(t)[domain.StatusSent] == 125 })
	elapsed := time.Since(start)

	p.mu.Lock()
	times := slices.Clone(p.times)
	p.mu.Unlock()
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	worst := 0
	for i, j := 0, 0; i < len(times); i++ {
		for j < len(times) && times[j].Before(times[i].Add(time.Second)) {
			j++
		}
		worst = max(worst, j-i)
	}
	t.Logf("125 notifications in %s; busiest one-second window: %d", elapsed.Round(time.Millisecond), worst)
	// Send slots are exactly 1/rate apart (the limiter tests prove that on
	// the limiter's own clock). Arrival times at the receiver also carry
	// network jitter: a request delayed by opening a new connection can land
	// right before the next, on-time one. A provider enforcing its limit
	// strictly on arrival should be configured with a little headroom
	// (e.g. WORKER_RATE_LIMIT=95 for a hard 100/s).
	assert.LessOrEqual(t, worst, rate+3, "allowance for network jitter between slot and arrival")
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "125 at 50/s cannot finish faster")
}

// TestWorker_GracefulShutdownLeavesNothingStuck stops a worker in the
// middle of a backlog: started deliveries finish, waiting ones are handed
// back untouched, and nothing is left in "processing".
func TestWorker_GracefulShutdownLeavesNothingStuck(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, alwaysAccept)
	p.latency = 100 * time.Millisecond
	create(t, 200, nil)
	stop := startWorker(t, p, opts{rate: 50})

	time.Sleep(700 * time.Millisecond)
	stop()

	counts := statusCounts(t)
	assert.Zero(t, counts[domain.StatusProcessing], "no claimed work may be abandoned: %v", counts)
	assert.Positive(t, counts[domain.StatusSent])
	assert.Positive(t, counts[domain.StatusPending])
	assert.Equal(t, 200, counts[domain.StatusSent]+counts[domain.StatusPending])

	var wrongAttempts int
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT count(*) FROM notifications WHERE status = 'pending' AND attempts <> 0`).Scan(&wrongAttempts))
	assert.Zero(t, wrongAttempts, "released work gets its attempt back")

	p.mu.Lock()
	delivered := len(p.order)
	p.mu.Unlock()
	assert.Equal(t, counts[domain.StatusSent], delivered, "every provider call was recorded")
}

func TestWorker_ScheduledNotificationWaitsForItsTime(t *testing.T) {
	reset(t)
	p := newFakeProvider(t, alwaysAccept)
	startWorker(t, p, opts{})
	at := time.Now().Add(1500 * time.Millisecond)
	ns := create(t, 1, func(_ int, p *domain.NewNotificationParams) { p.ScheduledAt = &at })

	time.Sleep(time.Second)
	assert.Equal(t, 1, statusCounts(t)[domain.StatusScheduled], "not sent early")

	waitFor(t, 5*time.Second, func() bool { return statusCounts(t)[domain.StatusSent] == 1 })
	n, err := store.GetNotification(t.Context(), ns[0].ID)
	require.NoError(t, err)
	assert.False(t, n.SentAt.Before(at), "sent at %s, scheduled for %s", n.SentAt, at)
}
