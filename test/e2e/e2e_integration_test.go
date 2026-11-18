//go:build integration

// Package e2e runs the whole system in one process against real PostgreSQL
// and Redis: the HTTP API (with its WebSocket hub), a delivery worker, the
// outbox relay, and the callback consumer and dispatcher. Only the
// notification provider and the client's callback endpoint are stand-ins.
//
// It is the acceptance test for the event pipeline: a client watching over
// WebSocket sees every status change in order, and a client's callback
// endpoint receives signed callbacks it can verify.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/callback"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/handler"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/router"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/mockprovider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/outbox"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/realtime"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/worker"
)

const (
	apiKey = "e2e-api-key"
	stream = "stream:e2e"
)

var secret = []byte("e2e-callback-secret-0123456789abcdef")

// system is everything running for one test.
type system struct {
	api       string // http://...
	ws        string // ws://...
	callbacks string // the client's callback URL
	receiver  *mockprovider.CallbackReceiver
	recvSrv   string
	sent      atomic.Int64 // provider calls
}

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
	var opts *goredis.Options
	if err == nil {
		opts, err = goredis.ParseURL(url)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rdb = goredis.NewClient(opts)
	store = postgres.NewStore(pool)
	code := m.Run()
	_ = rdb.Close()
	pool.Close()
	_ = rc.Terminate(ctx)
	_ = pg.Terminate()
	os.Exit(code)
}

// start runs every component until the test ends.
func start(t *testing.T) *system {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, notification_attempts, outbox_events, callback_deliveries CASCADE`)
	require.NoError(t, err)
	require.NoError(t, rdb.FlushAll(t.Context()).Err())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	sys := &system{}

	// The provider: accepts everything.
	prov := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := sys.sent.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"messageId":"msg-%d","status":"accepted"}`, n)
	}))
	t.Cleanup(prov.Close)

	// The client's webhook endpoint, verifying signatures.
	sys.receiver = mockprovider.NewCallbackReceiver(secret, nil)
	mux := http.NewServeMux()
	sys.receiver.Register(mux)
	recv := httptest.NewServer(mux)
	t.Cleanup(recv.Close)
	sys.recvSrv = recv.URL
	sys.callbacks = recv.URL + "/callbacks"

	// API with its WebSocket hub.
	hub := realtime.NewHub(realtime.Config{MaxClients: 100, BufferSize: 1024}, quiet)
	wg.Go(func() {
		hub.Run(ctx, func(ctx context.Context, publish func(events.Event)) {
			events.Tail(ctx, rdb, events.TailConfig{Stream: stream, Count: 500, Block: 100 * time.Millisecond, RetryEvery: 50 * time.Millisecond, Log: quiet}, publish)
		})
	})
	svc := service.New(store, time.Now, validation.Options{AllowInsecureCallbacks: true, AllowPrivateCallbacks: true})
	api := httptest.NewServer(router.New(router.Deps{
		Health:         health.NewRegistry(time.Second),
		API:            handler.New(svc, func(context.Context) service.RequestMeta { return service.RequestMeta{} }),
		Log:            quiet,
		MaxBodyBytes:   1 << 20,
		APIKeys:        []string{apiKey},
		Idempotency:    store.Idempotency(),
		IdempotencyTTL: time.Hour,
		Now:            time.Now,
		Realtime:       &realtime.Handler{Hub: hub, Log: quiet, Shutdown: ctx},
	}))
	t.Cleanup(api.Close)
	sys.api = api.URL
	sys.ws = "ws" + strings.TrimPrefix(api.URL, "http") + "/api/v1/ws"

	// Worker, relay, callbacks.
	w := worker.New(worker.Config{
		ID: "e2e-worker", Channels: domain.Channels, Concurrency: 20, BatchSize: 20,
		PollInterval: 50 * time.Millisecond, Lease: 10 * time.Second, ReapInterval: time.Second,
		Aging: time.Minute, ShutdownGrace: time.Second,
	}, worker.Deps{
		Queue:    store.Queue(),
		Limiter:  ratelimit.NewLocal(ratelimit.Config{Rate: 100, Horizon: 500 * time.Millisecond}, nil),
		Sender:   provider.NewHTTPClient(provider.HTTPConfig{URLs: map[domain.Channel]string{domain.ChannelSMS: prov.URL, domain.ChannelEmail: prov.URL, domain.ChannelPush: prov.URL}, Timeout: 2 * time.Second}),
		Policy:   retry.Policy{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond},
		Notifier: store.Queue(),
		Log:      quiet,
	})
	wg.Go(func() { _ = w.Run(ctx) })
	relay := outbox.New(store, events.NewPublisher(rdb, stream, 100000), outbox.Config{
		BatchSize: 500, PollInterval: 20 * time.Millisecond, RetryEvery: 50 * time.Millisecond, Retention: time.Hour, PruneInterval: time.Hour,
	}, quiet)
	wg.Go(func() { relay.Run(ctx) })
	enq := &callback.Enqueuer{Store: store}
	group := events.NewGroup(rdb, events.GroupConfig{
		Stream: stream, Group: "callbacks", Consumer: "e2e", Count: 100, Block: 100 * time.Millisecond,
		MinIdle: time.Second, RetryEvery: 50 * time.Millisecond, Log: quiet,
	})
	require.NoError(t, group.Ensure(ctx))
	wg.Go(func() { group.Run(ctx, enq.Handle) })
	disp := callback.NewDispatcher(store, callback.DispatcherConfig{
		Client: callback.ClientConfig{Timeout: 2 * time.Second, AllowInsecure: true, AllowPrivate: true},
		Secret: secret, Concurrency: 10, MaxAttempts: 5, Backoff: retry.Policy{Base: 50 * time.Millisecond, Max: time.Second},
		Lease: 5 * time.Second, PollInterval: 50 * time.Millisecond,
	}, quiet)
	wg.Go(func() { disp.Run(ctx) })
	return sys
}

func (s *system) call(t *testing.T, method, path string, body any) map[string]any {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, s.api+path, r)
	require.NoError(t, err)
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Less(t, resp.StatusCode, 300, "%s %s: %v", method, path, out)
	return out
}

type wsMessage struct {
	Type      string `json:"type"`
	EventType string `json:"event_type"`
	Data      struct {
		NotificationID string         `json:"notification_id"`
		BatchID        string         `json:"batch_id"`
		Status         string         `json:"status"`
		PreviousStatus *string        `json:"previous_status"`
		Counts         map[string]int `json:"counts"`
		Recipient      *string        `json:"recipient"`
	} `json:"data"`
}

func (s *system) watch(t *testing.T, query string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, s.ws+"?"+query, &websocket.DialOptions{HTTPHeader: http.Header{"X-API-Key": {apiKey}}})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	m := next(t, c)
	require.Equal(t, "subscribed", m.Type)
	return c
}

func next(t *testing.T, c *websocket.Conn) wsMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	var m wsMessage
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// nextChange returns the next status change, skipping notification.created
// events (which may arrive after a subscription made right after creation).
func nextChange(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	for {
		m := next(t, c)
		if m.EventType == events.TypeNotificationStatusChanged {
			return *m.Data.PreviousStatus + ">" + m.Data.Status
		}
	}
}

type received struct {
	Valid     int `json:"valid"`
	Invalid   int `json:"invalid"`
	Callbacks []struct {
		EventID string `json:"event_id"`
		Type    string `json:"type"`
		Body    struct {
			Type string `json:"type"`
			Data struct {
				NotificationID string `json:"notification_id"`
				Status         string `json:"status"`
			} `json:"data"`
		} `json:"body"`
	} `json:"callbacks"`
}

func (s *system) receivedCallbacks(t *testing.T) received {
	t.Helper()
	resp, err := http.Get(s.recvSrv + "/admin/callbacks") //nolint:noctx // test helper
	require.NoError(t, err)
	defer resp.Body.Close()
	var r received
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	return r
}

// TestE2E_BatchIsObservableLiveAndCalledBack is the Phase 4 acceptance test.
func TestE2E_BatchIsObservableLiveAndCalledBack(t *testing.T) {
	sys := start(t)
	// A batch's id is unknown until it is created: watch its channel
	// first, so no early event is missed, then add the batch.
	ws := sys.watch(t, "channel=sms")

	const n = 10
	items := make([]map[string]any, n)
	for i := range items {
		items[i] = map[string]any{"channel": "sms", "recipient": fmt.Sprintf("+9055500000%02d", i), "content": "Flash sale", "callback_url": sys.callbacks}
	}
	batchID := sys.call(t, http.MethodPost, "/api/v1/notifications/batch", map[string]any{"items": items})["batch_id"].(string)

	// Add the batch itself, for batch.completed (it has no channel). The
	// confirmation arrives in order with the events, long before the batch
	// can complete.
	b, err := json.Marshal(map[string]any{"action": "subscribe", "batch_ids": []string{batchID}})
	require.NoError(t, err)
	require.NoError(t, ws.Write(t.Context(), websocket.MessageText, b))
	seq := map[string][]string{}
	for completed := false; !completed; {
		m := next(t, ws)
		if m.Type == "subscribed" {
			continue
		}
		require.Equal(t, "event", m.Type)
		if m.Data.BatchID != batchID {
			continue
		}
		assert.Nil(t, m.Data.Recipient, "events carry no personal data")
		switch m.EventType {
		case events.TypeBatchCompleted:
			assert.Equal(t, map[string]int{"sent": n}, m.Data.Counts)
			completed = true
		default:
			prev := "-"
			if m.Data.PreviousStatus != nil {
				prev = *m.Data.PreviousStatus
			}
			seq[m.Data.NotificationID] = append(seq[m.Data.NotificationID], prev+">"+m.Data.Status)
		}
	}
	require.Len(t, seq, n)
	for id, s := range seq {
		assert.Equal(t, []string{"->pending", "pending>processing", "processing>sent"}, s, "notification %s: every change, in order", id)
	}

	// Callbacks: one per notification, signed, verified by the receiver.
	require.Eventually(t, func() bool { return sys.receivedCallbacks(t).Valid == n }, 10*time.Second, 50*time.Millisecond)
	r := sys.receivedCallbacks(t)
	assert.Zero(t, r.Invalid, "every signature verified")
	ids := map[string]bool{}
	for _, cb := range r.Callbacks {
		assert.Equal(t, "notification.sent", cb.Type)
		assert.Equal(t, "notification.sent", cb.Body.Type)
		assert.Equal(t, "sent", cb.Body.Data.Status)
		ids[cb.Body.Data.NotificationID] = true
	}
	assert.Len(t, ids, n, "one callback per notification")

	var delivered int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM callback_deliveries WHERE status = 'delivered'`).Scan(&delivered))
	assert.Equal(t, n, delivered)
}

// TestE2E_ScheduledNotification covers Step 17: a scheduled notification
// waits for its time, then goes out; one cancelled before its time never
// does, and its client is called back about the cancellation.
func TestE2E_ScheduledNotification(t *testing.T) {
	sys := start(t)
	at := time.Now().Add(2 * time.Second).UTC()
	body := func() map[string]any {
		return map[string]any{
			"channel": "email", "recipient": "ada@example.com", "subject": "Reminder", "content": "Your appointment is tomorrow.",
			"scheduled_at": at.Format(time.RFC3339Nano), "callback_url": sys.callbacks,
		}
	}
	kept := sys.call(t, http.MethodPost, "/api/v1/notifications", body())
	dropped := sys.call(t, http.MethodPost, "/api/v1/notifications", body())
	assert.Equal(t, "scheduled", kept["status"])
	ws := sys.watch(t, "notification_id="+kept["id"].(string)+"&notification_id="+dropped["id"].(string))

	cancelled := sys.call(t, http.MethodPost, "/api/v1/notifications/"+dropped["id"].(string)+"/cancel", nil)
	assert.Equal(t, "cancelled", cancelled["status"])
	assert.Equal(t, "scheduled>cancelled", nextChange(t, ws))

	time.Sleep(time.Until(at) - 500*time.Millisecond)
	assert.Zero(t, sys.sent.Load(), "nothing sent before its time")
	assert.Equal(t, "scheduled", sys.call(t, http.MethodGet, "/api/v1/notifications/"+kept["id"].(string), nil)["status"])

	assert.Equal(t, "scheduled>processing", nextChange(t, ws), "claimed when due")
	assert.Equal(t, "processing>sent", nextChange(t, ws))

	got := sys.call(t, http.MethodGet, "/api/v1/notifications/"+kept["id"].(string), nil)
	sentAt, err := time.Parse(time.RFC3339Nano, got["sent_at"].(string))
	require.NoError(t, err)
	assert.False(t, sentAt.Before(at), "sent at %s, scheduled for %s", sentAt, at)

	time.Sleep(time.Second)
	assert.Equal(t, int64(1), sys.sent.Load(), "the cancelled one was never sent")
	require.Eventually(t, func() bool { return sys.receivedCallbacks(t).Valid == 2 }, 5*time.Second, 50*time.Millisecond)
	types := map[string]string{}
	for _, cb := range sys.receivedCallbacks(t).Callbacks {
		types[cb.Body.Data.NotificationID] = cb.Type
	}
	assert.Equal(t, map[string]string{
		kept["id"].(string):    "notification.sent",
		dropped["id"].(string): "notification.cancelled",
	}, types)
}

// TestE2E_CallbackRetriesUntilTheReceiverRecovers: a receiver that is
// down at first still gets its callback once it is back.
func TestE2E_CallbackRetriesUntilTheReceiverRecovers(t *testing.T) {
	sys := start(t)
	var calls atomic.Int64
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(flaky.Close)

	n := sys.call(t, http.MethodPost, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "device-token", "subject": "Hi", "content": "Hello", "callback_url": flaky.URL,
	})
	id, err := uuid.Parse(n["id"].(string))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ds, err := store.ListCallbackDeliveries(t.Context(), id)
		return err == nil && len(ds) == 1 && ds[0].Status == postgres.CallbackDelivered
	}, 10*time.Second, 50*time.Millisecond)
	ds, err := store.ListCallbackDeliveries(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, 3, ds[0].Attempts, "two 503s, then delivered")
	assert.Equal(t, http.StatusOK, ds[0].LastStatusCode)
}

// insertCallback stores a notification and one pending callback for it,
// bypassing the event pipeline, for dispatcher-only tests.
func insertCallback(t *testing.T, url string, attempts int) int64 {
	t.Helper()
	_, err := pool.Exec(t.Context(), `TRUNCATE notifications, batches, notification_attempts, outbox_events, callback_deliveries CASCADE`)
	require.NoError(t, err)
	n, err := domain.NewNotification(domain.NewNotificationParams{Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "hi"}, time.Now())
	require.NoError(t, err)
	_, err = store.InsertNotifications(t.Context(), []*domain.Notification{n})
	require.NoError(t, err)
	var id int64
	require.NoError(t, pool.QueryRow(t.Context(), `
		INSERT INTO callback_deliveries (notification_id, event_id, event_type, url, payload, attempts, next_attempt_at)
		VALUES ($1, 1, 'notification.sent', $2, '{"type":"notification.sent"}', $3, now() - interval '1 second')
		RETURNING id`, n.ID, url, attempts).Scan(&id))
	return id
}

func dispatcher() *callback.Dispatcher {
	return callback.NewDispatcher(store, callback.DispatcherConfig{
		Client: callback.ClientConfig{Timeout: 5 * time.Second, AllowInsecure: true, AllowPrivate: true},
		Secret: secret, Concurrency: 4, MaxAttempts: 5, Backoff: retry.Policy{Base: time.Second, Max: time.Second},
		Lease: 10 * time.Second, PollInterval: 20 * time.Millisecond,
	}, quiet)
}

func callbackRow(t *testing.T, id int64) (status string, attempts int) {
	t.Helper()
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT status, attempts FROM callback_deliveries WHERE id = $1`, id).Scan(&status, &attempts))
	return status, attempts
}

// TestE2E_ShutdownLetsInFlightCallbacksFinish: a POST under way when the
// worker stops completes and is recorded, instead of being cut off and
// counted as a failed attempt.
func TestE2E_ShutdownLetsInFlightCallbacksFinish(t *testing.T) {
	started := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(slow.Close)
	id := insertCallback(t, slow.URL, 4) // its last attempt

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { dispatcher().Run(ctx); close(done) }()
	<-started
	cancel() // shutdown while the receiver is still answering
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not stop")
	}
	status, attempts := callbackRow(t, id)
	assert.Equal(t, postgres.CallbackDelivered, status)
	assert.Equal(t, 5, attempts)
}

// TestE2E_ExhaustedCallbackIsNotSentAgain: a callback whose last attempt
// was claimed but never recorded (the worker crashed) is given up on once
// its lease expires, not sent a sixth time.
func TestE2E_ExhaustedCallbackIsNotSentAgain(t *testing.T) {
	var calls atomic.Int64
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(recv.Close)
	id := insertCallback(t, recv.URL, 5)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go dispatcher().Run(ctx)
	require.Eventually(t, func() bool { s, _ := callbackRow(t, id); return s == postgres.CallbackFailed }, 5*time.Second, 20*time.Millisecond)
	assert.Zero(t, calls.Load())
	_, attempts := callbackRow(t, id)
	assert.Equal(t, 5, attempts)
}
