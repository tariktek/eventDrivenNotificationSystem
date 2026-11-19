package middleware_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/middleware"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

var writeErr = apierror.Writer{Log: discard}.Write

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	return body.Error.Code
}

func request(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	return httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
}

// ---- correlation ID --------------------------------------------------------

func TestCorrelationID(t *testing.T) {
	var seen string
	h := middleware.CorrelationID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = observability.CorrelationID(r.Context())
	}))

	t.Run("accepts a valid client id", func(t *testing.T) {
		req := request(t, http.MethodGet, "/", "")
		req.Header.Set(middleware.HeaderCorrelationID, "order-42:retry.1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, "order-42:retry.1", seen)
		assert.Equal(t, "order-42:retry.1", rec.Header().Get(middleware.HeaderCorrelationID))
	})

	t.Run("generates one when absent", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, request(t, http.MethodGet, "/", ""))
		assert.Len(t, seen, 36, "a UUID")
		assert.Equal(t, seen, rec.Header().Get(middleware.HeaderCorrelationID))
	})

	t.Run("replaces ids that could forge log lines", func(t *testing.T) {
		for _, bad := range []string{"a\nlevel=ERROR msg=forged", "has space", strings.Repeat("x", 129)} {
			req := request(t, http.MethodGet, "/", "")
			req.Header.Set(middleware.HeaderCorrelationID, bad)
			h.ServeHTTP(httptest.NewRecorder(), req)
			assert.NotEqual(t, bad, seen)
			assert.Len(t, seen, 36)
		}
	})
}

// ---- auth -------------------------------------------------------------------

func TestAPIKeyAuth(t *testing.T) {
	var client string
	h := middleware.APIKeyAuth([]string{"key-old", "key-new"}, writeErr)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			client = middleware.ClientID(r.Context())
			w.WriteHeader(http.StatusNoContent)
		}))

	for name, tc := range map[string]struct {
		key  string
		want int
	}{
		"missing":          {"", http.StatusUnauthorized},
		"wrong":            {"key-guess", http.StatusUnauthorized},
		"prefix of valid":  {"key-ne", http.StatusUnauthorized},
		"old key (rotate)": {"key-old", http.StatusNoContent},
		"new key":          {"key-new", http.StatusNoContent},
	} {
		t.Run(name, func(t *testing.T) {
			req := request(t, http.MethodGet, "/api/v1/notifications", "")
			if tc.key != "" {
				req.Header.Set(middleware.HeaderAPIKey, tc.key)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			assert.Equal(t, tc.want, rec.Code)
			if tc.want == http.StatusUnauthorized {
				assert.Equal(t, "unauthorized", errorCode(t, rec))
			}
		})
	}

	req := request(t, http.MethodGet, "/", "")
	req.Header.Set(middleware.HeaderAPIKey, "key-new")
	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Len(t, client, 16, "client id is a short digest")
	assert.NotContains(t, client, "key-new", "the key itself never leaves the middleware")
}

// ---- recovery ---------------------------------------------------------------

func TestRecovery(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	h := middleware.Recovery(log, writeErr)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom: secret internal detail")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request(t, http.MethodGet, "/x", ""))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "internal_error", errorCode(t, rec))
	assert.NotContains(t, rec.Body.String(), "secret internal detail", "panic details must not leak to clients")
	assert.Contains(t, logs.String(), "secret internal detail", "but they must be logged")
	assert.Contains(t, logs.String(), "stack")
}

func TestBufferBody_ClientAbortIsBadRequest(t *testing.T) {
	h := middleware.BufferBody(1<<20, writeErr)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run")
	}))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", io.MultiReader(strings.NewReader("{\"par"), errReader{}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "a client hang-up is not a server error")
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestRecovery_AbortHandlerPropagates(t *testing.T) {
	h := middleware.Recovery(discard, writeErr)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
		h.ServeHTTP(httptest.NewRecorder(), request(t, http.MethodGet, "/", ""))
	})
}

// ---- request log ------------------------------------------------------------

func TestRequestLog(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	h := middleware.RequestLog(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("hello"))
	}))

	h.ServeHTTP(httptest.NewRecorder(), request(t, http.MethodPost, "/api/v1/notifications", ""))
	h.ServeHTTP(httptest.NewRecorder(), request(t, http.MethodGet, "/health", ""))
	h.ServeHTTP(httptest.NewRecorder(), request(t, http.MethodGet, "/fail", ""))

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	require.Len(t, lines, 2, "health probes are logged at debug level")
	var ok, failed map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ok))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &failed))
	assert.Equal(t, "POST", ok["method"])
	assert.EqualValues(t, 200, ok["status"])
	assert.EqualValues(t, 5, ok["bytes"])
	assert.Contains(t, ok, "duration_ms")
	assert.Equal(t, "ERROR", failed["level"])
}

// ---- idempotency ------------------------------------------------------------

// memStore is an in-memory idempotency.Store with the same semantics as the
// PostgreSQL implementation, including lease fencing and the unit of work:
// what the handler records (see record) becomes visible in created only
// when Execute stores the response.
type memStore struct {
	mu         sync.Mutex
	rows       map[string]*memRow
	released   []string
	tokens     int
	failCommit int      // number of Execute commits that fail before one succeeds
	created    []string // committed work
}

type unitKey struct{}

type unitWork struct{ items []string }

// record notes work the handler did inside the request's unit of work.
func record(ctx context.Context, item string) {
	if u, ok := ctx.Value(unitKey{}).(*unitWork); ok {
		u.items = append(u.items, item)
	}
}

type memRow struct {
	hash    string
	token   string
	status  int
	body    []byte
	created time.Time
	expires time.Time
}

func newMemStore() *memStore { return &memStore{rows: map[string]*memRow{}} }

func (s *memStore) Acquire(_ context.Context, key, hash string, now time.Time, ttl, staleAfter time.Duration) (idempotency.Lease, idempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[key]
	if !ok || !now.Before(row.expires) ||
		(row.status == 0 && row.created.Before(now.Add(-staleAfter)) && row.hash == hash) {
		s.tokens++
		token := fmt.Sprint(s.tokens)
		s.rows[key] = &memRow{hash: hash, token: token, created: now, expires: now.Add(ttl)}
		return idempotency.Lease{Key: key, Token: token}, idempotency.Record{}, true, nil
	}
	return idempotency.Lease{}, idempotency.Record{RequestHash: row.hash, Status: row.status, Body: row.body}, false, nil
}

func (s *memStore) Execute(ctx context.Context, l idempotency.Lease,
	fn func(context.Context) (idempotency.Response, bool),
) (bool, error) {
	work := &unitWork{}
	resp, keep := fn(context.WithValue(ctx, unitKey{}, work))
	if !keep {
		return false, nil // rolled back: work is dropped
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCommit > 0 {
		s.failCommit--
		return false, errors.New("db blip")
	}
	row, ok := s.rows[l.Key]
	if !ok || row.token != l.Token || row.status != 0 {
		return false, idempotency.ErrLeaseLost
	}
	row.status, row.body = resp.Status, append([]byte(nil), resp.Body...)
	s.created = append(s.created, work.items...)
	return true, nil
}

func (s *memStore) committed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.created...)
}

func (s *memStore) Release(_ context.Context, l idempotency.Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, ok := s.rows[l.Key]; ok && row.token == l.Token && row.status == 0 {
		delete(s.rows, l.Key)
	}
	s.released = append(s.released, l.Key)
	return nil
}

type idemHarness struct {
	store *memStore
	now   time.Time
	calls int
	mu    sync.Mutex
	h     http.Handler
	// respond decides what the protected handler does on each call.
	respond func(w http.ResponseWriter, call int)
}

func newIdemHarness() *idemHarness {
	x := &idemHarness{store: newMemStore(), now: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)}
	x.respond = func(w http.ResponseWriter, call int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"call":`+string(rune('0'+call))+`}`)
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x.mu.Lock()
		x.calls++
		n := x.calls
		x.mu.Unlock()
		record(r.Context(), fmt.Sprint("call ", n))
		x.respond(w, n)
	})
	x.h = middleware.BufferBody(1<<20, writeErr)(middleware.Idempotency(middleware.IdempotencyConfig{
		Store:   x.store,
		TTL:     24 * time.Hour,
		Now:     func() time.Time { return x.now },
		Log:     discard,
		Applies: func(r *http.Request) bool { return r.Method == http.MethodPost },
	}, writeErr)(inner))
	return x
}

func (x *idemHarness) post(t *testing.T, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := request(t, http.MethodPost, "/api/v1/notifications", body)
	if key != "" {
		req.Header.Set(middleware.HeaderIdempotencyKey, key)
	}
	rec := httptest.NewRecorder()
	x.h.ServeHTTP(rec, req)
	return rec
}

func TestIdempotency_ReplaysTheFirstResponse(t *testing.T) {
	x := newIdemHarness()

	first := x.post(t, "k1", `{"a":1}`)
	second := x.post(t, "k1", `{"a":1}`)

	assert.Equal(t, 1, x.calls, "the handler runs once")
	assert.Equal(t, http.StatusCreated, second.Code)
	assert.Equal(t, first.Body.String(), second.Body.String())
	assert.Empty(t, first.Header().Get(middleware.HeaderIdempotentReplayed))
	assert.Equal(t, "true", second.Header().Get(middleware.HeaderIdempotentReplayed))
}

func TestIdempotency_WithoutKeyAlwaysRuns(t *testing.T) {
	x := newIdemHarness()
	x.post(t, "", `{"a":1}`)
	x.post(t, "", `{"a":1}`)
	assert.Equal(t, 2, x.calls)
}

func TestIdempotency_SameKeyDifferentBody(t *testing.T) {
	x := newIdemHarness()
	x.post(t, "k1", `{"a":1}`)
	rec := x.post(t, "k1", `{"a":2}`)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Equal(t, "idempotency_key_reused", errorCode(t, rec))
	assert.Equal(t, 1, x.calls)
}

func TestIdempotency_InProgress(t *testing.T) {
	x := newIdemHarness()
	started, release := make(chan struct{}), make(chan struct{})
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusCreated)
	}

	done := make(chan struct{})
	go func() { x.post(t, "k1", `{}`); close(done) }()
	<-started

	rec := x.post(t, "k1", `{}`)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, "request_in_progress", errorCode(t, rec))

	close(release)
	<-done
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code, "replayed once the first finishes")
	assert.Equal(t, 1, x.calls)
}

func TestIdempotency_ServerErrorReleasesKey(t *testing.T) {
	x := newIdemHarness()
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}

	assert.Equal(t, http.StatusInternalServerError, x.post(t, "k1", `{}`).Code)
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code, "the retry runs for real")
	assert.Equal(t, 2, x.calls)
}

func TestIdempotency_PanicReleasesKey(t *testing.T) {
	x := newIdemHarness()
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 {
			panic("boom")
		}
		w.WriteHeader(http.StatusCreated)
	}

	assert.Panics(t, func() { x.post(t, "k1", `{}`) })
	assert.Contains(t, x.store.released, ":k1", "a panic releases the key")
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code)
}

func TestIdempotency_ClientErrorsAreReplayed(t *testing.T) {
	x := newIdemHarness()
	x.respond = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, `{"error":{"code":"validation_failed","message":"x"}}`)
	}
	x.post(t, "k1", `{}`)
	rec := x.post(t, "k1", `{}`)
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code, "a deterministic 4xx is a final answer")
	assert.Equal(t, 1, x.calls)
}

func TestIdempotency_ExpiredKeyRunsAgain(t *testing.T) {
	x := newIdemHarness()
	x.post(t, "k1", `{}`)
	x.now = x.now.Add(25 * time.Hour)
	x.post(t, "k1", `{"different":"body is fine after expiry"}`)
	assert.Equal(t, 2, x.calls)
}

func TestIdempotency_StaleLockIsTakenOver(t *testing.T) {
	x := newIdemHarness()
	started, hang := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(hang) })
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 { // a request whose process hangs forever without answering
			close(started)
			<-hang
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
	go x.post(t, "k1", `{}`)
	<-started

	x.now = x.now.Add(30 * time.Second)
	assert.Equal(t, http.StatusConflict, x.post(t, "k1", `{}`).Code, "still within the stale window")

	x.now = x.now.Add(time.Minute)
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code, "the abandoned lock is taken over")
}

// TestIdempotency_TakenOverLeaseCannotReleaseNewOwner is the fencing case:
// request A runs past the stale timeout, B takes the key over, then A fails.
// A's release must not free B's key, or a third request could run in
// parallel with B and create a duplicate.
func TestIdempotency_TakenOverLeaseCannotReleaseNewOwner(t *testing.T) {
	x := newIdemHarness()
	aStarted, aFinish := make(chan struct{}), make(chan struct{})
	bStarted, bFinish := make(chan struct{}), make(chan struct{})
	x.respond = func(w http.ResponseWriter, call int) {
		switch call {
		case 1: // A: slow, then fails
			close(aStarted)
			<-aFinish
			w.WriteHeader(http.StatusInternalServerError)
		case 2: // B: took over, still running
			close(bStarted)
			<-bFinish
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	}

	aDone := make(chan struct{})
	go func() { x.post(t, "k1", `{}`); close(aDone) }()
	<-aStarted
	x.now = x.now.Add(2 * time.Minute)
	bDone := make(chan struct{})
	go func() { x.post(t, "k1", `{}`); close(bDone) }()
	<-bStarted

	close(aFinish) // A fails and tries to release
	<-aDone

	rec := x.post(t, "k1", `{}`)
	assert.Equal(t, http.StatusConflict, rec.Code, "B still owns the key; C must not run")

	close(bFinish)
	<-bDone
	assert.Equal(t, 2, x.calls)
}

// TestIdempotency_StoreFailureRollsBackAndReleases: if the response cannot
// be stored, nothing the request did is kept either, the client gets a 500
// (never the 201 it would otherwise have no record of), and its retry runs
// for real, creating exactly once.
func TestIdempotency_StoreFailureRollsBackAndReleases(t *testing.T) {
	x := newIdemHarness()
	x.store.failCommit = 1

	assert.Equal(t, http.StatusInternalServerError, x.post(t, "k1", `{}`).Code)
	assert.Empty(t, x.store.committed(), "the failed request left nothing behind")
	assert.Contains(t, x.store.released, ":k1")

	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code)
	assert.Equal(t, []string{"call 2"}, x.store.committed(), "created once, by the retry")
	rec := x.post(t, "k1", `{}`)
	assert.Equal(t, "true", rec.Header().Get(middleware.HeaderIdempotentReplayed))
}

// TestIdempotency_TakenOverRequestRollsBack: request A runs past the stale
// timeout, its client retries (B), B takes the key over and completes. When
// A finishes, its work must not commit, or the batch would exist twice.
func TestIdempotency_TakenOverRequestRollsBack(t *testing.T) {
	x := newIdemHarness()
	aStarted, aFinish := make(chan struct{}), make(chan struct{})
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 {
			close(aStarted)
			<-aFinish
		}
		w.WriteHeader(http.StatusCreated)
	}

	var aCode int
	aDone := make(chan struct{})
	go func() { aCode = x.post(t, "k1", `{}`).Code; close(aDone) }()
	<-aStarted
	x.now = x.now.Add(2 * time.Minute)
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code, "B took over and completed")

	close(aFinish)
	<-aDone
	assert.Equal(t, http.StatusConflict, aCode, "A learns a retry took over")
	assert.Equal(t, []string{"call 2"}, x.store.committed(), "only B's work committed")
}

func TestIdempotency_StaleAfterIsConfigurable(t *testing.T) {
	x := newIdemHarness()
	x.h = middleware.BufferBody(1<<20, writeErr)(middleware.Idempotency(middleware.IdempotencyConfig{
		Store: x.store, TTL: 24 * time.Hour, StaleAfter: 5 * time.Minute,
		Now: func() time.Time { return x.now }, Log: discard,
		Applies: func(*http.Request) bool { return true },
	}, writeErr)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		x.mu.Lock()
		x.calls++
		n := x.calls
		x.mu.Unlock()
		x.respond(w, n)
	})))
	started, hang := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(hang) })
	x.respond = func(w http.ResponseWriter, call int) {
		if call == 1 { // a request that never finishes
			close(started)
			<-hang
			return
		}
		w.WriteHeader(http.StatusCreated)
	}
	go x.post(t, "k1", `{}`)
	<-started

	x.now = x.now.Add(2 * time.Minute) // past the default minute, within the configured five
	assert.Equal(t, http.StatusConflict, x.post(t, "k1", `{}`).Code)
	x.now = x.now.Add(4 * time.Minute)
	assert.Equal(t, http.StatusCreated, x.post(t, "k1", `{}`).Code)
}

func TestIdempotency_ReplayIsByteIdentical(t *testing.T) {
	x := newIdemHarness()
	x.respond = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "{\"z\":1,\"a\":2}\n")
	}
	first := x.post(t, "k1", `{}`)
	second := x.post(t, "k1", `{}`)
	assert.Equal(t, first.Body.Bytes(), second.Body.Bytes())
}

func TestIdempotency_InvalidKey(t *testing.T) {
	x := newIdemHarness()
	for _, bad := range []string{"has space", strings.Repeat("k", 256), "tab\tchar"} {
		rec := x.post(t, bad, `{}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%q", bad)
	}
	assert.Zero(t, x.calls)
}

func TestIdempotency_StoreFailureIs500(t *testing.T) {
	failing := failingStore{}
	h := middleware.Idempotency(middleware.IdempotencyConfig{
		Store: failing, TTL: time.Hour, Now: time.Now, Log: discard,
		Applies: func(*http.Request) bool { return true },
	}, writeErr)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run when the key cannot be checked")
	}))
	req := request(t, http.MethodPost, "/", "")
	req.Header.Set(middleware.HeaderIdempotencyKey, "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

type failingStore struct{}

func (failingStore) Acquire(context.Context, string, string, time.Time, time.Duration, time.Duration) (idempotency.Lease, idempotency.Record, bool, error) {
	return idempotency.Lease{}, idempotency.Record{}, false, errors.New("db down")
}

func (failingStore) Execute(context.Context, idempotency.Lease, func(context.Context) (idempotency.Response, bool)) (bool, error) {
	return false, nil
}
func (failingStore) Release(context.Context, idempotency.Lease) error { return nil }

// ---- metrics ----------------------------------------------------------------

func TestMetrics_CountsPanicsAsServerErrors(t *testing.T) {
	m := metrics.New()
	// The router's order: metrics outside recovery.
	h := middleware.Metrics(m)(middleware.Recovery(discard, writeErr)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	h.ServeHTTP(httptest.NewRecorder(), request(t, http.MethodGet, "/", ""))
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.HTTPRequests.WithLabelValues("unmatched", "GET", "500")), 0)
}

func TestMetrics_UnknownMethodsShareOneLabel(t *testing.T) {
	m := metrics.New()
	h := middleware.Metrics(m)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	for _, method := range []string{"BREW", "X-RANDOM-1", "X-RANDOM-2", http.MethodDelete} {
		h.ServeHTTP(httptest.NewRecorder(), request(t, method, "/", ""))
	}
	assert.InDelta(t, 3, promtestutil.ToFloat64(m.HTTPRequests.WithLabelValues("unmatched", "other", "405")), 0)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.HTTPRequests.WithLabelValues("unmatched", "DELETE", "405")), 0)
	assert.Equal(t, 2, promtestutil.CollectAndCount(m.HTTPRequests), "no series per invented method")
}
