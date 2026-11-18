package realtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/realtime"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type server struct {
	hub      *realtime.Hub
	url      string
	shutdown context.CancelFunc
}

func newServer(t *testing.T, cfg realtime.Config) *server {
	t.Helper()
	hub := realtime.NewHub(cfg, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	h := &realtime.Handler{
		Hub: hub, Log: quiet, Shutdown: ctx,
		Errors: func(w http.ResponseWriter, _ *http.Request, err error) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		},
	}
	srv := httptest.NewServer(h)
	t.Cleanup(func() { cancel(); srv.Close() })
	return &server{hub: hub, url: "ws" + strings.TrimPrefix(srv.URL, "http"), shutdown: cancel}
}

type message struct {
	Type         string          `json:"type"`
	EventID      int64           `json:"event_id"`
	EventType    string          `json:"event_type"`
	Data         json.RawMessage `json:"data"`
	Error        string          `json:"error"`
	Subscription struct {
		All     bool     `json:"all"`
		Batches []string `json:"batch_ids"`
	} `json:"subscription"`
}

func dial(t *testing.T, s *server, query string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, s.url+"?"+query, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	m := read(t, c)
	require.Equal(t, "subscribed", m.Type, "the first message confirms the subscription")
	return c
}

func read(t *testing.T, c *websocket.Conn) message {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	var m message
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

func send(t *testing.T, c *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, c.Write(t.Context(), websocket.MessageText, b))
}

var nextID int64

func notificationEvent(nid, batch uuid.UUID, channel, status string) events.Event {
	nextID++
	return events.Event{
		ID: nextID, Type: events.TypeNotificationStatusChanged, AggregateType: "notification", AggregateID: nid,
		Payload:   json.RawMessage(fmt.Sprintf(`{"notification_id":%q,"batch_id":%q,"channel":%q,"status":%q}`, nid, batch, channel, status)),
		CreatedAt: time.Now(),
	}
}

// waitClients waits until the hub has registered n clients (the server
// registers a client before the handshake completes, but be safe).
func waitClients(t *testing.T, s *server, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return s.hub.Clients() == n }, 2*time.Second, 5*time.Millisecond)
}

func TestBatchSubscriptionGetsOnlyItsEventsInOrder(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})
	batch, other := uuid.New(), uuid.New()
	c := dial(t, s, "batch_id="+batch.String())
	waitClients(t, s, 1)

	n := uuid.New()
	for _, st := range []string{"pending", "processing", "sent"} {
		s.hub.Publish(notificationEvent(n, batch, "sms", st))
		s.hub.Publish(notificationEvent(uuid.New(), other, "sms", st)) // not subscribed
	}
	s.hub.Publish(events.Event{
		ID: 999, Type: events.TypeBatchCompleted, AggregateType: "batch", AggregateID: batch,
		Payload: json.RawMessage(`{"counts":{"sent":1}}`), CreatedAt: time.Now(),
	})

	var got []string
	for range 4 {
		m := read(t, c)
		require.Equal(t, "event", m.Type)
		var p struct{ Status string }
		require.NoError(t, json.Unmarshal(m.Data, &p))
		got = append(got, m.EventType+":"+p.Status)
	}
	assert.Equal(t, []string{
		"notification.status_changed:pending",
		"notification.status_changed:processing",
		"notification.status_changed:sent",
		"batch.completed:",
	}, got)
}

func TestSubscribeAndUnsubscribeAtRuntime(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})
	c := dial(t, s, "")
	waitClients(t, s, 1)
	n := uuid.New()

	send(t, c, map[string]any{"action": "subscribe", "notification_ids": []string{n.String()}})
	require.Equal(t, "subscribed", read(t, c).Type)
	s.hub.Publish(notificationEvent(n, uuid.New(), "email", "sent"))
	assert.Equal(t, "event", read(t, c).Type)

	send(t, c, map[string]any{"action": "unsubscribe", "notification_ids": []string{n.String()}})
	require.Equal(t, "subscribed", read(t, c).Type)
	send(t, c, map[string]any{"action": "subscribe", "channels": []string{"push"}})
	require.Equal(t, "subscribed", read(t, c).Type)

	s.hub.Publish(notificationEvent(n, uuid.New(), "email", "failed")) // no longer subscribed
	s.hub.Publish(notificationEvent(uuid.New(), uuid.New(), "push", "sent"))
	m := read(t, c)
	assert.Contains(t, string(m.Data), `"push"`, "the email event was skipped")
}

func TestAllReceivesEverything(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})
	c := dial(t, s, "all=true")
	waitClients(t, s, 1)
	s.hub.Publish(notificationEvent(uuid.New(), uuid.New(), "sms", "sent"))
	assert.Equal(t, "event", read(t, c).Type)
}

func TestInvalidInput(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})

	for _, q := range []string{"batch_id=nope", "channel=fax", "all=maybe"} {
		_, resp, err := websocket.Dial(t.Context(), s.url+"?"+q, nil)
		require.Error(t, err, q)
		require.NotNil(t, resp)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, q)
		_ = resp.Body.Close()
	}

	c := dial(t, s, "")
	for _, bad := range []string{`{"action":"explode"}`, `not json`, `{"action":"subscribe","channels":["fax"]}`} {
		require.NoError(t, c.Write(t.Context(), websocket.MessageText, []byte(bad)))
		m := read(t, c)
		assert.Equal(t, "error", m.Type, bad)
		assert.NotEmpty(t, m.Error)
	}
	send(t, c, map[string]any{"action": "subscribe", "all": true})
	assert.True(t, read(t, c).Subscription.All, "the connection survives bad messages")
}

func TestSlowClientIsDisconnected(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 4})
	c := dial(t, s, "all=true")
	waitClients(t, s, 1)

	// The client reads nothing while a flood arrives: once socket buffers
	// and its queue fill up, the server cuts it off rather than wait.
	big := strings.Repeat("x", 4096)
	for i := range 5000 {
		e := notificationEvent(uuid.New(), uuid.New(), "sms", "sent")
		e.Payload = json.RawMessage(fmt.Sprintf(`{"channel":"sms","pad":%q,"i":%d}`, big, i))
		s.hub.Publish(e)
		if s.hub.Clients() == 0 {
			break
		}
	}
	waitClients(t, s, 0)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		if _, _, err := c.Read(ctx); err != nil {
			// 1008 if the close frame got through; with a clogged
			// connection the server drops it without one (-1).
			assert.Contains(t, []websocket.StatusCode{websocket.StatusPolicyViolation, -1}, websocket.CloseStatus(err), "err: %v", err)
			assert.NotErrorIs(t, err, context.DeadlineExceeded, "the server closed it, not our timeout")
			return
		}
	}
}

func TestConnectionLimit(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 1, BufferSize: 4})
	dial(t, s, "all=true")
	_, resp, err := websocket.Dial(t.Context(), s.url, nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "the test error writer maps every error to 400")
}

func TestShutdownClosesWithGoingAway(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 4})
	c := dial(t, s, "all=true")
	s.shutdown()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _, err := c.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
	waitClients(t, s, 0)
}

func TestChannelSubscriptionFromQuery(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})
	c := dial(t, s, "channel=sms")
	waitClients(t, s, 1)
	s.hub.Publish(notificationEvent(uuid.New(), uuid.New(), "email", "sent"))
	s.hub.Publish(notificationEvent(uuid.New(), uuid.New(), "sms", "sent"))
	m := read(t, c)
	assert.Equal(t, "event", m.Type)
	assert.Contains(t, string(m.Data), `"sms"`)
}

func TestSubscriptionLimitCountsDistinctNewIDs(t *testing.T) {
	s := newServer(t, realtime.Config{MaxClients: 10, BufferSize: 64})
	c := dial(t, s, "")
	ids := make([]string, 600)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	send(t, c, map[string]any{"action": "subscribe", "batch_ids": ids})
	require.Equal(t, "subscribed", read(t, c).Type)
	send(t, c, map[string]any{"action": "subscribe", "batch_ids": append(ids, ids...)})
	assert.Equal(t, "subscribed", read(t, c).Type, "re-sending the same ids is not 1800 subscriptions")

	more := make([]string, 401) // pushes the total to one over the limit
	for i := range more {
		more[i] = uuid.NewString()
	}
	send(t, c, map[string]any{"action": "subscribe", "batch_ids": more})
	assert.Equal(t, "error", read(t, c).Type, "1001 distinct ids is over the limit")
}
