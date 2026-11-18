package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
)

// Connection limits.
const (
	maxSubscriptions = 1000     // ids per connection
	maxClientMessage = 64 << 10 // fits maxSubscriptions ids (~40 bytes each) in one message
	writeTimeout     = 10 * time.Second
	pingInterval     = 30 * time.Second
	pingTimeout      = 10 * time.Second
)

// subscription is what a client listens to. All matches every event.
type subscription struct {
	All           bool                        `json:"all"`
	Notifications map[uuid.UUID]struct{}      `json:"-"`
	Batches       map[uuid.UUID]struct{}      `json:"-"`
	Channels      map[domain.Channel]struct{} `json:"-"`
}

// subscriptionRequest is the client's subscribe/unsubscribe message, and
// the shape of the query string on connect.
type subscriptionRequest struct {
	Action          string      `json:"action"` // "subscribe" or "unsubscribe"
	All             *bool       `json:"all,omitempty"`
	NotificationIDs []uuid.UUID `json:"notification_ids,omitempty"`
	BatchIDs        []uuid.UUID `json:"batch_ids,omitempty"`
	Channels        []string    `json:"channels,omitempty"`
}

// subscriptionView is how a subscription is reported back.
type subscriptionView struct {
	All             bool        `json:"all"`
	NotificationIDs []uuid.UUID `json:"notification_ids"`
	BatchIDs        []uuid.UUID `json:"batch_ids"`
	Channels        []string    `json:"channels"`
}

func (s *subscription) view() subscriptionView {
	v := subscriptionView{All: s.All, NotificationIDs: []uuid.UUID{}, BatchIDs: []uuid.UUID{}, Channels: []string{}}
	for id := range s.Notifications {
		v.NotificationIDs = append(v.NotificationIDs, id)
	}
	for id := range s.Batches {
		v.BatchIDs = append(v.BatchIDs, id)
	}
	for ch := range s.Channels {
		v.Channels = append(v.Channels, string(ch))
	}
	return v
}

// apply adds (subscribe) or removes (unsubscribe) the request's targets.
func (s *subscription) apply(r *subscriptionRequest) error {
	add := r.Action == "subscribe"
	if !add && r.Action != "unsubscribe" {
		return fmt.Errorf("action must be subscribe or unsubscribe, got %q", r.Action)
	}
	chans := make([]domain.Channel, 0, len(r.Channels))
	for _, c := range r.Channels {
		ch, err := domain.ParseChannel(c)
		if err != nil {
			return fmt.Errorf("unknown channel %q", c)
		}
		chans = append(chans, ch)
	}
	if add && len(s.Notifications)+len(s.Batches)+
		countNew(s.Notifications, r.NotificationIDs)+countNew(s.Batches, r.BatchIDs) > maxSubscriptions {
		return fmt.Errorf("at most %d notification and batch ids per connection", maxSubscriptions)
	}
	if r.All != nil {
		s.All = *r.All && add
	}
	set(s.Notifications, r.NotificationIDs, add)
	set(s.Batches, r.BatchIDs, add)
	set(s.Channels, chans, add)
	return nil
}

// countNew counts the distinct keys not yet in m, so re-subscribing to
// what a client already has (e.g. a resync) never hits the limit.
func countNew[K comparable](m map[K]struct{}, keys []K) int {
	seen := make(map[K]struct{}, len(keys))
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			seen[k] = struct{}{}
		}
	}
	return len(seen)
}

func set[K comparable](m map[K]struct{}, keys []K, add bool) {
	for _, k := range keys {
		if add {
			m[k] = struct{}{}
		} else {
			delete(m, k)
		}
	}
}

func (s *subscription) matches(k *eventKeys) bool {
	if s.All {
		return true
	}
	if k.NotificationID != nil {
		if _, ok := s.Notifications[*k.NotificationID]; ok {
			return true
		}
	}
	if k.BatchID != nil {
		if _, ok := s.Batches[*k.BatchID]; ok {
			return true
		}
	}
	_, ok := s.Channels[domain.Channel(k.Channel)]
	return ok
}

// parseQuery reads the initial subscription from the URL:
// ?notification_id=…&batch_id=…&channel=sms&all=true (ids repeatable).
func parseQuery(q url.Values) (*subscriptionRequest, error) {
	r := &subscriptionRequest{Action: "subscribe", Channels: q["channel"]}
	for _, v := range q["notification_id"] {
		id, err := uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("notification_id %q is not a UUID", v)
		}
		r.NotificationIDs = append(r.NotificationIDs, id)
	}
	for _, v := range q["batch_id"] {
		id, err := uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("batch_id %q is not a UUID", v)
		}
		r.BatchIDs = append(r.BatchIDs, id)
	}
	switch q.Get("all") {
	case "":
	case "true":
		all := true
		r.All = &all
	case "false":
	default:
		return nil, errors.New("all must be true or false")
	}
	return r, nil
}

// client is one WebSocket connection.
type client struct {
	mu  sync.Mutex
	sub subscription

	send   chan []byte
	cancel context.CancelCauseFunc
	// abortWrites cuts off a write in progress. Only for a client that is
	// too slow: at shutdown the current write is let finish so the close
	// handshake (1001) can follow it.
	abortWrites context.CancelFunc
}

// errTooSlow and errShutdown are why a connection was closed by us.
var (
	errTooSlow  = errors.New("client too slow")
	errShutdown = errors.New("server shutting down")
)

func (c *client) matches(k *eventKeys) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sub.matches(k)
}

// enqueue queues msg, or disconnects the client if its queue is full.
func (c *client) enqueue(msg []byte) {
	select {
	case c.send <- msg:
	default:
		c.cancel(errTooSlow)
		c.abortWrites()
	}
}

// Handler serves the WebSocket endpoint.
type Handler struct {
	Hub *Hub
	// OriginPatterns lists the browser origins allowed to connect besides
	// the API's own host (see websocket.AcceptOptions). Non-browser clients
	// send no Origin and are always allowed; they still need the API key.
	OriginPatterns []string
	Log            *slog.Logger
	// Errors writes error responses before the upgrade (the API's JSON
	// error envelope).
	Errors func(http.ResponseWriter, *http.Request, error)
	// Shutdown closes every connection with "going away" when it is done:
	// http.Server.Shutdown does not track hijacked connections.
	Shutdown context.Context
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	initial, err := parseQuery(r.URL.Query())
	if err != nil {
		h.Errors(w, r, apierror.New(http.StatusBadRequest, api.ErrorCodeBadRequest, err.Error()))
		return
	}
	// The connection outlives the request context's usual scope; it ends
	// on a read/write error, on backpressure, or at server shutdown.
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(r.Context()))
	defer cancel(nil)
	stop := context.AfterFunc(h.Shutdown, func() { cancel(errShutdown) }) //nolint:contextcheck // server lifetime, deliberately not the request
	defer stop()

	writeCtx, abortWrites := context.WithCancel(context.WithoutCancel(ctx))
	defer abortWrites()
	c := &client{
		sub:         subscription{Notifications: map[uuid.UUID]struct{}{}, Batches: map[uuid.UUID]struct{}{}, Channels: map[domain.Channel]struct{}{}},
		send:        make(chan []byte, h.Hub.cfg.BufferSize),
		cancel:      cancel,
		abortWrites: abortWrites,
	}
	if err := c.sub.apply(initial); err != nil {
		h.Errors(w, r, apierror.New(http.StatusBadRequest, api.ErrorCodeBadRequest, err.Error()))
		return
	}
	if err := h.Hub.add(c); err != nil {
		h.Errors(w, r, apierror.New(http.StatusServiceUnavailable, api.ErrorCodeServiceUnavailable,
			"Too many WebSocket connections on this instance; retry later."))
		return
	}
	defer h.Hub.remove(c)

	// The server's read/write timeouts were set on the connection for
	// ordinary requests; a WebSocket lives far longer. Clear them before
	// the hijack (they would survive it).
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.OriginPatterns})
	if err != nil {
		return // Accept already wrote the error response
	}
	conn.SetReadLimit(maxClientMessage)

	log := h.Log.With("remote", r.RemoteAddr)
	log.InfoContext(ctx, "websocket connected", "clients", h.Hub.Clients())

	// Read and write calls get contexts of their own: coder/websocket
	// drops the connection without a close frame when the context of a
	// pending Read or Write is cancelled, and we want to say why we close.
	// The reader stops when the close handshake ends the connection.
	ioCtx, stopIO := context.WithCancel(context.WithoutCancel(ctx))
	defer stopIO()

	c.reply()
	go c.readLoop(ioCtx, conn)
	reason := c.writeLoop(ctx, writeCtx, ioCtx, conn)
	h.Hub.remove(c) // stop routing events to it while the close handshake runs

	switch {
	case errors.Is(reason, errTooSlow):
		log.WarnContext(ctx, "websocket client too slow, disconnected", "buffer", cap(c.send))
		_ = conn.Close(websocket.StatusPolicyViolation, "too slow: reconnect and reload state from the REST API")
	case errors.Is(reason, errShutdown):
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
	default:
		_ = conn.CloseNow()
	}
	log.InfoContext(ctx, "websocket disconnected", "reason", reason)
}

// writeLoop sends queued messages and pings until the connection ends,
// and returns why it ended.
func (c *client) writeLoop(ctx, writeCtx, ioCtx context.Context, conn *websocket.Conn) error {
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case msg := <-c.send:
			// If the client is dropped as too slow while this write is
			// blocked (its TCP buffer is full), abort at once: no close
			// frame could get through anyway. Shutdown does not abort it:
			// coder/websocket closes the connection when a write's context
			// ends, even one that has just completed, which would replace
			// the 1001 close handshake with a dropped connection.
			wctx, cancel := context.WithTimeout(writeCtx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return fmt.Errorf("write: %w", err)
			}
		case <-ping.C:
			// Detects dead peers (a vanished mobile client never sends a
			// close frame) and keeps proxies from timing the link out.
			pctx, cancel := context.WithTimeout(ioCtx, pingTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}

// readLoop handles subscription changes. It also has to run for pongs and
// close frames to be processed.
func (c *client) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			c.cancel(fmt.Errorf("read: %w", err))
			return
		}
		var req subscriptionRequest
		if typ != websocket.MessageText {
			err = errors.New("messages must be JSON text")
		} else if err = json.Unmarshal(data, &req); err == nil {
			c.mu.Lock()
			err = c.sub.apply(&req)
			c.mu.Unlock()
		}
		if err != nil {
			c.queueReply(map[string]any{"type": "error", "error": err.Error()})
			continue
		}
		c.reply()
	}
}

// reply queues the current subscription, so the client sees what it now
// listens to (in order with the events around it).
func (c *client) reply() {
	c.mu.Lock()
	v := c.sub.view()
	c.mu.Unlock()
	c.queueReply(map[string]any{"type": "subscribed", "subscription": v})
}

func (c *client) queueReply(v any) {
	b, err := json.Marshal(v)
	if err == nil {
		c.enqueue(b)
	}
}
