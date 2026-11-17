// Package realtime pushes notification events to WebSocket clients.
//
// Each API instance runs one Hub. The hub tails the event stream (every
// instance sees every event) and forwards each event to the connected
// clients whose subscription matches it. A client subscribes to specific
// notifications, batches or channels, or to everything.
//
// Ordering: the hub handles events one at a time, in stream order, and
// each client has a FIFO queue, so a client sees one notification's status
// changes in the order they happened.
//
// Backpressure: a client's queue holds BufferSize messages. A client that
// cannot keep up is disconnected instead of making the hub wait or buffer
// without limit: with close code 1008 when a close frame can still reach
// it, abruptly when its connection is clogged. It should reconnect and
// reload the current state through the REST API.
package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
)

// Config tunes a Hub.
type Config struct {
	// MaxClients caps concurrent connections per instance.
	MaxClients int
	// BufferSize is how many messages may wait for a slow client.
	BufferSize int
}

// Hub fans events out to subscribed clients.
type Hub struct {
	cfg Config
	log *slog.Logger

	mu      sync.RWMutex
	clients map[*client]struct{}
}

// NewHub returns a Hub.
func NewHub(cfg Config, log *slog.Logger) *Hub {
	return &Hub{cfg: cfg, log: log, clients: map[*client]struct{}{}}
}

// ErrFull is returned when the hub has MaxClients connections.
var ErrFull = errors.New("too many WebSocket connections")

func (h *Hub) add(c *client) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.clients) >= h.cfg.MaxClients {
		return ErrFull
	}
	h.clients[c] = struct{}{}
	return nil
}

func (h *Hub) remove(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// Clients returns the number of connected clients.
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// eventKeys are the fields subscriptions match on.
type eventKeys struct {
	NotificationID *uuid.UUID `json:"notification_id"`
	BatchID        *uuid.UUID `json:"batch_id"`
	Channel        string     `json:"channel"`
}

// outMessage is what clients receive for an event.
type outMessage struct {
	Type      string          `json:"type"` // "event"
	EventID   int64           `json:"event_id"`
	EventType string          `json:"event_type"`
	Data      json.RawMessage `json:"data"`
}

// Publish forwards an event to every matching client. It never blocks on
// a client.
func (h *Hub) Publish(e events.Event) {
	var k eventKeys
	if err := json.Unmarshal(e.Payload, &k); err != nil {
		h.log.Error("undecodable event payload", "event_id", e.ID, "error", err)
		return
	}
	if e.AggregateType == "batch" {
		k.BatchID = &e.AggregateID
	}
	var msg []byte // encoded once, only if someone wants it

	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.matches(&k) {
			continue
		}
		if msg == nil {
			var err error
			msg, err = json.Marshal(outMessage{Type: "event", EventID: e.ID, EventType: e.Type, Data: e.Payload})
			if err != nil {
				h.log.Error("encode event", "event_id", e.ID, "error", err)
				return
			}
		}
		c.enqueue(msg)
	}
}

// Run feeds the hub from the event stream until ctx is done.
func (h *Hub) Run(ctx context.Context, tail func(context.Context, func(events.Event))) {
	tail(ctx, h.Publish)
}
