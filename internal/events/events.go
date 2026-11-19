// Package events carries domain events over Redis Streams.
//
// Events are born in PostgreSQL: triggers write every notification status
// change into the outbox_events table, in the same transaction as the
// change itself (the transactional outbox). The outbox relay
// (package outbox) moves them to a Redis stream in order. Readers use
// Tail: every reader sees every event, from the moment it starts. That
// serves fan-out to live connections (WebSocket): a live feed, best
// effort, where a client that misses something reloads the state over the
// REST API. Work that must not be lost (status callbacks) does not go
// through the stream: the outbox trigger queues it in PostgreSQL, in the
// transaction of the change.
//
// Delivery is at least once: the relay may publish an event again if it
// crashes before marking it published. Event.ID (the outbox id) is the
// deduplication key.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Event types.
const (
	TypeNotificationCreated       = "notification.created"
	TypeNotificationStatusChanged = "notification.status_changed"
	TypeBatchCompleted            = "batch.completed"
)

// DefaultStream is the stream all domain events go to.
const DefaultStream = "stream:notification-events"

// Event is one domain event.
type Event struct {
	ID            int64 // outbox id: unique, increasing per aggregate
	Type          string
	AggregateType string // "notification" or "batch"
	AggregateID   uuid.UUID
	Payload       json.RawMessage
	CreatedAt     time.Time
	StreamID      string // Redis stream entry id; set when read from a stream
}

// NotificationPayload is the payload of notification.* events.
type NotificationPayload struct {
	NotificationID    uuid.UUID  `json:"notification_id"`
	BatchID           *uuid.UUID `json:"batch_id"`
	Channel           string     `json:"channel"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	PreviousStatus    *string    `json:"previous_status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"max_attempts"`
	ScheduledAt       *time.Time `json:"scheduled_at"`
	NextAttemptAt     *time.Time `json:"next_attempt_at"`
	ErrorCode         *string    `json:"error_code"`
	ProviderMessageID *string    `json:"provider_message_id"`
	CorrelationID     *string    `json:"correlation_id"`
	OccurredAt        time.Time  `json:"occurred_at"`
}

// BatchCompletedPayload is the payload of batch.completed.
type BatchCompletedPayload struct {
	BatchID     uuid.UUID      `json:"batch_id"`
	Total       int            `json:"total"`
	Created     int            `json:"created"`
	Counts      map[string]int `json:"counts"`
	CompletedAt time.Time      `json:"completed_at"`
	OccurredAt  time.Time      `json:"occurred_at"`
}

// Stream entry field names.
const (
	fieldID            = "event_id"
	fieldType          = "type"
	fieldAggregateType = "aggregate_type"
	fieldAggregateID   = "aggregate_id"
	fieldPayload       = "payload"
	fieldCreatedAt     = "created_at"
)

func (e *Event) values() map[string]any {
	return map[string]any{
		fieldID:            e.ID,
		fieldType:          e.Type,
		fieldAggregateType: e.AggregateType,
		fieldAggregateID:   e.AggregateID.String(),
		fieldPayload:       string(e.Payload),
		fieldCreatedAt:     e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// ErrMalformed marks a stream entry that cannot be decoded. Consumers skip
// (and acknowledge) such entries rather than retry them forever.
var ErrMalformed = errors.New("malformed event")

// Decode turns a stream entry back into an Event.
func Decode(m redis.XMessage) (Event, error) {
	str := func(k string) string { s, _ := m.Values[k].(string); return s }
	e := Event{
		Type:          str(fieldType),
		AggregateType: str(fieldAggregateType),
		Payload:       json.RawMessage(str(fieldPayload)),
		StreamID:      m.ID,
	}
	var err error
	if e.ID, err = strconv.ParseInt(str(fieldID), 10, 64); err != nil {
		return e, fmt.Errorf("%w %s: event_id: %w", ErrMalformed, m.ID, err)
	}
	if e.AggregateID, err = uuid.Parse(str(fieldAggregateID)); err != nil {
		return e, fmt.Errorf("%w %s: aggregate_id: %w", ErrMalformed, m.ID, err)
	}
	if e.CreatedAt, err = time.Parse(time.RFC3339Nano, str(fieldCreatedAt)); err != nil {
		return e, fmt.Errorf("%w %s: created_at: %w", ErrMalformed, m.ID, err)
	}
	if e.Type == "" || !json.Valid(e.Payload) {
		return e, fmt.Errorf("%w %s: missing type or invalid payload", ErrMalformed, m.ID)
	}
	return e, nil
}
