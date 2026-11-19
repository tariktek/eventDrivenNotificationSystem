package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Delivery attempt limits. A notification may override the default within
// [MinAttempts, MaxAttemptsLimit].
const (
	DefaultMaxAttempts = 5
	MinAttempts        = 1
	MaxAttemptsLimit   = 10
)

// Notification is a single message to deliver over one channel. It is both
// the record clients query and the unit of work in the queue.
//
// Optional text fields use "" for "absent"; optional timestamps and
// references use nil pointers.
type Notification struct {
	ID      uuid.UUID
	BatchID *uuid.UUID

	Channel   Channel
	Priority  Priority
	Recipient string
	Subject   string
	Content   string // final content; templates are rendered before storage
	Segments  int    // SMS segment count; 0 for other channels

	TemplateID      *uuid.UUID
	TemplateVersion int
	TemplateVars    map[string]any

	Status         Status
	IdempotencyKey string
	// RequestHash fingerprints the request that created the notification
	// (see RequestFingerprint), so a repeat of its IdempotencyKey can be told
	// apart from a different request reusing the key. Empty for rows
	// created before it existed.
	RequestHash string

	// Attempts is the attempt budget used so far: it counts towards
	// MaxAttempts and a manual retry resets it. TotalAttempts counts every
	// attempt ever claimed and is never reset; it numbers the attempt
	// history.
	Attempts      int
	TotalAttempts int
	MaxAttempts   int
	NextAttemptAt time.Time
	LockedBy      string
	LockedUntil   *time.Time

	ScheduledAt *time.Time
	SentAt      *time.Time
	FailedAt    *time.Time
	CancelledAt *time.Time

	ProviderMessageID string
	LastError         string
	LastErrorCode     string

	CallbackURL   string
	Metadata      map[string]any
	CorrelationID string
	TraceParent   string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewNotificationParams carries the client-supplied fields of a new
// notification. Input is expected to be validated already (see the
// validation package); NewNotification only applies defaults and derives
// the initial queue state.
type NewNotificationParams struct {
	BatchID *uuid.UUID

	Channel   Channel
	Priority  Priority // "" selects PriorityNormal
	Recipient string
	Subject   string
	Content   string
	Segments  int

	TemplateID      *uuid.UUID
	TemplateVersion int
	TemplateVars    map[string]any

	IdempotencyKey string
	RequestHash    string
	MaxAttempts    int // 0 selects DefaultMaxAttempts
	ScheduledAt    *time.Time

	CallbackURL   string
	Metadata      map[string]any
	CorrelationID string
	TraceParent   string
}

// NewNotification builds a notification ready to be stored and queued.
//
// A notification scheduled for the future starts as StatusScheduled and
// becomes due at its scheduled time; anything else starts as StatusPending
// and is due immediately. Using next_attempt_at for both means the queue
// needs no separate scheduler.
func NewNotification(p NewNotificationParams, now time.Time) (*Notification, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate notification id: %w", err)
	}

	priority := p.Priority
	if priority == "" {
		priority = PriorityNormal
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	metadata := p.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}

	n := &Notification{
		ID:              id,
		BatchID:         p.BatchID,
		Channel:         p.Channel,
		Priority:        priority,
		Recipient:       p.Recipient,
		Subject:         p.Subject,
		Content:         p.Content,
		Segments:        p.Segments,
		TemplateID:      p.TemplateID,
		TemplateVersion: p.TemplateVersion,
		TemplateVars:    p.TemplateVars,
		Status:          StatusPending,
		IdempotencyKey:  p.IdempotencyKey,
		RequestHash:     p.RequestHash,
		MaxAttempts:     maxAttempts,
		NextAttemptAt:   now,
		CallbackURL:     p.CallbackURL,
		Metadata:        metadata,
		CorrelationID:   p.CorrelationID,
		TraceParent:     p.TraceParent,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if p.ScheduledAt != nil && p.ScheduledAt.After(now) {
		at := *p.ScheduledAt
		n.Status = StatusScheduled
		n.ScheduledAt = &at
		n.NextAttemptAt = at
	}

	return n, nil
}

// HasAttemptsLeft reports whether another delivery attempt is allowed.
func (n *Notification) HasAttemptsLeft() bool {
	return n.Attempts < n.MaxAttempts
}

// RequestFingerprint hashes what a client asked for when creating a
// notification, so that a repeat of its idempotency key can be told apart
// from a different request reusing the key. It covers everything that
// changes what is delivered, to whom, when and how; defaults are applied
// first, so leaving out a field and sending its default value match. For a
// templated notification it covers the template and its variables, not the
// rendered text, so a later template version does not make an honest
// retry look different.
func RequestFingerprint(p *NewNotificationParams) string {
	priority := p.Priority
	if priority == "" {
		priority = PriorityNormal
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	f := struct {
		Channel      Channel        `json:"channel"`
		Priority     Priority       `json:"priority"`
		Recipient    string         `json:"recipient"`
		Subject      string         `json:"subject,omitempty"`
		Content      string         `json:"content,omitempty"`
		TemplateID   *uuid.UUID     `json:"template_id,omitempty"`
		TemplateVars map[string]any `json:"template_vars,omitempty"`
		MaxAttempts  int            `json:"max_attempts"`
		ScheduledAt  *time.Time     `json:"scheduled_at,omitempty"`
		CallbackURL  string         `json:"callback_url,omitempty"`
		Metadata     map[string]any `json:"metadata,omitempty"`
	}{
		Channel: p.Channel, Priority: priority, Recipient: p.Recipient,
		MaxAttempts: maxAttempts, CallbackURL: p.CallbackURL, Metadata: p.Metadata,
	}
	if p.TemplateID != nil {
		f.TemplateID, f.TemplateVars = p.TemplateID, p.TemplateVars
	} else {
		f.Subject, f.Content = p.Subject, p.Content
	}
	if p.ScheduledAt != nil {
		at := p.ScheduledAt.UTC()
		f.ScheduledAt = &at
	}
	// Marshal sorts map keys, so equal requests encode identically. It
	// cannot fail: every value came from decoded JSON.
	b, _ := json.Marshal(f) //nolint:errchkjson // see above
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
