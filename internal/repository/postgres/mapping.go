package postgres

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// Conversion between sqlc rows and domain types. Timestamps are normalised
// to UTC so values never depend on the server's or client's time zone.

func toNotification(r *sqlcgen.Notification) (*domain.Notification, error) {
	priority, err := domain.PriorityFromRank(r.Priority)
	if err != nil {
		return nil, err
	}
	n := &domain.Notification{
		ID:                r.ID,
		BatchID:           r.BatchID,
		Channel:           domain.Channel(r.Channel),
		Priority:          priority,
		Recipient:         r.Recipient,
		Subject:           deref(r.Subject),
		Content:           r.Content,
		Segments:          int(deref(r.Segments)),
		TemplateID:        r.TemplateID,
		TemplateVersion:   int(deref(r.TemplateVersion)),
		Status:            domain.Status(r.Status),
		IdempotencyKey:    deref(r.IdempotencyKey),
		Attempts:          int(r.Attempts),
		MaxAttempts:       int(r.MaxAttempts),
		NextAttemptAt:     r.NextAttemptAt.UTC(),
		LockedBy:          deref(r.LockedBy),
		LockedUntil:       utcPtr(r.LockedUntil),
		ScheduledAt:       utcPtr(r.ScheduledAt),
		SentAt:            utcPtr(r.SentAt),
		FailedAt:          utcPtr(r.FailedAt),
		CancelledAt:       utcPtr(r.CancelledAt),
		ProviderMessageID: deref(r.ProviderMessageID),
		LastError:         deref(r.LastError),
		LastErrorCode:     deref(r.LastErrorCode),
		CallbackURL:       deref(r.CallbackUrl),
		CorrelationID:     deref(r.CorrelationID),
		TraceParent:       deref(r.TraceParent),
		CreatedAt:         r.CreatedAt.UTC(),
		UpdatedAt:         r.UpdatedAt.UTC(),
	}
	md, err := unmarshalObject(r.Metadata)
	if err != nil {
		return nil, fmt.Errorf("notification %s metadata: %w", r.ID, err)
	}
	n.Metadata = md
	if len(r.TemplateVars) > 0 {
		if err := json.Unmarshal(r.TemplateVars, &n.TemplateVars); err != nil {
			return nil, fmt.Errorf("notification %s template_vars: %w", r.ID, err)
		}
	}
	return n, nil
}

func toNotifications(rows []sqlcgen.Notification) ([]*domain.Notification, error) {
	out := make([]*domain.Notification, len(rows))
	for i := range rows {
		n, err := toNotification(&rows[i])
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

func toAttempt(r *sqlcgen.NotificationAttempt) domain.Attempt {
	return domain.Attempt{
		ID:                r.ID,
		NotificationID:    r.NotificationID,
		AttemptNo:         int(r.AttemptNo),
		Outcome:           domain.AttemptOutcome(r.Outcome),
		StatusCode:        int(deref(r.StatusCode)),
		ErrorCode:         deref(r.ErrorCode),
		Error:             deref(r.Error),
		ProviderMessageID: deref(r.ProviderMessageID),
		Duration:          time.Duration(r.DurationMs) * time.Millisecond,
		CreatedAt:         r.CreatedAt.UTC(),
	}
}

func toBatch(r *sqlcgen.Batch) (domain.Batch, error) {
	b := domain.Batch{
		ID:            r.ID,
		Total:         int(r.Total),
		CorrelationID: deref(r.CorrelationID),
		CompletedAt:   utcPtr(r.CompletedAt),
		CreatedAt:     r.CreatedAt.UTC(),
	}
	md, err := unmarshalObject(r.Metadata)
	if err != nil {
		return domain.Batch{}, fmt.Errorf("batch %s metadata: %w", r.ID, err)
	}
	b.Metadata = md
	return b, nil
}

func toTemplate(r *sqlcgen.Template) (*domain.Template, error) {
	t := &domain.Template{
		ID:        r.ID,
		Name:      r.Name,
		Channel:   domain.Channel(r.Channel),
		Subject:   deref(r.Subject),
		Body:      r.Body,
		Version:   int(r.Version),
		DeletedAt: utcPtr(r.DeletedAt),
		CreatedAt: r.CreatedAt.UTC(),
		UpdatedAt: r.UpdatedAt.UTC(),
	}
	if err := json.Unmarshal(r.Variables, &t.Variables); err != nil {
		return nil, fmt.Errorf("template %s variables: %w", r.ID, err)
	}
	if t.Variables == nil {
		t.Variables = []string{}
	}
	return t, nil
}

// marshalObject encodes a map for a NOT NULL jsonb object column.
func marshalObject(m map[string]any) ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

func unmarshalObject(b []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// narrow converts an int to a smaller integer column type, failing instead
// of silently wrapping around: 70000 must not be stored as int16 4464.
// Validation keeps values far inside these ranges; this is the last guard.
func narrow[T int16 | int32](v int, field string) (T, error) {
	t := T(v)
	if int(t) != v {
		return 0, fmt.Errorf("%s: value %d out of range for column type", field, v)
	}
	return t, nil
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// nilIfZero turns a zero value into SQL NULL.
func nilIfZero[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
