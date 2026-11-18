package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// Callback delivery statuses.
const (
	CallbackPending   = "pending"
	CallbackDelivered = "delivered"
	CallbackFailed    = "failed"
)

// CallbackDelivery is a status callback to send to a client's URL.
type CallbackDelivery struct {
	ID             int64
	NotificationID uuid.UUID
	EventID        int64
	EventType      string
	URL            string
	Payload        []byte // the exact request body
	Attempts       int    // including the current one; also the fencing token
	Status         string
	LastStatusCode int
	LastError      string
	DeliveredAt    *time.Time
}

// EnqueueCallback records a callback for the event if the notification
// has a callback_url. It reports whether a row was created (false also for
// an event already enqueued).
func (r *Repo) EnqueueCallback(ctx context.Context, eventID int64, notificationID uuid.UUID, eventType string, payload []byte) (bool, error) {
	n, err := r.q.EnqueueCallback(ctx, sqlcgen.EnqueueCallbackParams{
		EventID: eventID, EventType: eventType, Payload: payload, NotificationID: notificationID,
	})
	if err != nil {
		return false, fmt.Errorf("enqueue callback: %w", err)
	}
	return n == 1, nil
}

// ClaimCallbacks takes up to limit due callbacks with attempts left for
// lease and counts the attempt.
func (r *Repo) ClaimCallbacks(ctx context.Context, now time.Time, lease time.Duration, limit, maxAttempts int) ([]*CallbackDelivery, error) {
	maxRows, err := narrow[int32](limit, "limit")
	if err != nil {
		return nil, err
	}
	attempts, err := narrow[int32](maxAttempts, "max_attempts")
	if err != nil {
		return nil, err
	}
	rows, err := r.q.ClaimCallbacks(ctx, sqlcgen.ClaimCallbacksParams{LeaseUntil: now.Add(lease), Now: now, MaxAttempts: attempts, MaxRows: maxRows})
	if err != nil {
		return nil, fmt.Errorf("claim callbacks: %w", err)
	}
	out := make([]*CallbackDelivery, len(rows))
	for i, row := range rows {
		out[i] = &CallbackDelivery{
			ID: row.ID, NotificationID: row.NotificationID, EventID: row.EventID, EventType: row.EventType,
			URL: row.Url, Payload: row.Payload, Attempts: int(row.Attempts), Status: CallbackPending,
		}
	}
	return out, nil
}

// ExpireExhaustedCallbacks fails callbacks that used every attempt
// without a recorded outcome and whose lease has run out.
func (r *Repo) ExpireExhaustedCallbacks(ctx context.Context, now time.Time, maxAttempts int) (int64, error) {
	attempts, err := narrow[int32](maxAttempts, "max_attempts")
	if err != nil {
		return 0, err
	}
	n, err := r.q.ExpireExhaustedCallbacks(ctx, sqlcgen.ExpireExhaustedCallbacksParams{MaxAttempts: attempts, Now: now})
	if err != nil {
		return 0, fmt.Errorf("expire exhausted callbacks: %w", err)
	}
	return n, nil
}

// CallbackOutcome is the result of one delivery attempt.
type CallbackOutcome struct {
	Status        string // CallbackDelivered, CallbackPending (retry) or CallbackFailed
	StatusCode    int    // 0 if no response
	Error         string
	NextAttemptAt time.Time // for a retry
	Now           time.Time
}

// FinishCallback records an attempt's outcome. It reports false if the
// claim was lost (its lease expired and another dispatcher took it).
func (r *Repo) FinishCallback(ctx context.Context, d *CallbackDelivery, o *CallbackOutcome) (bool, error) {
	attempts, err := narrow[int32](d.Attempts, "attempts")
	if err != nil {
		return false, err
	}
	var code *int32
	if o.StatusCode != 0 {
		c, err := narrow[int32](o.StatusCode, "status_code")
		if err != nil {
			return false, err
		}
		code = &c
	}
	var n int64
	switch o.Status {
	case CallbackDelivered:
		n, err = r.q.MarkCallbackDelivered(ctx, sqlcgen.MarkCallbackDeliveredParams{Now: &o.Now, StatusCode: code, ID: d.ID, Attempts: attempts})
	case CallbackPending:
		n, err = r.q.MarkCallbackRetry(ctx, sqlcgen.MarkCallbackRetryParams{
			NextAttemptAt: o.NextAttemptAt, StatusCode: code, LastError: nilIfZero(o.Error), ID: d.ID, Attempts: attempts,
		})
	case CallbackFailed:
		n, err = r.q.MarkCallbackFailed(ctx, sqlcgen.MarkCallbackFailedParams{StatusCode: code, LastError: nilIfZero(o.Error), ID: d.ID, Attempts: attempts})
	default:
		return false, fmt.Errorf("finish callback: unexpected status %q", o.Status)
	}
	if err != nil {
		return false, fmt.Errorf("finish callback: %w", err)
	}
	return n == 1, nil
}

// ListCallbackDeliveries returns a notification's callbacks, oldest first.
func (r *Repo) ListCallbackDeliveries(ctx context.Context, notificationID uuid.UUID) ([]*CallbackDelivery, error) {
	rows, err := r.q.ListCallbackDeliveries(ctx, notificationID)
	if err != nil {
		return nil, fmt.Errorf("list callback deliveries: %w", err)
	}
	out := make([]*CallbackDelivery, len(rows))
	for i := range rows {
		row := &rows[i]
		out[i] = &CallbackDelivery{
			ID: row.ID, NotificationID: row.NotificationID, EventID: row.EventID, EventType: row.EventType,
			URL: row.Url, Payload: row.Payload, Attempts: int(row.Attempts), Status: row.Status,
			LastStatusCode: int(deref(row.LastStatusCode)), LastError: deref(row.LastError), DeliveredAt: row.DeliveredAt,
		}
	}
	return out, nil
}
