package callback

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
)

// finalStatuses are the statuses clients are called back about.
var finalStatuses = map[string]bool{"sent": true, "failed": true, "cancelled": true}

// Body is the JSON a client receives.
type Body struct {
	EventID    int64           `json:"event_id"`
	Type       string          `json:"type"` // notification.sent, notification.failed, notification.cancelled
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"` // the notification event payload
}

// Enqueuer turns final-status events into pending callback deliveries.
// It runs as a consumer-group handler: each event is handled by one
// worker, and a repeat (at-least-once delivery) is absorbed by the unique
// event id.
type Enqueuer struct {
	Store *postgres.Store
}

// Handle implements the events.Group handler.
func (q *Enqueuer) Handle(ctx context.Context, e events.Event) error {
	if e.Type != events.TypeNotificationStatusChanged {
		return nil
	}
	var p events.NotificationPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return nil //nolint:nilerr // undecodable: retrying cannot fix it
	}
	if !finalStatuses[p.Status] {
		return nil
	}
	body, err := json.Marshal(Body{EventID: e.ID, Type: "notification." + p.Status, OccurredAt: p.OccurredAt, Data: e.Payload})
	if err != nil {
		return fmt.Errorf("encode callback body: %w", err)
	}
	_, err = q.Store.EnqueueCallback(ctx, e.ID, p.NotificationID, "notification."+p.Status, body)
	return err
}

// DispatcherConfig tunes a Dispatcher.
type DispatcherConfig struct {
	Client      ClientConfig
	Secret      []byte
	Concurrency int
	MaxAttempts int
	Backoff     retry.Policy
	// Lease hides a claimed callback from other dispatchers; it must
	// exceed the client timeout.
	Lease        time.Duration
	PollInterval time.Duration
}

// Dispatcher sends due callbacks.
type Dispatcher struct {
	store  *postgres.Store
	cfg    DispatcherConfig
	client *http.Client
	log    *slog.Logger
	now    func() time.Time
}

// NewDispatcher returns a Dispatcher.
func NewDispatcher(store *postgres.Store, cfg DispatcherConfig, log *slog.Logger) *Dispatcher {
	return &Dispatcher{store: store, cfg: cfg, client: NewHTTPClient(cfg.Client), log: log, now: time.Now}
}

// Run sends callbacks until ctx is done.
func (d *Dispatcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := d.RunOnce(ctx)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				d.log.WarnContext(ctx, "callback dispatch round failed", "error", err)
			}
		case n == d.cfg.Concurrency:
			continue // more are due
		}
		sleep(ctx, d.cfg.PollInterval)
	}
}

// RunOnce claims up to Concurrency due callbacks, sends them in parallel
// and records the outcomes. It returns how many it claimed.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	due, err := d.store.ClaimCallbacks(ctx, d.now(), d.cfg.Lease, d.cfg.Concurrency)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, cb := range due {
		wg.Go(func() { d.deliver(ctx, cb) })
	}
	wg.Wait()
	return len(due), nil
}

func (d *Dispatcher) deliver(ctx context.Context, cb *postgres.CallbackDelivery) {
	log := d.log.With("notification_id", cb.NotificationID, "event_id", cb.EventID, "attempt", cb.Attempts)
	res := Post(ctx, d.client, d.cfg.Client, d.cfg.Secret,
		Request{URL: cb.URL, EventID: cb.EventID, EventType: cb.EventType, Body: cb.Payload}, d.now())

	now := d.now()
	out := &postgres.CallbackOutcome{StatusCode: res.StatusCode, Now: now}
	switch {
	case res.Err == nil:
		out.Status = postgres.CallbackDelivered
	case res.Retryable && cb.Attempts < d.cfg.MaxAttempts:
		out.Status = postgres.CallbackPending
		out.NextAttemptAt = now.Add(d.cfg.Backoff.Backoff(cb.Attempts))
		out.Error = truncate(res.Err.Error())
	default:
		out.Status = postgres.CallbackFailed
		out.Error = truncate(res.Err.Error())
	}
	// Record even if shutdown began: the request was already sent.
	applied, err := d.store.FinishCallback(context.WithoutCancel(ctx), cb, out)
	switch {
	case err != nil:
		log.ErrorContext(ctx, "record callback outcome; it will be retried after the lease", "error", err)
	case !applied:
		log.WarnContext(ctx, "callback lease lost before recording the outcome")
	case out.Status == postgres.CallbackDelivered:
		log.DebugContext(ctx, "callback delivered", "status_code", res.StatusCode)
	case out.Status == postgres.CallbackPending:
		log.InfoContext(ctx, "callback failed, will retry", "error", out.Error, "retry_at", out.NextAttemptAt)
	default:
		log.WarnContext(ctx, "callback failed permanently", "error", out.Error, "status_code", res.StatusCode)
	}
}

func truncate(s string) string {
	const maxLen = 500
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
