package callback

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
//
// It keeps up to Concurrency POSTs in flight and claims more as soon as
// one finishes, so a receiver that hangs until the timeout occupies one
// slot, not the whole round. At shutdown it stops claiming and lets the
// POSTs already in flight finish (each is bounded by the client timeout,
// which is shorter than the shutdown grace): cutting them off would burn
// an attempt, or fail a callback on its last one, though nothing is wrong
// with the receiver.
func (d *Dispatcher) Run(ctx context.Context) {
	var (
		inflight sync.WaitGroup
		slots    = make(chan struct{}, d.cfg.Concurrency)
		freed    = make(chan struct{}, 1)
	)
	defer inflight.Wait()
	sendCtx := context.WithoutCancel(ctx)

	for ctx.Err() == nil {
		free := d.cfg.Concurrency - len(slots)
		if free == 0 {
			wait(ctx, freed, d.cfg.PollInterval)
			continue
		}
		now := d.now()
		if n, err := d.store.ExpireExhaustedCallbacks(ctx, now, d.cfg.MaxAttempts); err != nil {
			d.fail(ctx, "expire exhausted callbacks", err)
			continue
		} else if n > 0 {
			d.log.WarnContext(ctx, "callbacks failed: attempts exhausted without a recorded outcome", "count", n)
		}
		due, err := d.store.ClaimCallbacks(ctx, now, d.cfg.Lease, free, d.cfg.MaxAttempts)
		if err != nil {
			d.fail(ctx, "claim callbacks", err)
			continue
		}
		for _, cb := range due {
			slots <- struct{}{}
			inflight.Go(func() {
				defer func() {
					<-slots
					select {
					case freed <- struct{}{}:
					default:
					}
				}()
				d.deliver(sendCtx, cb)
			})
		}
		if len(due) < free { // nothing more due right now
			wait(ctx, nil, d.cfg.PollInterval)
		}
	}
}

func (d *Dispatcher) fail(ctx context.Context, what string, err error) {
	if ctx.Err() == nil {
		d.log.WarnContext(ctx, what, "error", err)
		wait(ctx, nil, d.cfg.PollInterval)
	}
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
	applied, err := d.store.FinishCallback(ctx, cb, out)
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

// truncate makes an error message safe to store: at most 500 bytes, cut
// on a character boundary, valid UTF-8 and free of NUL. PostgreSQL rejects
// invalid UTF-8 and NUL in text; a rejected write would leave the
// callback without a recorded outcome.
func truncate(s string) string {
	const maxLen = 500
	s = strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "\uFFFD")
	if len(s) <= maxLen {
		return s
	}
	cut := maxLen
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// wait returns after d, a signal on c (if not nil), or ctx is done.
func wait(ctx context.Context, c <-chan struct{}, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-c:
	case <-t.C:
	}
}
