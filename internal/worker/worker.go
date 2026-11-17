// Package worker delivers queued notifications.
//
// Each channel has its own dispatcher, so a slow or rate-limited channel
// never holds up the others. A dispatcher loop:
//
//  1. waits for free delivery capacity (Concurrency per channel);
//  2. reserves send slots from the rate limiter (≤ Rate per second, shared
//     by every worker instance);
//  3. claims that many due notifications, highest effective priority first;
//  4. hands each one a slot: a goroutine waits for the slot time, calls the
//     provider, and records the outcome (sent / retry later / failed).
//
// Around the dispatchers: a heartbeat keeps the leases of in-flight
// deliveries alive, a reaper recovers work from crashed workers, and an
// optional listener wakes a dispatcher as soon as new work arrives.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/queue"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
)

// Config tunes a Worker.
type Config struct {
	ID       string
	Channels []domain.Channel
	// Concurrency is the maximum in-flight deliveries per channel,
	// including ones waiting for their send slot.
	Concurrency int
	// BatchSize caps how many notifications one claim takes.
	BatchSize int
	// PollInterval is how often an idle dispatcher checks for due work
	// (scheduled sends, retries) when no wake-up signal arrives.
	PollInterval time.Duration
	Lease        time.Duration
	ReapInterval time.Duration
	Aging        time.Duration
	// ShutdownGrace is how long in-flight provider calls may finish after
	// shutdown begins. Deliveries still waiting for a slot are released.
	ShutdownGrace time.Duration
}

// Notifier signals when work becomes due on a channel (e.g. PostgreSQL
// LISTEN/NOTIFY). It only reduces latency; polling remains the fallback.
type Notifier interface {
	ListenDue(ctx context.Context, onDue func(domain.Channel), onErr func(error))
}

// Deps are the Worker's collaborators.
type Deps struct {
	Queue    queue.Queue
	Limiter  ratelimit.Limiter
	Sender   provider.Sender
	Policy   retry.Policy
	Notifier Notifier // optional
	Log      *slog.Logger
	Now      func() time.Time
}

// Worker runs the delivery pipeline.
type Worker struct {
	cfg Config
	Deps
	wake map[domain.Channel]chan struct{}

	mu       sync.Mutex
	inflight map[uuid.UUID]struct{}
}

// New builds a Worker.
func New(cfg Config, d Deps) *Worker {
	if d.Now == nil {
		d.Now = time.Now
	}
	w := &Worker{cfg: cfg, Deps: d, wake: map[domain.Channel]chan struct{}{}, inflight: map[uuid.UUID]struct{}{}}
	for _, ch := range cfg.Channels {
		w.wake[ch] = make(chan struct{}, 1)
	}
	return w
}

// Run delivers until ctx is cancelled, then shuts down gracefully: stops
// claiming, releases deliveries that have not started, lets started ones
// finish within ShutdownGrace, and returns when nothing is in flight.
func (w *Worker) Run(ctx context.Context) error {
	// Provider calls and result recording outlive ctx by up to ShutdownGrace,
	// so a delivery that already reached the provider still gets recorded.
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()
	stopGrace := context.AfterFunc(ctx, func() { time.AfterFunc(w.cfg.ShutdownGrace, cancelSends) })
	defer stopGrace()

	w.Log.InfoContext(ctx, "worker started", "worker_id", w.cfg.ID, "channels", w.cfg.Channels,
		"concurrency", w.cfg.Concurrency, "batch_size", w.cfg.BatchSize)

	var dispatchers, background sync.WaitGroup
	for _, ch := range w.cfg.Channels {
		dispatchers.Go(func() { w.dispatch(ctx, sendCtx, ch) })
	}
	bgCtx, stopBackground := context.WithCancel(sendCtx)
	background.Go(func() { w.heartbeat(bgCtx) })
	background.Go(func() { w.reap(ctx) })
	if w.Notifier != nil {
		background.Go(func() {
			w.Notifier.ListenDue(ctx, w.signal, func(err error) {
				w.Log.WarnContext(ctx, "work notifications interrupted, relying on polling", "error", err)
			})
		})
	}

	dispatchers.Wait() // returns once every delivery has finished or been released
	stopBackground()
	background.Wait()
	w.Log.Info("worker stopped", "worker_id", w.cfg.ID)
	return nil
}

func (w *Worker) signal(ch domain.Channel) {
	if c, ok := w.wake[ch]; ok {
		select {
		case c <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
}

// dispatch runs one channel's claim loop.
func (w *Worker) dispatch(ctx, sendCtx context.Context, ch domain.Channel) {
	var (
		deliveries sync.WaitGroup
		slots      = make(chan struct{}, w.cfg.Concurrency) // in-flight deliveries
		freed      = make(chan struct{}, 1)
	)
	defer deliveries.Wait()

	for ctx.Err() == nil {
		free := w.cfg.Concurrency - len(slots)
		if free == 0 {
			waitAny(ctx, freed, nil, w.cfg.PollInterval)
			continue
		}

		res, err := w.Limiter.Reserve(ctx, string(ch), min(free, w.cfg.BatchSize))
		if err != nil {
			if ctx.Err() == nil {
				w.Log.ErrorContext(ctx, "reserve send slots", "channel", ch, "error", err)
				sleep(ctx, w.cfg.PollInterval)
			}
			continue
		}
		if res.Count == 0 { // this second's slots are taken; wait for the next one
			sleep(ctx, res.First)
			continue
		}
		base := w.Now()

		// The claim runs on sendCtx, not ctx: if shutdown cancelled the query
		// mid-flight, PostgreSQL could still commit it while we discard the
		// result, leaving rows in "processing" that nobody would release
		// (until the reaper, a lease later). Letting it finish means every
		// claimed row reaches a delivery goroutine, which releases it at
		// once if shutdown has begun.
		ns, err := w.Queue.Claim(sendCtx, queue.ClaimRequest{
			Channel: ch, WorkerID: w.cfg.ID, Limit: res.Count, Now: base, Lease: w.cfg.Lease, Aging: w.cfg.Aging,
		})
		if err != nil {
			if ctx.Err() == nil {
				w.Log.ErrorContext(ctx, "claim notifications", "channel", ch, "error", err)
				sleep(ctx, w.cfg.PollInterval)
			}
			continue
		}
		if len(ns) == 0 {
			// Nothing due. The unused slots simply lapse: the limiter only
			// matters when there is a backlog, and then claims are full.
			waitAny(ctx, w.wake[ch], nil, w.cfg.PollInterval)
			continue
		}

		for i, n := range ns {
			slots <- struct{}{}
			w.track(n.ID, true)
			at := base.Add(res.Delay(i))
			deliveries.Go(func() {
				defer func() {
					w.track(n.ID, false)
					<-slots
					select {
					case freed <- struct{}{}:
					default:
					}
				}()
				w.deliver(ctx, sendCtx, n, at)
			})
		}
	}
}

// deliver waits for the notification's send slot, sends it and records the
// outcome.
func (w *Worker) deliver(ctx, sendCtx context.Context, n *domain.Notification, at time.Time) {
	logCtx := observability.WithCorrelationID(sendCtx, n.CorrelationID)
	log := w.Log.With("notification_id", n.ID, "channel", n.Channel, "attempt", n.Attempts)

	if !sleep(ctx, time.Until(at)) {
		// Shutdown before this delivery started: give it back untouched.
		if _, err := w.Queue.Release(logCtx, w.cfg.ID, []uuid.UUID{n.ID}); err != nil {
			log.ErrorContext(logCtx, "release unsent notification", "error", err)
		}
		return
	}

	res := w.Sender.Send(sendCtx, &provider.Message{
		NotificationID: n.ID,
		Channel:        n.Channel,
		To:             n.Recipient,
		Subject:        n.Subject,
		Content:        n.Content,
		CorrelationID:  n.CorrelationID,
	})
	now := w.Now()
	verdict := w.Policy.Decide(n, &res, now)

	applied, err := w.Queue.Complete(logCtx, &queue.Completion{
		NotificationID: n.ID,
		WorkerID:       w.cfg.ID,
		Now:            now,
		Status:         verdict.Status,
		NextAttemptAt:  verdict.NextAttemptAt,
		Attempt: domain.Attempt{
			NotificationID:    n.ID,
			AttemptNo:         n.Attempts,
			Outcome:           res.Outcome,
			StatusCode:        res.StatusCode,
			ErrorCode:         res.ErrorCode,
			Error:             res.Error,
			ProviderMessageID: res.MessageID,
			Duration:          res.Duration,
		},
	})
	switch {
	case err != nil:
		// The lease will expire and the reaper will requeue it: the message
		// may be sent again (at-least-once), never lost.
		log.ErrorContext(logCtx, "record delivery outcome", "error", err, "outcome", res.Outcome)
	case !applied:
		log.WarnContext(logCtx, "lease lost before the outcome was recorded", "outcome", res.Outcome)
	case verdict.Status == domain.StatusSent:
		log.DebugContext(logCtx, "notification sent", "provider_message_id", res.MessageID, "duration_ms", res.Duration.Milliseconds())
	case verdict.Status == domain.StatusPending:
		log.InfoContext(logCtx, "delivery failed, will retry",
			"error_code", res.ErrorCode, "retry_at", verdict.NextAttemptAt)
	default:
		log.WarnContext(logCtx, "notification failed",
			"error_code", res.ErrorCode, "error", res.Error, "outcome", res.Outcome)
	}
}

// heartbeat extends the leases of in-flight deliveries so that a slow
// provider call is not mistaken for a dead worker.
func (w *Worker) heartbeat(ctx context.Context) {
	t := time.NewTicker(w.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ids := w.inflightIDs()
		if len(ids) == 0 {
			continue
		}
		if _, err := w.Queue.ExtendLeases(ctx, w.cfg.ID, ids, w.Now().Add(w.cfg.Lease)); err != nil && ctx.Err() == nil {
			w.Log.ErrorContext(ctx, "extend leases", "error", err, "count", len(ids))
		}
	}
}

// reap periodically recovers notifications whose worker stopped
// heartbeating. Every worker runs it; the UPDATE is idempotent.
func (w *Worker) reap(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		res, err := w.Queue.ReapExpiredLeases(ctx, w.Now())
		switch {
		case err != nil && ctx.Err() == nil:
			w.Log.ErrorContext(ctx, "reap expired leases", "error", err)
		case res.Requeued > 0 || res.Failed > 0:
			w.Log.WarnContext(ctx, "recovered notifications from expired leases",
				"requeued", res.Requeued, "failed_no_attempts_left", res.Failed)
		}
	}
}

func (w *Worker) track(id uuid.UUID, on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if on {
		w.inflight[id] = struct{}{}
	} else {
		delete(w.inflight, id)
	}
}

func (w *Worker) inflightIDs() []uuid.UUID {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]uuid.UUID, 0, len(w.inflight))
	for id := range w.inflight {
		ids = append(ids, id)
	}
	return ids
}

// sleep waits for d or until ctx is done; it reports whether the full
// duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// waitAny waits for a signal on a or b, the timeout, or ctx.
func waitAny(ctx context.Context, a, b <-chan struct{}, timeout time.Duration) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-a:
	case <-b:
	case <-t.C:
	}
}
