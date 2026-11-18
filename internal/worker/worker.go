// Package worker delivers queued notifications.
//
// Each channel has its own dispatcher and circuit breaker, so a slow,
// rate-limited or failing channel never holds up the others. A dispatcher
// loop:
//
//  1. waits for free delivery capacity (Concurrency per channel);
//  2. asks the channel's circuit breaker how much it may send: everything
//     when closed, nothing when open, a few probes when half-open;
//  3. reserves send slots from the rate limiter (≤ Rate per second, shared
//     by every worker instance);
//  4. claims that many due notifications, highest effective priority first;
//  5. hands each one a slot: a goroutine waits for the slot time, calls the
//     provider, and records the outcome (sent / retry later / failed).
//
// While a breaker is open its channel's notifications stay queued and keep
// their attempt budget: a provider outage delays them instead of failing
// them.
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/breaker"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
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
	// Breaker configures each channel's circuit breaker.
	Breaker breaker.Config
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
	Notifier Notifier         // optional
	Metrics  *metrics.Metrics // optional
	Log      *slog.Logger
	Now      func() time.Time
}

// Worker runs the delivery pipeline.
type Worker struct {
	cfg Config
	Deps
	wake     map[domain.Channel]chan struct{}
	breakers map[domain.Channel]*breaker.Breaker

	mu       sync.Mutex
	inflight map[uuid.UUID]struct{}
}

// New builds a Worker.
func New(cfg Config, d Deps) *Worker {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Metrics == nil {
		d.Metrics = metrics.New() // a private registry nobody scrapes: no nil checks below
	}
	if cfg.Breaker == (breaker.Config{}) {
		cfg.Breaker = breaker.DefaultConfig()
	}
	w := &Worker{
		cfg: cfg, Deps: d,
		wake:     map[domain.Channel]chan struct{}{},
		breakers: map[domain.Channel]*breaker.Breaker{},
		inflight: map[uuid.UUID]struct{}{},
	}
	for _, ch := range cfg.Channels {
		w.wake[ch] = make(chan struct{}, 1)
		w.Metrics.BreakerState.WithLabelValues(string(ch)).Set(0)
		w.breakers[ch] = breaker.New(string(ch), cfg.Breaker, func(from, to breaker.State) {
			w.Log.Warn("circuit breaker state changed", "channel", ch, "from", from, "to", to)
			w.Metrics.BreakerState.WithLabelValues(string(ch)).Set(breakerValue(to))
		})
	}
	return w
}

// BreakerStates reports each channel's circuit breaker state.
func (w *Worker) BreakerStates() map[domain.Channel]breaker.State {
	out := make(map[domain.Channel]breaker.State, len(w.breakers))
	for ch, b := range w.breakers {
		out[ch] = b.State()
	}
	return out
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
		cb         = w.breakers[ch]
	)
	defer deliveries.Wait()

	for ctx.Err() == nil {
		free := w.cfg.Concurrency - len(slots)
		if free == 0 {
			waitAny(ctx, freed, nil, w.cfg.PollInterval)
			continue
		}
		want := cb.Capacity(min(free, w.cfg.BatchSize))
		if want == 0 {
			// Breaker open (or all half-open probes out): claim nothing, so
			// queued notifications keep their attempts. Re-check when a
			// probe finishes or after a poll interval; the breaker moves to
			// half-open on its own once its timeout passes.
			waitAny(ctx, freed, nil, w.cfg.PollInterval)
			continue
		}

		res, err := w.Limiter.Reserve(ctx, string(ch), want)
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

		var rejected []uuid.UUID
		for i, n := range ns {
			ticket := cb.Admit()
			if ticket == nil { // the breaker opened since Capacity
				rejected = append(rejected, n.ID)
				continue
			}
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
				w.deliver(ctx, sendCtx, n, at, ticket)
			})
		}
		w.release(sendCtx, rejected)
	}
}

// release hands claimed-but-unsent notifications back to the queue with
// their attempt refunded.
func (w *Worker) release(ctx context.Context, ids []uuid.UUID) {
	if len(ids) == 0 {
		return
	}
	if _, err := w.Queue.Release(ctx, w.cfg.ID, ids); err != nil {
		// The reaper requeues them once their lease expires.
		w.Log.ErrorContext(ctx, "release unsent notifications", "error", err, "count", len(ids))
	}
}

// deliver waits for the notification's send slot, sends it and records the
// outcome.
func (w *Worker) deliver(ctx, sendCtx context.Context, n *domain.Notification, at time.Time, ticket *breaker.Ticket) {
	logCtx := observability.WithCorrelationID(sendCtx, n.CorrelationID)
	log := w.Log.With("notification_id", n.ID, "channel", n.Channel, "attempt", n.Attempts)

	// Not sent yet when shutdown begins or the breaker opens while this
	// delivery waited for its slot: give it back untouched.
	w.Metrics.RateLimitWait.WithLabelValues(string(n.Channel)).Observe(max(0, time.Until(at)).Seconds())
	if !sleep(ctx, time.Until(at)) || ticket.Open() {
		ticket.Skip()
		w.release(logCtx, []uuid.UUID{n.ID})
		return
	}

	// One trace per delivery attempt. It links to the API request that
	// created the notification rather than joining its trace: the delivery
	// happens later, maybe many times, as separate work.
	logCtx, span := tracer.Start(logCtx, "deliver notification",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(observability.LinkFrom(logCtx, n.TraceParent)...),
		trace.WithAttributes(
			attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.channel", string(n.Channel)),
			attribute.String("notification.priority", string(n.Priority)),
			attribute.Int("notification.attempt", n.Attempts),
			attribute.String("correlation_id", n.CorrelationID),
		))
	defer span.End()

	res := w.Sender.Send(logCtx, &provider.Message{
		NotificationID: n.ID,
		Channel:        n.Channel,
		To:             n.Recipient,
		Subject:        n.Subject,
		Content:        n.Content,
		CorrelationID:  n.CorrelationID,
	})
	w.Metrics.ProviderDuration.WithLabelValues(string(n.Channel), metrics.StatusClass(res.StatusCode)).Observe(res.Duration.Seconds())
	switch {
	case sendCtx.Err() != nil:
		ticket.Skip() // cut off by shutdown; says nothing about the provider
	case res.Outcome == domain.OutcomeTransientError:
		ticket.Failure()
	default:
		// A permanent error (bad recipient, rejected content) is about the
		// message, not the provider's health.
		ticket.Success()
	}
	now := w.Now()
	verdict := w.Policy.Decide(n, &res, now)
	span.SetAttributes(
		attribute.String("delivery.outcome", string(res.Outcome)),
		attribute.String("delivery.next_status", string(verdict.Status)),
		attribute.Int("http.response.status_code", res.StatusCode),
	)
	if res.Outcome != domain.OutcomeSuccess {
		span.SetStatus(codes.Error, res.ErrorCode)
	}

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
		start := n.CreatedAt
		if n.ScheduledAt != nil {
			start = *n.ScheduledAt
		}
		w.Metrics.DeliveryLatency.WithLabelValues(string(n.Channel), string(n.Priority)).Observe(now.Sub(start).Seconds())
		log.DebugContext(logCtx, "notification sent", "provider_message_id", res.MessageID, "duration_ms", res.Duration.Milliseconds())
	case verdict.Status == domain.StatusPending:
		w.Metrics.Retries.WithLabelValues(string(n.Channel)).Inc()
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
			w.Metrics.LeaseExpired.Add(float64(res.Requeued + res.Failed))
			w.Log.WarnContext(ctx, "recovered notifications from expired leases",
				"requeued", res.Requeued, "failed_no_attempts_left", res.Failed)
		}
	}
}

var tracer = otel.Tracer("github.com/tariktek/eventDrivenNotificationSystem/internal/worker")

func breakerValue(s breaker.State) float64 {
	switch s {
	case breaker.StateHalfOpen:
		return 1
	case breaker.StateOpen:
		return 2
	default:
		return 0
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
