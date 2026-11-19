// Package stats samples system-wide delivery statistics from the database:
// queue depth, recent success/failure counts, latency percentiles and
// outbox lag.
//
// The figures come from PostgreSQL, not from in-process counters, so every
// API instance reports the same system-wide numbers whichever worker did
// the work. Sampling runs in the background every few seconds and the
// result is cached: GET /api/v1/metrics never queries the database, so
// polling it cannot load the system.
package stats

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
)

// Snapshot is the JSON summary served by GET /api/v1/metrics.
type Snapshot struct {
	GeneratedAt time.Time               `json:"generated_at"`
	Queue       map[string]QueueStats   `json:"queue"` // by channel
	Deliveries  map[string]WindowStats  `json:"deliveries"`
	Latency     map[string]LatencyStats `json:"latency"` // by channel, plus "all"
	Outbox      OutboxStats             `json:"outbox"`
	Callbacks   CallbackStats           `json:"callbacks"`
}

// QueueStats is one channel's backlog.
type QueueStats struct {
	Pending                 int64            `json:"pending"`
	Scheduled               int64            `json:"scheduled"`
	Processing              int64            `json:"processing"`
	PendingByPriority       map[string]int64 `json:"pending_by_priority"`
	OldestPendingAgeSeconds float64          `json:"oldest_pending_age_seconds"`
}

// WindowStats are deliveries finished in recent windows ("1m", "5m",
// "15m"), keyed by window, for one channel or "all".
type WindowStats map[string]Window

// Window is one time window's outcome.
type Window struct {
	Sent        int64   `json:"sent"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"success_rate"` // sent / (sent + failed); 1 when idle
	PerSecond   float64 `json:"per_second"`   // sent per second
}

// LatencyStats are delivery latency percentiles over the last 15 minutes.
type LatencyStats struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50_seconds"`
	P95   float64 `json:"p95_seconds"`
	P99   float64 `json:"p99_seconds"`
}

// OutboxStats describe events waiting to be published.
type OutboxStats struct {
	Backlog    int64   `json:"backlog"`
	LagSeconds float64 `json:"lag_seconds"`
}

// CallbackStats describe status callbacks waiting to be delivered.
type CallbackStats struct {
	Pending int64 `json:"pending"`
}

type channelTally struct{ sent, failed [3]int64 }

var windows = [3]struct {
	name string
	d    time.Duration
}{{"1m", time.Minute}, {"5m", 5 * time.Minute}, {"15m", 15 * time.Minute}}

// Sampler keeps a fresh Snapshot.
type Sampler struct {
	store    *postgres.Store
	metrics  *metrics.Metrics // optional: gauges to update
	interval time.Duration
	log      *slog.Logger
	now      func() time.Time
	last     atomic.Pointer[Snapshot]
}

// New returns a Sampler. m may be nil.
func New(store *postgres.Store, m *metrics.Metrics, interval time.Duration, log *slog.Logger) *Sampler {
	return &Sampler{store: store, metrics: m, interval: interval, log: log, now: time.Now}
}

// Run samples every interval until ctx is done.
func (s *Sampler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		if _, err := s.Sample(ctx); err != nil && ctx.Err() == nil {
			s.log.WarnContext(ctx, "sample delivery statistics", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Snapshot returns the latest sample, taking one if none is recent enough
// (before the first background sample, or when Run is not running).
func (s *Sampler) Snapshot(ctx context.Context) (*Snapshot, error) {
	if snap := s.last.Load(); snap != nil && s.now().Sub(snap.GeneratedAt) < 2*s.interval {
		return snap, nil
	}
	return s.Sample(ctx)
}

// Sample queries the database, stores and returns a new Snapshot.
func (s *Sampler) Sample(ctx context.Context) (*Snapshot, error) {
	now := s.now().UTC()
	snap := &Snapshot{
		GeneratedAt: now,
		Queue:       map[string]QueueStats{},
		Deliveries:  map[string]WindowStats{},
		Latency:     map[string]LatencyStats{},
	}
	for _, ch := range domain.Channels {
		snap.Queue[string(ch)] = QueueStats{PendingByPriority: map[string]int64{"high": 0, "normal": 0, "low": 0}}
	}

	depth, err := s.store.QueueDepth(ctx, now)
	if err != nil {
		return nil, err
	}
	if s.metrics != nil {
		s.metrics.QueueDepth.Reset() // drop series that emptied
	}
	for _, r := range depth {
		q := snap.Queue[r.Channel]
		prio := priorityName(r.Priority)
		switch r.Status {
		case "pending":
			q.Pending += r.Total
			q.PendingByPriority[prio] += r.Total
			q.OldestPendingAgeSeconds = max(q.OldestPendingAgeSeconds, now.Sub(r.OldestDue).Seconds())
		case "scheduled":
			q.Scheduled += r.Total
			// A scheduled notification whose time has come waits like a
			// pending one: it stays "scheduled" until a worker claims it.
			q.OldestPendingAgeSeconds = max(q.OldestPendingAgeSeconds, now.Sub(r.OldestDue).Seconds())
		case "processing":
			q.Processing += r.Total
		}
		snap.Queue[r.Channel] = q
		if s.metrics != nil {
			s.metrics.QueueDepth.WithLabelValues(r.Channel, prio, r.Status).Set(float64(r.Total))
		}
	}
	if s.metrics != nil {
		for ch, q := range snap.Queue {
			s.metrics.QueueOldestAge.WithLabelValues(ch).Set(q.OldestPendingAgeSeconds)
		}
	}

	if err := s.deliveries(ctx, snap, now); err != nil {
		return nil, err
	}
	lat, err := s.store.DeliveryLatency(ctx, now)
	if err != nil {
		return nil, err
	}
	for _, r := range lat {
		snap.Latency[r.Channel] = LatencyStats{Count: r.Total, P50: r.P50, P95: r.P95, P99: r.P99}
	}

	backlog, oldest, err := s.store.OutboxBacklog(ctx)
	if err != nil {
		return nil, err
	}
	snap.Outbox = OutboxStats{Backlog: backlog, LagSeconds: max(0, now.Sub(oldest).Seconds())}
	if s.metrics != nil {
		s.metrics.OutboxBacklog.Set(float64(backlog))
		s.metrics.OutboxLag.Set(snap.Outbox.LagSeconds)
	}
	if snap.Callbacks.Pending, err = s.store.PendingCallbacks(ctx); err != nil {
		return nil, err
	}

	s.last.Store(snap)
	return snap, nil
}

func (s *Sampler) deliveries(ctx context.Context, snap *Snapshot, now time.Time) error {
	rows, err := s.store.DeliveryWindows(ctx, now)
	if err != nil {
		return err
	}
	all, byChannel := &channelTally{}, map[string]*channelTally{}
	for _, r := range rows {
		t := &channelTally{
			sent:   [3]int64{r.Sent1m, r.Sent5m, r.Sent15m},
			failed: [3]int64{r.Failed1m, r.Failed5m, r.Failed15m},
		}
		byChannel[r.Channel] = t
		for i := range windows {
			all.sent[i] += t.sent[i]
			all.failed[i] += t.failed[i]
		}
	}
	for _, ch := range domain.Channels {
		t := byChannel[string(ch)]
		if t == nil {
			t = &channelTally{}
		}
		snap.Deliveries[string(ch)] = t.windows()
	}
	snap.Deliveries["all"] = all.windows()
	return nil
}

func (t *channelTally) windows() WindowStats {
	out := WindowStats{}
	for i, w := range windows {
		rate := 1.0
		if done := t.sent[i] + t.failed[i]; done > 0 {
			rate = float64(t.sent[i]) / float64(done)
		}
		out[w.name] = Window{
			Sent: t.sent[i], Failed: t.failed[i], SuccessRate: round(rate),
			PerSecond: round(float64(t.sent[i]) / w.d.Seconds()),
		}
	}
	return out
}

func priorityName(p int16) string {
	switch p {
	case 0:
		return "high"
	case 1:
		return "normal"
	case 2:
		return "low"
	default:
		return strconv.Itoa(int(p))
	}
}

func round(f float64) float64 {
	const p = 10000
	return float64(int64(f*p+0.5)) / p
}
