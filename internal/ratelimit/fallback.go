package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Fallback uses the shared limiter and, when it fails (Redis down), a local
// one with a proportional share of the rate: Rate / expected instances.
// That is deliberately conservative: sending slower during an outage beats
// exceeding the provider's limit. After a failure, the shared limiter is
// retried only every RetryEvery, so an outage does not add a connection
// timeout to every reservation.
//
// Each call to the shared limiter is bounded by Timeout. An outage rarely
// fails fast: with Redis stopped, the client retried its dials for 79 s
// before returning an error, and the channel sent nothing meanwhile.
//
// The two limiters keep separate timelines, and slots already handed out
// on one are still to be used when the other takes over. Granting slots on
// the new timeline at once would let both sets of sends overlap and exceed
// the rate. So when a key switches timelines, Fallback first lets the
// slots it reserved on the previous one run out (it returns no slots and
// says how long to wait), then reserves on the new one.
type Fallback struct {
	primary    Limiter
	local      Limiter
	log        *slog.Logger
	now        func() time.Time
	retryEvery time.Duration
	timeout    time.Duration

	mu        sync.Mutex
	downUntil time.Time
	degraded  bool
	// reserved tracks, per key, which timeline its latest slots are on and
	// when the last of them has passed.
	reserved map[string]timeline
}

type timeline struct {
	local bool
	until time.Time
}

// FallbackConfig configures NewFallback.
type FallbackConfig struct {
	Primary Limiter
	// Rate is the full per-key rate; the local share is Rate / Instances.
	Rate      Config
	Instances int
	Log       *slog.Logger
	// RetryEvery is how long to stay on the local limiter after a failure.
	RetryEvery time.Duration
	// Timeout bounds each call to the shared limiter (default
	// DefaultFallbackTimeout); a slower answer counts as a failure.
	Timeout time.Duration
	Now     func() time.Time
}

// DefaultFallbackTimeout is far above a healthy Redis round trip (about a
// millisecond) and short enough that an outage barely pauses deliveries.
const DefaultFallbackTimeout = 250 * time.Millisecond

// NewFallback wraps a shared limiter with a local safety net.
func NewFallback(c FallbackConfig) *Fallback {
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultFallbackTimeout
	}
	return &Fallback{
		primary:    c.Primary,
		local:      NewLocal(LocalShare(c.Rate, c.Instances), c.Now),
		log:        c.Log,
		now:        c.Now,
		retryEvery: c.RetryEvery,
		timeout:    c.Timeout,
		reserved:   map[string]timeline{},
	}
}

// LocalShare is the rate each of instances gets when they cannot share a
// timeline: Rate / instances (at least 1/s). Its horizon is widened to fit
// at least one of its slots: the horizon is sized for the full rate (10 ms
// fits one slot at 100/s), and at 50/s a 20 ms slot would never fit, so the
// fallback would grant nothing and the channel would stop.
func LocalShare(full Config, instances int) Config {
	share := full
	share.Rate = max(full.Rate/max(instances, 1), 1)
	share.Horizon = max(full.Horizon, share.interval())
	return share
}

// Reserve implements Limiter. It only fails if ctx is done.
func (f *Fallback) Reserve(ctx context.Context, key string, n int) (Reservation, error) {
	if !f.useLocal() {
		if wait := f.drain(key, false); wait > 0 {
			return Reservation{First: wait}, nil
		}
		pctx, cancel := context.WithTimeout(ctx, f.timeout)
		res, err := f.primary.Reserve(pctx, key, n)
		cancel()
		if err == nil {
			f.recovered()
			f.note(key, false, res)
			return res, nil
		}
		if ctx.Err() != nil {
			return Reservation{}, ctx.Err()
		}
		f.failed(ctx, err)
	}
	if wait := f.drain(key, true); wait > 0 {
		return Reservation{First: wait}, nil
	}
	res, err := f.local.Reserve(ctx, key, n)
	if err == nil {
		f.note(key, true, res)
	}
	return res, err
}

// drain returns how long to wait before reserving on the local (or shared)
// timeline, because slots this process got on the other one have not all
// passed yet.
func (f *Fallback) drain(key string, local bool) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.reserved[key]
	if !ok || t.local == local {
		return 0
	}
	return max(t.until.Sub(f.now()), 0)
}

// note records slots just reserved on a timeline.
func (f *Fallback) note(key string, local bool, res Reservation) {
	if res.Count == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	until := f.now().Add(res.Delay(res.Count)) // one interval past the last slot
	if t, ok := f.reserved[key]; ok && t.local == local && t.until.After(until) {
		until = t.until
	}
	f.reserved[key] = timeline{local: local, until: until}
}

// Degraded reports whether the local fallback is in use.
func (f *Fallback) Degraded() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.degraded
}

func (f *Fallback) useLocal() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now().Before(f.downUntil)
}

func (f *Fallback) failed(ctx context.Context, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downUntil = f.now().Add(f.retryEvery)
	if !f.degraded {
		f.degraded = true
		f.log.WarnContext(ctx, "shared rate limiter unavailable, using local fallback", "error", err)
	}
}

func (f *Fallback) recovered() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.degraded {
		f.degraded = false
		f.log.Info("shared rate limiter recovered")
	}
}
