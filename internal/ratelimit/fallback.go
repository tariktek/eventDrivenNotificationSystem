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
type Fallback struct {
	primary    Limiter
	local      Limiter
	log        *slog.Logger
	now        func() time.Time
	retryEvery time.Duration

	mu        sync.Mutex
	downUntil time.Time
	degraded  bool
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
	Now        func() time.Time
}

// NewFallback wraps a shared limiter with a local safety net.
func NewFallback(c FallbackConfig) *Fallback {
	if c.Now == nil {
		c.Now = time.Now
	}
	share := c.Rate
	share.Rate = max(c.Rate.Rate/max(c.Instances, 1), 1)
	return &Fallback{
		primary:    c.Primary,
		local:      NewLocal(share, c.Now),
		log:        c.Log,
		now:        c.Now,
		retryEvery: c.RetryEvery,
	}
}

// Reserve implements Limiter. It only fails if ctx is done.
func (f *Fallback) Reserve(ctx context.Context, key string, n int) (Reservation, error) {
	if !f.useLocal() {
		res, err := f.primary.Reserve(ctx, key, n)
		if err == nil {
			f.recovered()
			return res, nil
		}
		if ctx.Err() != nil {
			return Reservation{}, ctx.Err()
		}
		f.failed(ctx, err)
	}
	return f.local.Reserve(ctx, key, n)
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
