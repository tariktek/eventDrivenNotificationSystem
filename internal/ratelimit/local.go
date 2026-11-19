package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Local is an in-process Limiter: the reference implementation, used in
// tests. It is not a fallback for Redis: a limit shared by several workers
// cannot be enforced from one process (see Guard).
type Local struct {
	cfg Config
	now func() time.Time

	mu  sync.Mutex
	tat map[string]time.Time
}

// NewLocal returns an in-process limiter. now may be nil (time.Now).
func NewLocal(cfg Config, now func() time.Time) *Local {
	if now == nil {
		now = time.Now
	}
	return &Local{cfg: cfg, now: now, tat: map[string]time.Time{}}
}

// Reserve implements Limiter.
func (l *Local) Reserve(_ context.Context, key string, n int) (Reservation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	interval := l.cfg.interval()
	k, first, wait := gcra(l.tat[key].Sub(now), interval, l.cfg.Horizon, n)
	if k == 0 {
		return Reservation{First: wait, Interval: interval}, nil
	}
	l.tat[key] = now.Add(first + time.Duration(k)*interval)
	return Reservation{Count: k, First: first, Interval: interval}, nil
}
