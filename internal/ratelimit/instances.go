package ratelimit

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Instances tracks how many workers share the rate, for the local fallback:
// when Redis is down each worker sends at Rate / Instances. A fixed,
// configured count is wrong as soon as the workers are scaled without
// updating it (three workers configured as two would together send 1.5×
// the limit), so the count is read from a live source, typically the
// workers connected to the database, which stays reachable when Redis is
// not.
type Instances struct {
	count func(context.Context) (int, error)
	every time.Duration
	log   *slog.Logger
	n     atomic.Int64
}

// NewInstances starts at initial (used until the first successful count)
// and refreshes every interval from count.
func NewInstances(initial int, every time.Duration, count func(context.Context) (int, error), log *slog.Logger) *Instances {
	i := &Instances{count: count, every: every, log: log}
	i.n.Store(int64(max(initial, 1)))
	return i
}

// Get returns the current count, at least 1.
func (i *Instances) Get() int { return int(i.n.Load()) }

// Run refreshes the count now and every interval until ctx is done. A
// failed count keeps the last one.
func (i *Instances) Run(ctx context.Context) {
	t := time.NewTicker(i.every)
	defer t.Stop()
	for {
		i.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Refresh reads the count once.
func (i *Instances) Refresh(ctx context.Context) {
	n, err := i.count(ctx)
	if err != nil {
		if ctx.Err() == nil {
			i.log.WarnContext(ctx, "count worker instances; keeping the last count", "error", err, "count", i.Get())
		}
		return
	}
	n = max(n, 1) // this worker is running, whatever the count says
	if old := i.n.Swap(int64(n)); old != int64(n) {
		i.log.InfoContext(ctx, "worker instances sharing the rate limit", "count", n, "previous", old)
	}
}
