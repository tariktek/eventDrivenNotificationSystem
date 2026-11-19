package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Guard wraps the shared limiter (Redis). It bounds every call and reports
// whether the shared limiter is reachable. It does not fall back to a local
// limiter, on purpose: the limit is global, and a worker that cannot reach
// the shared timeline cannot know what the others are sending. A local
// share is wrong in the common partial outage, where only some workers lose
// Redis: two workers, one cut off at 95/2 while the other still gets the
// full 95 through Redis, send 142/s together. So when the shared limiter is
// unreachable, Reserve fails and the worker stops sending on that channel
// (it hands its claimed work back to the queue for workers that can still
// reach the limiter) until a call succeeds again.
//
// Each call is bounded by Timeout because an outage rarely fails fast: with
// Redis stopped, the client retried its dials for 79 s before returning an
// error.
type Guard struct {
	shared  Limiter
	timeout time.Duration
	log     *slog.Logger

	mu          sync.Mutex
	unavailable bool
}

// DefaultTimeout is far above a healthy Redis round trip (about a
// millisecond) and short enough to notice an outage at once.
const DefaultTimeout = 250 * time.Millisecond

// NewGuard wraps shared. timeout <= 0 uses DefaultTimeout.
func NewGuard(shared Limiter, timeout time.Duration, log *slog.Logger) *Guard {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Guard{shared: shared, timeout: timeout, log: log}
}

// Reserve implements Limiter. It fails if the shared limiter does not
// answer within the timeout; a reservation of 0 slots probes it without
// taking any.
func (g *Guard) Reserve(ctx context.Context, key string, n int) (Reservation, error) {
	cctx, cancel := context.WithTimeout(ctx, g.timeout)
	res, err := g.shared.Reserve(cctx, key, n)
	cancel()
	if err == nil {
		g.set(ctx, false, nil)
		return res, nil
	}
	if ctx.Err() != nil {
		return Reservation{}, ctx.Err() // shutting down: not an outage
	}
	g.set(ctx, true, err)
	return Reservation{}, fmt.Errorf("shared rate limiter unavailable: %w", err)
}

// Unavailable reports whether the last call to the shared limiter failed.
func (g *Guard) Unavailable() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unavailable
}

func (g *Guard) set(ctx context.Context, down bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unavailable == down {
		return
	}
	g.unavailable = down
	if down {
		g.log.WarnContext(ctx, "shared rate limiter unavailable: deliveries pause until it answers", "error", err)
	} else {
		g.log.InfoContext(ctx, "shared rate limiter reachable again: deliveries resume")
	}
}
