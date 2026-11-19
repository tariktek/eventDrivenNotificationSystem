package ratelimit_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
)

var cfg = ratelimit.Config{Rate: 100, Horizon: time.Second}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestLocal_ReservationArithmetic(t *testing.T) {
	clk := newClock()
	l := ratelimit.NewLocal(cfg, clk.now)
	ctx := t.Context()

	// Idle timeline: slots start now, one horizon's worth at most.
	r, err := l.Reserve(ctx, "sms", 150)
	require.NoError(t, err)
	assert.Equal(t, ratelimit.Reservation{Count: 100, First: 0, Interval: 10 * time.Millisecond}, r)
	assert.Equal(t, 990*time.Millisecond, r.Delay(99))

	// Horizon full: nothing granted, wait one interval.
	r, _ = l.Reserve(ctx, "sms", 10)
	assert.Equal(t, 0, r.Count)
	assert.Equal(t, 10*time.Millisecond, r.First)

	// One interval later, exactly one slot frees up, at the end of the horizon.
	clk.advance(10 * time.Millisecond)
	r, _ = l.Reserve(ctx, "sms", 10)
	assert.Equal(t, 1, r.Count)
	assert.Equal(t, 990*time.Millisecond, r.First)

	// Other keys (channels) have their own timeline.
	r, _ = l.Reserve(ctx, "email", 5)
	assert.Equal(t, ratelimit.Reservation{Count: 5, First: 0, Interval: 10 * time.Millisecond}, r)

	// After the timeline drains, it starts at "now" again: idle time does
	// not accumulate into a burst.
	clk.advance(10 * time.Second)
	r, _ = l.Reserve(ctx, "sms", 3)
	assert.Equal(t, 0*time.Millisecond, r.First)
	assert.Equal(t, 3, r.Count)
}

// assertRate checks the property the limiter exists for: slots are at least
// one interval apart, so no half-open one-second window holds more than
// cfg.Rate of them.
func assertRate(t *testing.T, slots []time.Time, c ratelimit.Config) {
	t.Helper()
	slices.SortFunc(slots, func(a, b time.Time) int { return a.Compare(b) })
	interval := time.Second / time.Duration(c.Rate)
	for i := 1; i < len(slots); i++ {
		require.GreaterOrEqual(t, slots[i].Sub(slots[i-1]), interval, "slots %d and %d too close", i-1, i)
	}
	worst := 0
	for i, j := 0, 0; i < len(slots); i++ {
		for j < len(slots) && slots[j].Before(slots[i].Add(time.Second)) {
			j++
		}
		worst = max(worst, j-i)
	}
	assert.LessOrEqual(t, worst, c.Rate, "a one-second window carried %d sends", worst)
}

// TestLocal_NeverExceedsRate drives the limiter with an irregular mix of
// requests and pauses and checks every one-second window.
func TestLocal_NeverExceedsRate(t *testing.T) {
	clk := newClock()
	l := ratelimit.NewLocal(cfg, clk.now)
	rng := rand.New(rand.NewPCG(1, 2))

	var slots []time.Time
	for len(slots) < 2000 {
		now := clk.now()
		r, err := l.Reserve(t.Context(), "sms", 1+rng.IntN(60))
		require.NoError(t, err)
		for i := range r.Count {
			slots = append(slots, now.Add(r.Delay(i)))
		}
		if r.Count == 0 {
			clk.advance(r.First)
		} else {
			clk.advance(time.Duration(rng.IntN(300)) * time.Millisecond)
		}
	}
	assertRate(t, slots, cfg)
}

// ---- guard ------------------------------------------------------------------

type flakyLimiter struct {
	mu    sync.Mutex
	down  bool
	calls int
}

func (f *flakyLimiter) Reserve(context.Context, string, int) (ratelimit.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.down {
		return ratelimit.Reservation{}, errors.New("connection refused")
	}
	return ratelimit.Reservation{Count: 1, Interval: time.Millisecond}, nil
}

func (f *flakyLimiter) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// TestGuard_FailsClosed: when the shared limiter is unreachable the guard
// grants nothing (no local share: a worker cut off from Redis cannot know
// what the others send), reports it, and recovers on the next success.
func TestGuard_FailsClosed(t *testing.T) {
	shared := &flakyLimiter{down: true}
	g := ratelimit.NewGuard(shared, time.Second, quiet)

	r, err := g.Reserve(t.Context(), "sms", 50)
	require.Error(t, err, "no slots without the shared limiter")
	assert.Zero(t, r.Count)
	assert.True(t, g.Unavailable())

	shared.setDown(false)
	r, err = g.Reserve(t.Context(), "sms", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, r.Count)
	assert.False(t, g.Unavailable())
}

// hangingLimiter is Redis as an outage really looks: the client retries its
// dials and answers only when the context gives up, not at once like
// flakyLimiter. Measured with go-redis defaults and Redis stopped: 79 s.
type hangingLimiter struct{}

func (hangingLimiter) Reserve(ctx context.Context, _ string, _ int) (ratelimit.Reservation, error) {
	<-ctx.Done()
	return ratelimit.Reservation{}, ctx.Err()
}

func TestGuard_SlowOutageFailsWithinTimeout(t *testing.T) {
	g := ratelimit.NewGuard(hangingLimiter{}, 50*time.Millisecond, quiet)
	start := time.Now()
	_, err := g.Reserve(t.Context(), "sms", 1)
	require.Error(t, err)
	assert.Less(t, time.Since(start), time.Second, "the outage is noticed after the timeout, not after the client gives up")
	assert.True(t, g.Unavailable())
}

func TestGuard_CancelledContextIsNotAnOutage(t *testing.T) {
	g := ratelimit.NewGuard(hangingLimiter{}, time.Minute, quiet)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := g.Reserve(ctx, "sms", 1)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, g.Unavailable(), "shutting down is not a Redis failure")
}

// TestLocal_ZeroReservationIsAProbe: workers probe the limiter with a
// reservation of 0 slots before resuming; it must not take any.
func TestLocal_ZeroReservationIsAProbe(t *testing.T) {
	clk := newClock()
	l := ratelimit.NewLocal(cfg, clk.now)
	r, err := l.Reserve(t.Context(), "sms", 0)
	require.NoError(t, err)
	assert.Zero(t, r.Count)
	r, err = l.Reserve(t.Context(), "sms", 1)
	require.NoError(t, err)
	assert.Zero(t, r.First, "the probe left the timeline untouched")
}
