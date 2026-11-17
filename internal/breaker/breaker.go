// Package breaker is a circuit breaker for one provider channel.
//
// When a provider starts failing, retrying every queued notification
// against it only burns their attempt budgets: after a few minutes of
// outage everything would end up "failed" even though nothing was wrong
// with the notifications themselves. The breaker notices the failure rate
// and opens; while it is open the worker stops claiming that channel, so
// queued notifications wait without spending attempts. After OpenTimeout a
// few probe deliveries are let through (half-open): if they all succeed the
// breaker closes and delivery resumes, if one fails it opens again.
//
// Only transient failures (5xx, 429, timeouts, connection errors) count
// against the provider. A permanent error such as "invalid recipient" says
// nothing about the provider's health and counts as a success.
//
// Each worker instance keeps its own breakers. Every instance sees the same
// provider, so each one reaches the same conclusion within MinRequests
// deliveries; sharing state through Redis would save only those few calls.
package breaker

import (
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"
)

// State is the breaker state.
type State string

// Breaker states.
const (
	StateClosed   State = "closed"    // normal operation
	StateOpen     State = "open"      // provider considered down; nothing is sent
	StateHalfOpen State = "half_open" // probing whether the provider recovered
)

// Config tunes a Breaker.
type Config struct {
	// Window is the rolling period over which failures are counted.
	Window time.Duration
	// MinRequests is the minimum number of deliveries in Window before the
	// failure ratio is trusted (2 failures out of 3 is not an outage).
	MinRequests uint32
	// FailureRatio opens the breaker when failures/requests reaches it.
	FailureRatio float64
	// OpenTimeout is how long the breaker stays open before probing.
	OpenTimeout time.Duration
	// Probes is the number of deliveries let through while half-open; all
	// of them must succeed to close the breaker.
	Probes uint32
}

// DefaultConfig opens on ≥50% failures over ≥20 deliveries in 10 seconds
// and probes with 5 deliveries after 30 seconds.
func DefaultConfig() Config {
	return Config{Window: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 30 * time.Second, Probes: 5}
}

// Breaker guards one channel.
type Breaker struct {
	cb     *gobreaker.TwoStepCircuitBreaker[struct{}]
	probes uint32
}

// errFailure and errSkipped are what Ticket.Done reports to gobreaker.
var (
	errFailure = errors.New("provider failure")
	errSkipped = errors.New("delivery skipped")
)

// New builds a Breaker. onChange, if set, is called on every state change;
// it runs under the breaker's lock and must not call back into it.
func New(name string, cfg Config, onChange func(from, to State)) *Breaker {
	// Count in 10 rolling buckets so old failures fade out gradually
	// instead of all at once at the end of a fixed window.
	bucket := max(cfg.Window/10, time.Millisecond)
	st := gobreaker.Settings{
		Name:         name,
		MaxRequests:  cfg.Probes,
		Interval:     cfg.Window,
		BucketPeriod: bucket,
		Timeout:      cfg.OpenTimeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			// Judge only finished deliveries. c.Requests also counts
			// admitted ones still waiting for their send slot; with a
			// batch of 50 admitted at once, 5 failures out of the first 5
			// would look like 5 out of 50 and the breaker would trip late.
			done := c.TotalSuccesses + c.TotalFailures
			return done >= cfg.MinRequests && float64(c.TotalFailures) >= cfg.FailureRatio*float64(done)
		},
		IsExcluded: func(err error) bool { return errors.Is(err, errSkipped) },
	}
	if onChange != nil {
		st.OnStateChange = func(_ string, from, to gobreaker.State) { onChange(convert(from), convert(to)) }
	}
	return &Breaker{cb: gobreaker.NewTwoStepCircuitBreaker[struct{}](st), probes: max(cfg.Probes, 1)}
}

// State returns the current state. Reading it also performs the timed
// open → half-open transition.
func (b *Breaker) State() State {
	return convert(b.cb.State())
}

// Capacity is how many deliveries may be admitted right now, at most want:
// all of them when closed, none when open, and the probes not yet handed
// out when half-open.
func (b *Breaker) Capacity(want int) int {
	switch b.State() {
	case StateClosed:
		return want
	case StateHalfOpen:
		used := valid(b.cb.Counts())
		if used >= b.probes {
			return 0
		}
		return min(want, int(b.probes-used))
	default:
		return 0
	}
}

// Admit asks to start one delivery. It returns nil when the breaker
// rejects it (open, or out of probes); the caller must then not send.
func (b *Breaker) Admit() *Ticket {
	done, err := b.cb.Allow()
	if err != nil {
		return nil
	}
	return &Ticket{b: b, done: done}
}

// Ticket is an admitted delivery. Exactly one of Success, Failure or Skip
// must be called.
type Ticket struct {
	b    *Breaker
	done func(error)
}

// Open reports whether the breaker opened since the ticket was issued, in
// which case the delivery should be skipped rather than sent.
func (t *Ticket) Open() bool { return t.b.State() == StateOpen }

// Success records a delivery that says the provider is healthy.
func (t *Ticket) Success() { t.done(nil) }

// Failure records a delivery that says the provider is not healthy.
func (t *Ticket) Failure() { t.done(errFailure) }

// Skip records a delivery that never reached the provider; it counts
// neither way.
func (t *Ticket) Skip() { t.done(errSkipped) }

// valid counts the deliveries that were admitted and not skipped.
func valid(c gobreaker.Counts) uint32 {
	if c.Requests < c.TotalExclusions {
		return 0
	}
	return c.Requests - c.TotalExclusions
}

func convert(s gobreaker.State) State {
	switch s {
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateClosed
	}
}
