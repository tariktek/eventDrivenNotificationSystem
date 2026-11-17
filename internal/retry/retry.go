// Package retry decides what happens to a notification after a delivery
// attempt: done, try again later, or give up.
package retry

import (
	"math/rand/v2"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
)

// Policy computes retry delays with capped exponential backoff and full
// jitter: the delay before retry n is uniformly random in
// [0, min(Max, Base × 2^(n-1))].
//
// Jitter matters when a provider recovers from an outage: thousands of
// notifications failed at about the same moment, and without jitter they
// would all retry at the same moment too and knock it over again (the
// "thundering herd"). Full jitter spreads them over the whole window.
type Policy struct {
	Base time.Duration
	Max  time.Duration
	// Rand returns a float in [0, 1); nil uses math/rand. Injected for tests.
	Rand func() float64
}

// Backoff returns the delay before the retry that follows attempt number
// `attempt` (1-based: after the first failure, attempt is 1).
func (p Policy) Backoff(attempt int) time.Duration {
	ceiling := p.Max
	// Base << (attempt-1), stopping before it overflows or passes Max.
	if shift := attempt - 1; shift < 62 {
		if d := p.Base << shift; d > 0 && d < ceiling {
			ceiling = d
		}
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64 //nolint:gosec // jitter, not security
	}
	return time.Duration(r() * float64(ceiling))
}

// Verdict is the decision for one attempt.
type Verdict struct {
	Status        domain.Status // sent, pending (retry) or failed
	NextAttemptAt time.Time     // when to retry; set only for pending
}

// Decide maps an attempt result to the notification's next state.
// n.Attempts already counts the attempt that produced res.
func (p Policy) Decide(n *domain.Notification, res *provider.Result, now time.Time) Verdict {
	switch res.Outcome {
	case domain.OutcomeSuccess:
		return Verdict{Status: domain.StatusSent}
	case domain.OutcomePermanentError:
		return Verdict{Status: domain.StatusFailed}
	}
	if !n.HasAttemptsLeft() {
		return Verdict{Status: domain.StatusFailed}
	}

	delay := p.Backoff(n.Attempts)
	// Honour the provider's Retry-After, but never beyond our own cap: a
	// provider asking for an hour should not park a notification that long.
	if res.RetryAfter > delay {
		delay = min(res.RetryAfter, p.Max)
	}
	return Verdict{Status: domain.StatusPending, NextAttemptAt: now.Add(delay)}
}
