package retry_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
)

var now = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func policy(r float64) retry.Policy {
	return retry.Policy{Base: 2 * time.Second, Max: 5 * time.Minute, Rand: func() float64 { return r }}
}

func TestBackoff_ExponentialWithCap(t *testing.T) {
	p := policy(0.999999) // jitter at the top of its range shows the ceiling
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 64 * time.Second, 128 * time.Second, 256 * time.Second, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		assert.InDelta(t, w.Seconds(), p.Backoff(i+1).Seconds(), 0.01, "attempt %d", i+1)
	}
	assert.Equal(t, 5*time.Minute, p.Backoff(1000).Round(time.Second), "huge attempt numbers must not overflow")
}

func TestBackoff_FullJitterSpreadsRetries(t *testing.T) {
	assert.Zero(t, policy(0).Backoff(3), "jitter can retry immediately")
	assert.Equal(t, 4*time.Second, policy(0.5).Backoff(3), "half of the 8s ceiling")

	p := retry.Policy{Base: 2 * time.Second, Max: 5 * time.Minute} // real randomness
	seen := map[time.Duration]bool{}
	for range 50 {
		d := p.Backoff(5)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.Less(t, d, 32*time.Second)
		seen[d] = true
	}
	assert.Greater(t, len(seen), 40, "simultaneous failures must not retry in lockstep")
}

func TestDecide(t *testing.T) {
	n := func(attempts int) *domain.Notification {
		return &domain.Notification{Attempts: attempts, MaxAttempts: 5}
	}
	transient := &provider.Result{Outcome: domain.OutcomeTransientError, ErrorCode: "http_503"}

	tests := []struct {
		name string
		n    *domain.Notification
		res  *provider.Result
		want retry.Verdict
	}{
		{"success", n(1), &provider.Result{Outcome: domain.OutcomeSuccess}, retry.Verdict{Status: domain.StatusSent}},
		{
			"permanent error fails at once", n(1), &provider.Result{Outcome: domain.OutcomePermanentError},
			retry.Verdict{Status: domain.StatusFailed},
		},
		{
			"transient retries with backoff", n(2), transient,
			retry.Verdict{Status: domain.StatusPending, NextAttemptAt: now.Add(2 * time.Second)},
		}, // 0.5 × 4s
		{"transient on the last attempt fails", n(5), transient, retry.Verdict{Status: domain.StatusFailed}},
		{
			"Retry-After longer than backoff wins", n(1),
			&provider.Result{Outcome: domain.OutcomeTransientError, RetryAfter: 30 * time.Second},
			retry.Verdict{Status: domain.StatusPending, NextAttemptAt: now.Add(30 * time.Second)},
		},
		{
			"Retry-After is capped", n(1),
			&provider.Result{Outcome: domain.OutcomeTransientError, RetryAfter: 2 * time.Hour},
			retry.Verdict{Status: domain.StatusPending, NextAttemptAt: now.Add(5 * time.Minute)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, policy(0.5).Decide(tt.n, tt.res, now))
		})
	}
}
