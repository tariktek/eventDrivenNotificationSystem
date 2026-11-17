package breaker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/breaker"
)

func cfg() breaker.Config {
	return breaker.Config{Window: 10 * time.Second, MinRequests: 10, FailureRatio: 0.5, OpenTimeout: 100 * time.Millisecond, Probes: 3}
}

// run admits n deliveries and records each as a failure when fail(i).
func run(t *testing.T, b *breaker.Breaker, n int, fail func(i int) bool) {
	t.Helper()
	for i := range n {
		tk := b.Admit()
		require.NotNil(t, tk, "delivery %d rejected", i)
		if fail(i) {
			tk.Failure()
		} else {
			tk.Success()
		}
	}
}

func TestBreaker_StaysClosedBelowMinRequests(t *testing.T) {
	b := breaker.New("sms", cfg(), nil)
	run(t, b, 9, func(int) bool { return true }) // 100% failures, but only 9
	assert.Equal(t, breaker.StateClosed, b.State())
	assert.Equal(t, 50, b.Capacity(50))
}

func TestBreaker_StaysClosedBelowFailureRatio(t *testing.T) {
	b := breaker.New("sms", cfg(), nil)
	run(t, b, 40, func(i int) bool { return i%3 == 0 }) // ~33% failures
	assert.Equal(t, breaker.StateClosed, b.State())
}

func TestBreaker_OpensThenProbesThenCloses(t *testing.T) {
	var (
		mu          sync.Mutex
		transitions []string
	)
	b := breaker.New("sms", cfg(), func(from, to breaker.State) {
		mu.Lock()
		transitions = append(transitions, string(from)+"->"+string(to))
		mu.Unlock()
	})

	run(t, b, 10, func(i int) bool { return i%2 == 1 }) // 50% of 10; trips on the 10th
	assert.Equal(t, breaker.StateOpen, b.State())
	assert.Zero(t, b.Capacity(50), "open: claim nothing")
	assert.Nil(t, b.Admit(), "open: send nothing")

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, breaker.StateHalfOpen, b.State())
	assert.Equal(t, 3, b.Capacity(50), "half-open: only the probes")

	probes := []*breaker.Ticket{b.Admit(), b.Admit(), b.Admit()}
	for _, p := range probes {
		require.NotNil(t, p)
	}
	assert.Zero(t, b.Capacity(50), "all probes handed out")
	assert.Nil(t, b.Admit(), "no fourth probe")

	for _, p := range probes {
		p.Success()
	}
	assert.Equal(t, breaker.StateClosed, b.State())
	assert.Equal(t, 50, b.Capacity(50))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"closed->open", "open->half_open", "half_open->closed"}, transitions)
}

func TestBreaker_FailedProbeReopens(t *testing.T) {
	b := breaker.New("sms", cfg(), nil)
	run(t, b, 10, func(int) bool { return true })
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, breaker.StateHalfOpen, b.State())

	p1, p2 := b.Admit(), b.Admit()
	p1.Failure()
	assert.Equal(t, breaker.StateOpen, b.State())
	assert.True(t, p2.Open(), "an in-flight probe learns the breaker reopened")
	p2.Skip() // reported against an old generation: ignored
	assert.Equal(t, breaker.StateOpen, b.State())
}

func TestBreaker_SkippedDeliveriesDoNotCount(t *testing.T) {
	b := breaker.New("sms", cfg(), nil)
	for range 30 {
		b.Admit().Skip() // e.g. released during shutdown
	}
	run(t, b, 5, func(int) bool { return true })
	assert.Equal(t, breaker.StateClosed, b.State(), "5 real failures are below MinRequests")

	// Half-open: a skipped probe frees its place for another one.
	run(t, b, 5, func(int) bool { return true })
	require.Equal(t, breaker.StateOpen, b.State())
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 3, b.Capacity(10))
	b.Admit().Skip()
	assert.Equal(t, 3, b.Capacity(10))
}

// TestBreaker_InFlightDeliveriesDoNotDiluteFailures: the worker admits a
// whole claim at once; the ratio must be judged on finished deliveries.
func TestBreaker_InFlightDeliveriesDoNotDiluteFailures(t *testing.T) {
	b := breaker.New("sms", cfg(), nil)
	tickets := make([]*breaker.Ticket, 50)
	for i := range tickets {
		tickets[i] = b.Admit()
		require.NotNil(t, tickets[i])
	}
	for _, tk := range tickets[:10] {
		tk.Failure()
	}
	assert.Equal(t, breaker.StateOpen, b.State(), "10 of 10 finished failed")
	assert.True(t, tickets[10].Open(), "waiting deliveries see it and skip")
}

func TestBreaker_OldFailuresFadeOut(t *testing.T) {
	c := cfg()
	c.Window = 200 * time.Millisecond
	b := breaker.New("sms", c, nil)
	run(t, b, 9, func(int) bool { return true })
	time.Sleep(300 * time.Millisecond) // the 9 failures leave the window
	run(t, b, 9, func(int) bool { return false })
	run(t, b, 1, func(int) bool { return true })
	assert.Equal(t, breaker.StateClosed, b.State(), "1 failure in the last 10, not 10 in 19")
}
