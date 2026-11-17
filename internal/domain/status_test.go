package domain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

// TestStatus_TransitionMatrix checks every one of the 36 (from, to) pairs, so
// adding or removing a transition anywhere is a deliberate, visible change.
func TestStatus_TransitionMatrix(t *testing.T) {
	const (
		pending    = domain.StatusPending
		scheduled  = domain.StatusScheduled
		processing = domain.StatusProcessing
		sent       = domain.StatusSent
		failed     = domain.StatusFailed
		cancelled  = domain.StatusCancelled
	)
	allowed := map[[2]domain.Status]bool{
		{pending, processing}:   true, // worker claims
		{pending, cancelled}:    true, // client cancels
		{scheduled, processing}: true, // scheduled time reached, worker claims
		{scheduled, cancelled}:  true, // client cancels before the scheduled time
		{processing, sent}:      true, // provider accepted
		{processing, pending}:   true, // transient error with attempts left, or lease expired
		{processing, failed}:    true, // permanent error or attempts exhausted
		{failed, pending}:       true, // manual retry from the DLQ
	}

	for _, from := range domain.Statuses {
		for _, to := range domain.Statuses {
			want := allowed[[2]domain.Status{from, to}]
			t.Run(string(from)+"->"+string(to), func(t *testing.T) {
				assert.Equal(t, want, from.CanTransitionTo(to))

				err := from.TransitionTo(to)
				if want {
					assert.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrInvalidTransition)
				var te *domain.TransitionError
				require.True(t, errors.As(err, &te))
				assert.Equal(t, from, te.From)
				assert.Equal(t, to, te.To)
			})
		}
	}
}

func TestStatus_IsFinal(t *testing.T) {
	final := map[domain.Status]bool{
		domain.StatusSent: true, domain.StatusFailed: true, domain.StatusCancelled: true,
	}
	for _, s := range domain.Statuses {
		assert.Equal(t, final[s], s.IsFinal(), s)
	}
}

func TestStatus_IsCancellable(t *testing.T) {
	cancellable := map[domain.Status]bool{domain.StatusPending: true, domain.StatusScheduled: true}
	for _, s := range domain.Statuses {
		assert.Equal(t, cancellable[s], s.IsCancellable(), s)
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range domain.Statuses {
		got, err := domain.ParseStatus(string(s))
		require.NoError(t, err)
		assert.Equal(t, s, got)
	}

	for _, bad := range []string{"", "PENDING", "done", "queued"} {
		_, err := domain.ParseStatus(bad)
		assert.Error(t, err, bad)
	}
}

func TestStatus_UnknownStatusHasNoTransitions(t *testing.T) {
	unknown := domain.Status("bogus")
	assert.False(t, unknown.Valid())
	for _, to := range domain.Statuses {
		assert.False(t, unknown.CanTransitionTo(to))
	}
}
