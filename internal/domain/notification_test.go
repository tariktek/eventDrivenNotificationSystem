package domain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
)

var now = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func smsParams() domain.NewNotificationParams {
	return domain.NewNotificationParams{
		Channel:   domain.ChannelSMS,
		Recipient: "+905551234567",
		Content:   "Your code is 1234",
	}
}

func TestNewNotification_ImmediateDefaults(t *testing.T) {
	n, err := domain.NewNotification(smsParams(), now)
	require.NoError(t, err)

	assert.Equal(t, uuid.Version(7), n.ID.Version(), "IDs are time-ordered UUIDv7")
	assert.Equal(t, domain.StatusPending, n.Status)
	assert.Equal(t, domain.PriorityNormal, n.Priority, "omitted priority defaults to normal")
	assert.Equal(t, domain.DefaultMaxAttempts, n.MaxAttempts)
	assert.Equal(t, 0, n.Attempts)
	assert.Equal(t, now, n.NextAttemptAt, "due immediately")
	assert.Nil(t, n.ScheduledAt)
	assert.NotNil(t, n.Metadata, "metadata is never nil so it serialises as {}")
	assert.Equal(t, now, n.CreatedAt)
	assert.True(t, n.HasAttemptsLeft())
}

func TestNewNotification_KeepsExplicitValues(t *testing.T) {
	batchID := uuid.New()
	p := smsParams()
	p.BatchID = &batchID
	p.Priority = domain.PriorityHigh
	p.MaxAttempts = 3
	p.IdempotencyKey = "order-42-sms"
	p.Metadata = map[string]any{"campaign": "black-friday"}
	p.CorrelationID = "corr-1"

	n, err := domain.NewNotification(p, now)
	require.NoError(t, err)

	assert.Equal(t, &batchID, n.BatchID)
	assert.Equal(t, domain.PriorityHigh, n.Priority)
	assert.Equal(t, 3, n.MaxAttempts)
	assert.Equal(t, "order-42-sms", n.IdempotencyKey)
	assert.Equal(t, "black-friday", n.Metadata["campaign"])
	assert.Equal(t, "corr-1", n.CorrelationID)
}

func TestNewNotification_ScheduledInFuture(t *testing.T) {
	at := now.Add(2 * time.Hour)
	p := smsParams()
	p.ScheduledAt = &at

	n, err := domain.NewNotification(p, now)
	require.NoError(t, err)

	assert.Equal(t, domain.StatusScheduled, n.Status)
	assert.Equal(t, at, n.NextAttemptAt, "becomes due at the scheduled time")
	require.NotNil(t, n.ScheduledAt)
	assert.Equal(t, at, *n.ScheduledAt)

	at = at.Add(time.Hour) // mutate the caller's value
	assert.NotEqual(t, at, *n.ScheduledAt, "must not alias the caller's time")
}

func TestNewNotification_ScheduledAtNowOrPastIsImmediate(t *testing.T) {
	for name, at := range map[string]time.Time{"now": now, "past": now.Add(-time.Minute)} {
		t.Run(name, func(t *testing.T) {
			p := smsParams()
			p.ScheduledAt = &at

			n, err := domain.NewNotification(p, now)
			require.NoError(t, err)

			assert.Equal(t, domain.StatusPending, n.Status)
			assert.Equal(t, now, n.NextAttemptAt)
			assert.Nil(t, n.ScheduledAt)
		})
	}
}

func TestNewNotification_UniqueIDs(t *testing.T) {
	seen := make(map[uuid.UUID]bool)
	for range 1000 {
		n, err := domain.NewNotification(smsParams(), now)
		require.NoError(t, err)
		require.False(t, seen[n.ID], "duplicate id %s", n.ID)
		seen[n.ID] = true
	}
}

func TestNotification_HasAttemptsLeft(t *testing.T) {
	n := &domain.Notification{MaxAttempts: 3}
	for attempts, want := range []bool{true, true, true, false, false} {
		n.Attempts = attempts
		assert.Equal(t, want, n.HasAttemptsLeft(), "attempts=%d", attempts)
	}
}

func TestBatchSummary_Progress(t *testing.T) {
	s := domain.BatchSummary{
		Batch: domain.Batch{Total: 10},
		Counts: map[domain.Status]int{
			domain.StatusSent:       4,
			domain.StatusFailed:     1,
			domain.StatusCancelled:  1,
			domain.StatusPending:    3,
			domain.StatusProcessing: 1,
		},
	}
	assert.Equal(t, 6, s.Done())
	assert.Equal(t, 10, s.Created())
	assert.InDelta(t, 0.6, s.Progress(), 1e-9)

	// 3 of 5 items were duplicates of earlier notifications: progress is
	// measured over the 2 this batch created, so it can reach 100%.
	dup := domain.BatchSummary{Batch: domain.Batch{Total: 5}, Counts: map[domain.Status]int{domain.StatusSent: 2}}
	assert.InDelta(t, 1.0, dup.Progress(), 1e-9)

	assert.InDelta(t, 1.0, domain.BatchSummary{}.Progress(), 1e-9, "nothing created: nothing left to do")
}
