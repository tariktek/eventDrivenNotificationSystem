//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/queue"
)

func claimReq(ch domain.Channel, worker string, limit int, now time.Time) queue.ClaimRequest {
	return queue.ClaimRequest{
		Channel: ch, WorkerID: worker, Limit: limit, Now: now,
		Lease: 30 * time.Second, Aging: 5 * time.Minute,
	}
}

// due creates a pending notification that became due at `at`.
func due(t *testing.T, ch domain.Channel, prio domain.Priority, at time.Time) *domain.Notification {
	t.Helper()
	p := domain.NewNotificationParams{Channel: ch, Priority: prio, Recipient: "r", Content: "c"}
	if ch == domain.ChannelEmail {
		p.Subject = "s"
	}
	n, err := domain.NewNotification(p, at)
	require.NoError(t, err)
	return insertOne(t, n)
}

func TestClaim_LeasesDueWorkOfOneChannel(t *testing.T) {
	reset(t)
	now := baseTime.Add(time.Minute)
	sms := due(t, domain.ChannelSMS, domain.PriorityNormal, baseTime)
	due(t, domain.ChannelEmail, domain.PriorityNormal, baseTime)           // other channel
	due(t, domain.ChannelSMS, domain.PriorityNormal, now.Add(time.Second)) // not due yet

	got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 10, now))
	require.NoError(t, err)
	require.Len(t, got, 1)

	n := got[0]
	assert.Equal(t, sms.ID, n.ID)
	assert.Equal(t, domain.StatusProcessing, n.Status)
	assert.Equal(t, "w1", n.LockedBy)
	require.NotNil(t, n.LockedUntil)
	assert.True(t, now.Add(30*time.Second).Equal(*n.LockedUntil))
	assert.Equal(t, 1, n.Attempts, "the attempt is counted at claim time")

	again, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w2", 10, now))
	require.NoError(t, err)
	assert.Empty(t, again, "a claimed notification is not handed out twice")
}

func TestClaim_PriorityOrder(t *testing.T) {
	reset(t)
	now := baseTime.Add(time.Minute)
	low := due(t, domain.ChannelSMS, domain.PriorityLow, baseTime)
	normal := due(t, domain.ChannelSMS, domain.PriorityNormal, baseTime)
	high := due(t, domain.ChannelSMS, domain.PriorityHigh, baseTime.Add(30*time.Second)) // newest, still first

	var order []uuid.UUID
	for range 3 {
		got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w", 1, now))
		require.NoError(t, err)
		require.Len(t, got, 1)
		order = append(order, got[0].ID)
	}
	assert.Equal(t, []uuid.UUID{high.ID, normal.ID, low.ID}, order)
}

// TestClaim_AgingPreventsStarvation: a low-priority notification waiting
// for 10 minutes (two aging steps) ranks as high and goes before a fresh
// normal one; without aging, a constant stream of normal traffic could
// postpone it indefinitely.
func TestClaim_AgingPreventsStarvation(t *testing.T) {
	reset(t)
	now := baseTime.Add(10 * time.Minute)
	oldLow := due(t, domain.ChannelSMS, domain.PriorityLow, baseTime)
	due(t, domain.ChannelSMS, domain.PriorityNormal, now)

	got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w", 1, now))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, oldLow.ID, got[0].ID)
}

func TestClaim_ScheduledWaitsForItsTime(t *testing.T) {
	reset(t)
	at := baseTime.Add(time.Hour)
	n, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelPush, Recipient: "t", Content: "c", ScheduledAt: &at,
	}, baseTime)
	require.NoError(t, err)
	insertOne(t, n)

	got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelPush, "w", 10, at.Add(-time.Second)))
	require.NoError(t, err)
	assert.Empty(t, got)

	got, err = store.Queue().Claim(t.Context(), claimReq(domain.ChannelPush, "w", 10, at))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, domain.StatusProcessing, got[0].Status)
}

// TestClaim_ConcurrentWorkersNeverShareWork drains a 600-item queue with six
// workers claiming at the same time. SKIP LOCKED must hand every
// notification to exactly one worker.
func TestClaim_ConcurrentWorkersNeverShareWork(t *testing.T) {
	reset(t)
	const total, workers = 600, 6
	ns := make([]*domain.Notification, total)
	for i := range ns {
		prio := []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}[i%3]
		n, err := domain.NewNotification(domain.NewNotificationParams{
			Channel: domain.ChannelSMS, Priority: prio, Recipient: "r", Content: "c",
		}, baseTime)
		require.NoError(t, err)
		ns[i] = n
	}
	_, err := store.InsertNotifications(t.Context(), ns)
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		claimed = map[uuid.UUID]string{}
		dupes   int
		wg      sync.WaitGroup
	)
	for w := range workers {
		worker := uuid.NewString()[:8]
		_ = w
		wg.Go(func() {
			for {
				got, err := store.Queue().Claim(context.Background(), claimReq(domain.ChannelSMS, worker, 25, baseTime.Add(time.Minute)))
				if !assert.NoError(t, err) || len(got) == 0 {
					return
				}
				mu.Lock()
				for _, n := range got {
					if _, seen := claimed[n.ID]; seen {
						dupes++
					}
					claimed[n.ID] = worker
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	assert.Zero(t, dupes, "no notification may be claimed twice")
	assert.Len(t, claimed, total, "every notification is claimed exactly once")
}

func TestExtendLeases_OnlyOwnLeases(t *testing.T) {
	reset(t)
	now := baseTime.Add(time.Minute)
	due(t, domain.ChannelSMS, domain.PriorityNormal, baseTime)
	got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
	require.NoError(t, err)
	ids := []uuid.UUID{got[0].ID}

	n, err := store.Queue().ExtendLeases(t.Context(), "someone-else", ids, now.Add(time.Hour))
	require.NoError(t, err)
	assert.Zero(t, n)

	n, err = store.Queue().ExtendLeases(t.Context(), "w1", ids, now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	reloaded, err := store.GetNotification(t.Context(), got[0].ID)
	require.NoError(t, err)
	assert.True(t, now.Add(time.Hour).Equal(*reloaded.LockedUntil))
}

func TestReapExpiredLeases(t *testing.T) {
	reset(t)
	claimAt := baseTime.Add(time.Minute)

	// A notification with attempts left, and a "poison pill" on its last one.
	retryable := due(t, domain.ChannelSMS, domain.PriorityNormal, baseTime)
	poison, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelSMS, Recipient: "r", Content: "c", MaxAttempts: 1,
	}, baseTime)
	require.NoError(t, err)
	insertOne(t, poison)

	got, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "dead-worker", 10, claimAt))
	require.NoError(t, err)
	require.Len(t, got, 2)

	res, err := store.Queue().ReapExpiredLeases(t.Context(), claimAt.Add(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, queue.ReapResult{}, res, "leases still valid: nothing reaped")

	reapAt := claimAt.Add(time.Minute)
	res, err = store.Queue().ReapExpiredLeases(t.Context(), reapAt)
	require.NoError(t, err)
	assert.Equal(t, queue.ReapResult{Requeued: 1, Failed: 1}, res)

	r, err := store.GetNotification(t.Context(), retryable.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, r.Status)
	assert.Equal(t, 1, r.Attempts, "the lost attempt still counts")
	assert.Empty(t, r.LockedBy)
	assert.Equal(t, "lease_expired", r.LastErrorCode)
	assert.True(t, reapAt.Equal(r.NextAttemptAt), "due again immediately")

	p, err := store.GetNotification(t.Context(), poison.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusFailed, p.Status, "no attempts left: fail instead of looping")
	require.NotNil(t, p.FailedAt)

	again, err := store.Queue().Claim(t.Context(), claimReq(domain.ChannelSMS, "w2", 10, reapAt))
	require.NoError(t, err)
	require.Len(t, again, 1, "requeued work is claimable by another worker")
	assert.Equal(t, retryable.ID, again[0].ID)
}

func TestListenDue(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	signals := make(chan domain.Channel, 10)
	go store.Queue().ListenDue(ctx, func(ch domain.Channel) { signals <- ch }, func(err error) { t.Log(err) })
	time.Sleep(300 * time.Millisecond) // let LISTEN register

	// A notification scheduled for later must not wake anyone.
	later := time.Now().Add(time.Hour)
	scheduled, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelPush, Recipient: "t", Content: "c", ScheduledAt: &later,
	}, time.Now())
	require.NoError(t, err)
	insertOne(t, scheduled)
	select {
	case ch := <-signals:
		t.Fatalf("scheduled notification sent a wake-up for %s", ch)
	case <-time.After(300 * time.Millisecond):
	}

	// A batch of due notifications wakes the channel, once per transaction.
	ns := make([]*domain.Notification, 50)
	for i := range ns {
		ns[i] = newSMS(t)
	}
	_, err = store.InsertNotifications(t.Context(), ns)
	require.NoError(t, err)

	select {
	case ch := <-signals:
		assert.Equal(t, domain.ChannelSMS, ch)
	case <-time.After(2 * time.Second):
		t.Fatal("no wake-up for due notifications")
	}
	select {
	case <-signals:
		t.Fatal("50 rows in one statement must produce a single wake-up")
	case <-time.After(300 * time.Millisecond):
	}
}
