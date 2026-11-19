//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/queue"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
)

// complete stores a response for the lease through Execute, writing
// nothing else.
func complete(t *testing.T, l idempotency.Lease, status int, body []byte) error {
	t.Helper()
	_, err := store.Idempotency().Execute(t.Context(), l, func(context.Context) (idempotency.Response, bool) {
		return idempotency.Response{Status: status, Body: body}, true
	})
	return err
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	return countRowsCtx(t.Context(), t, query, args...)
}

func countRowsCtx(ctx context.Context, t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, query, args...).Scan(&n))
	return n
}

// ---- idempotency_key on the notification ------------------------------------

func withKeyAndHash(key, content string) func(p *domain.NewNotificationParams) {
	return func(p *domain.NewNotificationParams) {
		p.IdempotencyKey, p.Content = key, content
		p.RequestHash = domain.RequestFingerprint(p)
	}
}

func TestInsertNotifications_KeyReusedForDifferentRequestIsRejected(t *testing.T) {
	reset(t)
	first := insertOne(t, newSMS(t, withKeyAndHash("order-42", "your order shipped")))

	// The same request again: a duplicate, the original comes back.
	res, err := store.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, withKeyAndHash("order-42", "your order shipped"))})
	require.NoError(t, err)
	assert.True(t, res[0].Duplicate)
	assert.Equal(t, first.ID, res[0].Notification.ID)

	// The same key for another message must not silently return the first.
	other := newSMS(t, withKeyAndHash("order-42", "a different message"))
	_, err = store.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t), other})
	var reused *domain.IdempotencyKeyReusedError
	require.ErrorAs(t, err, &reused)
	assert.Equal(t, 1, reused.Index, "points at the offending item")
	assert.Equal(t, "order-42", reused.Key)
}

func TestInsertNotifications_LegacyRowWithoutHashStillDeduplicates(t *testing.T) {
	reset(t)
	legacy := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "old" }))
	_, err := pool.Exec(t.Context(), `UPDATE notifications SET request_hash = NULL WHERE id = $1`, legacy.ID)
	require.NoError(t, err)

	res, err := store.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, withKeyAndHash("old", "anything"))})
	require.NoError(t, err)
	assert.True(t, res[0].Duplicate, "a row from before request hashes cannot be compared")
}

// ---- Idempotency-Key: request and response in one transaction ------------

func TestIdempotencyStore_ExecuteCommitsWritesWithTheResponse(t *testing.T) {
	reset(t)
	idem := store.Idempotency()
	lease, _, ok, err := idem.Acquire(t.Context(), "c:k", "h", baseTime, time.Hour, time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	kept, err := idem.Execute(t.Context(), lease, func(ctx context.Context) (idempotency.Response, bool) {
		require.NoError(t, store.InTx(ctx, func(r *postgres.Repo) error {
			_, err := r.InsertNotifications(ctx, []*domain.Notification{newSMS(t)})
			return err
		}))
		// Not visible outside the unit until it commits.
		assert.Zero(t, countRowsCtx(ctx, t, `SELECT count(*) FROM notifications`))
		return idempotency.Response{Status: 201, Body: []byte(`{"ok":true}`)}, true
	})
	require.NoError(t, err)
	assert.True(t, kept)
	assert.Equal(t, 1, countRows(t, `SELECT count(*) FROM notifications`))
	assert.Equal(t, 1, countRows(t, `SELECT count(*) FROM idempotency_keys WHERE response_status = 201`))
}

func TestIdempotencyStore_ExecuteRollsBackUnkeptAndTakenOverRequests(t *testing.T) {
	reset(t)
	idem := store.Idempotency()
	write := func(ctx context.Context) {
		require.NoError(t, store.InTx(ctx, func(r *postgres.Repo) error {
			_, err := r.InsertNotifications(ctx, []*domain.Notification{newSMS(t)})
			return err
		}))
	}

	t.Run("5xx", func(t *testing.T) {
		lease, _, _, err := idem.Acquire(t.Context(), "c:5xx", "h", baseTime, time.Hour, time.Minute)
		require.NoError(t, err)
		kept, err := idem.Execute(t.Context(), lease, func(ctx context.Context) (idempotency.Response, bool) {
			write(ctx)
			return idempotency.Response{Status: 500}, false
		})
		require.NoError(t, err)
		assert.False(t, kept)
		assert.Zero(t, countRows(t, `SELECT count(*) FROM notifications`))
		require.NoError(t, idem.Release(t.Context(), lease))
		assert.Zero(t, countRows(t, `SELECT count(*) FROM idempotency_keys WHERE key = 'c:5xx'`))
	})

	t.Run("taken over", func(t *testing.T) {
		slow, _, _, err := idem.Acquire(t.Context(), "c:slow", "h", baseTime, time.Hour, time.Minute)
		require.NoError(t, err)
		_, err = idem.Execute(t.Context(), slow, func(ctx context.Context) (idempotency.Response, bool) {
			write(ctx)
			// Meanwhile the client's retry takes the stale key over.
			_, _, ok, err := idem.Acquire(ctx, "c:slow", "h", baseTime.Add(2*time.Minute), time.Hour, time.Minute)
			require.NoError(t, err)
			require.True(t, ok)
			return idempotency.Response{Status: 201}, true
		})
		require.ErrorIs(t, err, idempotency.ErrLeaseLost)
		assert.Zero(t, countRows(t, `SELECT count(*) FROM notifications`), "the slow request's work is rolled back")
	})

	t.Run("panic", func(t *testing.T) {
		lease, _, _, err := idem.Acquire(t.Context(), "c:panic", "h", baseTime, time.Hour, time.Minute)
		require.NoError(t, err)
		assert.Panics(t, func() {
			_, _ = idem.Execute(t.Context(), lease, func(ctx context.Context) (idempotency.Response, bool) {
				write(ctx)
				panic("boom")
			})
		})
		assert.Zero(t, countRows(t, `SELECT count(*) FROM notifications`))
	})
}

// ---- attempt history across manual retries -----------------------------------

func TestAttemptHistory_SurvivesManualRetry(t *testing.T) {
	reset(t)
	q := store.Queue()
	n := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.MaxAttempts = 1 }))
	now := baseTime.Add(time.Minute)

	deliver := func(at time.Time, outcome domain.AttemptOutcome, status domain.Status) {
		t.Helper()
		got, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, at))
		require.NoError(t, err)
		require.Len(t, got, 1)
		done, err := q.Complete(t.Context(), &queue.Completion{
			NotificationID: n.ID, WorkerID: "w1", Now: at, Status: status,
			Attempt: domain.Attempt{NotificationID: n.ID, AttemptNo: got[0].TotalAttempts, Outcome: outcome, Duration: time.Millisecond},
		})
		require.NoError(t, err)
		require.True(t, done.Applied)
		assert.False(t, done.DuplicateAttempt)
	}

	deliver(now, domain.OutcomePermanentError, domain.StatusFailed)
	_, err := store.RetryNotification(t.Context(), n.ID, now.Add(time.Minute))
	require.NoError(t, err)
	deliver(now.Add(2*time.Minute), domain.OutcomeSuccess, domain.StatusSent)

	attempts, err := store.ListAttempts(t.Context(), n.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 2, "the attempt after the retry is recorded too")
	assert.Equal(t, 1, attempts[0].AttemptNo)
	assert.Equal(t, domain.OutcomePermanentError, attempts[0].Outcome)
	assert.Equal(t, 2, attempts[1].AttemptNo)
	assert.Equal(t, domain.OutcomeSuccess, attempts[1].Outcome)

	got, err := store.GetNotification(t.Context(), n.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, got.Attempts, "the budget was reset by the retry")
	assert.Equal(t, 2, got.TotalAttempts, "the history count was not")
}

func TestReleaseClaims_GivesBackTheAttemptNumber(t *testing.T) {
	reset(t)
	q := store.Queue()
	n := insertOne(t, newSMS(t))
	now := baseTime.Add(time.Minute)
	_, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
	require.NoError(t, err)
	_, err = q.Release(t.Context(), "w1", []uuid.UUID{n.ID})
	require.NoError(t, err)

	got, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 1, got[0].TotalAttempts, "an unsent claim does not use up a number")
}

// ---- a send that outlived its lease ------------------------------------------

func TestComplete_LateSuccessCorrectsWhatTheReaperDid(t *testing.T) {
	q := store.Queue()
	now := baseTime.Add(time.Minute)
	sendLate := func(t *testing.T, n *domain.Notification, total int) queue.CompleteResult {
		t.Helper()
		done, err := q.Complete(t.Context(), &queue.Completion{
			NotificationID: n.ID, WorkerID: "w1", Now: now.Add(2 * time.Minute), Status: domain.StatusSent,
			Attempt: domain.Attempt{
				NotificationID: n.ID, AttemptNo: total, Outcome: domain.OutcomeSuccess,
				ProviderMessageID: "late-1", Duration: 90 * time.Second,
			},
		})
		require.NoError(t, err)
		return done
	}

	t.Run("failed on its last attempt", func(t *testing.T) {
		reset(t)
		n := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.MaxAttempts = 1 }))
		claimed, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
		require.NoError(t, err)
		res, err := q.ReapExpiredLeases(t.Context(), now.Add(time.Minute)) // the lease ran out mid-call
		require.NoError(t, err)
		require.EqualValues(t, 1, res.Failed)

		done := sendLate(t, n, claimed[0].TotalAttempts)
		assert.True(t, done.Applied)
		assert.Equal(t, domain.StatusFailed, done.RecoveredFrom)
		got, err := store.GetNotification(t.Context(), n.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusSent, got.Status, "the provider accepted it: it is sent")
		assert.Nil(t, got.FailedAt)
		assert.Empty(t, got.LastErrorCode)
		assert.Equal(t, "late-1", got.ProviderMessageID)
	})

	t.Run("requeued and cancelled", func(t *testing.T) {
		reset(t)
		n := insertOne(t, newSMS(t))
		claimed, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
		require.NoError(t, err)
		_, err = q.ReapExpiredLeases(t.Context(), now.Add(time.Minute))
		require.NoError(t, err)
		_, err = store.CancelNotification(t.Context(), n.ID, now.Add(90*time.Second))
		require.NoError(t, err)

		done := sendLate(t, n, claimed[0].TotalAttempts)
		assert.Equal(t, domain.StatusCancelled, done.RecoveredFrom)
		got, err := store.GetNotification(t.Context(), n.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.StatusSent, got.Status)
		assert.Nil(t, got.CancelledAt)
	})

	t.Run("another worker holds it", func(t *testing.T) {
		reset(t)
		n := insertOne(t, newSMS(t))
		claimed, err := q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w1", 1, now))
		require.NoError(t, err)
		_, err = q.ReapExpiredLeases(t.Context(), now.Add(time.Minute))
		require.NoError(t, err)
		_, err = q.Claim(t.Context(), claimReq(domain.ChannelSMS, "w2", 1, now.Add(time.Minute)))
		require.NoError(t, err)

		done := sendLate(t, n, claimed[0].TotalAttempts)
		assert.False(t, done.Applied, "w2 owns it now; its own outcome decides")
		assert.Empty(t, done.RecoveredFrom)
		attempts, err := store.ListAttempts(t.Context(), n.ID)
		require.NoError(t, err)
		assert.Len(t, attempts, 1, "the late attempt is still history")
	})
}

// ---- batches -----------------------------------------------------------------

func TestCompleteEmptyBatch(t *testing.T) {
	reset(t)
	existing := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.IdempotencyKey = "dup" }))
	batch := &domain.Batch{ID: uuid.New(), Total: 1, CreatedAt: baseTime}

	err := store.InTx(t.Context(), func(r *postgres.Repo) error {
		if err := r.InsertBatch(t.Context(), batch); err != nil {
			return err
		}
		res, err := r.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, func(p *domain.NewNotificationParams) {
			p.IdempotencyKey, p.BatchID = "dup", &batch.ID
		})})
		if err != nil {
			return err
		}
		if !res[0].Duplicate || res[0].Notification.ID != existing.ID {
			return errors.New("expected the existing notification")
		}
		return r.CompleteEmptyBatch(t.Context(), batch.ID, baseTime)
	})
	require.NoError(t, err)

	summary, err := store.GetBatchSummary(t.Context(), batch.ID)
	require.NoError(t, err)
	require.NotNil(t, summary.CompletedAt, "a batch of duplicates is complete at once")
	assert.Equal(t, 1, countRows(t,
		`SELECT count(*) FROM outbox_events WHERE event_type = 'batch.completed' AND aggregate_id = $1`, batch.ID))

	// A batch that created something is left to the relay.
	other := &domain.Batch{ID: uuid.New(), Total: 1, CreatedAt: baseTime}
	require.NoError(t, store.InTx(t.Context(), func(r *postgres.Repo) error {
		if err := r.InsertBatch(t.Context(), other); err != nil {
			return err
		}
		if _, err := r.InsertNotifications(t.Context(), []*domain.Notification{newSMS(t, func(p *domain.NewNotificationParams) {
			p.BatchID = &other.ID
		})}); err != nil {
			return err
		}
		return r.CompleteEmptyBatch(t.Context(), other.ID, baseTime)
	}))
	summary, err = store.GetBatchSummary(t.Context(), other.ID)
	require.NoError(t, err)
	assert.Nil(t, summary.CompletedAt)
}

// ---- callbacks ---------------------------------------------------------------

func TestClaimCallbacks_OneNotificationInEventOrder(t *testing.T) {
	reset(t)
	n := insertOne(t, newSMS(t, func(p *domain.NewNotificationParams) { p.CallbackURL = "https://hooks.example.com/x" }))
	now := time.Now().Add(time.Minute) // callbacks are due from their insert (database clock)
	for _, ev := range []struct {
		id  int64
		typ string
	}{{10, "notification.failed"}, {20, "notification.sent"}} {
		// Normally written by the outbox trigger on each final status
		// (migration 19); inserted directly here to test the claim order.
		_, err := pool.Exec(t.Context(), `INSERT INTO callback_deliveries (notification_id, event_id, event_type, url, payload)
			VALUES ($1, $2, $3, 'https://hooks.example.com/x', '{}')`, n.ID, ev.id, ev.typ)
		require.NoError(t, err)
	}

	first, err := store.ClaimCallbacks(t.Context(), now, time.Minute, 10, 5)
	require.NoError(t, err)
	require.Len(t, first, 1, "the later callback waits for the earlier one")
	assert.EqualValues(t, 10, first[0].EventID)

	// The earlier one fails and is scheduled for a retry: still pending,
	// so the later one keeps waiting even though it is due.
	ok, err := store.FinishCallback(t.Context(), first[0], &postgres.CallbackOutcome{
		Status: postgres.CallbackPending, NextAttemptAt: now.Add(time.Hour), Error: "503", Now: now,
	})
	require.NoError(t, err)
	require.True(t, ok)
	none, err := store.ClaimCallbacks(t.Context(), now.Add(2*time.Minute), time.Minute, 10, 5)
	require.NoError(t, err)
	assert.Empty(t, none)

	// Once it is retried and delivered, the next one goes.
	retry, err := store.ClaimCallbacks(t.Context(), now.Add(2*time.Hour), time.Minute, 10, 5)
	require.NoError(t, err)
	require.Len(t, retry, 1)
	ok, err = store.FinishCallback(t.Context(), retry[0], &postgres.CallbackOutcome{Status: postgres.CallbackDelivered, Now: now})
	require.NoError(t, err)
	require.True(t, ok)
	next, err := store.ClaimCallbacks(t.Context(), now.Add(2*time.Hour), time.Minute, 10, 5)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.EqualValues(t, 20, next[0].EventID)
}
