package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/queue"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// QueueStore implements queue.Queue on the notifications table.
type QueueStore struct {
	repo *Repo
	pool *pgxpool.Pool
}

var _ queue.Queue = QueueStore{}

// Queue returns the work-queue view of the store.
func (s *Store) Queue() QueueStore { return QueueStore{repo: s.Repo, pool: s.pool} }

// claimSQL picks and leases due work in one statement.
//
//  1. candidates: for each priority, the oldest due rows. Each branch is an
//     index range scan on notifications_queue_idx (channel, priority,
//     next_attempt_at), so the cost does not grow with the backlog.
//  2. picked: re-checks those rows under FOR UPDATE SKIP LOCKED (the
//     candidate read is unlocked and may be stale) and orders them by
//     effective priority (priority minus one level per $aging of waiting),
//     then by how long they have been due. Ties go to the longer wait, not
//     the original priority: otherwise a fully aged low notification would
//     still lose to every fresh high one, and sustained high traffic would
//     starve low traffic forever, the very thing aging exists to prevent.
//     The bound is: nothing waits longer than 2 × $aging behind newer work.
//     Within each priority the oldest rows rank first under this order, so
//     taking the top of each branch cannot miss a row that would sort first.
//  3. The UPDATE moves the picked rows to processing under our lease and
//     counts the attempt twice over: against the budget (attempts, reset by
//     a manual retry) and in total (total_attempts, never reset, which
//     numbers the attempt history).
//
// SKIP LOCKED is what lets many workers claim concurrently: rows another
// worker is claiming are skipped instead of waited on, and no row can be
// claimed twice. Candidates are over-fetched ($5 per priority) so
// concurrent workers that see the same candidates still find unlocked ones.
const claimSQL = `
WITH candidates AS (
    (SELECT id FROM notifications
     WHERE channel = $1 AND status IN ('pending', 'scheduled') AND next_attempt_at <= $2 AND priority = 0
     ORDER BY next_attempt_at LIMIT $5)
    UNION ALL
    (SELECT id FROM notifications
     WHERE channel = $1 AND status IN ('pending', 'scheduled') AND next_attempt_at <= $2 AND priority = 1
     ORDER BY next_attempt_at LIMIT $5)
    UNION ALL
    (SELECT id FROM notifications
     WHERE channel = $1 AND status IN ('pending', 'scheduled') AND next_attempt_at <= $2 AND priority = 2
     ORDER BY next_attempt_at LIMIT $5)
),
picked AS (
    SELECT n.id FROM notifications n
    WHERE n.id IN (SELECT id FROM candidates)
      AND n.status IN ('pending', 'scheduled') AND n.next_attempt_at <= $2
    ORDER BY GREATEST(n.priority - floor(extract(epoch FROM ($2 - n.next_attempt_at)) / $6)::int, 0),
             n.next_attempt_at
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE notifications n
SET status = 'processing', locked_by = $3, locked_until = $7,
    attempts = n.attempts + 1, total_attempts = n.total_attempts + 1
FROM picked
WHERE n.id = picked.id
RETURNING n.*`

// candidateFactor is how many candidates per priority are read for each
// row requested.
const candidateFactor = 4

// Claim implements queue.Queue.
func (q QueueStore) Claim(ctx context.Context, req queue.ClaimRequest) ([]*domain.Notification, error) {
	if req.Limit <= 0 {
		return nil, nil
	}
	if req.Aging <= 0 {
		return nil, errors.New("claim: aging must be positive")
	}
	rows, err := q.repo.db.Query(ctx, claimSQL,
		string(req.Channel), req.Now, req.WorkerID, req.Limit, req.Limit*candidateFactor,
		req.Aging.Seconds(), req.Now.Add(req.Lease))
	if err != nil {
		return nil, fmt.Errorf("claim notifications: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, pgx.RowToStructByPos[sqlcgen.Notification])
	if err != nil {
		return nil, fmt.Errorf("claim notifications: %w", err)
	}
	ns, err := toNotifications(claimed)
	if err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING does not preserve the picking order. Restore it,
	// since the worker hands out send slots in this order.
	sortByClaimOrder(ns, req.Now, req.Aging)
	return ns, nil
}

// sortByClaimOrder mirrors the claim query's ORDER BY.
func sortByClaimOrder(ns []*domain.Notification, now time.Time, aging time.Duration) {
	effective := func(n *domain.Notification) int64 {
		steps := int64(now.Sub(n.NextAttemptAt) / aging)
		return max(int64(n.Priority.Rank())-steps, 0)
	}
	slices.SortStableFunc(ns, func(a, b *domain.Notification) int {
		if c := cmp.Compare(effective(a), effective(b)); c != 0 {
			return c
		}
		return a.NextAttemptAt.Compare(b.NextAttemptAt)
	})
}

// Complete implements queue.Queue.
func (q QueueStore) Complete(ctx context.Context, c *queue.Completion) (queue.CompleteResult, error) {
	a := &c.Attempt
	var res queue.CompleteResult
	durationMS, err := narrow[int32](int(a.Duration.Milliseconds()), "duration_ms")
	if err != nil {
		return res, err
	}
	attemptNo, err := narrow[int32](a.AttemptNo, "attempt_no")
	if err != nil {
		return res, err
	}
	var statusCode *int32
	if a.StatusCode != 0 {
		sc, err := narrow[int32](a.StatusCode, "status_code")
		if err != nil {
			return res, err
		}
		statusCode = &sc
	}

	var rows int64
	err = pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		res = queue.CompleteResult{} // a retried transaction starts over
		qq := sqlcgen.New(tx)
		// The attempt goes first. The status update fires the outbox
		// trigger, which locks the notification's batch row until commit;
		// updating last keeps that lock to one round trip, so completions
		// of the same batch barely wait for each other.
		inserted, err := qq.InsertAttempt(ctx, sqlcgen.InsertAttemptParams{
			NotificationID:    c.NotificationID,
			AttemptNo:         attemptNo,
			Outcome:           string(a.Outcome),
			StatusCode:        statusCode,
			ErrorCode:         nilIfZero(a.ErrorCode),
			Error:             nilIfZero(a.Error),
			ProviderMessageID: nilIfZero(a.ProviderMessageID),
			DurationMs:        durationMS,
			CreatedAt:         c.Now,
		})
		if err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		res.DuplicateAttempt = inserted == 0
		switch c.Status {
		case domain.StatusSent:
			var previous string
			previous, err = qq.CompleteSent(ctx, sqlcgen.CompleteSentParams{
				ID: c.NotificationID, WorkerID: &c.WorkerID, Now: &c.Now, ProviderMessageID: nilIfZero(a.ProviderMessageID),
			})
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				err = nil
			case err == nil:
				rows = 1
				if previous != string(domain.StatusProcessing) {
					res.RecoveredFrom = domain.Status(previous)
				}
			}
		case domain.StatusPending:
			rows, err = qq.CompleteRetry(ctx, sqlcgen.CompleteRetryParams{
				ID: c.NotificationID, WorkerID: &c.WorkerID, NextAttemptAt: c.NextAttemptAt,
				LastError: nilIfZero(a.Error), LastErrorCode: nilIfZero(a.ErrorCode),
			})
		case domain.StatusFailed:
			rows, err = qq.CompleteFailed(ctx, sqlcgen.CompleteFailedParams{
				ID: c.NotificationID, WorkerID: &c.WorkerID, Now: &c.Now,
				LastError: nilIfZero(a.Error), LastErrorCode: nilIfZero(a.ErrorCode),
			})
		default:
			return fmt.Errorf("complete: unexpected status %q", c.Status)
		}
		if err != nil {
			return fmt.Errorf("update notification: %w", err)
		}
		return nil
	})
	if err != nil {
		return queue.CompleteResult{}, fmt.Errorf("complete delivery: %w", err)
	}
	res.Applied = rows == 1
	return res, nil
}

// Release implements queue.Queue.
func (q QueueStore) Release(ctx context.Context, workerID string, ids []uuid.UUID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := q.repo.q.ReleaseClaims(ctx, sqlcgen.ReleaseClaimsParams{Ids: ids, WorkerID: &workerID})
	if err != nil {
		return 0, fmt.Errorf("release claims: %w", err)
	}
	return n, nil
}

// ExtendLeases implements queue.Queue.
func (q QueueStore) ExtendLeases(ctx context.Context, workerID string, ids []uuid.UUID, until time.Time) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := q.repo.q.ExtendLeases(ctx, sqlcgen.ExtendLeasesParams{Ids: ids, WorkerID: &workerID, LockedUntil: &until})
	if err != nil {
		return 0, fmt.Errorf("extend leases: %w", err)
	}
	return n, nil
}

// ReapExpiredLeases implements queue.Queue.
func (q QueueStore) ReapExpiredLeases(ctx context.Context, now time.Time) (queue.ReapResult, error) {
	rows, err := q.repo.q.ReapExpiredLeases(ctx, now)
	if err != nil {
		return queue.ReapResult{}, fmt.Errorf("reap expired leases: %w", err)
	}
	var res queue.ReapResult
	for _, status := range rows {
		if status == string(domain.StatusFailed) {
			res.Failed++
		} else {
			res.Requeued++
		}
	}
	return res, nil
}

// notifyChannel is the LISTEN channel the outbox relay signals (NotifyDue)
// when work becomes due; the payload is the notification channel.
const notifyChannel = "notification_due"

// ListenDue calls onDue whenever a notification becomes due on a channel.
// It holds one dedicated connection and blocks until ctx is cancelled,
// reconnecting with backoff after failures (reported via onErr). Missed
// signals are harmless: workers also poll, so LISTEN only cuts latency.
func (q QueueStore) ListenDue(ctx context.Context, onDue func(domain.Channel), onErr func(error)) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := q.listen(ctx, onDue)
		if ctx.Err() != nil {
			return
		}
		onErr(err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

func (q QueueStore) listen(ctx context.Context, onDue func(domain.Channel)) error {
	conn, err := q.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire listen connection: %w", err)
	}
	// A connection that has executed LISTEN must not go back to the pool,
	// where another caller would inherit the subscription, so take it out
	// of the pool for good. (Hijack must run here, not inside a defer
	// expression: defer evaluates its receiver immediately.)
	pc := conn.Hijack()
	defer pc.Close(context.WithoutCancel(ctx))

	if _, err := pc.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	for {
		n, err := pc.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		if ch, err := domain.ParseChannel(n.Payload); err == nil {
			onDue(ch)
		}
	}
}
