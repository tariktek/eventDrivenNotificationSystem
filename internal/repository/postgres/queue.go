package postgres

import (
	"context"
	"errors"
	"fmt"
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
//  3. The UPDATE moves the picked rows to processing under our lease.
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
SET status = 'processing', locked_by = $3, locked_until = $7, attempts = n.attempts + 1
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
	return toNotifications(claimed)
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

// notifyChannel is the LISTEN channel the notify_due_notification trigger
// signals; the payload is the notification channel.
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
