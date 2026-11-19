package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// retentionLockKey is the advisory lock that lets one worker at a time
// run retention.
const retentionLockKey int64 = 0x726574656e74 // "retent"

// TryRetentionLock takes the retention lock for the current transaction and
// reports whether it got it. Call it inside InTx.
func (r *Repo) TryRetentionLock(ctx context.Context) (bool, error) {
	ok, err := r.q.TryRetentionLock(ctx, retentionLockKey)
	if err != nil {
		return false, fmt.Errorf("try retention lock: %w", err)
	}
	return ok, nil
}

// DeleteExpiredNotifications deletes up to limit notifications that reached
// a final status before the cutoff, oldest first, with their attempts and
// callback deliveries.
func (r *Repo) DeleteExpiredNotifications(ctx context.Context, before time.Time, limit int) (int64, error) {
	n, err := narrow[int32](limit, "limit")
	if err != nil {
		return 0, err
	}
	deleted, err := r.q.DeleteExpiredNotifications(ctx, sqlcgen.DeleteExpiredNotificationsParams{Before: before, MaxRows: n})
	if err != nil {
		return 0, fmt.Errorf("delete expired notifications: %w", err)
	}
	return deleted, nil
}

// DeleteEmptyBatches deletes up to limit batches created before the cutoff
// that no longer have any notification.
func (r *Repo) DeleteEmptyBatches(ctx context.Context, before time.Time, limit int) (int64, error) {
	n, err := narrow[int32](limit, "limit")
	if err != nil {
		return 0, err
	}
	deleted, err := r.q.DeleteEmptyBatches(ctx, sqlcgen.DeleteEmptyBatchesParams{Before: before, MaxRows: n})
	if err != nil {
		return 0, fmt.Errorf("delete empty batches: %w", err)
	}
	return deleted, nil
}

// DeleteExpiredIdempotencyKeys deletes up to limit idempotency keys whose
// TTL ended before now.
func (r *Repo) DeleteExpiredIdempotencyKeys(ctx context.Context, now time.Time, limit int) (int64, error) {
	n, err := narrow[int32](limit, "limit")
	if err != nil {
		return 0, err
	}
	deleted, err := r.q.DeleteExpiredIdempotencyKeys(ctx, sqlcgen.DeleteExpiredIdempotencyKeysParams{Now: now, MaxRows: n})
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return deleted, nil
}
