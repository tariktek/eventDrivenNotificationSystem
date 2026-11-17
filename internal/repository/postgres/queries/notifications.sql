-- Bulk insert and filtered listing are hand-written in notifications.go:
-- they need unnest() over many typed arrays and a dynamic WHERE clause,
-- which sqlc does not model well.

-- name: GetNotification :one
SELECT * FROM notifications WHERE id = $1;

-- name: GetNotificationsByIdempotencyKeys :many
SELECT * FROM notifications WHERE idempotency_key = ANY(@keys::text[]);

-- name: GetNotificationStatus :one
SELECT status FROM notifications WHERE id = $1;

-- The WHERE clause mirrors domain.Status.IsCancellable: a concurrent worker
-- claim and a cancel cannot both succeed.
-- name: CancelNotification :one
UPDATE notifications
SET status = 'cancelled', cancelled_at = @now
WHERE id = @id AND status IN ('pending', 'scheduled')
RETURNING *;

-- name: CancelBatchNotifications :execrows
UPDATE notifications
SET status = 'cancelled', cancelled_at = @now
WHERE batch_id = @batch_id AND status IN ('pending', 'scheduled');

-- Manual retry from the dead-letter state with a fresh attempt budget.
-- last_error is kept for context; the attempts table holds the full history.
-- name: RetryNotification :one
UPDATE notifications
SET status = 'pending', attempts = 0, next_attempt_at = @now, failed_at = NULL
WHERE id = @id AND status = 'failed'
RETURNING *;

-- name: ListAttempts :many
SELECT * FROM notification_attempts
WHERE notification_id = $1
ORDER BY attempt_no;
