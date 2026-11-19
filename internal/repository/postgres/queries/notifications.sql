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
-- last_error is kept for context; the attempts table holds the full history
-- (its rows are numbered by total_attempts, which a retry does not reset).
-- name: RetryNotification :one
UPDATE notifications
SET status = 'pending', attempts = 0, next_attempt_at = @now, failed_at = NULL
WHERE id = @id AND status = 'failed'
RETURNING *;

-- Bulk dead-letter redrive, e.g. after a provider outage. The filters are
-- optional; LIMIT keeps one call's write bounded (callers repeat until
-- nothing matches). SKIP LOCKED lets two concurrent redrives split the work
-- instead of blocking each other. Oldest first, via the status index. Like
-- a single retry, it resets the attempt budget but not total_attempts.
-- name: RetryFailedNotifications :execrows
WITH picked AS (
    SELECT id FROM notifications
    WHERE status = 'failed'
      AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
      AND (sqlc.narg(batch_id)::uuid IS NULL OR batch_id = sqlc.narg(batch_id)::uuid)
      AND (sqlc.narg(error_code)::text IS NULL OR last_error_code = sqlc.narg(error_code)::text)
      AND (sqlc.narg(failed_from)::timestamptz IS NULL OR failed_at >= sqlc.narg(failed_from)::timestamptz)
      AND (sqlc.narg(failed_to)::timestamptz IS NULL OR failed_at < sqlc.narg(failed_to)::timestamptz)
    ORDER BY created_at, id
    LIMIT @max_rows
    FOR UPDATE SKIP LOCKED
)
UPDATE notifications n
SET status = 'pending', attempts = 0, next_attempt_at = @now, failed_at = NULL
FROM picked
WHERE n.id = picked.id;

-- name: CountFailedNotifications :one
SELECT count(*) FROM notifications
WHERE status = 'failed'
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (sqlc.narg(batch_id)::uuid IS NULL OR batch_id = sqlc.narg(batch_id)::uuid)
  AND (sqlc.narg(error_code)::text IS NULL OR last_error_code = sqlc.narg(error_code)::text)
  AND (sqlc.narg(failed_from)::timestamptz IS NULL OR failed_at >= sqlc.narg(failed_from)::timestamptz)
  AND (sqlc.narg(failed_to)::timestamptz IS NULL OR failed_at < sqlc.narg(failed_to)::timestamptz);

-- name: ListAttempts :many
SELECT * FROM notification_attempts
WHERE notification_id = $1
ORDER BY attempt_no, id;
