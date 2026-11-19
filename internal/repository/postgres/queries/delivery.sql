-- Delivery outcomes. Each update only applies while this worker still holds
-- the lease, so a worker whose lease expired cannot overwrite the state a
-- newer owner set.

-- A sent message is recorded even if this worker lost its lease meanwhile,
-- as long as no other worker holds the notification now: the provider is
-- the source of truth, and the message did go out. That covers a lease that
-- expired during a slow provider call, after which the reaper put the
-- notification back to pending (leaving it would send it twice) or, on its
-- last attempt, failed it (leaving it would report a delivered message as
-- failed, and a redrive would send it again), and a cancel that came after
-- the reaper. Returns the status it replaced; no row if nothing changed.
-- name: CompleteSent :one
WITH cur AS (
    SELECT c.id, c.status AS previous_status FROM notifications c WHERE c.id = sqlc.arg(id) FOR UPDATE
)
UPDATE notifications n
SET status = 'sent', sent_at = @now, provider_message_id = @provider_message_id,
    failed_at = NULL, cancelled_at = NULL,
    locked_by = NULL, locked_until = NULL, last_error = NULL, last_error_code = NULL
FROM cur
WHERE n.id = cur.id
  AND ((n.status = 'processing' AND n.locked_by = @worker_id)
       OR (n.status IN ('pending', 'failed', 'cancelled') AND n.locked_by IS NULL))
RETURNING cur.previous_status;

-- name: CompleteRetry :execrows
UPDATE notifications
SET status = 'pending', next_attempt_at = @next_attempt_at,
    last_error = @last_error, last_error_code = @last_error_code,
    locked_by = NULL, locked_until = NULL
WHERE id = @id AND status = 'processing' AND locked_by = @worker_id;

-- name: CompleteFailed :execrows
UPDATE notifications
SET status = 'failed', failed_at = @now,
    last_error = @last_error, last_error_code = @last_error_code,
    locked_by = NULL, locked_until = NULL
WHERE id = @id AND status = 'processing' AND locked_by = @worker_id;

-- The attempt is recorded whatever happened to the lease: it is history.
-- attempt_no comes from total_attempts, which is never reset, so a
-- conflict means the same attempt was recorded twice; it is reported to
-- the caller (0 rows) rather than failing the whole outcome.
-- name: InsertAttempt :execrows
INSERT INTO notification_attempts
    (notification_id, attempt_no, outcome, status_code, error_code, error, provider_message_id, duration_ms, created_at)
VALUES (@notification_id, @attempt_no, @outcome, @status_code, @error_code, @error, @provider_message_id, @duration_ms, @created_at)
ON CONFLICT (notification_id, attempt_no) DO NOTHING;

-- Graceful shutdown: hand back work that was claimed but never sent. The
-- attempt counted at claim time is given back, since nothing was tried
-- (its number was never used for an attempt row).
-- name: ReleaseClaims :execrows
UPDATE notifications
SET status = 'pending', attempts = attempts - 1, total_attempts = total_attempts - 1,
    locked_by = NULL, locked_until = NULL
WHERE id = ANY(@ids::uuid[]) AND status = 'processing' AND locked_by = @worker_id;
