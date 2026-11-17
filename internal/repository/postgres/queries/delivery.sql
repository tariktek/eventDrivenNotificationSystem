-- Delivery outcomes. Each update only applies while this worker still holds
-- the lease, so a worker whose lease expired cannot overwrite the state a
-- newer owner set.

-- A sent message is recorded even if our lease expired and the reaper put
-- the notification back to pending (not yet re-claimed): it did go out, and
-- leaving it pending would send it a second time.
-- name: CompleteSent :execrows
UPDATE notifications
SET status = 'sent', sent_at = @now, provider_message_id = @provider_message_id,
    locked_by = NULL, locked_until = NULL, last_error = NULL, last_error_code = NULL
WHERE id = @id
  AND ((status = 'processing' AND locked_by = @worker_id) OR (status = 'pending' AND locked_by IS NULL));

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
-- name: InsertAttempt :exec
INSERT INTO notification_attempts
    (notification_id, attempt_no, outcome, status_code, error_code, error, provider_message_id, duration_ms, created_at)
VALUES (@notification_id, @attempt_no, @outcome, @status_code, @error_code, @error, @provider_message_id, @duration_ms, @created_at)
ON CONFLICT (notification_id, attempt_no) DO NOTHING;

-- Graceful shutdown: hand back work that was claimed but never sent. The
-- attempt counted at claim time is given back, since nothing was tried.
-- name: ReleaseClaims :execrows
UPDATE notifications
SET status = 'pending', attempts = attempts - 1, locked_by = NULL, locked_until = NULL
WHERE id = ANY(@ids::uuid[]) AND status = 'processing' AND locked_by = @worker_id;
