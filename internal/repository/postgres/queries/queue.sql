-- The claim query is hand-written in queue.go (dynamic candidate union).

-- Heartbeat: keep leases alive while deliveries are in flight. Only leases
-- still held by this worker are extended.
-- name: ExtendLeases :execrows
UPDATE notifications
SET locked_until = @locked_until
WHERE id = ANY(@ids::uuid[]) AND locked_by = @worker_id AND status = 'processing';

-- Recover work whose lease expired. The attempt was already counted at claim
-- time, so a notification that has used all its attempts fails instead of
-- being retried: if processing it crashes the worker every time, it would
-- otherwise loop forever.
-- name: ReapExpiredLeases :many
UPDATE notifications
SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
    failed_at = CASE WHEN attempts >= max_attempts THEN @now::timestamptz END,
    next_attempt_at = @now,
    locked_by = NULL,
    locked_until = NULL,
    last_error = 'worker lease expired before delivery completed',
    last_error_code = 'lease_expired'
WHERE status = 'processing' AND locked_until < @now
RETURNING status;
