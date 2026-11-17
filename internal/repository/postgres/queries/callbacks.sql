-- Status callbacks. A row per (event, notification with a callback_url);
-- delivery state lives here, so retries survive restarts.

-- Enqueue only if the notification has a callback_url; a redelivered
-- event hits the unique event_id and does nothing.
-- name: EnqueueCallback :execrows
INSERT INTO callback_deliveries (notification_id, event_id, event_type, url, payload)
SELECT n.id, @event_id, @event_type, n.callback_url, @payload
FROM notifications n
WHERE n.id = @notification_id AND n.callback_url IS NOT NULL
ON CONFLICT (event_id) DO NOTHING;

-- Claim due callbacks. Pushing next_attempt_at forward by the lease hides
-- them from other dispatchers; if this one dies, they become due again
-- when the lease runs out. attempts doubles as the fencing token.
-- name: ClaimCallbacks :many
UPDATE callback_deliveries c
SET attempts = c.attempts + 1, next_attempt_at = @lease_until
WHERE c.id IN (
    SELECT d.id FROM callback_deliveries d
    WHERE d.status = 'pending' AND d.next_attempt_at <= @now
    ORDER BY d.next_attempt_at
    LIMIT @max_rows
    FOR UPDATE SKIP LOCKED
)
RETURNING c.id, c.notification_id, c.event_id, c.event_type, c.url, c.payload, c.attempts;

-- name: MarkCallbackDelivered :execrows
UPDATE callback_deliveries
SET status = 'delivered', delivered_at = @now, last_status_code = @status_code, last_error = NULL
WHERE id = @id AND attempts = @attempts AND status = 'pending';

-- name: MarkCallbackRetry :execrows
UPDATE callback_deliveries
SET next_attempt_at = @next_attempt_at, last_status_code = @status_code, last_error = @last_error
WHERE id = @id AND attempts = @attempts AND status = 'pending';

-- name: MarkCallbackFailed :execrows
UPDATE callback_deliveries
SET status = 'failed', last_status_code = @status_code, last_error = @last_error
WHERE id = @id AND attempts = @attempts AND status = 'pending';

-- name: ListCallbackDeliveries :many
SELECT * FROM callback_deliveries WHERE notification_id = $1 ORDER BY id;
