-- Status callbacks. A row per final status change of a notification with a
-- callback_url, written by the outbox trigger in the transaction of the
-- change (migration 19); delivery state lives here, so retries survive
-- restarts.

-- Claim due callbacks. Pushing next_attempt_at forward by the lease hides
-- them from other dispatchers; if this one dies, they become due again
-- when the lease runs out. attempts doubles as the fencing token.
--
-- One notification's callbacks go out in event order: a callback is not
-- claimed while an earlier one of the same notification is still pending
-- (being sent or waiting for a retry). Otherwise a failed callback waiting
-- for its retry could reach the client after the sent callback of a
-- successful redrive, and the client would move the notification back to
-- failed. A callback that gave up (failed) no longer holds the next one.
--
-- Since callbacks are created by the status change itself (migration 19),
-- every earlier callback exists before a later one does, so this rule
-- sees all of them. As a second guard, a callback is never claimed once a
-- newer one of the same notification was delivered: the client already
-- has a later state, and this one would move it backwards.
-- name: ClaimCallbacks :many
UPDATE callback_deliveries c
SET attempts = c.attempts + 1, next_attempt_at = @lease_until
WHERE c.id IN (
    SELECT d.id FROM callback_deliveries d
    WHERE d.status = 'pending' AND d.next_attempt_at <= @now AND d.attempts < @max_attempts
      AND NOT EXISTS (
          SELECT 1 FROM callback_deliveries e
          WHERE e.notification_id = d.notification_id AND e.status = 'pending' AND e.event_id < d.event_id)
      AND NOT EXISTS (
          SELECT 1 FROM callback_deliveries e
          WHERE e.notification_id = d.notification_id AND e.status = 'delivered' AND e.event_id > d.event_id)
    ORDER BY d.next_attempt_at
    LIMIT @max_rows
    FOR UPDATE SKIP LOCKED
)
RETURNING c.id, c.notification_id, c.event_id, c.event_type, c.url, c.payload, c.attempts;

-- A claim whose outcome was never recorded (the dispatcher crashed or
-- could not write) comes back after its lease. Once it has used every
-- attempt it must not be sent again: give up on it.
-- name: ExpireExhaustedCallbacks :execrows
UPDATE callback_deliveries
SET status = 'failed', last_error = coalesce(last_error, 'attempts exhausted without a recorded outcome')
WHERE status = 'pending' AND attempts >= @max_attempts AND next_attempt_at <= @now;

-- Pending callbacks of a notification whose newer callback was already
-- delivered will never be claimed (see ClaimCallbacks): close them as
-- superseded instead of leaving them pending.
-- name: SupersedeCallbacks :execrows
UPDATE callback_deliveries d
SET status = 'superseded', last_error = 'a newer callback of this notification was already delivered'
WHERE d.status = 'pending'
  AND EXISTS (
      SELECT 1 FROM callback_deliveries e
      WHERE e.notification_id = d.notification_id AND e.status = 'delivered' AND e.event_id > d.event_id);

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
