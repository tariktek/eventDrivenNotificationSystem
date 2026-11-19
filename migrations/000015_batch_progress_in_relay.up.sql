-- Batch completion is counted by the outbox relay instead of the trigger.
--
-- The trigger locked the batch row in every transaction that finished one
-- of its notifications. In a flash sale the notifications finishing at the
-- same moment mostly belong to the same batch, so every delivery of every
-- channel and worker queued on that one row, one commit at a time: the
-- load test drained 50,000 notifications at ~52/s per channel instead of
-- the 100/s limit, and the waiting transactions held connections the
-- claims needed. The relay sees every status change anyway, is the only
-- writer, and updates each batch once per round, in the transaction that
-- marks the events published (so each change is counted exactly once).
-- created_count is still set by the insert trigger, in the batch's own
-- transaction, where nothing else contends for the row.
CREATE OR REPLACE FUNCTION notifications_outbox_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
    SELECT 'notification', n.id, 'notification.status_changed', notification_event_payload(n::notifications, o.status)
    FROM new_rows n JOIN old_rows o ON o.id = n.id
    WHERE n.status <> o.status
    ORDER BY n.id;
    RETURN NULL;
END;
$$;

-- Hand the count over without counting anything twice. Status changes
-- committed under migration 14 were already counted by its trigger; the
-- relay would count those whose events are still unpublished a second
-- time, and could announce a batch as completed before all its items are.
-- So each such batch starts from its true finished count minus the
-- changes still waiting in the outbox: once the relay publishes them, the
-- count lands exactly on the truth. completed_at is left alone; the relay
-- announces a batch only when completed_at goes from empty to set, so a
-- batch the old trigger already announced is not announced again.
--
-- The lock makes the handover atomic: transactions that already counted
-- under the old trigger commit before this runs, and none counts under it
-- afterwards.
LOCK TABLE batches IN SHARE ROW EXCLUSIVE MODE;

WITH waiting AS (
    SELECT (payload->>'batch_id')::uuid AS batch_id,
           sum((payload->>'status' IN ('sent', 'failed', 'cancelled'))::int
             - (payload->>'previous_status' IN ('sent', 'failed', 'cancelled'))::int)::int AS delta
    FROM outbox_events
    WHERE published_at IS NULL
      AND event_type = 'notification.status_changed'
      AND payload->>'batch_id' IS NOT NULL
    GROUP BY 1
),
actual AS (
    SELECT batch_id, count(*) FILTER (WHERE status IN ('sent', 'failed', 'cancelled'))::int AS finished
    FROM notifications
    WHERE batch_id IN (SELECT batch_id FROM waiting)
    GROUP BY batch_id
)
UPDATE batches b
SET finished_count = greatest(a.finished - w.delta, 0)
FROM waiting w JOIN actual a USING (batch_id)
WHERE b.id = w.batch_id;
