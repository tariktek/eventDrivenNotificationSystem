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
