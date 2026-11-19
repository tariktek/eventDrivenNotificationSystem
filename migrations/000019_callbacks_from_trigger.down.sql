-- Callbacks go back to being queued from the event stream (migration 15's
-- trigger). Superseded callbacks were never going to be sent: failed.
UPDATE callback_deliveries SET status = 'failed' WHERE status = 'superseded';

ALTER TABLE callback_deliveries
    DROP CONSTRAINT callback_deliveries_status_check,
    ADD CONSTRAINT callback_deliveries_status_check
        CHECK (status IN ('pending', 'delivered', 'failed'));

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
