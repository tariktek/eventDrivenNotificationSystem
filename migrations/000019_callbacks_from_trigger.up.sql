-- Status callbacks are queued by the status change itself.
--
-- They used to go through the event stream: the relay published each
-- event to Redis and a consumer group turned final-status events into
-- callback_deliveries rows. Two flaws came with that hop:
--
--   - A callback could be lost: the stream is trimmed, and Redis persists
--     once a second, so an event the relay had marked published could be
--     gone before the group read it.
--   - Callbacks could arrive out of order: the group spreads events over
--     the workers, so a notification's older event (failed) could be
--     recorded after its newer one (sent) was already delivered, and the
--     client would see sent, then failed.
--
-- Now the outbox trigger writes the callback row in the same statement as
-- the event, in the transaction of the status change. A callback exists
-- if and only if its final status committed, and one notification's
-- callbacks are created in the order its changes commit (each waits for
-- the previous one on the row lock), so the dispatcher's "wait for the
-- earlier pending one" rule always sees every earlier callback.
CREATE OR REPLACE FUNCTION notifications_outbox_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    WITH ev AS (
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
        SELECT 'notification', n.id, 'notification.status_changed', notification_event_payload(n::notifications, o.status)
        FROM new_rows n JOIN old_rows o ON o.id = n.id
        WHERE n.status <> o.status
        ORDER BY n.id
        RETURNING id, aggregate_id, payload
    )
    INSERT INTO callback_deliveries (notification_id, event_id, event_type, url, payload)
    SELECT ev.aggregate_id, ev.id, 'notification.' || (ev.payload->>'status'), n.callback_url,
           jsonb_build_object(
               'event_id',    ev.id,
               'type',        'notification.' || (ev.payload->>'status'),
               'occurred_at', ev.payload->'occurred_at',
               'data',        ev.payload)
    FROM ev JOIN new_rows n ON n.id = ev.aggregate_id
    WHERE n.callback_url IS NOT NULL AND n.status IN ('sent', 'failed', 'cancelled')
    ORDER BY ev.id;
    RETURN NULL;
END;
$$;

-- Defense in depth against regression: a callback whose notification
-- already had a newer callback delivered is never sent ("superseded").
ALTER TABLE callback_deliveries
    DROP CONSTRAINT callback_deliveries_status_check,
    ADD CONSTRAINT callback_deliveries_status_check
        CHECK (status IN ('pending', 'delivered', 'failed', 'superseded'));
