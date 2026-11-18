-- Restores counting batch completion in the trigger (migration 11).
CREATE OR REPLACE FUNCTION notifications_outbox_update() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    b record;
BEGIN
    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
    SELECT 'notification', n.id, 'notification.status_changed', notification_event_payload(n::notifications, o.status)
    FROM new_rows n JOIN old_rows o ON o.id = n.id
    WHERE n.status <> o.status
    ORDER BY n.id;

    -- Batches whose number of finished items changes. Lock them in id
    -- order so two statements touching the same batches cannot deadlock.
    -- Two workers finishing a batch's last two items serialize here, and
    -- the second one sees the first one's count.
    PERFORM 1 FROM batches
    WHERE id IN (
        SELECT n.batch_id FROM new_rows n JOIN old_rows o ON o.id = n.id
        WHERE n.batch_id IS NOT NULL
          AND (n.status IN ('sent', 'failed', 'cancelled')) <> (o.status IN ('sent', 'failed', 'cancelled')))
    ORDER BY id FOR UPDATE;

    -- Net change of finished items per batch. A redrive (failed → pending)
    -- makes it negative and reopens the batch.
    FOR b IN
        WITH delta AS (
            SELECT n.batch_id,
                   sum((n.status IN ('sent', 'failed', 'cancelled'))::int
                     - (o.status IN ('sent', 'failed', 'cancelled'))::int)::int AS d
            FROM new_rows n JOIN old_rows o ON o.id = n.id
            WHERE n.batch_id IS NOT NULL AND n.status <> o.status
            GROUP BY n.batch_id
        )
        UPDATE batches bt
        SET finished_count = bt.finished_count + delta.d,
            completed_at   = CASE WHEN bt.finished_count + delta.d >= bt.created_count THEN now() END
        FROM delta
        WHERE bt.id = delta.batch_id AND delta.d <> 0
        RETURNING bt.id, bt.total, bt.created_count, bt.finished_count, bt.completed_at, delta.d
    LOOP
        IF b.d > 0 AND b.finished_count >= b.created_count THEN
            -- A separate statement, so its snapshot (taken after the lock
            -- above) sees every committed status of the batch.
            INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
            SELECT 'batch', b.id, 'batch.completed', jsonb_build_object(
                'batch_id',     b.id,
                'total',        b.total,
                'created',      b.created_count,
                'counts',       (SELECT jsonb_object_agg(status, c)
                                 FROM (SELECT status, count(*) AS c FROM notifications
                                       WHERE batch_id = b.id GROUP BY status) s),
                'completed_at', b.completed_at,
                'occurred_at',  now());
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;
