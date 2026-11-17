-- Domain events for the transactional outbox, written by triggers.
--
-- Every change to a notification's status, whoever makes it (API, worker,
-- reaper, a bulk redrive or an operator's manual SQL), inserts its event
-- into outbox_events in the same transaction. An event exists if and only
-- if the change committed, and no code path can forget to emit one. The
-- alternative, writing the event from application code, would have to be
-- repeated in about a dozen queries, and one omission would lose events
-- without anyone noticing.
--
-- The triggers are per statement and use transition tables, so a
-- 1000-row batch insert costs one extra INSERT ... SELECT, not 1000 calls.
--
-- Event order: for a single notification, every change waits for the
-- previous one to commit (row lock), so its events have increasing ids and
-- the relay publishes them in order. Events of different notifications
-- have no defined order.

-- Batch completion is counted, not re-derived: created_count rows exist in
-- the batch (duplicates by idempotency key are not created) and
-- finished_count of them are in a final status. Counting on every
-- completion would scan the whole batch each time (O(n²) per batch).
ALTER TABLE batches
    ADD COLUMN created_count  integer NOT NULL DEFAULT 0 CHECK (created_count >= 0),
    ADD COLUMN finished_count integer NOT NULL DEFAULT 0 CHECK (finished_count >= 0);

UPDATE batches b
SET created_count  = c.created,
    finished_count = c.finished,
    completed_at   = CASE WHEN c.finished = c.created AND c.created > 0 THEN now() END
FROM (
    SELECT batch_id,
           count(*)::int AS created,
           count(*) FILTER (WHERE status IN ('sent', 'failed', 'cancelled'))::int AS finished
    FROM notifications WHERE batch_id IS NOT NULL GROUP BY batch_id
) c
WHERE b.id = c.batch_id;

-- The event body. Content and recipient are left out on purpose: events
-- reach WebSocket clients and logs, and they need the state, not the
-- personal data.
CREATE FUNCTION notification_event_payload(n notifications, previous_status text)
RETURNS jsonb LANGUAGE sql STABLE AS $$
    SELECT jsonb_build_object(
        'notification_id',     n.id,
        'batch_id',            n.batch_id,
        'channel',             n.channel,
        'priority',            CASE n.priority WHEN 0 THEN 'high' WHEN 1 THEN 'normal' ELSE 'low' END,
        'status',              n.status,
        'previous_status',     previous_status,
        'attempts',            n.attempts,
        'max_attempts',        n.max_attempts,
        'scheduled_at',        n.scheduled_at,
        'next_attempt_at',     CASE WHEN n.status IN ('pending', 'scheduled') THEN n.next_attempt_at END,
        'error_code',          n.last_error_code,
        'provider_message_id', n.provider_message_id,
        'correlation_id',      n.correlation_id,
        'occurred_at',         now()
    )
$$;

CREATE FUNCTION notifications_outbox_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
    SELECT 'notification', n.id, 'notification.created', notification_event_payload(n::notifications, NULL)
    FROM inserted n
    ORDER BY n.id;

    UPDATE batches b
    SET created_count = b.created_count + c.n
    FROM (SELECT batch_id, count(*)::int AS n FROM inserted WHERE batch_id IS NOT NULL GROUP BY batch_id) c
    WHERE b.id = c.batch_id;
    RETURN NULL;
END;
$$;

CREATE FUNCTION notifications_outbox_update() RETURNS trigger
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

CREATE TRIGGER notifications_outbox_insert
    AFTER INSERT ON notifications
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION notifications_outbox_insert();

-- Fires for every UPDATE statement (transition tables do not allow an
-- "OF status" column list). Statements that change no status, such as
-- lease heartbeats, only pay for an empty join.
CREATE TRIGGER notifications_outbox_update
    AFTER UPDATE ON notifications
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notifications_outbox_update();
