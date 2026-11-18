-- Outbox relay. Only one relay publishes at a time (transaction-scoped
-- advisory lock), so events leave in id order even with several workers.

-- name: TryOutboxLock :one
SELECT pg_try_advisory_xact_lock(@lock_key::bigint) AS locked;

-- name: ListUnpublishedEvents :many
SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY id
LIMIT @max_rows;

-- name: MarkEventsPublished :exec
UPDATE outbox_events SET published_at = @now WHERE id = ANY(@ids::bigint[]);

-- Deletes in bounded chunks so one prune never holds locks for long.
-- name: PrunePublishedEvents :execrows
DELETE FROM outbox_events
WHERE id IN (
    SELECT e.id FROM outbox_events e
    WHERE e.published_at < sqlc.arg(before)::timestamptz
    ORDER BY e.published_at
    LIMIT sqlc.arg(max_rows)::int
);

-- name: OutboxBacklog :one
SELECT count(*) AS events, coalesce(min(created_at), now())::timestamptz AS oldest -- now() when empty: zero lag
FROM outbox_events
WHERE published_at IS NULL;

-- Wakes workers listening on notification_due for these channels. Sent
-- by the relay once per round: NOTIFY serializes the commits of every
-- transaction that uses it, so it must stay out of request transactions.
-- name: NotifyDue :exec
SELECT pg_notify('notification_due', ch) FROM unnest(@channels::text[]) AS ch;

-- Batch progress, maintained by the relay from the status changes it
-- publishes. delta is the change in finished (sent/failed/cancelled)
-- items; a redrive makes it negative and reopens the batch.
-- name: AddBatchFinished :many
UPDATE batches b
SET finished_count = b.finished_count + d.delta,
    completed_at   = CASE WHEN b.finished_count + d.delta >= b.created_count
                          THEN coalesce(b.completed_at, sqlc.arg(now)::timestamptz) END
FROM (SELECT unnest(sqlc.arg(ids)::uuid[]) AS id, unnest(sqlc.arg(deltas)::int[]) AS delta) d
WHERE b.id = d.id
RETURNING b.id, b.total, b.created_count, b.finished_count, d.delta::int AS delta, b.completed_at;

-- name: BatchStatusCounts :one
SELECT coalesce(jsonb_object_agg(status, c), '{}'::jsonb)::jsonb AS counts
FROM (SELECT status, count(*) AS c FROM notifications WHERE batch_id = $1 GROUP BY status) s;

-- name: InsertOutboxEvent :exec
INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
VALUES (@aggregate_type, @aggregate_id, @event_type, @payload);
