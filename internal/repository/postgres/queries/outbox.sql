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
