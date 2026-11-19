-- Data retention. One worker at a time runs it (transaction-scoped
-- advisory lock, as for the outbox relay), and every delete is bounded so
-- no statement holds locks for long.

-- name: TryRetentionLock :one
SELECT pg_try_advisory_xact_lock(@lock_key::bigint) AS locked;

-- Final notifications past retention, oldest first. Their attempts and
-- callback deliveries go with them (ON DELETE CASCADE).
-- name: DeleteExpiredNotifications :execrows
DELETE FROM notifications
WHERE id IN (
    SELECT n.id FROM notifications n
    WHERE n.status IN ('sent', 'failed', 'cancelled') AND n.updated_at < sqlc.arg(before)::timestamptz
    ORDER BY n.updated_at
    LIMIT sqlc.arg(max_rows)::int
);

-- Batches past retention whose notifications are all gone.
-- name: DeleteEmptyBatches :execrows
DELETE FROM batches
WHERE id IN (
    SELECT b.id FROM batches b
    WHERE b.created_at < sqlc.arg(before)::timestamptz
      AND NOT EXISTS (SELECT 1 FROM notifications n WHERE n.batch_id = b.id)
    ORDER BY b.created_at
    LIMIT sqlc.arg(max_rows)::int
);

-- Idempotency keys past their TTL: no longer replayable, only taking space.
-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys
WHERE key IN (
    SELECT k.key FROM idempotency_keys k
    WHERE k.expires_at < sqlc.arg(now)::timestamptz
    ORDER BY k.expires_at
    LIMIT sqlc.arg(max_rows)::int
);
