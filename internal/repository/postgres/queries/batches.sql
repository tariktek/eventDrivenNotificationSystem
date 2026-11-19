-- name: InsertBatch :exec
INSERT INTO batches (id, total, metadata, correlation_id, created_at)
VALUES (@id, @total, @metadata, @correlation_id, @created_at);

-- name: GetBatch :one
SELECT * FROM batches WHERE id = $1;

-- name: BatchExists :one
SELECT EXISTS (SELECT 1 FROM batches WHERE id = $1);

-- name: CountBatchStatuses :many
SELECT status, count(*)::int AS count
FROM notifications
WHERE batch_id = $1
GROUP BY status;

-- A batch whose items all existed already (every idempotency_key was a
-- duplicate) creates no notification, so no status change will ever
-- complete it: it is complete as soon as it is stored. Runs in the
-- batch's own transaction, after its notifications were inserted.
-- name: CompleteEmptyBatch :one
UPDATE batches
SET completed_at = @now
WHERE id = @id AND created_count = 0 AND completed_at IS NULL
RETURNING id, total, created_count, completed_at;
