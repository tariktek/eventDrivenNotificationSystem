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
