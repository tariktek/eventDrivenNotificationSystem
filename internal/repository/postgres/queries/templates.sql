-- name: InsertTemplate :one
INSERT INTO templates (id, name, channel, subject, body, variables, created_at, updated_at)
VALUES (@id, @name, @channel, @subject, @body, @variables, @created_at, @created_at)
RETURNING *;

-- name: GetTemplate :one
SELECT * FROM templates WHERE id = $1 AND deleted_at IS NULL;

-- name: UpdateTemplate :one
UPDATE templates
SET name = @name, channel = @channel, subject = @subject, body = @body,
    variables = @variables, version = version + 1
WHERE id = @id AND deleted_at IS NULL
RETURNING *;

-- name: SoftDeleteTemplate :execrows
UPDATE templates SET deleted_at = @now WHERE id = @id AND deleted_at IS NULL;

-- Keyset pagination, newest first. The cursor is the (created_at, id) of the
-- last row of the previous page.
-- name: ListTemplates :many
SELECT * FROM templates
WHERE deleted_at IS NULL
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel))
  AND (sqlc.narg(after_created_at)::timestamptz IS NULL
       OR (created_at, id) < (sqlc.narg(after_created_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT @row_limit;
