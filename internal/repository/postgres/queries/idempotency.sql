-- Claim a free key. Returns no row if the key already exists.
-- name: InsertIdempotencyKey :one
INSERT INTO idempotency_keys (key, request_hash, lock_token, created_at, expires_at)
VALUES (@key, @request_hash, @lock_token, @now, @expires_at)
ON CONFLICT (key) DO NOTHING
RETURNING key;

-- Take over a key that expired, or whose owner has been "in progress" for
-- too long with the same request (its process presumably died). The new
-- lock_token fences the previous owner out. Returns no row if neither applies.
-- name: TakeOverIdempotencyKey :one
UPDATE idempotency_keys
SET request_hash = @request_hash, lock_token = @lock_token, response_status = NULL, response_body = NULL,
    created_at = @now, expires_at = @expires_at
WHERE key = @key
  AND (expires_at <= @now
       OR (response_status IS NULL AND created_at < @stale_before AND request_hash = @request_hash))
RETURNING key;

-- name: GetIdempotencyKey :one
SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE key = $1;

-- Only the current owner may complete the key.
-- name: CompleteIdempotencyKey :execrows
UPDATE idempotency_keys
SET response_status = @response_status, response_body = @response_body
WHERE key = @key AND lock_token = @lock_token AND response_status IS NULL;

-- Only the current owner may release the key, and only while unfinished.
-- name: ReleaseIdempotencyKey :exec
DELETE FROM idempotency_keys WHERE key = @key AND lock_token = @lock_token AND response_status IS NULL;
