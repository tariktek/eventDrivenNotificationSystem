-- API-level idempotency (Idempotency-Key header).
--
-- A row is inserted (INSERT ... ON CONFLICT DO NOTHING) before the request is
-- processed, so two concurrent requests with the same key cannot both run.
-- While processing, response_status is NULL; afterwards the response is
-- stored and replayed for retries. request_hash covers method, path and body,
-- so reusing a key for a different request is detected.
CREATE TABLE idempotency_keys (
    key              text        PRIMARY KEY,
    request_hash     text        NOT NULL,
    response_status  integer,
    response_body    jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NOT NULL,

    CONSTRAINT idempotency_keys_response_complete
        CHECK ((response_status IS NULL) = (response_body IS NULL))
);

-- Retention job deletes expired keys.
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);
