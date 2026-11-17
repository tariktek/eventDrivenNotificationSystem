-- 1. lock_token fences the owner of an in-progress key. Without it, a request
--    whose key was taken over (stale or expired) could still delete or
--    overwrite the new owner's row when it finally finishes.
-- 2. response_body becomes bytea: jsonb reorders keys and normalises
--    whitespace, so replays were equivalent but not byte-identical.
TRUNCATE idempotency_keys; -- short-lived replay cache; safe to drop

ALTER TABLE idempotency_keys
    ADD COLUMN lock_token uuid NOT NULL,
    ALTER COLUMN response_body TYPE bytea USING NULL;
