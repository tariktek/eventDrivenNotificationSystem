TRUNCATE idempotency_keys;

ALTER TABLE idempotency_keys
    DROP COLUMN lock_token,
    ALTER COLUMN response_body TYPE jsonb USING NULL;
