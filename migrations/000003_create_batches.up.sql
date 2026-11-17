-- A batch groups notifications created in one request (1..1000 items).
-- Per-status counts are computed from notifications on read, so they can
-- never drift. completed_at is set once every item reaches a final status
-- and guards against emitting batch.completed twice.
CREATE TABLE batches (
    id              uuid        PRIMARY KEY,
    total           integer     NOT NULL CHECK (total BETWEEN 1 AND 1000),
    metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    correlation_id  text,
    completed_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT batches_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX batches_created_at_idx ON batches (created_at);
