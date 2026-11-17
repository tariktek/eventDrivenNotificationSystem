-- Transactional outbox. Every state change writes its domain event here in
-- the same transaction as the change itself, so an event exists if and only
-- if the change committed. A relay publishes unpublished rows to Redis
-- Streams in id order and stamps published_at.
CREATE TABLE outbox_events (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type  text        NOT NULL CHECK (aggregate_type IN ('notification', 'batch')),
    aggregate_id    uuid        NOT NULL,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz
);

-- Relay: unpublished events in order. Partial, so it holds only the backlog.
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

-- Retention job deletes old published events.
CREATE INDEX outbox_events_published_at_idx ON outbox_events (published_at) WHERE published_at IS NOT NULL;
