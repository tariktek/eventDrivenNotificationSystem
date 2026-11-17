-- Status callbacks to the client's callback_url, with their own retry state.
-- The event payload is copied here (not referenced) because published outbox
-- events are deleted by the retention job.
CREATE TABLE callback_deliveries (
    id                bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id   uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    event_type        text        NOT NULL,
    payload           jsonb       NOT NULL,
    status            text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at   timestamptz NOT NULL DEFAULT now(),
    last_status_code  integer,
    last_error        text,
    delivered_at      timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    -- The relay delivers at least once; this keeps a redelivered event from
    -- producing a second callback.
    CONSTRAINT callback_deliveries_unique_event UNIQUE (notification_id, event_type)
);

CREATE INDEX callback_deliveries_due_idx
    ON callback_deliveries (next_attempt_at) WHERE status = 'pending';

CREATE TRIGGER callback_deliveries_set_updated_at
    BEFORE UPDATE ON callback_deliveries
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
