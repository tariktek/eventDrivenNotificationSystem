-- Notifications are both the system of record and the work queue.
--
-- Queue mechanics: workers claim rows whose status is pending/scheduled and
-- whose next_attempt_at has passed, using SELECT ... FOR UPDATE SKIP LOCKED.
-- next_attempt_at is the single "when is this due" column for first
-- delivery, retries (backoff) and scheduled sends alike.
--
-- Lease: a claimed row is 'processing' with locked_by/locked_until set. If
-- the worker dies, the reaper returns rows with an expired lease to pending.
CREATE TABLE notifications (
    id                   uuid        PRIMARY KEY,           -- UUIDv7, time-ordered
    batch_id             uuid        REFERENCES batches (id),

    channel              text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    priority             smallint    NOT NULL DEFAULT 1 CHECK (priority BETWEEN 0 AND 2), -- 0 high, 1 normal, 2 low
    recipient            text        NOT NULL,
    subject              text,
    content              text        NOT NULL,              -- final, rendered content
    segments             smallint    CHECK (segments BETWEEN 1 AND 6),  -- SMS only

    template_id          uuid        REFERENCES templates (id),
    template_version     integer,
    template_vars        jsonb,

    status               text        NOT NULL CHECK (status IN
                             ('pending', 'scheduled', 'processing', 'sent', 'failed', 'cancelled')),
    idempotency_key      text,

    attempts             integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts         integer     NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    next_attempt_at      timestamptz NOT NULL DEFAULT now(),
    locked_by            text,
    locked_until         timestamptz,

    scheduled_at         timestamptz,
    sent_at              timestamptz,
    failed_at            timestamptz,
    cancelled_at         timestamptz,

    provider_message_id  text,
    last_error           text,
    last_error_code      text,

    callback_url         text,
    metadata             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    correlation_id       text,
    trace_parent         text,

    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),

    -- Invariants enforced by the database, independent of application code.
    CONSTRAINT notifications_email_requires_subject
        CHECK (channel <> 'email' OR subject IS NOT NULL),
    CONSTRAINT notifications_template_version_with_template
        CHECK ((template_id IS NULL) = (template_version IS NULL)),
    CONSTRAINT notifications_processing_has_lease
        CHECK (status <> 'processing' OR (locked_by IS NOT NULL AND locked_until IS NOT NULL)),
    CONSTRAINT notifications_sent_has_timestamp
        CHECK (status <> 'sent' OR sent_at IS NOT NULL),
    CONSTRAINT notifications_failed_has_timestamp
        CHECK (status <> 'failed' OR failed_at IS NOT NULL),
    CONSTRAINT notifications_cancelled_has_timestamp
        CHECK (status <> 'cancelled' OR cancelled_at IS NOT NULL),
    CONSTRAINT notifications_metadata_is_object
        CHECK (jsonb_typeof(metadata) = 'object')
);

-- Idempotency: one notification per client-supplied key.
CREATE UNIQUE INDEX notifications_idempotency_key_uidx
    ON notifications (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Claim query: per channel, highest priority first, oldest due first. Partial,
-- so it only contains claimable rows and stays small however large the
-- table grows.
CREATE INDEX notifications_queue_idx
    ON notifications (channel, priority, next_attempt_at)
    WHERE status IN ('pending', 'scheduled');

-- Reaper: find processing rows whose lease expired.
CREATE INDEX notifications_lease_idx
    ON notifications (locked_until) WHERE status = 'processing';

-- Listing with keyset (cursor) pagination on (created_at, id), optionally
-- filtered by status or channel.
CREATE INDEX notifications_created_idx         ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_status_created_idx  ON notifications (status, created_at DESC, id DESC);
CREATE INDEX notifications_channel_created_idx ON notifications (channel, created_at DESC, id DESC);

CREATE INDEX notifications_batch_idx ON notifications (batch_id) WHERE batch_id IS NOT NULL;

CREATE TRIGGER notifications_set_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
