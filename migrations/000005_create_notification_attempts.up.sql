-- One row per delivery attempt: the audit trail behind a notification's
-- current status, shown in GET /notifications/{id}.
CREATE TABLE notification_attempts (
    id                   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id      uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no           integer     NOT NULL CHECK (attempt_no >= 1),
    outcome              text        NOT NULL CHECK (outcome IN ('success', 'transient_error', 'permanent_error')),
    status_code          integer,                       -- HTTP status; NULL on network errors
    error_code           text,
    error                text,
    provider_message_id  text,
    duration_ms          integer     NOT NULL CHECK (duration_ms >= 0),
    created_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT notification_attempts_unique_attempt UNIQUE (notification_id, attempt_no)
);
