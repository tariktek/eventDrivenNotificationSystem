-- Reusable message templates with {{.variable}} placeholders.
-- Updating a template bumps `version`; notifications record the version they
-- were rendered with. Deletion is soft so old notifications keep a valid FK.
CREATE TABLE templates (
    id          uuid        PRIMARY KEY,
    name        text        NOT NULL,
    channel     text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    subject     text,
    body        text        NOT NULL,
    variables   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    version     integer     NOT NULL DEFAULT 1 CHECK (version >= 1),
    deleted_at  timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT templates_email_requires_subject
        CHECK (channel <> 'email' OR subject IS NOT NULL),
    CONSTRAINT templates_variables_is_array
        CHECK (jsonb_typeof(variables) = 'array')
);

-- Names are unique among live templates; a deleted name can be reused.
CREATE UNIQUE INDEX templates_name_live_uidx ON templates (name) WHERE deleted_at IS NULL;

CREATE TRIGGER templates_set_updated_at
    BEFORE UPDATE ON templates
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
