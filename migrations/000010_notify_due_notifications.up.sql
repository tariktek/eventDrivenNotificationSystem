-- Wake workers immediately when work becomes due, instead of them polling
-- the table in a tight loop. Workers LISTEN on 'notification_due'; the
-- payload is the channel. PostgreSQL delivers identical notifications of one
-- transaction only once, so a 1000-item batch insert sends at most three
-- (one per channel). Scheduled and backed-off notifications are not due yet
-- and send nothing; the workers' fallback poll picks them up on time.
CREATE OR REPLACE FUNCTION notify_due_notification() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'pending' AND NEW.next_attempt_at <= now() THEN
        PERFORM pg_notify('notification_due', NEW.channel);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER notifications_notify_due
    AFTER INSERT OR UPDATE OF status, next_attempt_at ON notifications
    FOR EACH ROW EXECUTE FUNCTION notify_due_notification();
