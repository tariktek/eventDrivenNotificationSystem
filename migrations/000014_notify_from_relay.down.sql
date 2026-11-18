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
