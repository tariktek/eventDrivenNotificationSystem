DROP TRIGGER IF EXISTS notifications_outbox_update ON notifications;
DROP TRIGGER IF EXISTS notifications_outbox_insert ON notifications;
DROP FUNCTION IF EXISTS notifications_outbox_update();
DROP FUNCTION IF EXISTS notifications_outbox_insert();
DROP FUNCTION IF EXISTS notification_event_payload(notifications, text);
ALTER TABLE batches DROP COLUMN IF EXISTS finished_count, DROP COLUMN IF EXISTS created_count;
