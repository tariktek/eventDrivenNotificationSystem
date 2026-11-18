DROP INDEX IF EXISTS callback_deliveries_notification_idx;

-- The old key allows one row per (notification, event type). Keep the
-- latest of any repeats (a redriven notification that failed twice) so it
-- can be restored.
DELETE FROM callback_deliveries d
USING callback_deliveries newer
WHERE newer.notification_id = d.notification_id
  AND newer.event_type = d.event_type
  AND newer.id > d.id;

ALTER TABLE callback_deliveries
    DROP CONSTRAINT callback_deliveries_unique_event,
    DROP COLUMN url,
    DROP COLUMN event_id,
    ADD CONSTRAINT callback_deliveries_unique_event UNIQUE (notification_id, event_type);
