DROP INDEX IF EXISTS callback_deliveries_notification_idx;
ALTER TABLE callback_deliveries
    DROP CONSTRAINT callback_deliveries_unique_event,
    DROP COLUMN url,
    DROP COLUMN event_id,
    ADD CONSTRAINT callback_deliveries_unique_event UNIQUE (notification_id, event_type);
