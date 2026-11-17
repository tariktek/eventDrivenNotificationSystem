-- Callbacks are keyed by the event that caused them. The previous key,
-- (notification_id, event_type), would drop the second callback of a
-- notification that failed, was redriven and failed again. The URL is
-- copied so delivery needs no join and is not affected by later changes.
ALTER TABLE callback_deliveries
    DROP CONSTRAINT callback_deliveries_unique_event,
    ADD COLUMN event_id bigint NOT NULL,
    ADD COLUMN url      text   NOT NULL,
    ADD CONSTRAINT callback_deliveries_unique_event UNIQUE (event_id);

CREATE INDEX callback_deliveries_notification_idx ON callback_deliveries (notification_id);
