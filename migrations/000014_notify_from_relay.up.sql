-- Wake-ups move from a per-row trigger to the outbox relay.
--
-- A transaction that runs NOTIFY takes a database-wide lock while it
-- commits, so every transaction that notifies commits one at a time. With
-- the trigger, that was every API request creating a notification: under
-- load (k6, 150 creates/s) the median create took 855 ms instead of 27 ms
-- and throughput capped at about 85/s. The relay already sees every
-- "created" and "back to pending" event within ~100 ms; it now sends at
-- most one NOTIFY per channel per round, from its own transaction, and
-- API commits never touch the lock. See loadtest/README.md.
DROP TRIGGER IF EXISTS notifications_notify_due ON notifications;
DROP FUNCTION IF EXISTS notify_due_notification();
