-- The metrics sampler counts deliveries finished in the last 1/5/15
-- minutes and computes latency percentiles every few seconds. Partial
-- indexes on the finish times keep that to a range scan of recent rows
-- instead of a scan of the whole table.
CREATE INDEX notifications_sent_at_idx   ON notifications (sent_at)   WHERE sent_at   IS NOT NULL;
CREATE INDEX notifications_failed_at_idx ON notifications (failed_at) WHERE failed_at IS NOT NULL;
