-- Aggregates for the metrics sampler. Run every few seconds, not per
-- request; each is an index range scan.

-- oldest_due is the earliest next_attempt_at among queued rows that are
-- already due: how long due work has been waiting. That includes
-- scheduled rows whose time has come (they stay "scheduled" until a worker
-- claims them, so with the workers down they would otherwise never look
-- late). Future (backed-off or scheduled) work is not "waiting".
-- name: QueueDepth :many
SELECT channel, priority, status, count(*)::bigint AS total,
       -- now when nothing is due: zero wait.
       coalesce(min(next_attempt_at) FILTER (WHERE status IN ('pending', 'scheduled') AND next_attempt_at <= sqlc.arg(now)::timestamptz),
                sqlc.arg(now)::timestamptz)::timestamptz AS oldest_due
FROM notifications
WHERE status IN ('pending', 'scheduled', 'processing')
GROUP BY channel, priority, status;

-- name: DeliveryWindows :many
SELECT channel,
       count(*) FILTER (WHERE sent_at   >= sqlc.arg(now)::timestamptz - interval '1 minute')::bigint   AS sent_1m,
       count(*) FILTER (WHERE sent_at   >= sqlc.arg(now)::timestamptz - interval '5 minutes')::bigint  AS sent_5m,
       count(*) FILTER (WHERE sent_at   >= sqlc.arg(now)::timestamptz - interval '15 minutes')::bigint AS sent_15m,
       count(*) FILTER (WHERE failed_at >= sqlc.arg(now)::timestamptz - interval '1 minute')::bigint   AS failed_1m,
       count(*) FILTER (WHERE failed_at >= sqlc.arg(now)::timestamptz - interval '5 minutes')::bigint  AS failed_5m,
       count(*) FILTER (WHERE failed_at >= sqlc.arg(now)::timestamptz - interval '15 minutes')::bigint AS failed_15m
FROM notifications
WHERE sent_at >= sqlc.arg(now)::timestamptz - interval '15 minutes'
   OR failed_at >= sqlc.arg(now)::timestamptz - interval '15 minutes'
GROUP BY channel;

-- Delivery latency of the last 15 minutes: from creation, or from the
-- scheduled time for scheduled notifications, to acceptance. Per channel
-- and over all channels ("all"): percentiles cannot be combined later.
-- The "all" row exists even with no deliveries (total 0, percentiles 0).
-- name: DeliveryLatency :many
SELECT coalesce(channel, 'all')::text AS channel,
       count(*)::bigint AS total,
       coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - coalesce(scheduled_at, created_at))), 0)::float8 AS p50,
       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - coalesce(scheduled_at, created_at))), 0)::float8 AS p95,
       coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - coalesce(scheduled_at, created_at))), 0)::float8 AS p99
FROM notifications
WHERE sent_at >= sqlc.arg(now)::timestamptz - interval '15 minutes'
GROUP BY GROUPING SETS ((channel), ());

-- name: PendingCallbacks :one
SELECT count(*)::bigint FROM callback_deliveries WHERE status = 'pending';
