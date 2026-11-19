# Runbook

Operational procedures for the notification system. Examples assume the
API at `http://localhost:8080` and `X-API-Key: $KEY`.

## Provider outage: circuit breaker open

**Symptoms.** Worker logs show `circuit breaker state changed` with
`"state":"open"`. The worker's `GET /ready` reports `degraded` with
`circuit_breakers: provider failing, deliveries paused: sms=open`.
Notifications on that channel stay `pending`; other channels keep sending.

**What the system does by itself.** Each worker keeps one breaker per
channel. It opens when at least `BREAKER_MIN_REQUESTS` deliveries in
`BREAKER_WINDOW` failed at `BREAKER_FAILURE_RATIO` or more. Only transient
errors count: 5xx, 429, timeouts and connection errors. While the breaker
is open the worker does not claim that channel, so queued notifications
keep their attempt budget. Anything claimed but not yet sent is handed
back with its attempt refunded. Every `BREAKER_OPEN_TIMEOUT` it lets
`BREAKER_PROBES` deliveries through. If all of them succeed the breaker
closes and the backlog drains at the normal rate limit. No action is
needed for an outage the provider recovers from.

**What to do.**

1. Check the provider's status page, and the `last_error_code` of recent
   attempts (`GET /api/v1/notifications/{id}` → `delivery_attempts`).
2. If the outage will be long, nothing has to be paused: the backlog waits.
   The cost is one attempt per probe, roughly `PROBES` attempts every
   `OPEN_TIMEOUT` per worker. The size of the backlog does not change it.
3. Once the provider has recovered, redrive what failed during the outage
   (next section). These are notifications that ran out of attempts before
   the breaker opened, or that a failed probe pushed over the limit.

## Draining the dead-letter queue

A notification ends in `failed` (the dead-letter state) after a permanent
error (for example `http_400`, an invalid recipient) or after it has used
all `max_attempts`. Its `last_error_code` and its attempt history say which.

**Inspect.**

```bash
curl -s "localhost:8080/api/v1/notifications?status=failed&channel=sms&limit=100" -H "X-API-Key: $KEY"
```

**Redrive one.**

```bash
curl -s -X POST localhost:8080/api/v1/notifications/$ID/retry -H "X-API-Key: $KEY"
```

**Redrive in bulk after an outage.** Filter on what the outage caused,
usually transient error codes in a time window. Count first with a dry
run:

```bash
curl -s -X POST localhost:8080/api/v1/notifications/retry-failed \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"channel":"sms","error_code":"http_503",
       "failed_from":"2026-11-18T10:00:00Z","failed_to":"2026-11-18T11:00:00Z",
       "dry_run":true}'
# {"requeued":0,"remaining":5230}
```

Then run the same request without `dry_run`. Repeat it while `remaining`
is above zero; each call re-queues at most `limit` (default 1000, max
10000) notifications, oldest first:

```bash
while :; do
  r=$(curl -s -X POST localhost:8080/api/v1/notifications/retry-failed \
      -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
      -d '{"channel":"sms","error_code":"http_503","failed_from":"2026-11-18T10:00:00Z","failed_to":"2026-11-18T11:00:00Z"}')
  echo "$r"; echo "$r" | grep -q '"remaining":0' && break
done
```

Re-queued notifications get a fresh attempt budget. The per-channel rate
limit paces their delivery, so redriving a large backlog cannot overload
the provider.

**Do not redrive permanent errors blindly.** `http_400`, `http_404` and
`http_422` mean the provider rejected the message itself. Fix the cause
(recipient, content) first, or the redrive only fails again.

Unknown fields in the request body are rejected with `400`. A misspelled
filter such as `"chanel"` cannot widen a redrive to every channel.

**Equivalent SQL** (only if the API is unavailable):

```sql
UPDATE notifications
SET status = 'pending', attempts = 0, next_attempt_at = now(), failed_at = NULL
WHERE id IN (
  SELECT id FROM notifications
  WHERE status = 'failed' AND channel = 'sms' AND last_error_code = 'http_503'
    AND failed_at >= '2026-11-18T10:00:00Z' AND failed_at < '2026-11-18T11:00:00Z'
  ORDER BY created_at LIMIT 1000
  FOR UPDATE SKIP LOCKED);
```

## Queue growing or messages arriving late

**Symptoms.** Alert `QueueBacklogStale` (oldest due notification waiting
over 5 minutes); `queue` in `GET /api/v1/metrics` keeps rising; the
dashboard's "Oldest due pending" panel climbs.

**Check, in order:**

1. **Is a breaker open?** The dashboard's "Circuit breakers" panel, or
   worker `/ready`. If so, see "Provider outage" above: the backlog is
   intentional and drains on recovery.
2. **Are workers running and healthy?** `docker compose ps`. With no
   worker, nothing is claimed; `/ready` of each worker shows its
   dependencies.
3. **Is it just the rate limit?** A backlog of N on one channel takes at
   least N / `WORKER_RATE_LIMIT` seconds (50,000 at 100/s ≈ 8 minutes).
   "Sent per second" sitting at the limit means the system is healthy and
   saturated: nothing to fix except the provider contract.
4. **Is low priority starving?** It cannot for long: waiting work rises
   one priority level every `WORKER_PRIORITY_AGING` (5 minutes).

**Act:**

- More throughput needs a higher provider limit (`WORKER_RATE_LIMIT`).
  More workers do not help once the limit is reached; they only help when
  workers, not the limit, are the bottleneck (sends per second below the
  limit while the backlog grows).
- To drop a campaign that is no longer worth sending:
  `POST /api/v1/batches/{id}/cancel` cancels everything not yet sent.

## Redis unavailable

**Symptoms.** Alert `RateLimiterFallback`; API and worker `/ready` report
`degraded` (`redis` check failing); logs: "shared limiter unavailable" and
"outbox relay round failed; events stay queued"; alert `OutboxLagging`
after 30 seconds.

**What keeps working:** creating notifications and delivering them.
Each worker rate-limits itself at `WORKER_RATE_LIMIT /
WORKER_EXPECTED_INSTANCES` (keep that setting equal to the real replica
count, or the provider may see more than the limit, or less).

**What pauses:** WebSocket events. They are safe in the `outbox_events`
table and are published, in order, when Redis returns. Status callbacks
keep going: they are queued in PostgreSQL with each status change, not
read from Redis. Nothing needs replaying by hand.

**Act:** restore Redis (`docker compose up -d redis`). Workers switch
back to the shared limiter within seconds; the outbox backlog drains
(watch `outbox_backlog_events` fall to zero).

**If Redis lost its data** (compose runs it with an append-only file, so
a restart does not): no callback is affected. WebSocket clients may have
missed the events of the outage; they resync over REST when they
reconnect. The events remain in `outbox_events` for `OUTBOX_RETENTION`.

## Outbox lag without a Redis outage

**Symptoms.** Alert `OutboxLagging`, Redis healthy.

**Check:** is any worker running? The relay runs inside workers; with
none, events accumulate (safely). Worker logs show relay errors, if any.

**Act:** start a worker. A large backlog (after an outage) publishes at
`OUTBOX_BATCH_SIZE` events per round back to back, ~several thousand per
second.

## Sends that outlived their lease

Alert `SendRecoveredAfterLeaseLoss`; metric
`delivery_recovered_after_lease_loss_total{channel, previous_status}`.

A provider call took longer than the worker's lease (`WORKER_LEASE`,
heartbeats included), so the reaper treated the worker as dead: it put the
notification back to `pending`, or failed it on its last attempt, and it
may have been cancelled after that. Then the provider accepted the
original call. The worker recorded it as `sent` anyway, since the provider
is the source of truth, and logged `lease was lost during the provider
call`.

What to check:

1. If another worker claimed the notification in between, the recipient
   got the message twice (at-least-once). Its attempts show two successes.
2. Why heartbeats stopped extending the lease: a worker starved of CPU, a
   database that could not be reached, or provider calls slower than the
   lease (`provider_request_duration_seconds`). Raise `WORKER_LEASE` above
   the slowest calls, or lower `PROVIDER_TIMEOUT` below it.

## A worker crashed or was killed

Nothing to do. Its claimed notifications keep their lease for at most
`WORKER_LEASE` (30 s); then any worker's reaper returns them to `pending`
(`worker_lease_expired_total` counts them) and they are delivered.
A notification may be sent twice if the provider had already accepted it
when the worker died (at-least-once; the provider receives the same
`X-Idempotency-Key` both times).
