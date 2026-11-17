# Runbook

Operational procedures for the notification system. Examples assume the
API at `http://localhost:8080` and `X-API-Key: $KEY`.

## Provider outage: circuit breaker open

**Symptoms.** Worker logs show `circuit breaker state changed` with
`"to":"open"`. The worker's `GET /ready` reports `degraded` with
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
