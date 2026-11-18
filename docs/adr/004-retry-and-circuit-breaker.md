# ADR 004: Retry policy and circuit breaker

**Status:** accepted

## Context

Provider calls fail for different reasons: overload and outages (worth
retrying), and rejected messages (never worth retrying). Retrying
everything wastes attempts and provider capacity; retrying in lockstep
after an outage hammers the provider as it recovers; and during a long
outage, retries alone would exhaust every queued notification's attempts.

## Decision

**Classification** (`internal/provider`):

| Response | Outcome |
|---|---|
| 2xx with a message id | success |
| 408, 429, 5xx, timeouts, connection errors, 2xx with an unexpected body | transient: retry |
| Other 4xx, 3xx | permanent: fail at once |

**Backoff:** `random(0, min(base · 2^attempt, max))` (full jitter; base 2 s,
max 5 min), or the provider's `Retry-After` if longer (capped at max).
Retrying is setting `next_attempt_at`; the worker never sleeps on a retry,
and any worker can pick it up. Priority is kept.

**Attempts:** `max_attempts` per notification (default 5, 1–10); then
`failed`, the dead-letter state, with single and bulk redrive.

**Circuit breaker** per channel per worker (`sony/gobreaker`, rolling 10 s
window): it opens when at least 20 deliveries finished and half or more
failed transiently (permanent errors count as successes: they say nothing
about the provider). While open, the dispatcher claims nothing, so queued
notifications keep their attempts; deliveries that were waiting for a
slot are handed back with the attempt refunded. After 30 s, 5 probes go
through; all must succeed to close it.

## Consequences

- A provider outage delays notifications instead of failing them:
  measured, 60 queued notifications through an outage cost 14–16 provider
  calls and no failures (without the breaker: up to 600 calls and every
  notification failed).
- The ratio is judged on finished deliveries only; counting deliveries
  still waiting for a slot hid failures (5 failures out of "50") and
  tripped the breaker late. Fixed and tested.
- Each worker learns about an outage on its own, within ~20 deliveries;
  a shared breaker state would save those few calls at the cost of a
  Redis dependency on the failure path.

## Alternatives

- **Fixed or exponential backoff without jitter:** retries synchronize.
- **Immediate in-worker retries:** block a delivery slot and ignore
  priority.
- **No breaker, longer backoff:** an outage longer than the backoff
  budget still fails everything.
