# ADR 003: Rate limiting with GCRA slots in Redis

**Status:** accepted (revised after load testing)

## Context

The provider accepts at most 100 messages per second per channel. The
limit is global: it must hold however many worker instances run, and the
provider counts arrivals, so bursts within a second matter, not only the
average.

## Decision

A **GCRA** (generic cell rate algorithm) limiter in Redis, as a Lua script
using Redis's clock, hands out **send slots**: `Reserve(n)` returns up to
`n` slot times spaced exactly `1/rate` apart (10 ms at 100/s), within a
horizon of one second. Each delivery waits for its own slot. All workers
share the key per channel, so the spacing holds across instances.

If Redis is unreachable, each worker falls back to a local limiter at
`rate / WORKER_EXPECTED_INSTANCES` and reports itself degraded; it retries
Redis every few seconds. Each Redis call is bounded at 250 ms: an outage
does not fail fast (with Redis stopped, the client retried its dials for
79 s, and the channel sent nothing meanwhile). Bounded, 30 SMS went out
in under 2 s with Redis down.

**Order of operations: claim, then reserve.** The first version reserved
slots and then claimed that many notifications. Under load a claim took
hundreds of milliseconds, the slots were in the past when it returned,
and those deliveries went out together, on top of the next reservation's:
the provider saw 175/s at a 100/s limit. Now the worker claims first and
reserves slots for the work in hand; notifications it gets no slot for
wait in the worker (leases kept alive) for the next reservation.

## Consequences

- Sends leave exactly 10 ms apart. At the receiver, network jitter can
  shift arrivals by a few milliseconds: the busiest calendar second
  measured 101–103 at a limit of 100. A provider that enforces its limit
  strictly on arrival should get headroom (`WORKER_RATE_LIMIT=95`).
- Smooth pacing rather than bursts: GCRA does not allow saving up unused
  capacity.
- A Redis outage degrades accuracy (the fallback assumes the configured
  instance count), never availability.
- Regression test: `TestWorker_SlowClaimsDoNotBurstPastTheLimit` (69/s at
  a 50/s limit with the old order).

## Alternatives

- **Token bucket:** allows bursts up to the bucket size, which the
  provider would see as exceeding the limit within a second.
- **Fixed window counter:** allows twice the rate across a window boundary.
- **Per-worker limits (rate ÷ workers):** wastes capacity when workers are
  uneven and breaks when the instance count changes.
