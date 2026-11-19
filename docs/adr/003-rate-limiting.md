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
`rate / workers` and reports itself degraded; it retries Redis every few
seconds. The workers count each other every 5 s from their PostgreSQL
connections (each names its connections `notification-worker:<id>` and
holds one open for LISTEN): PostgreSQL stays reachable when Redis does
not, and no worker delivers without it anyway. A configured count goes
stale as soon as the workers are scaled: three workers configured as two
would each send at 95/2 and together at ~142/s, against a 95/s limit.
Counted live, that scenario (three workers, setting left at two, Redis
stopped, 300 SMS) peaked at 92 in any second. `WORKER_EXPECTED_INSTANCES`
is only the value before the first count.

Each Redis call is bounded at 250 ms: an outage does not fail fast (with Redis stopped, the client retried its dials for
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

- Sends leave exactly 1/rate apart. At the receiver, network jitter can
  shift arrivals by a few milliseconds: sending at 100/s, the busiest
  calendar second measured 101–103. The requirement counts at the
  provider, so the default rate is 95/s: 5% headroom for that jitter.
  Checked with no tolerance by `TestWorker_ProviderNeverSeesMoreThan100PerSecond`
  (busiest sliding second: 96) and by the k6 thresholds.
- When a worker switches between the shared and the local limiter (Redis
  lost or back), it first lets the slots it holds on the old timeline run
  out, so the two timelines never overlap. The local share's horizon is
  widened to fit at least one of its (longer) intervals.
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
