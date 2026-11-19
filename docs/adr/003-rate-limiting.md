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

If the shared limiter is unreachable, the worker **fails closed**: it
gets no slots, hands the notifications it claimed back to the queue with
their attempts refunded, and claims nothing on that channel until the
limiter answers again (it probes with a reservation of zero slots every
poll interval), the same way it behaves while a circuit breaker is open.
Workers that still reach Redis go on sending and pick up what was handed
back. Each Redis call is bounded at 250 ms: an outage does not fail fast
(with Redis stopped, the client retried its dials for 79 s).

An earlier version fell back to a local limiter at `rate / workers`. It
cannot be right when only some workers lose Redis: one cut off at 95/2
while another still gets 95 through Redis send ~142/s together, against
a 95/s limit. No local rule can know what the others send; only the
shared timeline can. So without it, nothing is sent.

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
- **Cost: if Redis is unreachable from every worker, deliveries stop**
  until it returns. Notifications wait in the queue without spending
  attempts; the API keeps accepting them. Waiting is preferred to
  exceeding the provider's limit, which is a hard requirement.
- A partial outage (some workers cut off) only lowers throughput: the
  workers that reach Redis keep the full rate and take over the handed
  back work. Test: `TestWorker_LimiterOutageFailsClosed` (one worker cut
  off, then back: busiest second at the provider 96, the same as with
  two healthy workers; no attempt spent while waiting).
- Regression test: `TestWorker_SlowClaimsDoNotBurstPastTheLimit` (69/s at
  a 50/s limit with the old order).

## Alternatives

- **Token bucket:** allows bursts up to the bucket size, which the
  provider would see as exceeding the limit within a second.
- **Fixed window counter:** allows twice the rate across a window boundary.
- **Per-worker limits (rate ÷ workers):** wastes capacity when workers are
  uneven and breaks when the instance count changes.
