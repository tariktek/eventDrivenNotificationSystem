# Load tests

Three [k6](https://k6.io) scenarios run against the docker compose stack
(2 workers, the mock provider with 20–50 ms latency). k6 runs in a
container, so nothing needs installing:

```bash
make up          # the stack must be running
make test-load   # all three, ~10 minutes
# or one of them:
docker compose --profile load run --rm k6 run /scripts/flash-sale.js
```

Each scenario checks its results from the receiving side: the mock
provider counts every request per channel per second and detects any
notification accepted twice.

| Scenario | Load | What it checks |
|---|---|---|
| `steady.js` | 150 single creates/s for 2 min, mixed channels and priorities | API latency under continuous traffic |
| `flash-sale.js` | 50,000 notifications in 60 s, as 50 batches of 1,000 | Burst absorbed, drained at the limit, never above it, nothing lost or duplicated |
| `provider-faults.js` | 100/s for 60 s while the provider fails 25% of calls (503/429) | Retries deliver everything; the breaker stays closed at that error rate |

## Results

Measured on a laptop (12 cores, Docker Desktop on Windows, 7.7 GB VM).

| Scenario | Result |
|---|---|
| Steady, 150/s | Create latency **p50 24 ms, p95 70 ms, p99 122 ms**; 17,984 delivered, 0 failed, 0 duplicates |
| Flash sale, 50,000 | Batch create p99 **393 ms**; all delivered in **188 s** (~90/s per channel); busiest second per channel **101–103** while sending at 100/s (see below); 0 failed, 0 duplicates |
| Provider failing 25% | 5,998 created, **5,993 delivered**, 1,879 retried calls; **5 dead-lettered** after 5 attempts (expected: 0.25⁵ × 6,000 ≈ 6); 0 duplicates |

Those runs sent at 100/s, and the busiest second at the provider read
101–103: sends leave exactly 10 ms apart, but arrival times carry network
jitter, and the mock counts per calendar second. The requirement is "at
most 100", counted where the provider counts. So the workers now send at
**95/s by default** (`WORKER_RATE_LIMIT`), and the threshold in
[`lib.js`](lib.js) is the requirement itself: any second over 100 at the
provider fails the run, with no tolerance. The same is checked in the Go
suite on every run: `TestWorker_ProviderNeverSeesMoreThan100PerSecond`
(two workers at the default rate, every sliding one-second window at the
provider; busiest measured: 96).

## What the load tests found

The first runs failed. Each finding was fixed and has a regression test.

1. **NOTIFY serialized every API write.** A trigger called `pg_notify`
   on every insert to wake the workers. PostgreSQL makes every
   transaction that used NOTIFY take a database-wide lock while it
   commits, so creates committed one at a time: **p50 855 ms** at 150
   creates/s, and throughput capped near 85/s. The wake-up moved to the
   outbox relay, which sends at most one NOTIFY per channel per round from
   its own transaction (migration 14). Result: **p50 27 ms**.

2. **Slow claims broke the rate limit.** Send slots were reserved
   *before* claiming the notifications. Under load a claim took hundreds
   of milliseconds, the slots were in the past when it returned, and
   those deliveries all went out at once on top of the next reservation's:
   the provider saw **175/s at a 100/s limit**. Slots are now reserved
   after the claim, with the work in hand; anything the limiter has no
   slot for yet waits in the worker for the next reservation. Result:
   **101–103/s**. Test: `TestWorker_SlowClaimsDoNotBurstPastTheLimit`
   (69/s at a 50/s limit with the old code).

3. **One batch row serialized all deliveries.** Each delivery's
   transaction locked its batch's row to count completion. In a flash
   sale the notifications finishing together mostly share a batch, so
   every delivery of every channel and worker queued on one row: the
   drain ran at **~52/s per channel**. Completion is now counted by the
   outbox relay, once per batch per round, in the transaction that marks
   the events published (so exactly once) (migration 15). Result:
   **~90/s per channel**, drain time 320 s → 188 s.

4. **Jaeger kept every trace in memory.** With 100% sampling the load
   tests' ~300,000 delivery traces filled 6 GB of the 7.7 GB Docker VM and
   starved the other containers (p99 19 s, Docker errors). Jaeger now
   keeps the latest 20,000 traces (`MEMORY_MAX_TRACES`); production would
   use a real trace store and a lower `TRACING_SAMPLE_RATIO`.
