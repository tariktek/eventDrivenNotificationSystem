# ADR 007: Side effects owned by the outbox relay

**Status:** accepted (from load testing)

## Context

Two side effects of a status change originally ran inside the
transaction that made the change:

1. **Waking workers:** a trigger called `pg_notify` on every insert, so
   workers started on new work immediately instead of at their next poll.
2. **Batch completion:** a trigger locked the batch row and counted
   finished items, to emit `batch.completed` exactly once.

Both were correct and passed every functional test. The k6 load tests
showed what they cost:

- PostgreSQL serializes the commits of all transactions that used
  `NOTIFY` (a database-wide lock during commit). At 150 creates/s the API's
  median latency was **855 ms** instead of 27 ms.
- In a flash sale the notifications finishing together mostly belong to
  one batch, so every delivery of every channel and worker queued on that
  batch's row: drain at **~52/s per channel** instead of the 100/s limit,
  with waiting transactions holding the connections claims needed.

## Decision

Move both into the **outbox relay** (ADR 002), which already sees every
status change, runs in one place at a time, and processes events in
batches:

- **Wake-ups:** after publishing a round, the relay sends at most one
  `NOTIFY` per channel that got due work (new or handed-back
  notifications). Request transactions never notify.
- **Batch progress:** the relay adds each round's net change of finished
  items per batch with one `UPDATE` per batch, and emits `batch.completed`
  when a batch's finished count reaches its created count. This happens
  in the transaction that marks the events published, so each change is
  counted exactly once even if the relay crashes and republishes.

`created_count` is still set by the insert trigger, inside the batch's own
transaction, where nothing contends for the row.

## Consequences

- API p50 855 ms → **24–27 ms** at 150 creates/s; flash-sale drain
  ~52/s → **~90/s** per channel (320 s → 188 s for 50,000).
- Wake-up and batch-completion latency become the relay's (~100 ms).
- Delivery transactions touch only their own notification row.
- Migrations 14 and 15; tests: `TestListenDue`,
  `TestRelay_WakesWorkersForDueWork`,
  `TestTriggers_BatchCompletesOnceUnderConcurrency`, and the load tests.

## Lesson

Correct is not the same as scalable: both designs were right, and only
measurement under realistic load showed the global serialization points.
