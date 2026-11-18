# ADR 001: PostgreSQL as the work queue

**Status:** accepted

## Context

Notifications must be stored (status queries, listing, cancellation,
history) and delivered asynchronously by several workers, by priority,
with retries and scheduled sends. A separate broker (RabbitMQ, Kafka,
SQS) would hold a second copy of each notification, and every operation
that touches both (create, cancel, retry, schedule) would have to keep
them consistent: the classic dual-write problem.

## Decision

The `notifications` table is the queue. A notification is due when its
status is `pending` or `scheduled` and `next_attempt_at <= now()`.
Workers claim with:

```sql
UPDATE notifications SET status = 'processing', locked_by = $worker, locked_until = now() + lease, attempts = attempts + 1
WHERE id IN (SELECT id FROM … ORDER BY effective_priority, next_attempt_at LIMIT n FOR UPDATE SKIP LOCKED)
```

`SKIP LOCKED` lets workers claim concurrently without blocking on each
other's rows. A lease (`locked_until`), extended by a heartbeat, marks
work in progress; a reaper requeues work whose worker stopped
heartbeating. Retries and scheduled sends are just a future
`next_attempt_at`; no separate scheduler or delay queue. A partial index
covers only due work, so its size tracks the backlog, not the history.

## Consequences

- Creating, cancelling or retrying a notification is one transaction.
  A cancel racing a claim has exactly one winner (both are conditional
  updates).
- One system less to run, back up and monitor.
- Throughput is bounded by the primary's write capacity. Measured: 50,000
  notifications drained at the provider limit (~300/s across channels) on
  a laptop; the claim stays an index scan (~11 ms at 200,000 queued).
- Polling costs a query per channel per second when idle; LISTEN/NOTIFY
  wakes workers early (sent by the outbox relay, see ADR 007).

## Alternatives

- **RabbitMQ / SQS:** simpler consumption, but a dual write with the
  database, and priorities, delayed retries and cancellation of queued
  messages are awkward or impossible.
- **Kafka:** the right tool beyond one database's write capacity (see
  "Limitations and scaling further" in the README), but per-message
  cancellation and priorities do not fit a log, and it is a large
  operational addition at this scale.
