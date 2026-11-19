# ADR 002: Transactional outbox to Redis Streams

**Status:** accepted

## Context

Status changes must reach WebSocket clients and client callbacks.
Publishing to a message bus from application code after a database
commit loses events when the process dies between the two, and
publishing before the commit announces changes that may roll back.

## Decision

Every status change writes a domain event to the `outbox_events` table in
the same transaction, through statement-level triggers on
`notifications` (one `INSERT … SELECT` per statement, so a 1,000-row batch
insert costs one extra statement). A trigger rather than application code
because about a dozen code paths change statuses (API, worker, reaper,
redrive, manual SQL); none can forget.

An **outbox relay** in each worker publishes unpublished events, in id
order, to a Redis Stream and marks them published. A transaction-scoped
advisory lock lets only one relay publish at a time, which keeps events in
order and lets another worker take over if one dies.

Consumers:

- **WebSocket hubs** (each API instance) tail the stream: every instance
  sees every event.
- **Callbacks** do not read the stream. The same trigger writes a
  `callback_deliveries` row when a notification with a `callback_url`
  reaches a final status, in the transaction of the change (migration 19),
  and dispatchers on every worker claim those rows. They first went
  through the stream with a consumer group; that lost callbacks when the
  stream was trimmed or Redis lost its last second of writes, and the group
  spread one notification's events over several workers, so an older
  callback (failed) could reach the client after a newer one (sent).

## Consequences

- An event exists if and only if its change committed. Redis down: events
  wait in the table and are published in order when it returns.
- At least once: a relay that crashes after publishing republishes;
  the WebSocket hub drops repeats by event id.
- Guarantees differ by consumer: the outbox table and callbacks are
  lossless; the stream, and so WebSocket, is a best-effort live feed (a
  client that misses an event reloads the state over REST).
- One notification's callbacks are created in commit order and sent in
  that order; one older than an already delivered callback is never sent
  (`superseded`).
- One notification's events are always in order (each change commits
  after the previous one). Events of different notifications are not
  globally ordered.
- Publication latency is the relay's poll interval (100 ms) when idle.
- Published events are pruned after `OUTBOX_RETENTION`; the stream is
  trimmed to `EVENTS_STREAM_MAX_LEN`.

## Alternatives

- **Publish from application code after commit:** loses events on crash.
- **Change data capture (Debezium on the WAL):** no triggers, but a Kafka
  Connect deployment for one table.
- **Redis Pub/Sub:** no persistence or acknowledgement; a consumer that
  restarts misses events.
