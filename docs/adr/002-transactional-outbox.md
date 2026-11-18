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
- **Callbacks**: a consumer group shares the events among workers, with
  acknowledgement and takeover of events left by a dead consumer. A new
  group starts at the end of the stream, so a deployment does not act on
  history.

## Consequences

- An event exists if and only if its change committed. Redis down: events
  wait in the table and are published in order when it returns.
- At least once: a relay that crashes after publishing republishes;
  consumers deduplicate by event id (`callback_deliveries.event_id` is
  unique).
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
