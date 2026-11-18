# ADR 005: At-least-once delivery

**Status:** accepted

## Context

Between "the provider accepted the message" and "the database recorded
it" there is always a moment where the worker can die. No design on this
side alone can make that window disappear: either the message may be
sent twice (record after sending) or may be lost (record before sending).

## Decision

**At least once, never lost.** A notification is marked as in progress
with a lease before the call and recorded after it. If the worker dies in
between, the lease expires and the reaper hands the notification to
another worker, which sends it again.

The duplicate window is kept small and the duplicate made harmless:

- The outcome is recorded in one short transaction right after the call.
- Every provider request carries `X-Idempotency-Key: <notification id>`,
  the same on every attempt, so a provider that deduplicates drops the
  repeat. Exactly-once delivery is only possible with that provider-side
  deduplication.
- The attempt counts even if its outcome was never recorded (the provider
  may have sent it), so a message that crashes a worker cannot be retried
  forever.

The same model applies to events (ADR 002) and callbacks: consumers
deduplicate by event id; callbacks carry `X-Event-Id`.

## Consequences

- No silent loss: every accepted notification ends `sent`, `failed` or
  `cancelled`, or is still being worked on.
- Rare duplicates on worker crashes, when the provider does not
  deduplicate. Measured: none in normal operation, chaos tests and load
  tests (the mock provider counts repeats).
- The client's own retries are covered separately by the API's
  `Idempotency-Key` and per-notification `idempotency_key`.

## Alternatives

- **At most once** (mark sent, then call): a crash loses the message;
  wrong for OTPs and transactional mail.
- **Distributed transactions with the provider:** not offered by
  providers, and they would still be at-least-once on the network.
