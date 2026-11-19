# Event-Driven Notification System: Implementation Roadmap (v2)

> Design and implementation plan for the Insider One Software Engineer Assessment (Golang).
> This version was written without a time constraint. The priority is a near-production-quality system rather than an MVP.
> The reasoning behind every decision is written next to it.
>
> This is the plan as written before and during implementation. Where the code differs, the code,
> the [ADRs](adr/), [architecture.md](architecture.md) and the [README](../README.md) are authoritative.
> The two largest departures came from the final review: the rate limiter fails closed instead of
> falling back to a local limiter when Redis is unreachable ([ADR 003](adr/003-rate-limiting.md)), and
> status callbacks are queued by the status change itself instead of read from a Redis consumer group
> ([ADR 002](adr/002-transactional-outbox.md)).

## What changed from v1?

| Area | v1 | v2 |
|---|---|---|
| Event-driven backbone | Status broadcast over Redis pub/sub (fire-and-forget) | **Transactional outbox + Redis Streams.** Domain events are never lost and are processed through consumer groups |
| Provider | webhook.site only | webhook.site (supported, as the brief requires) + **a fault-injecting mock provider inside docker-compose**. webhook.site's free request limit is too low for load testing |
| API contract | Hand-written OpenAPI | **Spec-first:** types and interfaces generated from `openapi.yaml` with `oapi-codegen`. The spec and the code cannot drift apart |
| SQL | Hand-written pgx | `sqlc` for static queries (type-safe), a hand-written builder for the dynamic list filter |
| Redis outage | Undefined | **Safe fallback** to a local limiter (degraded mode), reported by readiness |
| Customer visibility | Queries + WebSocket | Additionally a per-notification **status callback (webhook)**, HMAC-signed |
| Security | None | API key, body limits, timeouts, SSRF protection for callbacks |
| Tests | Unit + integration + e2e | Additionally OpenAPI contract tests, fuzzing, chaos (killing workers), k6 load tests with results published in the README |
| Operations | Prometheus | Additionally a provisioned **Grafana dashboard**, alert rules, a data retention job |
| Documentation | README | Additionally **ADRs** (architecture decision records), a runbook, a `.http` examples file |
| CI | Lint + test + build | Additionally `govulncheck`, Trivy image scan, coverage report, Dependabot |

---

## 1. Technology Choices

| Topic | Choice | Reasoning |
|---|---|---|
| Language | Go 1.23 | The brief asks for "1.2x" |
| HTTP router | `go-chi/chi` | Lightweight, `net/http` compatible, supported by oapi-codegen |
| API code | `oapi-codegen` (spec-first) | Request and response types are generated from the spec, no manual syncing |
| Database | PostgreSQL 16 + `pgx/v5` | The system of record; supports transactions, `SKIP LOCKED` and `LISTEN/NOTIFY` |
| Queries | `sqlc` + a small builder for the dynamic list | Compile-time type safety |
| Migrations | `golang-migrate` | Versioned up/down SQL files |
| Job queue | **PostgreSQL-based** (`FOR UPDATE SKIP LOCKED`), behind a `Queue` interface | §1.1 |
| Event bus | **Transactional outbox → Redis Streams** | §1.2 |
| Rate limit | GCRA / token bucket on Redis (Lua) | One limit shared by all instances |
| Logging | `log/slog` (JSON) | Standard library |
| Metrics | Prometheus client + Grafana | |
| Tracing | OpenTelemetry + Jaeger | |
| Circuit breaker | `sony/gobreaker` | |
| WebSocket | `coder/websocket` | Actively maintained, context-aware |
| Testing | `testify`, `testcontainers-go`, `kin-openapi` (contract), Go fuzzing, k6 | |
| Quality | `golangci-lint`, `govulncheck`, Trivy | |

### 1.1 Why a Postgres job queue? (ADR-001)

Writing the notification to the database and then publishing it separately to RabbitMQ or Kafka creates a *dual-write* problem: one can succeed while the other fails. A Postgres queue provides all of the following in a single transaction:

- durability,
- priority,
- delayed retries,
- scheduled delivery,
- atomic cancellation.

No extra infrastructure is needed for this.

**Capacity:** With a limit of 100/s per channel, the system's ceiling is 300 msg/s, about 26 million messages a day. This meets the "millions per day" goal. A Postgres `SKIP LOCKED` queue can go well beyond this volume; we will measure it with load tests (§13) and publish the result in the README.

**Migration path:** Thanks to the `Queue` interface, the system can move to Kafka if scale grows. That requires a topic per channel, a topic per priority and delayed topics for retries. This will be described in the README and an ADR.

### 1.2 Event-driven backbone: Transactional Outbox (ADR-002)

Every status change is written as a domain event to the `outbox_events` table in the same transaction. Event types:

- `notification.created`
- `notification.status_changed`
- `batch.completed`

An **outbox relay** goroutine moves these events to Redis Streams (`stream:notification-events`) and marks them as published. The consumers are:

- **WebSocket hubs:** Each API instance reads with its own consumer and broadcasts to its connected clients.
- **Callback dispatcher:** Sends a signed POST to the `callback_url` the customer provided.
- **Metrics collector:** If needed.

The difference from Redis pub/sub: events are not lost (the stream is durable, consumer groups have ACKs), and a restarted consumer resumes where it left off. This is the part that makes the system event-driven in the real sense.

---

## 2. Folder Structure

```
insider-one/
├── cmd/
│   ├── api/                   # HTTP API + WebSocket + outbox relay
│   ├── worker/                # Queue workers + reaper + retention + callback dispatcher
│   └── mockprovider/          # Fault-injecting fake provider (for local use and load tests)
├── internal/
│   ├── config/                # env → struct, validation (fail-fast at startup)
│   ├── domain/                # Entities, value objects, state machine, domain errors, events
│   ├── validation/            # Per-channel content and recipient rules, SMS segment count
│   ├── template/              # Template rendering
│   ├── service/               # Use cases (create, batch, cancel, list, retry, template)
│   ├── repository/postgres/   # sqlc output + dynamic queries + tx manager
│   ├── queue/                 # Queue interface + postgres implementation
│   ├── outbox/                # Outbox writer + relay
│   ├── events/                # Redis Streams publisher/consumer
│   ├── ratelimit/             # Redis GCRA (Lua) + local fallback + in-memory (tests)
│   ├── provider/              # HTTP client, error classification, circuit breaker wrapper
│   ├── retry/                 # Backoff policy
│   ├── worker/                # Dispatcher, channel pools, processing loop, reaper
│   ├── callback/              # Customer webhooks, HMAC signature, SSRF protection
│   ├── http/
│   │   ├── api/               # oapi-codegen output (generated)
│   │   ├── handler/           # Strict server implementation
│   │   ├── middleware/        # auth, correlation-id, logging, recovery, tracing, metrics, body limit
│   │   └── ws/                # WebSocket hub
│   ├── observability/         # logger, metrics, tracer setup
│   └── platform/              # graceful shutdown, health check registry, clock (testable time)
├── migrations/
├── api/openapi.yaml
├── deploy/
│   ├── prometheus/            # prometheus.yml, alert rules
│   └── grafana/               # datasource + dashboard provisioning
├── docs/
│   ├── adr/                   # Architecture decision records
│   ├── architecture.md        # Diagrams (Mermaid): components, sequence, state
│   └── runbook.md             # Operations: draining the DLQ, what to do if a breaker stays open, etc.
├── test/
│   ├── integration/
│   ├── e2e/
│   └── load/                  # k6 scenarios
├── examples/requests.http     # VS Code / JetBrains HTTP client examples
├── .github/workflows/ci.yml
├── .github/dependabot.yml
├── .golangci.yml
├── Dockerfile                 # Multi-stage, distroless, non-root, one image + three binaries
├── docker-compose.yml
├── Makefile
├── .env.example
└── README.md
```

We do not impose a strict hexagonal architecture, but dependencies point one way: `handler → service → (repository, queue, provider interfaces)`. `domain` depends on nothing. Time (`clock`) and randomness (jitter) are injected, so tests are deterministic.

---

## 3. Data Model (Migrations)

### `notifications`
| Column | Type | Note |
|---|---|---|
| `id` | UUID v7 PK | Time-ordered, index-friendly |
| `batch_id` | UUID NULL FK | |
| `channel` | enum `sms, email, push` | |
| `priority` | smallint | 0 = high, 1 = normal, 2 = low |
| `recipient` | text | |
| `subject` | text NULL | For email |
| `content` | text | Final rendered content |
| `template_id`, `template_vars` | UUID NULL, JSONB | For traceability |
| `status` | enum `pending, scheduled, processing, sent, failed, cancelled` | |
| `idempotency_key` | text NULL | `UNIQUE` (partial, excluding NULL) |
| `attempts`, `max_attempts` | int | |
| `next_attempt_at` | timestamptz | One field for queue order, retry time and schedule |
| `locked_until`, `locked_by` | timestamptz, text | Worker lease |
| `scheduled_at`, `sent_at`, `failed_at`, `cancelled_at` | timestamptz NULL | |
| `provider_message_id` | text NULL | The `messageId` returned by the provider |
| `last_error`, `last_error_code` | text | |
| `callback_url` | text NULL | Optional status webhook |
| `metadata` | JSONB | Customer's free-form tags (campaign ID, etc.) |
| `correlation_id`, `trace_parent` | text | Log and trace chain |
| `created_at`, `updated_at` | timestamptz | |

Indexes:
- Partial index for the queue: `(channel, priority, next_attempt_at) WHERE status IN ('pending','scheduled')`
- Partial index for expired leases: `(locked_until) WHERE status = 'processing'`
- For listing: `(created_at DESC, id DESC)`, `(status, created_at)`, `(channel, created_at)`, `(batch_id)`

### `batches`
`id`, `total`, `metadata`, `correlation_id`, `created_at`. The status summary is computed at query time with `GROUP BY status`. When every notification reaches a final state, a `batch.completed` event is produced.

### `notification_attempts`
`id`, `notification_id`, `attempt_no`, `status_code`, `error_class` (transient/permanent), `error`, `provider_message_id`, `duration_ms`, `created_at`. Kept for audit and debugging; shown in the `GET /notifications/{id}` response.

### `templates`
`id`, `name` UNIQUE, `channel`, `subject`, `body`, `variables` (expected variable list, JSON), `version`, `created_at`, `updated_at`. Updating a template increments its version; a notification stores which version it was rendered with.

### `idempotency_keys`
`key` PK, `request_hash`, `response_status`, `response_body` JSONB, `locked_at`, `created_at`. TTL 24 hours; cleaned up by the retention job. The `locked_at` field prevents two concurrent requests with the same key from both being processed (§7).

### `outbox_events`
`id` BIGSERIAL, `aggregate_id`, `event_type`, `payload` JSONB, `created_at`, `published_at` NULL. The relay sends rows with `published_at IS NULL` in order.

### `callback_deliveries`
Record of customer webhook attempts: `id`, `notification_id`, `event_id`, `attempt_no`, `status_code`, `next_attempt_at`, `status`.

### Data retention
The retention job deletes rows in a final state (`sent/failed/cancelled`) older than `RETENTION_DAYS` (default 30). It also cleans up published outbox events and expired idempotency keys. The README notes that at larger scale, monthly **partitioning** by `created_at` would make deletion as cheap as `DROP PARTITION`.

---

## 4. State Machine

```
           ┌──────────► cancelled (only from pending/scheduled)
           │
pending/scheduled ──claim──► processing ──202──► sent
           ▲                     │
           │   transient error,  │ permanent error or attempts exhausted
           └── attempts<max ─────┤
               (next_attempt_at  ▼
                moved forward)  failed  ──(manual /retry)──► pending
   when the lease expires (reaper):
   processing ──────────────────► pending
```

- Transitions are defined in a single table in the `domain` package. An invalid transition returns `ErrInvalidTransition`, and every transition is covered by table-driven tests.
- Database updates are conditional (`UPDATE ... WHERE id=$1 AND status='processing' AND locked_by=$me`). If no row is affected, another actor changed the record and the operation is safely dropped.
- **Cancel vs claim race:** Cancellation uses the condition `WHERE status IN ('pending','scheduled')`. If a worker has already claimed the record, cancel returns 409 ("already being sent"). Both actors can never win.
- Every transition writes an outbox event in the same transaction.

---

## 5. API (`/api/v1`)

### 5.1 Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/notifications` | Create one. Responds 201. Supports the `Idempotency-Key` header |
| POST | `/notifications/batch` | 1–1000 notifications. Behaviour below |
| GET | `/notifications/{id}` | Details + attempt history |
| GET | `/notifications` | Filters: `status, channel, priority, batch_id, created_from, created_to`. Cursor pagination (`limit` ≤ 100, `cursor`) |
| POST | `/notifications/{id}/cancel` | Only pending/scheduled. Otherwise 409 |
| POST | `/notifications/{id}/retry` | Requeues a `failed` notification (takes it out of the DLQ). `attempts` is reset |
| GET | `/batches/{id}` | Summary (counts per status, progress percentage) |
| GET | `/batches/{id}/notifications` | The batch's notifications, paginated |
| POST | `/batches/{id}/cancel` | Cancels the batch's waiting notifications and returns how many were cancelled |
| POST | `/templates` | Create a template (with a render pre-check) |
| GET | `/templates`, `/templates/{id}` | |
| PUT | `/templates/{id}` | Creates a new version |
| DELETE | `/templates/{id}` | Soft delete |
| POST | `/templates/{id}/preview` | Renders with the given variables and returns the result without saving |
| GET | `/ws` | WebSocket. `notification_id` / `batch_id` are chosen with a subscribe message |
| GET | `/health` | Liveness |
| GET | `/ready` | DB + Redis ping, per-component status (`ok/degraded/down`) |
| GET | `/metrics` | Prometheus |
| GET | `/api/v1/metrics` | JSON summary (queue depth, success/failure rates, p50/p95/p99 latency) |
| GET | `/docs`, `/openapi.yaml` | Swagger UI and the spec |

### 5.2 Batch semantics
- **Default behaviour is all-or-nothing:** If a single item is invalid, the response is 422 and the error details include the item index and field name. Nothing is written.
- This choice removes the "half of it went out, half didn't" ambiguity for a customer sending a campaign.
- Rows are written with `COPY`, so 1000 rows go in one round trip.
- Each item in a batch can have its own `idempotency_key`.

### 5.3 Common rules
- **Authentication:** The `X-API-Key` header. Keys are read from the environment. Required everywhere except `/health`, `/ready` and `/metrics`.
- **Error format:** RFC 7807-style, uniform:
  ```json
  {"error": {"code": "validation_failed", "message": "...", "details": [{"field": "items[3].recipient", "reason": "..."}]}, "correlation_id": "..."}
  ```
- **Pagination:** The cursor is the base64-encoded `(created_at, id)` pair. A cursor is used because `OFFSET` gets slow over millions of rows. The response returns `next_cursor` and `has_more`.
- **Limits:** Request body at most 5 MB (enough for 1000 emails), handler timeout 10 s, `ReadHeaderTimeout` set (against Slowloris).
- **Date format:** RFC 3339 / UTC everywhere.

---

## 6. Validation Rules

| Channel | Recipient | Content |
|---|---|---|
| SMS | E.164: `^\+[1-9]\d{7,14}$` | One segment is 160 characters in the GSM-7 character set and 70 in UCS-2 (Turkish characters or emoji). **Multi-part SMS is supported:** at most 6 segments (GSM-7: 153×6, UCS-2: 67×6). The segment count is computed and stored |
| Email | RFC 5322 (`net/mail`), at most 254 characters | `subject` required, at most 255 characters; body at most 100 KB |
| Push | Device token: non-empty, at most 4096 characters (FCM tokens can be long) | At most 1024 characters; `subject` optional (used as the title) |

Common rules:
- `priority` defaults to `normal`.
- `content` and `template_id` are mutually exclusive; one of them is required.
- `scheduled_at` cannot be in the past and can be at most 30 days ahead.
- `callback_url` must be `https`, and SSRF protection applies (§9.6).
- `metadata` at most 4 KB.
- If a template is used, it is rendered first and limits are checked against the rendered content.
- Validation errors are returned all at once (validation does not stop at the first error), each with its field path.
- Validation functions are fuzz-tested: no input may cause a panic.

---

## 7. Idempotency (Two Layers)

### 7.1 API layer (`Idempotency-Key` header)
1. When a request arrives, an "in progress" record is created for the key with `INSERT ... ON CONFLICT DO NOTHING` (`locked_at`).
2. If the record already exists:
   - if its response is stored and the request hash (SHA-256) matches, the original response is returned as is (with an `Idempotent-Replayed: true` header);
   - if the hash differs, 422 (`idempotency_key_reused`) is returned;
   - if it is still in progress, 409 (`request_in_progress`) is returned.
3. When processing finishes, the response is stored. If processing ends with an error, the record is deleted so the client can retry.

This flow also handles concurrent requests with the same key correctly. Proven with an integration test.

### 7.2 Notification layer
- The `idempotency_key` field has a partial unique constraint. A conflicting item returns the existing record and nothing new is sent.
- A worker only sends a record that is in `processing` and whose lease it owns. A `sent` record is never sent again.
- The provider receives an `X-Idempotency-Key: <notification_id>` header.

### 7.3 Delivery guarantee
The system is **at-least-once**. If a worker crashes after getting 202 from the provider but before updating the database, the lease expires and the record may be sent again. This window is narrowed in two ways:

- The update right after the provider call is short and a single query.
- The provider side gets an idempotency key. Only the provider can guarantee exactly-once.

The README states this explicitly.

---

## 8. Processing Engine (Worker)

### 8.1 Structure
```
                 ┌───────────── Dispatcher (per channel) ─────────────┐
LISTEN/NOTIFY ──►│  claim(batch=N) → local buffer → worker goroutines  │──► rate limiter ──► circuit breaker ──► provider
   + poll        └─────────────────────────────────────────────────────┘
Reaper (takes back expired leases) · Retention job · Queue depth sampler · Outbox relay
```

- **A separate pool per channel:** SMS hitting its rate limit or opening its circuit breaker does not affect email.
- **Claim query:**
  ```sql
  UPDATE notifications
  SET status='processing', locked_until=now()+$lease, locked_by=$worker, attempts=attempts+1
  WHERE id IN (
    SELECT id FROM notifications
    WHERE channel=$1 AND status IN ('pending','scheduled') AND next_attempt_at <= now()
    ORDER BY priority, next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT $n)
  RETURNING ...;
  ```
  Thanks to `SKIP LOCKED`, N worker instances can work in parallel without taking the same record.
- **The claim size is tied to the rate limit:** At most as many records as there are tokens available right now are claimed at once. This keeps records from sitting in `processing` long enough for their lease to expire.
- **Priority and aging:** Strict priority applies. To prevent starvation of low priority, records waiting longer than 5 minutes move up one level in effective priority: `ORDER BY GREATEST(priority - floor(wait/5min), 0), next_attempt_at`. The aging threshold is configurable and verified by tests.
- **Lease and reaper:** A lease lasts 30 s; it is extended (heartbeat) during long provider calls. The reaper moves `processing` records with expired leases back to `pending` and increments the `worker_lease_expired_total` metric.
- **Wake-up strategy:** After an insert, `NOTIFY notifications_<channel>` is sent and the worker wakes up immediately. If the connection drops or a NOTIFY is missed, a 1 s poll acts as a fallback. This keeps latency low with no database load while idle.
- **Graceful shutdown:** On SIGTERM, no new claims are made and work in hand is finished (with a configurable timeout). Anything unfinished is taken back by the reaper when its lease expires, so no record is lost.

### 8.2 Rate limiting
- **GCRA** (a single-key, memory-friendly variant of the token bucket) running atomically on Redis as a Lua script. Key `ratelimit:{channel}`, limit 100/s, burst 100 (configurable).
- The limit applies **across all worker instances**. When a request is not allowed, the script returns how long to wait, and the worker sleeps for that long. No record is lost.
- **If Redis is unreachable:** The worker falls back to a local token bucket running at `limit / expected_instance_count`. This is a conservative choice: sending slowly is better than exceeding the limit. `/ready` returns "degraded" and the `rate_limiter_fallback_active` metric becomes 1.
- Test: with multiple goroutines and simulated instances, measure that no 1-second slice in a 10-second window exceeds 100.

### 8.3 Burst traffic
The API only writes to the database and returns immediately. The queue acts as the buffer, and workers drain it at a steady rate (100/s per channel). During a flash sale the system does not fall over; only the queue depth grows. This is visible in the metrics and in Grafana. The load test measures exactly this scenario (§13).

---

## 9. Delivery and Retry Design

### 9.1 Provider client
- The provider URL is configurable per channel (`PROVIDER_URL_SMS`, `_EMAIL`, `_PUSH`). If one is not set, the shared `PROVIDER_URL` is used.
- The request uses the format from the brief: `{"to": "...", "channel": "sms", "content": "..."}`. The `X-Idempotency-Key`, `X-Correlation-ID` and `traceparent` headers are also sent.
- The response is parsed as `{messageId, status, timestamp}`. If `status != "accepted"` or `messageId` is empty, the response is considered invalid.
- 10 s timeout, connection pooling (`MaxIdleConnsPerHost` set), cancellable through `context`, response body read up to 1 MB.

### 9.2 Error classification

| Case | Class | Behaviour |
|---|---|---|
| 202 + valid body | Success | `sent`, `provider_message_id` stored |
| 2xx but unexpected body | Transient | Retry, `error_code=invalid_provider_response` |
| 408, 429 | Transient | `Retry-After` honoured if present (capped at the max delay) |
| 5xx | Transient | Retry with backoff |
| Timeout, connection refused/reset, DNS error | Transient | Retry with backoff |
| 400, 401, 403, 404, 410, 413, 422 | Permanent | No retry, straight to `failed` |
| Other 4xx | Permanent | Same |

### 9.3 Backoff
- Formula: `delay = rand(0, min(base · 2^attempt, max))` (AWS "full jitter").
- Defaults: base 2 s, max 5 min, `max_attempts` 5. All configurable through the environment; `max_attempts` can also be set per notification (1–10).
- Jitter keeps all retries from hitting the provider at the same moment when it recovers (thundering herd).
- A retry is just moving the record's `next_attempt_at` forward. There is no `sleep` in the worker; the record goes back to the queue and any worker can pick it up.
- **A retried record keeps its priority:** a high-priority notification stays high on retry.

### 9.4 Circuit breaker (per channel)
- The breaker opens when there are at least 20 requests in a 10-second window and the failure rate exceeds 50%.
- **While open**, the dispatcher does not claim from that channel. Records wait in the queue and their retry attempts are **not spent**. This is critical: a provider outage must not make notifications fail for nothing.
- After 30 s it moves to **half-open**; if a few probe requests succeed, the breaker closes.
- The breaker state is published as a metric and reflected in `/ready` as "degraded".

### 9.5 DLQ
- A record that runs out of attempts or gets a permanent error becomes `failed`; `last_error` and `last_error_code` are filled in.
- The `failed` status serves as the DLQ: it is listed with `?status=failed` and requeued one by one with `/retry`.
- The runbook describes the procedure for bulk-draining the DLQ after a provider outage (filtered bulk retry SQL and an endpoint example).

### 9.6 Status callbacks (customer webhook)
- If a `callback_url` is given, the customer receives a POST on final status changes (`sent`, `failed`, `cancelled`).
- The body is the event payload. `X-Signature: sha256=HMAC(secret, timestamp + "." + body)` and `X-Timestamp` headers are added, so the customer can verify that the request comes from us and is not a replay.
- Callbacks have their own retry policy (backoff, at most 5 attempts) and are recorded in the `callback_deliveries` table.
- **SSRF protection:** Only `https` is accepted. After the hostname is resolved, private, loopback and link-local IPs are rejected; the check is repeated at connect time (in a custom dialer) against DNS rebinding. It can be turned off through the environment for local development.

---

## 10. Observability

### 10.1 Metrics (Prometheus)
| Metric | Type | Labels |
|---|---|---|
| `notifications_created_total` | counter | channel, priority |
| `notifications_sent_total` | counter | channel |
| `notifications_failed_total` | counter | channel, reason |
| `notifications_cancelled_total` | counter | channel |
| `notification_retries_total` | counter | channel |
| `notification_queue_depth` | gauge | channel, priority, status (pending/scheduled/processing) |
| `notification_queue_oldest_age_seconds` | gauge | channel |
| `notification_delivery_latency_seconds` | histogram | channel, priority |
| `provider_request_duration_seconds` | histogram | channel, status_class |
| `rate_limit_wait_seconds` | histogram | channel |
| `rate_limiter_fallback_active` | gauge | |
| `circuit_breaker_state` | gauge | channel |
| `worker_lease_expired_total` | counter | |
| `outbox_lag_seconds` | gauge | |
| `callback_deliveries_total` | counter | result |
| `http_requests_total`, `http_request_duration_seconds` | counter, histogram | route, method, status |

Notes:
- `notification_delivery_latency_seconds` is the time from creation until `sent`. For scheduled notifications, `scheduled_at` is the starting point.
- Queue depth is not computed with `COUNT(*)` on every request. It is sampled in the background every 5 s, so the database is not loaded.
- The `/api/v1/metrics` endpoint returns a JSON summary of the same data: queue depth per channel, success/failure rates for the last 1/5/15 minutes, p50/p95/p99 latency. This directly covers the "real-time metrics endpoint" item of the brief.

### 10.2 Grafana and alerts
- A provisioned dashboard comes automatically with docker-compose. Panels: throughput per channel, queue depth, error rate, latency percentiles, breaker state, rate limit wait times.
- Example alert rules:
  - the oldest record in the queue is older than 5 min,
  - the error rate is above 10% for 5 min,
  - a breaker stays open longer than 2 min,
  - outbox lag exceeds 30 s.

### 10.3 Logs
- JSON format (`slog`). Common fields: `time, level, msg, service, version, correlation_id, trace_id, span_id`; depending on context, `notification_id, batch_id, channel, attempt` are added.
- A middleware reads the `X-Correlation-ID` header, generates one if missing, and adds it to the response. The ID travels in the `context` and is stored in the database. When a worker picks the record up, it logs with the same ID and forwards it to the provider as a header. This makes it possible to **trace a request from the API call to the provider call with a single ID**.
- PII protection: phone numbers and emails are masked in logs (`+90555***4567`). Content is never logged.
- The log level is set through the environment.

### 10.4 Tracing
- OpenTelemetry records the following as spans: HTTP (otelhttp), database (pgx tracer), Redis, the provider call and the worker processing steps.
- The `traceparent` from the API is stored in the database. The worker span is connected to it with a **link**; at an asynchronous boundary, a link rather than a parent is the right model.
- Results are visible in the Jaeger UI. The sampling rate is set through the environment.

### 10.5 Health
- `/health`: always 200 while the process is up (liveness).
- `/ready`: pings the database and Redis with a 1 s timeout. Returns per-component JSON. 503 if the database is down; 200 + `degraded` if Redis is down or a breaker is open.

---

## 11. Bonus Features (all to be implemented)

| Bonus | Implementation |
|---|---|
| Failure handling | Circuit breaker, DLQ + manual retry, lease/reaper, graceful shutdown, Redis fallback, lossless events through the outbox (§8, §9) |
| Scheduled notifications | A record with `scheduled_at` set enters the `scheduled` status and `next_attempt_at = scheduled_at`. The claim query picks it up at the right time, so no separate scheduler is needed. It can be cancelled until it is due |
| Template system | Go `text/template` (`missingkey=error`). Variables and channel compatibility are validated when a template is saved. Versioning and a `preview` endpoint. For safety, templates have a restricted function set; only safe helpers such as `upper`, `lower`, `default` are available |
| WebSocket updates | On connect, the client sends `{"action":"subscribe","notification_ids":[...],"batch_ids":[...]}`. The hub filters events coming from Redis Streams and forwards them. Liveness is checked with ping/pong; a slow client whose buffer fills up is disconnected (backpressure). Authentication uses the API key in a query parameter or the first message |
| Distributed tracing | OpenTelemetry + Jaeger, span links at the asynchronous boundary (§10.4) |
| GitHub Actions CI/CD | §14 |

---

## 12. Security and Robustness

- API keys are checked with a constant-time comparison from `crypto/subtle`.
- Request body limit, unknown JSON fields rejected (`DisallowUnknownFields`), server timeouts set.
- A panic recovery middleware; the stack trace goes to the log, never to the client.
- No SQL injection: every query is parameterized. The dynamic list filter is built with a whitelisting builder too.
- SSRF protection for callbacks (§9.6).
- Container: distroless base image, non-root user, read-only filesystem.
- Secrets are read only from the environment. `.env.example` contains no real values.
- Config is validated at startup; with a missing or invalid value, the service refuses to start with a clear error message (fail-fast).

---

## 13. Test Strategy

| Layer | Scope | Tools |
|---|---|---|
| Unit | Validation tables (including the SMS segment count), state machine, backoff (seeded jitter, injected clock), error classification, template rendering, cursor encode/decode, HMAC signature, SSRF IP filter | `testing`, `testify` |
| Fuzz | Validation and cursor decoding: no input may cause a panic | Go native fuzzing |
| Handler | Success and failure paths for every endpoint (with a mock service) | `httptest` |
| Contract | Every handler response validated against the OpenAPI spec | `kin-openapi` |
| Integration | Repository queries; concurrent claims under `SKIP LOCKED` (a record must never be taken twice); concurrent same-key idempotency; the cancel-claim race; the outbox relay; the Redis rate limiter never exceeding 100/s | `testcontainers-go` |
| E2E | With the real API + worker + mock provider: 503→503→202 (retry and attempt records), 400 (straight to failed), the breaker opening during a provider outage while retry attempts are preserved, scheduled delivery, batch cancellation, events reaching WebSocket, callback signatures | `testcontainers-go`, mock provider |
| Chaos | A worker is killed mid-work; verify that after the lease expires another worker completes the records and nothing is lost | Within E2E |
| Load | k6 scenarios: (a) sustained load, (b) a flash sale burst (50,000 notifications in 60 s), (c) the provider failing 20% of calls. Measured: API p99 latency, the actual send rate per channel never exceeding 100/s, queue drain time. **Results are published as a table in the README** | k6 + mock provider |

Commands:
- `make test`: unit + integration + e2e (needs Docker), with `-race`.
- `make test-unit`: no Docker needed, finishes in seconds.
- `make test-load`: runs the k6 scenarios.
- `make cover`: produces a coverage report. Target: 80%+ in the domain, service and worker packages (the number is not a goal in itself; critical paths must be tested).

---

## 14. CI/CD (GitHub Actions)

- **Triggers:** push and pull request.
- **Jobs** (in parallel):
  1. `lint`: `golangci-lint` (errcheck, govet, staticcheck, gosec, revive, gofumpt…).
  2. `generate-check`: `oapi-codegen` and `sqlc` output is regenerated and the git diff must be empty. This way the spec and the code cannot drift apart.
  3. `test-unit`: runs with the race detector, coverage uploaded as an artifact.
  4. `test-integration`: runs with testcontainers (Docker is available on GitHub runners).
  5. `security`: `govulncheck` and a Trivy image scan.
  6. `build`: builds the Docker image. On the main branch it is pushed to GHCR; this is the CD part.
- **Dependabot:** for Go modules, GitHub Actions and Docker base images.

---

## 15. Docker Compose

Services started by `docker-compose up`:

| Service | Port | Note |
|---|---|---|
| `postgres` | 5432 | With healthcheck and volume |
| `redis` | 6379 | With healthcheck |
| `migrate` | — | Runs once and exits; `api` and `worker` depend on it (`service_completed_successfully`) |
| `api` | 8080 | |
| `worker` | — | `replicas: 2` to demonstrate horizontal scaling and the shared rate limit |
| `mockprovider` | 9000 | The default provider. Failure rate, latency and 429 rate are set through the environment |
| `jaeger` | 16686 | |
| `prometheus` | 9090 | With alert rules |
| `grafana` | 3000 | Dashboard provisioned, anonymous read access |

- **Switching to webhook.site:** setting `PROVIDER_URL=https://webhook.site/<uuid>` in the `.env` file is enough. Why is the mock the default? webhook.site's free plan has request and time limits; load testing and reproducible failure scenarios need a mock under our own control. The README explains both setups step by step.
- `make up`, `make down`, `make logs`, `make seed` (sample templates and notifications) commands are provided.

---

## 16. Documentation

- **README.md:**
  - project summary,
  - starting it in 30 seconds,
  - architecture diagram (Mermaid),
  - design decisions and trade-offs,
  - API examples (curl),
  - configuration table (every environment variable),
  - test commands,
  - load test results,
  - known limitations and a "what we would do at larger scale" section.
- **docs/architecture.md:** Component diagram, a "life of a notification" sequence diagram (create → claim → send → retry → event → WebSocket/callback), state diagram.
- **docs/adr/:** A short record for each architecture decision (context, decision, consequences):
  - 001 Postgres queue,
  - 002 Transactional outbox + Redis Streams,
  - 003 Redis GCRA rate limit + fallback,
  - 004 Retry and circuit breaker policy,
  - 005 At-least-once delivery guarantee,
  - 006 Spec-first API.
- **docs/runbook.md:** What to do when draining the DLQ, when a breaker stays open, when the queue keeps growing, and during a Redis outage.
- **api/openapi.yaml:** Every endpoint, schema, example and error response. Served with Swagger UI at `/docs`.
- **examples/requests.http:** Example requests runnable with one click from the IDE.

---

## 17. Step-by-Step Implementation Order

Commits follow the **Conventional Commits** format (`feat:`, `fix:`, `test:`, `docs:`, `chore:`). Every commit builds and passes its tests. At the end of each phase, its definition of done (DoD) is checked.

**Progress:** `[x]` done, `[ ]` pending. Each step is ticked when it is finished, in that step's commit.

### Phase 0: Preparation
- [x] 0. Go 1.27 and Docker Desktop installed ✅ · GitHub repo ✅ · webhook.site URL ✅
   - Open a URL on webhook.site: "Edit" → status 202, content type `application/json`, body `{"messageId":"$request.uuid$","status":"accepted","timestamp":"$time$"}` (using webhook.site's variable support).
   - Create an empty repository on GitHub.
   - **DoD:** `go version` and `docker compose version` work; a curl request to webhook.site returns 202.

### Phase 1: Foundation
- [x] 1. **Skeleton:** `go mod init`, folder structure, config (fail-fast), logger, Makefile, `.golangci.yml`, `.gitignore`, `.env.example`, `git init`. First CI skeleton (lint + build).
- [x] 2. **Infrastructure:** First version of the Dockerfile and docker-compose (postgres, redis, migrate), graceful shutdown skeleton, `/health` and `/ready`.
- [x] 3. **Migrations and domain:** All tables; entities, the state machine and its tests.
- [x] 4. **Validation and the template engine:** Including the SMS segment count, table-driven tests and fuzzing.
   - **DoD:** `docker compose up` migrates the database, `make test-unit` passes.

### Phase 2: API
- [x] 5. **OpenAPI spec (first complete version)** and the `oapi-codegen` setup.
- [x] 6. **Repository layer:** sqlc queries, the dynamic list builder, the tx manager, a single-query batch insert with `unnest` (instead of `COPY`, to support `ON CONFLICT`), integration tests.
- [x] 7. **Services and handlers:** create, batch, get, list, cancel, retry, template CRUD and preview.
- [x] 8. **Middleware:** auth, correlation ID, request logging, recovery, body limit; idempotency (with a concurrency test).
   - **DoD:** Every endpoint works with curl, contract tests pass.

### Phase 3: Processing engine
- [x] 9. **Queue:** Claim, lease, heartbeat, reaper, `LISTEN/NOTIFY` + poll.
- [x] 10. **Rate limiter:** Redis GCRA in Lua, local fallback, accuracy test.
- [x] 11. **Provider client and mock provider:** Error classification, fault injection.
- [x] 12. **Worker:** Channel pools, priority and aging, claim size tied to the limit, graceful shutdown.
- [x] 13. **Retry and failure handling:** Backoff, `Retry-After`, circuit breaker, DLQ, attempt records.
    - **DoD:** E2E tests (retry, permanent error, breaker opening, chaos) pass; a notification reaches webhook.site end to end.

### Phase 4: Events and real time
- [x] 14. **Outbox and relay:** Redis Streams publisher, consumer group management.
- [x] 15. **WebSocket hub:** Subscriptions, filtering, ping/pong, backpressure.
- [x] 16. **Status callbacks:** HMAC signature, retries, SSRF protection.
- [x] 17. **Scheduled notifications:** End-to-end test (scheduled → pending → sent, and cancellation).
    - **DoD:** When a batch is sent, a WebSocket client sees every status change in order; a callback receiver can verify the signature.

### Phase 5: Observability
- [x] 18. **Prometheus metrics** and the `/api/v1/metrics` JSON summary, queue depth sampler.
- [x] 19. **OpenTelemetry + Jaeger:** Span links, `trace_id` in the logs.
- [x] 20. **Grafana dashboard and alert rules:** Including provisioning.
- [x] 21. **PII masking in the logs.**
    - **DoD:** A single notification can be followed end to end through the logs by correlation ID and in Jaeger by trace ID; the dashboard shows live data.

### Phase 6: Quality and evidence
- [x] 22. **Completing the E2E test suite**; `make test` runs everything with one command.
- [x] 23. **k6 load tests:** Measuring the results; tuning indexes and batch size if needed (findings written into an ADR).
- [x] 24. **Completing CI:** generate-check, integration, govulncheck, Trivy, coverage, Dependabot, GHCR push.
- [x] 25. **Security review:** gosec findings, container hardening, secret scanning.
    - **DoD:** CI is green; the load test shows that the 100/s per-channel limit is never exceeded and that a burst drains without loss.

### Phase 7: Delivery
- [x] 26. **Final OpenAPI:** Examples and error responses.
- [x] 27. **Documentation:** README, architecture.md, ADRs, runbook, `requests.http`.
- [x] 28. **Clean install rehearsal:** The repo is cloned from scratch and everything is verified to work with only `docker-compose up` and the steps in the README. The commit history is reviewed.
- [x] 29. **Delivery:** The repository link is shared.

---

## 18. Requirements Coverage

| Requirement from the brief | Covered in |
|---|---|
| Create (recipient, channel, content, priority) | §5, §6 |
| Batch (≤1000) | §5.2 |
| Status by ID / batch ID | §5.1 |
| Cancel pending | §4, §5 |
| List + filter + pagination | §5.1, §5.3 |
| Async queue workers | §8 |
| 100 msg/s per channel rate limit | §8.2 |
| Priority queue (high/normal/low) | §8.1 |
| Content validation | §6 |
| Idempotency | §7 |
| Delivery & retry (candidate's design) | §9 |
| Metrics endpoint (queue depth, success/failure, latency) | §10.1 |
| Structured logging + correlation ID | §10.3 |
| Health check | §10.5 |
| webhook.site provider | §9.1, §15, §17 Phase 0 |
| Burst traffic | §8.3, §13 (load test) |
| Visibility for internal teams and API users | §5, §9.6, §10, §11 (WebSocket) |
| Bonus: failure handling | §8, §9, §11 |
| Bonus: scheduled | §11 |
| Bonus: template | §11 |
| Bonus: WebSocket | §11 |
| Bonus: distributed tracing | §10.4 |
| Bonus: GitHub Actions CI/CD | §14 |
| Repository + clean commit history | §17 |
| README | §16 |
| docker-compose up | §15 |
| Swagger/OpenAPI | §5, §16 |
| Versioned migrations | §3 |
| Single-command testing | §13 |
