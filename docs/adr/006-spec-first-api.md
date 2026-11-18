# ADR 006: Spec-first API

**Status:** accepted

## Context

An API documented separately from its code drifts: fields get renamed,
status codes added, and the documentation stops being true. Clients of a
notification service (and the reviewers of this assessment) rely on the
documentation.

## Decision

`api/openapi.yaml` is the source of truth. `oapi-codegen` generates the
request and response types and a **strict server** interface from it: a
handler receives typed input and must return one of the responses the
spec declares for that operation, so an undocumented status code or body
does not compile.

Kept honest by tests:

- **Contract tests** send real requests through the router and validate
  every response against the spec (`kin-openapi`).
- **Spec tests** validate the spec itself, every example in it against
  its schema, conventions (every `/api/v1` operation documents 401 and
  500; every one with a body documents 400 and 413) and that enums match
  the domain.
- **CI** regenerates the code and fails if it differs from what is
  committed.
- Request bodies are decoded strictly (unknown fields rejected); a guard
  test fails if an operation with a body is not covered.

The spec is embedded in the binary and served at `/openapi.yaml`, with
Swagger UI at `/docs`.

## Consequences

- Documentation and behaviour cannot drift without a test or the
  compiler noticing.
- Changing the API starts with the YAML, then `make generate`.
- Hand-written endpoints outside the generator (health probes, the
  WebSocket upgrade) are documented in the spec but checked only by their
  own tests.

## Alternatives

- **Code-first** (annotations generating the spec): the spec describes
  what the code does, not what was agreed, and drifts the moment an
  annotation is forgotten.
- **Hand-written spec, unchecked:** drifts.
