# ADR-030 — Standalone projections and operations API

- Status: Accepted, 2026-11-22
- Authority: Requested projections/API milestone and routine implementation decisions

## Decision

Use the existing isolated `projection` PostgreSQL database/login and selected
FastAPI/Starlette/Uvicorn/HTTPX/psycopg/confluent-kafka versions. Keep a separate
CPython 3.12 venv and a hash lock verified on this computer (macOS ARM64).
The [runtime and runbook](../../projection/README.md) implement only operations
projections/API; NiceGUI, Keycloak, Tool Gateway, Copilot, live internal services
and full kind packaging remain separate milestones.

One transaction owns immutable event-ID dedup, entity-version updates, source
checkpoint and durable SSE publication. Kafka acknowledgement follows commit;
assignments resume from SQL checkpoints. Reject checkpoints outside Kafka
retention and source-prefix/bootstrap changes rather than skipping missing
history. Keep the journal for deterministic rebuild; a rebuild rotates the SSE
generation while retaining context, original events, source positions and local
operator audit. Restore an empty database only from a complete retained source.

A shared transaction lock orders publication commits across API/consumer
replicas. Sequence allocation rolls back with its data. SSE polls shared durable
rows, with at most 1000 notifications or five minutes retained; a missed range,
foreign generation or invalid position emits `snapshot.required`. REST snapshots
return the matching publication cursor in `X-Event-ID` and evaluation time in `X-Snapshot-At`, closing the snapshot to
subscribe race without changing any frozen JSON response. Stable opaque list
pages live five minutes in owned storage, are query-bound and work across
replicas. Slow-client batches are bounded and independent of other clients.

## Resolved contract/document gaps

1. The executable Transformer schema has only NORMAL/WARNING/CRITICAL/STALE,
   with required load/policy fields. The conceptual RECOVERING state remains an
   Action lifecycle concept, not a new v1 Transformer enum. Projection code must
   not implement Control's hysteresis, threshold classification or recovery.
2. v1 Kafka has no inventory, policy, transformer-state, scenario or health
   events. Require explicit validated Transformer/Device fixture context for
   standalone operation. It is not a Registry/Control table mirror acquired by
   direct reads. Unknown entities remain journaled without fabricated metadata;
   overview/inventory freshness and SSE health explicitly signal missing context;
   obtaining complete context through an owned boundary is a later live gate.
3. Preserve the fixture's classified state only for its original load, or use an
   unresolved owner alert for the exact load window/revision. A load without
   classification has STALE state and at best PARTIAL freshness. A data age over
   15 seconds or STALE completeness is stale. Missing data is never NORMAL/zero.
4. Actions order by saga version; loads by window/revision; telemetry by instant
   and sequence with deterministic boot/event tie breaks. Alerts have no version
   in v1: order by event instant, revision, status rank and event ID. This is a
   deterministic tie rule, not a substitute for an authoritative alert version;
   producer event-time errors cannot be reconstructed by projections.
5. Public action detail is the frozen ActionPayload; device history is the
   frozen bounded ResultPayload array. Commands and all result events remain in
   the journal rather than adding incompatible response fields. History lists
   return the greatest revision of each load window over `[start,end)`.
6. Alert acknowledgement is an audited Projection operator annotation, overlaying
   OPEN as ACKNOWLEDGED while preserving owner RESOLVED state. It neither writes
   Control state nor changes decisions and survives view rebuild.
7. Policy/scenario operations use explicit deterministic contract mocks. They
   validate requests and persist optimistic versions, idempotency, scenario
   conflicts and mock outcomes. Actor/role, service identity, request ID, trace
   and the 3-second deadline contract are carried; conflict/unavailable/timeout
   are injectable. Live service authentication and network deadlines are not
   claimed. Mock policy writes provide read-your-writes and SSE publication.
8. Mock bearer identities must be explicitly configured; spoofed actor headers
   do not grant roles. The standalone CLI binds loopback. OIDC/Keycloak and
   public exposure remain the identity milestone. SSE payloads describe their
   publication instant; clients refresh snapshots on reconnect and periodically
   for current age even when heartbeats continue without domain events.

## Alternatives, consequences and verification boundary

No in-memory replica pub/sub, SQLite replacement, custom broker, framework DTO
mirror, dependency upgrade or cross-service import is needed. The shared lock and
materialized page lists deliberately target the finite local profile. Scope
locks/SQL queries only after measurement requires more concurrency. Monitor the
journal/disk; retention and full source availability bound disaster rebuild.

Acceptance uses real PostgreSQL schemas, recorded expected snapshots, real
Kafka topics with a killed/restarted consumer process, two HTTP API instances,
real TCP SSE disconnect/reconnect, heartbeats, expiry/gaps and concurrent
mutations. No broker/database outage failover, Kubernetes replicas, distributed
identity, packaging/security scan or closed-loop integration claim follows from
those tests. Executed evidence is in [implementation status](../implementation/IMPLEMENTATION_STATUS.md).

## Prepublication correctness review — 2026-11-22

Replay now verifies every sequence position through the high-water mark, rather
than only checking the oldest retained row. This detects interior/tail holes
from clock-skewed retention as well as ordinary prefix expiry. Evaluate freshness,
publication time and expiry after acquiring the store lock. Initial SSE store
failure returns a contract-shaped 503; established streams close and resume
from the last delivered cursor after store recovery.

Reject conflicting equal-version fixture/event loads in either bootstrap order,
future event/load timestamps beyond five seconds, non-RFC3339 date strings and
excessively nested request JSON. Non-ASCII bearer input returns unauthenticated
rather than an internal error. Consumer subscription failures close the client;
source-binding validation occurs before constructing it. Mutation failure after
notification insertion rolls back policy, audit and stream/cursor together.

Cached pages expose their original `X-Snapshot-At`; frozen freshness is explicitly
as of that time. These changes preserve all frozen v1 JSON shapes and dependency
pins. Regression and publication evidence is in implementation status.
