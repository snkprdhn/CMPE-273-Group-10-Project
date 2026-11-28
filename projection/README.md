# Projection and operations API

Standalone Python/FastAPI backend with real PostgreSQL and Kafka consumers.
The projection owns these tables and credentials. No imports or table reads cross
the boundaries to the device, aggregation or Action services.

The [operator dashboard](../dashboard/README.md) connects through this REST/SSE
boundary and has an independent fixture mode; it does not import the store.

## Run locally

Prerequisite: existing [local dependencies](../infra/compose/README.md), PostgreSQL
on loopback 15432. Recorded replay needs no Kafka or other running service.
Use the managed Python 3.12.9 and an independent venv:

```sh
.venv/bin/python -m venv .tools/projection-venv
.tools/projection-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r projection/requirements.txt
export PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection
.tools/projection-venv/bin/python -m projection migrate
.tools/projection-venv/bin/python -m projection replay
export AUTH_MODE=mock
export MOCK_IDENTITIES='{"local-admin-token":{"actor":"local-admin","role":"Administrator"}}'
.tools/projection-venv/bin/python -m projection serve
```

The example above explicitly selects fixture authentication. For real browser
login and server-side OIDC use the [identity runbook](../identity/README.md);
OIDC is the default when `AUTH_MODE` is absent.
This configured mock bearer identity is for loopback development only.
The CLI always binds 127.0.0.1. No TLS, Gateway, Copilot, external model,
or full kind workload deployment is claimed by this milestone. The existing kind
on Docker Desktop target remains required for the later integration gate.
The hash lock is verified for CPython 3.12/macOS ARM64; Linux image packaging and
its additional native wheel hashes remain the deployment gate.

From another terminal:

```sh
curl -i -H 'Authorization: Bearer local-admin-token' http://127.0.0.1:18084/api/v1/overview
curl -i -H 'Authorization: Bearer local-admin-token' 'http://127.0.0.1:18084/api/v1/transformers?state=STALE&limit=1'
curl -N -H 'Authorization: Bearer local-admin-token' http://127.0.0.1:18084/api/v1/events
```

Historical recorded fixtures intentionally appear **STALE** at today's time.
[expected.json](expected.json) fixes the full expected snapshot at
2026-11-09T18:30:06Z; tests inject that evaluation instant without changing event
timestamps. No time rewriting or automatic zero filling is used.

Implemented routes are `/api/v1/{overview,transformers,devices,alerts,actions,
scenarios,health,ready,events}`, entity details, transformer load history, policy
PATCH, alert acknowledgement, and scenario start/stop. Filters and response
shapes follow [OpenAPI](../schemas/v1/openapi.json). Investigations and the Tool
Gateway remain later milestones. Errors have the frozen `HttpError` shape.
Load history uses `[start,end)` on UTC window start and returns the highest
retained revision of each window. Device command history exposes the latest 200
recorded results, including duplicate-effect responses as distinct audit events;
full command envelopes remain in the journal. Action detail retains the frozen
`ActionPayload` response; it does not add an incompatible commands field.

## Snapshot and live recovery protocol

1. Fetch REST state and retain its `X-Event-ID` response header. The snapshot and
   cursor are read in the same transaction as projection publication.
   `X-Snapshot-At` identifies the evaluation instant; cached pages retain that
   original time and freshness, so label them as that historical snapshot.
2. Subscribe to `/events` with `Last-Event-ID: <that header>`. All replicas share
   one durable PostgreSQL stream; each connection polls it independently.
3. Retain every SSE `id`; reconnect to either replica with that ID. Compare
   versions within a stream generation and discard duplicate delivery. Event
   versions are monotonic publication sequences, not domain saga versions.
4. A missing/foreign/future/expired cursor, retention gap or rebuild generation
   produces `snapshot.required`. Reload REST state, replace the local view, then
   resume from the REST header. Page-cursor conflicts likewise require a new
   snapshot. Do not resume from a reset ID without first fetching state.
5. Heartbeat comments arrive every 15 seconds. A heartbeat proves transport
   liveness, not fresh grid telemetry. Refresh REST freshness on a local timer
   (15 seconds or less) even when no domain events arrive.

Retain at most 1000 events or five minutes, whichever is shorter. A cursor at the
immediate predecessor of the earliest retained event is valid. Durable sequence
allocation and notifications commit with the projection; transaction rollback
cannot create a lost-notification gap. Slow clients hold at most one retained
batch and do not own a shared in-memory queue; if they fall behind retention,
they receive the reset. Every replay checks all sequence positions through the
current high-water mark, including interior/tail holes caused by expiry or clock
skew. Initial database failure returns a safe 503 before stream headers; a later
database failure closes the stream, forcing recovery
from the last received cursor. Replay log errors are never advanced over.

Lists default to 50, cap at 200. Opaque page tokens refer to immutable snapshots
for five minutes, bound to the endpoint/entity/filters/limit, and work across
replicas. Concurrent writes cannot silently shift an existing page. Pagination
keeps the original snapshot cursor; clients may replay overlapping updates after
finishing the pages. Journal/view reads and writes serialize with a database
advisory lock. This finite local profile favors correctness; SQL keysets and
scoped locks are deferred until measured scale justifies them.

## Kafka and rebuild

Seed context once before starting consumers/APIs. Context consists of validated
contract Transformer/Device snapshots, supplied as fixture data rather than
Registry/Control table reads. v1 has no inventory/policy/state/health/scenario
Kafka events. Unknown device/transformer events remain journaled but cannot invent
inventory or policy. Overview/inventory freshness becomes partial when events
reference unknown context, with a degraded `system.health` notification for live
clients. The [local integration profile](../docs/decisions/ADR-033-live-component-integration.md)
fetches device capabilities from Registry gRPC using explicit topology IDs and
initializes the transformer context with a committed Flink load. No control thresholds or recovery state machine are implemented here.

Loads order by window end and revision; action transitions use `saga_version`;
telemetry uses event-time/sequence, then boot/event identity for a deterministic
tie; alerts lack an authoritative version, so event-time, load revision, status
rank and event ID order their transitions. Results retain each logical event.
Command IDs preserve the greatest epoch/saga version. Reusing an event ID with
different contents, or a current load/action version with different payload,
fails without committing either the input checkpoint or the journal. A recorded
load that conflicts with the same version of its fixture snapshot also fails,
regardless of which was loaded first. Future event/load timestamps beyond the
five-second clock allowance are rejected rather than remaining falsely fresh.
An unresolved alert classifies only its matching load window/revision. The
explicit owner NORMAL classification also applies only to its exact revision. Missing
classification for a new load yields `state=STALE` and at best `freshness=PARTIAL`;
old/STALE completeness yields stale data. The API never infers NORMAL or recovery
from absent alerts. The frozen schema has no RECOVERING Transformer state.

For Kafka, create all six canonical topics first using the Kafka topic setup, or
use a dedicated prefix with the same topic suffixes. The automatic live test
creates/deletes only its own random prefix. Start a consumer in another terminal:

```sh
export KAFKA_BOOTSTRAP=127.0.0.1:19092
export KAFKA_PREFIX=''  # choose the intended recorded/live namespace before first use
.tools/projection-venv/bin/python -m projection consume
```

The source bootstrap/prefix is bound to the projection database. Keep the source
and its topics intact while using retained checkpoints. Standalone consumers
subscribe to all six topics by default. The kind Deployment runs two workers in
the same Pod. `integration-kind.projection` handles loads, alerts, actions,
commands and results. `integration-kind.projection-telemetry` handles raw
telemetry with independent Kafka fetches and commits. Topic subsets are disjoint
across groups that share the store. Replicas of a worker use its group and topic
list. Read-committed consumption disables automatic offset storage/commit.
Journal, projection, durable source position and SSE event commit before broker
acknowledgement. Restart assignments use the SQL position, including after a
crash between SQL and Kafka commits. The worker logs invalid records and durably
advances their positions, as it does for out-of-scope records. Database failures
restart the worker from its SQL checkpoint. Kafka positions outside retention
fail closed with the default settings. The hosted context-scoped workers log and
resume from the oldest retained offset when retention has removed earlier records.
The load worker runs retention pruning. The SQL checkpoints are keyed by topic
and partition, so the new telemetry group resumes the existing position without
resetting offsets. The Deployment uses Recreate to stop the previous worker
before either new worker starts. Apply the same stop-before-start order on rollback.

Rebuild owned views from the retained journal (stop the consumer for maintenance):

```sh
.tools/projection-venv/bin/python -m projection rebuild
```

This preserves context, audit/mutation outcomes, original events and source
positions, and rotates the stream generation. For loss of the whole database,
create a new isolated projection store, migrate/seed context and replay the full
retained Kafka/archive source. Never reset the shared development database or
other services' topics. Journal retention is the project lifetime; monitor disk
use. A partial Kafka log cannot reconstruct missing history.

## Mock operations and ownership

The scenario and policy adapters use **contract mocks** with real
validation, optimistic versions, active-run conflict, durable idempotency and
failure injection (`ContractMocks('conflict'|'unavailable'|'timeout')`). They
perform no simulator or control effects. Policy changes in mock mode provide
read-your-writes and SSE updates in the owned store. Calls carry authenticated
actor/role, request ID, traceparent, idempotency key, mock service identity and a
3-second deadline contract. No unauthenticated browser actor headers are trusted.
Real service authentication, network deadlines and live operation delivery are
not exercised by a mock and remain a future integration gate.

Alert acknowledgements are owned operator annotations, durably audited separately
from domain events. They overlay OPEN as ACKNOWLEDGED; they never reopen RESOLVED
alerts or change control behavior, and survive view rebuild. All mutation keys
are scoped to actor across operations; reusing a key with different input returns
409. Role enforcement uses validated Keycloak identities by default, or explicit
mock identities in selected test mode; see [identity](../identity/README.md).

## Verify

```sh
PROJECTION_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection PROJECTION_TEST_KAFKA=127.0.0.1:19092 make projection-test
make contracts BASE_REF=HEAD
.tools/projection-venv/bin/python -m pip check
git diff --check
```

Tests create/remove random owned schemas and Kafka topics, run a real consumer
subprocess through SIGKILL/restart, and run two HTTP/SSE servers over real TCP.
Without the DSN/broker environment, dependency tests explicitly skip rather than
claim live acceptance. Tests exercise expected snapshots, fresh/partial/stale
state, duplicates/reordering, rebuild, cursor expiry/count gaps, disconnection,
heartbeat, cross-replica replay/pages/mutations, rollback and safe outages.

## Runtime configuration

| Setting | Type/default | Allowed values / classification |
|---|---|---|
| `PROJECTION_DSN` | Required PostgreSQL connection string | Owned projection store only; secret; never logged |
| `AUTH_MODE` | Default OIDC; `mock` explicitly selects test identities | Local development only |
| `MOCK_IDENTITIES` | Required JSON token-to-actor/role mapping in mock mode | Nonempty, tokens ≥16 characters, contract IDs/roles; secret; local mock only |
| `KAFKA_BOOTSTRAP` | String, `127.0.0.1:19092` | Local broker address, bound to the store on first consumer run; nonsecret |
| `KAFKA_PREFIX` | String, empty | Existing local topic namespace, immutable for a store; nonsecret |
| `PROJECTION_GROUP` | String, `projection.v1` | Kafka consumer group; use the same group for consumer replicas; nonsecret |
| `PROJECTION_TOPICS` | Optional comma-separated canonical topic names, defaults to all six | Canonical unprefixed names, each appearing once; nonsecret |
| `--port` | Integer, 18084 | Available TCP port 1–65535; CLI always binds loopback; nonsecret |
| `--fixtures` | Path, `projection/fixtures.json` | Synthetic validated contract context/events; nonsecret |
| `PROJECTION_TEST_DSN` | Optional PostgreSQL connection string | Test-owned schema privileges in projection store; secret; absence skips DB tests |
| `PROJECTION_TEST_KAFKA` | Optional local broker address | Create/delete permissions for isolated test topics; nonsecret; absence skips broker test |
