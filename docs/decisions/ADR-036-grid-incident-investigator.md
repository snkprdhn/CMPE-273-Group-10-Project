# ADR-036 — Bounded deterministic Grid Incident Investigator

- Status: Accepted within the requested Investigator milestone, 2026-11-23.
- Owners: Member_4 projection/API/UI/identity/Copilot; Member_1 simulation/Registry/observability;
  Member_2 Kafka/PyFlink; Member_3 control/PostgreSQL/Debezium.

## Decision

Use an independent Python FastAPI process, existing HTTPX/jsonschema/OIDC pins,
seven frozen typed tools, and a deterministic provider. No framework, provider
SDK, external call, shell, raw query input, producer, control calculation, or
new public contract is introduced. The separate hash lock excludes Kafka and
PostgreSQL drivers. The mock's fixed plan reads snapshot/history/action journal,
then command/result evidence for discovered actions. It generates observations
from typed fields; it never interprets retrieved text as instructions.

The projection API owns a new Operator-only bounded internal read of its own
journal. This resolves ADR-031's earlier limitation: full retained Action/Alert
and Command envelopes exist in the owned projection journal even though public
snapshots expose only payloads. Do not manufacture envelopes from those snapshots.
The journal read caps at 200 source events and rejects larger reads. Tool results
remain limited to 50 items by this runtime and 64 KiB. Retention/replay coverage
is not proof of a complete physical device ledger; timeline and command evidence
remain PARTIAL. Missing confirmed reduction is an error, never an invented zero.

Model proposals carry no trusted identity/context. Every dispatch revalidates
the signed caller, Operator role, tool enum and strict frozen schema. Server-owned
entity/time scope, discovered action/correlation IDs, and deadline bound dispatch.
Trace lookup requires a previously evidence-authorized ID. Relative deep links
open the authenticated actor-owned investigation view. Results are recursively
restricted to schema-declared fields. Tool errors expose safe codes, never backend
queries or exception details. Retrieved text is displayed as data, not executable
HTML or instructions. Questions requesting writes/instruction overrides are denied
and hashed in audit. The guard is conservative; the capability boundary, rather
than keyword classification, prevents writes.

The default wall deadline is 30 seconds, with 3-second tools, 5-second model calls,
12 calls, 6 turns, two active investigations and six starts/minute per actor,
and eight active investigations per process. Calls execute sequentially. Three
provider/validation failures open an agent-only 30-second breaker. Mock failures
are not retried. Cancellation, invalid drafts, invented IDs and timeouts terminate
with explicit uncertainty; invalid draft text never reaches the UI. Natural-language
truth beyond typed mock observations is not established by citation validation.

Single-process SQLite contains only actor/scope, hashes, safe audit records,
versioned evidence and output. It is independently pruned after 24 hours on access.
SSE events retain at most 1,000 events globally or five minutes, checked on access;
gaps request a REST snapshot. Restart marks interrupted jobs FAILED. A Recreate
single-replica kind Deployment uses a dedicated PVC; this is not an HA audit store.
Provider tokens do not exist in MOCK mode, so no placeholder provider Secret is
created. User bearer tokens remain server-held and are not persisted in audit.

The UI polls authenticated REST for progress/final output, displays evidence,
freshness and unknowns, and provides an authorized investigation deep link.
The optional investigation SSE route has persisted cursors and expiry checks.
The existing main operations SSE and deterministic control components have no
Copilot dependency. Kind network policy permits UI ingress and API/identity egress,
plus existing namespace DNS/telemetry rules. No ServiceAccount token is mounted.
The gateway shares the projection API process, authenticated using the caller's
OIDC bearer rather than inventing unsigned service headers. This local design
has no separate workload mTLS identity; the model has no HTTP client capability.

## Explicit unavailable evidence and scope limits

The frozen stream summary requires non-null lag, watermark delay, checkpoint age,
readiness and restart scalars for every component. Existing raw backend series do
not supply every scalar for every component. The bounded adapter reads optional
`grid_stream_<field>{component="ENUM"}` recording-rule series and succeeds only
when all are finite, nonnegative and within 15 seconds. Missing signals yield
DEPENDENCY_UNAVAILABLE, never healthy zeroes. No recording rules are invented in
this milestone. Metric enums use fixed aggregate queries; entity-filtered metrics,
DATA_AGE without an exporter, and CHECKPOINT_AGE without a true age signal remain
unavailable. Checkpoint duration is explicitly not checkpoint age. Tempo output
omits attributes/events/names and uses redacted span labels; correlation search
without a known trace ID remains unavailable. The current mock plan does not
request observability tools or trace discovery. These are explicit bounded
capabilities, not a claim of full observability diagnosis or evaluation coverage.

v1 Investigation fixes provider_mode to MOCK. External providers, paid API calls,
provider credentials and SDKs remain deferred. A Codex subscription is not treated
as application API entitlement. No external adapter is needed for this gate.

## Verification boundary

Real PostgreSQL seeded projection/gateway/provider tests, local TCP handler tests,
negative identity/scope/write/citation tests, cancellation/replay/budget tests,
Chromium drawer/failure/keyboard checks, container build and kind rendering are
recorded in implementation status. This does not establish live kind Investigator
rollout, production security, complete evaluation-case coverage, or a measured
full Kafka-to-device closed loop during provider outage. Those acceptance gates
remain explicit follow-up work; earlier platform acceptance is preserved.
