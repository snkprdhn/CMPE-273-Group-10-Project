# Implementation status

Updated **2026-11-22 (America/Los_Angeles)** for Member_3 Action domain acceptance. Member_1 simulation/Registry, Member_2 aggregation and the Member_3 standalone Action domain are implemented. Member_1 retains simulation/Registry/observability, Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium and Member_4 projection/API/UI/identity/Copilot ownership. Historical evidence is preserved below.

## Completed milestones

| Milestone | Result | Evidence |
|---|---|---|
| Repository/machine preparation | Accepted small Python/container stack, ADRs, manual prerequisites, resource profiles and root conventions | [Matrix](VERSION_MATRIX.md), [prerequisites](LOCAL_PREREQUISITES.md), [preparation evidence](PREPARATION_VERIFICATION.md) |
| Existing local prerequisites/empty cluster | Managed Python 3.12.9, isolated preparation venv, pinned native tools, Docker Desktop with built-in Kubernetes disabled, empty kind Kubernetes 1.34.0 | Read-only inventory reconfirmed below; earlier setup commands/digests retained in preparation evidence |
| Minimum local development infrastructure | One Kafka broker, PostgreSQL primary, on-demand Toxiproxy; isolated stores, real smoke checks, restart persistence, guarded reset | [Compose runbook](../../infra/compose/README.md), executed acceptance below |
| Executable contract bundle v1 | Eight Kafka events; canonical topology, REST/internal/SSE and seven agent-tool/evidence/output schemas; Registry proto; versioned examples and fixed golden scenarios; compatibility tests and Ubuntu CI workflow | [Bundle](../../schemas/README.md), [ADR-025](../decisions/ADR-025-executable-contract-freeze.md), `make contracts` |
| Member_1 standalone edge and Registry | Deterministic three-device scenario; Kafka telemetry/commands/results; durable simulated effects/dedup/fences; PostgreSQL gRPC Registry; operational endpoints and independent ARM64 images | [Runbook](../../member1/README.md), [ADR-026](../decisions/ADR-026-member1-standalone-state.md), executed evidence below |

| Member_2 standalone Kafka/PyFlink aggregation | Native ARM64 source-built runtime, 56-package hash lock; fixed golden telemetry, transactional load/DLQ/late output; checkpoint/replay proof with an open transaction at TaskManager crash | [Runbook](../../member2/README.md), [ADR-027](../decisions/ADR-027-member2-standalone-aggregation.md), evidence below |
| Member_3 Action domain and durable saga | PostgreSQL transactional state/inbox/outbox, deterministic fixture allocation, retry/timeout/hold/restore, optimistic versions and restart recovery | [Runbook](../../member3/README.md), [ADR-028](../decisions/ADR-028-member3-action-domain.md), 42 PostgreSQL tests and demo below |

## Current milestone and verification

**Member_3 Action domain logic and durable state machine are complete locally.**
Real PostgreSQL acceptance covers threshold/hysteresis/cooldown/freshness,
deterministic allocation, stable identities, optimistic versions, transactional
inbox/outbox, partial/timeout review, safe hold, restore and restart recovery.
All 14 load/result scenarios assert fixed durable transition sequences, command
targets and outbox records without duplicate logical plans. The separate-process
demo reads back COMPLETED, four commands and 13 canonical-valid outbox rows.
See [Member_3 runbook](../../member3/README.md) and
[ADR-028](../decisions/ADR-028-member3-action-domain.md).

No blocker remains for this requested fixture-driven domain slice. Live charger
observation acquisition, Kafka/CDC publication, Kubernetes Lease/epoch allocation,
operator compensation API and full platform deployment remain later gates.
Member_1/2 services were already running and were neither required nor modified.
No system installations or cloud/model calls were performed. The initial domain
implementation made no commits or pushes; the user subsequently authorized review,
fixes, and a normal commit/push to main (review evidence below).

## Member_3 Action domain — executed 2026-11-22

| Exact command / check | Result |
|---|---|
| `.venv/bin/python -m venv .tools/member3-venv` | Independent CPython 3.12.9 component environment created |
| `.tools/member3-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --require-hashes -r member3/requirements.txt` | 12 locked packages installed offline; accepted psycopg 3.2.9/jsonschema 4.23.0/pytest 8.3.5 pins and hashes reused |
| `.tools/member3-venv/bin/python -m pip check` | No broken requirements |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml ps --all` | Existing PostgreSQL healthy on loopback 15432; existing unrelated component containers preserved. Docker/host TCP checks required sandbox escalation; no infra restart/reset was performed |
| `.tools/member3-venv/bin/python -m pytest -q tests/member3` (before final added cases, DSN absent) | 2 pure checks passed; then 20 database checks explicitly skipped. Not acceptance evidence |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3` | Initial 22, expanded 36 and final **37 passed** against real PostgreSQL; isolated random schemas removed after each test |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | Initial **37 passed**; post-review **42 passed**, including fixed fixtures, concurrent deliveries, atomic rollback after an outbox insert, optimistic conflicts, stale epochs, restore safety, cooldown and separate-process recovery |
| `.tools/member3-venv/bin/python -m member3 cases` | Lists all 14 fixture scenarios |
| `CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m member3 replay --schema member3_demo --case 'safe restore and restart' > .tools/member3-safe-restore.json` | Durable demo completes; synthetic `member3_demo` schema intentionally retained for user inspection |
| `CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m member3 inspect --schema member3_demo > .tools/member3-inspected.json` | Fresh process returns identical committed state |
| `.tools/contracts-venv/bin/python` inline validation of those two JSON files using `tools.contracts.validate` on every outbox event/key, plus state equality/count assertions | COMPLETED, four commands, 13 semantically valid outbox rows; exact fresh-process equality |
| `make contracts BASE_REF=HEAD` | **214 passed**; frozen schema/OpenAPI/Protobuf compatibility and canonical Member_3 fixture validation pass; no contract shapes changed |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed**; existing infrastructure/preparation/aggregation behavior and documentation links preserved |
| `git diff --check` | Passed |

### Member_3 prepublication review — 2026-11-22

The user authorized review, fixes, commit and a normal push to `main`. Review
found and fixed three issues: a same-window correction could overwrite visible
review hold with NORMAL; delayed results could bypass exhausted deadlines when
no timer ran during downtime; and extremely small/large finite loads could
produce an empty ACKS_PENDING plan (allocation resolution/intermediate overflow).
Four new cases reproduced the failures before the fix. A fifth regression checks
exhaustion of the final retry before TTL. Deadline exhaustion is shared by result
handling and the timer; late effects are recorded while the saga remains held.
Inspection now reads one consistent, read-only PostgreSQL snapshot.

| Exact command / check | Final result |
|---|---|
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; `0 0` before creating the Action commit |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3 -k 'review_hold_survives or late_results_cannot or extreme_finite'` | **4 failed before fixes**, proving the regressions |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | **42 passed**, no skipped database tests |
| `make contracts BASE_REF=origin/main` | **214 passed**; canonical schemas, inputs and compatibility verified |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/member1-venv/bin/python -m pytest -q tests/member1/test_edge.py tests/member1/test_operations.py` | **16 passed** with localhost permission; initial sandbox run had 15 pass/1 socket-bind permission failure, resolved by rerunning with host access |
| `.tools/member3-venv/bin/python -m pip check`; `.tools/member3-venv/bin/python -m compileall -q member3 tests/member3`; `git diff --check` | All pass; no broken requirements or compilation/whitespace errors |

Only Action milestone files and related documentation/test-target changes are
included. Existing Member_1/Member_2 services and local data are preserved.
Publication result is recorded by Git history/remote state; hosted CI is a
separate check, not claimed by these local results.

### Member_3 decisions and limitations

- PostgreSQL is authoritative; inbox, domain state, commands, saga progress and
  outbox commit atomically. A bounded global advisory lock serializes the small
  standalone store; database versions/unique constraints enforce conflicts and
  one active saga per transformer. No SQLite or success-returning relay stub.
- Registry v1 lacks current-rate/online/charging observations. Explicit fresh
  fixture observations supplement canonical capability records; no rate is
  inferred from maximum capability or missing telemetry. This gap and the
  outbox-commit meaning of dispatch states are resolved in affected design/docs.
- Insufficient capacity records review/hold without issuing an inadequate plan.
  Corrections preserve action history; retries preserve command/event IDs and
  original expiry, with at most two total attempts. Physical outbox retry rows
  have distinct delivery IDs. Partial/time-out/late results remain held until
  explicit operator reconciliation; no automatic restore from unknown state.
- Restore requires distinct consecutive post-confirmation recovery windows,
  current matching capabilities/rates and projected restored load below critical.
  Missing/stale state never means zero load or safe restoration.
- Supplied positive epochs are stored/compared. Distributed Lease acquisition and
  durable cluster epoch allocation, Kafka consumers/offsets, Debezium routing,
  live observation adapters, authenticated operator compensation, kind packaging,
  metrics and backup/export remain unimplemented here. Database-server restart,
  actual CDC delivery and end-to-end physical effects are not claimed. Mac ARM64
  offline install is verified; Linux runtime/security scans are future gates.

## Historical minimum-infrastructure verification

**Minimum local development infrastructure is complete and accepted locally.** Clean startup, Kafka publish/consume, four-store persistence/authentication/isolation, container-recreation retention, Toxiproxy outage/recovery, shutdown and disposable reset all pass. No blocker remains in this milestone. See [local dependency runbook](../../infra/compose/README.md). The preparation and executable-contract work is complete locally. No system software was installed, Docker settings/cluster resources were not changed, and no cloud resources or external model calls were made. Contract authoring made no commits or pushes; the user subsequently authorized verification, committing and pushing these changes to main. Only pinned Python contract tools were installed into ignored component venvs. The existing `.venv` preparation tools remain intact.

Executed evidence:

| Exact command / check | Result |
|---|---|
| `uname -m`; `sysctl -n hw.memsize hw.ncpu machdep.cpu.brand_string`; `df -h .`; `memory_pressure` | ARM64 Apple M5, 10 cores, 17,179,869,184 bytes RAM; 258 GiB disk available; 36% free-memory snapshot. sysctl required approved host access after sandbox denial. |
| `git --version`; `.venv/bin/python --version`; `.venv/bin/kind version`; `.venv/bin/kubectl version --client`; `.venv/bin/helm version --short` | Git 2.54.0; CPython 3.12.9; kind 0.30.0; kubectl 1.34.0/Kustomize 5.7.1; Helm 3.19.0 |
| `python3 --version`; `command -v node`; `node --version`; `command -v java`; `java -version`; `colima version`; `colima status` | Global Python 3.14.7 preserved; Node absent; Java launcher present but no runtime; Colima 0.10.3 installed/stopped. Node/Java/Colima are not selected prerequisites. |
| `docker version`; `docker info --format '{{.Architecture}} {{.NCPU}} {{.MemTotal}}'`; `docker desktop kubernetes status`; `.venv/bin/kubectl --context kind-smart-grid-dev get nodes` | Desktop 4.99.0, CLI 29.8.0, Engine 29.9.3; Linux ARM64, 4 CPUs, 6,210,510,848 usable guest bytes; built-in Kubernetes disabled; existing project node Ready/v1.34.0. Docker socket checks required approved host access after sandbox denial. |
| `.tools/contracts-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: --require-hashes -r requirements-contracts.txt --dest .tools/contract-wheels-macos` | All 27 locked distributions have hash-verified compatible wheels |
| Same download with `--dest .tools/contract-wheels-linux --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All 27 Linux x86_64 wheel artifacts resolved and verified; not a Linux execution claim |
| `.venv/bin/python -m venv .tools/contracts-lock-check`; `.tools/contracts-lock-check/bin/python -m pip install --no-index --find-links .tools/contract-wheels-macos --only-binary=:all: --require-hashes -r requirements-contracts.txt`; `.tools/contracts-lock-check/bin/python -m pip check`; `.tools/contracts-lock-check/bin/python tools/contracts.py` | Clean offline hash-checked installation and pip check passed; full contract suite passed at that checkpoint (195 checks before final additional cases) |
| `make contracts`; `make bindings`; `make contracts BASE_REF=HEAD` | Final suite: **212 checks pass**; OpenAPI/schema validation, Protobuf compilation and generated capability consistency pass. Git HEAD has no executable v1 baseline: explicitly reported as initial freeze; deliberate incompatible JSON, OpenAPI and Protobuf changes are detected. |
| `.tools/contracts-venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py`; `git diff --check` | **10 checks pass**; whitespace check passes |

The bundle contains **77 valid JSON examples, 76 invalid JSON examples with precise expected failure reasons, six valid/four invalid Registry examples, 17 stream/control golden scenarios and five agent authorization/evidence scenarios**. It covers stable UUID identities, UTC times, kW signs, partition keys, duplicates, out-of-order samples, exact window/watermark/lateness/freshness/completeness boundaries, corrected histories, checkpoint-output replay, stale/missing capacity, expiry, epochs, scoped read-only tools and evidence grounding. The reference replay is an executable specification, not production aggregation/control code.

## Decisions and limitations

- [ADR-002](../decisions/ADR-002-python-stream-processing.md), [ADR-004](../decisions/ADR-004-python-dashboard.md), [ADR-006](../decisions/ADR-006-local-kind.md) and [ADR-024](../decisions/ADR-024-pinned-local-baseline.md) remain accepted: PyFlink 2.2.1/Python 3.12.9, NiceGUI, container-only JVMs, Docker Desktop/local kind. Upstream Flink/Helm compatibility was rechecked; full application metadata was not re-resolved. No host Java/Maven/Node/npm is required.
- [ADR-025](../decisions/ADR-025-executable-contract-freeze.md) fixes Registry pagination/errors/deadlines, stable window/load UUIDs, corrected-window semantics, command expiry/fencing, control policy defaults, OIDC realm/client/role names, REST/internal errors/idempotency, SSE replay, and agent scope/budgets. The system-design recovery proposal is now accepted at **75% for two fresh COMPLETE windows**. No blocking choice remains in this contract milestone.
- The new independent compiler/validator environment has a portable hashed lock. Application packages remain independently scoped; no PyFlink/Beam dependencies were merged into it.
- CI workflow is authored for standard GitHub Ubuntu runners and its local command sequence passes. Hosted CI results are tracked by GitHub Actions for the published commit; local checks are not a hosted-run claim. The compatibility gate deliberately rejects some safe changes and cannot detect arbitrary changes in business meaning; producer/consumer review remains required.
- Member_1 standalone Kafka routing, durable simulated effects/fencing, Registry local-token authorization and process/container restart proofs now pass (evidence below). PyFlink ARM64 import/checkpoint proof now passes under ADR-027; replicated Kafka recovery, Registry mTLS/production identity, OIDC, SSE, policy state machines, full-stack behavior, CNI allow/deny and Kubernetes PVC I/O/recovery remain unverified. The empty cluster does not establish them.
- Lightweight development remains 4 CPUs/6 GiB VM; staged review remains 8 CPUs/10 GiB on this 16 GiB host. Larger-profile fit, performance, security scans and application/chart/image artifact gates remain unmeasured. Licensing remains the owner's decision before public release.

## Next milestone

**Member_3 standalone control delivery/integration:** implement Kafka fixture
consumption and Debezium outbox publication with duplicate-delivery proof, then
explicit live charger observations and Lease/fencing allocation before pairwise
control acceptance. Retain Member_1 simulation/Registry/observability, Member_2
Kafka/PyFlink and Member_4 projection/API/UI/identity/Copilot boundaries. Operator
compensation and operational packaging remain explicit follow-on work. Member_2
kind/operator deployment and distributed rescaling remain later gates.

## Prepublication verification — 2026-11-21

The user authorized verification and a normal push to `main`. `git fetch origin main` succeeded and `git rev-list --left-right --count origin/main...HEAD` returned `0 0` before creating the contract commit. Review found and fixed two validator edge cases: compatibility now compares JSON value types recursively (including booleans inside arrays), and load/unit checks enforce the documented absolute 1e-9 tolerance without relative tolerance. Four regression cases failed before the fixes and pass afterward.

Executed `make contracts BASE_REF=origin/main` (**212 passed**), `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py` (**10 passed**), `.tools/contracts-venv/bin/python -m pip check` (no broken requirements), and `git diff --check` (passed). Publication state is recorded in Git history and GitHub Actions; no force push is authorized or needed.

## Minimum local development infrastructure — accepted 2026-11-22

Member_2 owns Kafka/topics; Member_3 owns PostgreSQL/control; Member_1 owns Registry
and fault tooling; Member_4 owns projection and Copilot audit. One Kafka 3.9.2 KRaft
broker and PostgreSQL 17.4 primary run in the fixed `smart-grid-local-dev` Compose
project. Toxiproxy 2.12.0 runs only on demand. All bases retain their accepted digest
pins; no runtime/library versions changed. Four service-owned databases have separate
nonprivileged roles, SCRAM host authentication and no cross-store CONNECT. Topics
come from executable `x-events`, with `local-dev.` physical prefixes, three partitions,
RF=1/minISR=1 and 24-hour retention. Consumer-group conventions and safe application
connection examples are documented in the [runbook](../../infra/compose/README.md).

### Resolved startup failure and implementation decisions

Earlier runs on 2026-11-21 stalled in Created. Repeated restarts and API 1.51 did not
fix it. Local-only Docker diagnostics identified a macOS FSEvents watcher stuck in
`EventStreamCreate`/`AddWatch`, with `VolumeShare.Add` and container-start rewrites
waiting for `recomputeWatchesSync`. The earlier inference that every new container
was independently broken was too broad: a volume-free Toxiproxy check passed before
Compose triggered the shared watcher stall, then unrelated starts queued behind it.

The fixed profile has **no runtime host bind mounts**. A small multi-target Dockerfile
copies config into local images from the same pinned upstream bases; `up --build`
uses cache and includes configuration edits. Named volumes still persist data.
One authorized Docker Desktop force stop/start cleared old stuck requests; no
factory reset, software update, storage-engine change or unrelated deletion occurred.
Local diagnostics were not uploaded and their temporary bundle was deleted.

Once startup was unblocked, real Kafka startup exposed missing/wrongly owned data
and runtime-log directories. The derived image prepares `/var/lib/kafka/data` and
`/opt/kafka/logs` for UID 1001, and Kafka runs as that non-root user. Regression checks
cover no host binds/loopback ports and rebuilding configuration images. No full
monitoring stack, application services, cloud or external model integration was added.

### Executed acceptance evidence

| Exact command / check | Result |
|---|---|
| `make infra-reset CONFIRM=smart-grid-local-dev`; `make infra-up` | Clean volumes initialized; Kafka and PostgreSQL healthy; eight canonical domain topics created; all four database logins/queries pass |
| `make infra-health`; `make infra-smoke` | Kafka key/value publish (`acks=all`) and consume pass; writes/reads in four databases pass; four bad-password attempts and all 12 cross-store connections rejected |
| `make infra-verify` | Same Kafka record and all four database rows survive full `down`/`up` container removal/recreation; auth/isolation checks pass again |
| `make infra-faults-up`; `make infra-fault-smoke`; `make infra-faults-down` | Proxy API ready; PostgreSQL query forwarded, rejected during disabled-proxy outage, and forwarded after recovery; proxy removed |
| `.venv/bin/python` inline configuration/endpoint assertions | Compose JSON has no host binds and only loopback ports; host TCP 19092/15432 reachable; exactly eight domain topics after excluding Kafka internal names; `kafka id -u` returns 1001 |
| [Disposable reset acceptance proof](../../infra/compose/README.md#disposable-reset-acceptance-proof) (Python seed, `make infra-down`, volume inspection, confirmed reset, `make infra-up`, Python absence assertions, `make infra-smoke`, `make infra-down`) | Probe topic and rows seeded in all four stores; normal down retains exactly two volumes; reset removes both; startup recreates eight contract topics and probes/smoke tables are absent in all four stores; fresh smoke passes |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml --profile faults ps --all`; `docker volume ls --filter label=com.docker.compose.project=smart-grid-local-dev` | Final shutdown leaves no project containers and retains exactly `smart-grid-local-dev_kafka-data` / `smart-grid-local-dev_postgres-data` |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed**, including reset guard, failure propagation, timeout-byte regression, fault cleanup, image rebuild, no host bind mounts and loopback bindings |
| `make infra-health` after final shutdown; `make infra-reset` without confirmation | Both correctly return nonzero (make exit 2): stopped dependencies reported; destructive reset rejected before Docker |
| `.venv/bin/kubectl --context kind-smart-grid-dev get nodes`; `docker desktop kubernetes status`; `docker ps --all --format '{{.Names}} {{.Status}}'` | Existing kind Ready/v1.34.0; built-in Kubernetes disabled; project containers absent and unrelated resources preserved |
| `make contracts` | **212 passed**; executable contracts unchanged |
| `git diff --check` | Passed |

### Acceptance boundary and remaining work

This requested infrastructure milestone is complete. Full component packaging,
PyFlink native ARM64 proof, domain migrations, CDC, Kafka ACLs, application client
integration, replicated availability, backup/restore, crash recovery, resource/load
measurements and security scans remain later component gates. Host TCP reachability
is proven; independently locked host application client environments are not created
here. Toxiproxy's live fault proof covers PostgreSQL; a Kafka proxy still needs
advertised-listener routing for its owning future fault scenario.

Docker Desktop's built-in Kubernetes remains disabled; the existing kind cluster
is preserved. No system software was installed and no commit or push was made.
Final local state: dependencies shut down, persistent project volumes retained,
Toxiproxy removed. Run `make infra-up` to inspect/reuse them. Next recommended
milestone remains repository foundation/component packaging, not full Kubernetes
or monitoring deployment.

## Infrastructure publication verification — 2026-11-22

The user explicitly authorized committing and pushing this milestone to `main`.
`git fetch origin main` succeeded and `git rev-list --left-right --count origin/main...HEAD`
returned `0 0` before the infrastructure commit. Final publication checks:
`.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py`
(**21 passed**), `make contracts BASE_REF=HEAD` (**212 passed**) and `git diff --check`
(passed). Full live acceptance evidence above remains valid; these checks did not
restart dependencies or alter retained volumes. Publication results are recorded in
Git history; no force push is required.


## Member_1 standalone acceptance — 2026-11-22

### What is implemented

Member_1 owns `member1/`, simulator/Registry images, fixture clients and standalone
tests. Member_2/3/4 applications remain untouched. This is one 20 kW transformer
with one EV, meter and solar device, one owning simulator shard and the existing
local Kafka/PostgreSQL dependencies. Fixed seed/start/run/config reproduce exact
offline JSON and event IDs; live runs use fresh boot IDs and actual UTC. Sampling
defaults to one second, with normal/overload/recovery thirds. The finite waveform
ends after 30 ticks by default; the fixture command consumer stays running.

The edge SQLite WAL/FULL transaction owns the simulated effect, greatest accepted
epoch, command payload/result, and durable result/follow-up outbox. IDs are kept
for the state volume's lifetime. New boots never clear dedup/fencing/rates.
Applied duplicates return ALREADY_APPLIED even after expiry/new leadership;
expired/stale/conflicting/invalid commands cannot repeat an effect. Output replay
retains event IDs and trace headers. Kafka offsets follow acknowledged output;
publication failure leaves pending outputs and offsets replayable. At-least-once
outputs still require downstream event deduplication. Wiping/rolling back the
edge volume voids the restart-safety claim; this is not real-hardware atomicity.

The Registry persists capabilities/registrations, monotonic heartbeats and
operation-ID responses in its own PostgreSQL database. All four generated gRPC
RPCs enforce validation/version conflicts, bounded pagination, freshness,
departure eligibility and service permissions. Client deadlines/retries share a
two-second budget; database/lock waits are bounded. No read returns fabricated
capabilities on dependency failure. The simulator and Registry have separate
runtime locks/venvs and Docker targets; tests have a combined Member_1-only lock.

### Executed commands and results

All commands below were executed in this repository on the existing Python
3.12.9/macOS ARM64/Docker Desktop local profile. Loopback/Docker/network checks
used approved sandbox access; no host software was installed.

| Exact command / check | Result |
|---|---|
| `git status --short`; `pwd`; repository instruction/design/contract/ADR reads | Initially clean working tree; existing contracts take precedence; no unrelated changes present |
| `.venv/bin/python -m venv .tools/member1-venv`; `.tools/member1-venv/bin/python -m pip install --cache-dir .tools/pip-cache 'confluent-kafka==2.8.2' 'psycopg[binary]==3.2.9' 'grpcio==1.85.3' 'protobuf==5.29.6' 'jsonschema==4.23.0' 'pytest==8.3.5' --report .tools/member1-install.json` | Pinned selected libraries resolved/installed only in ignored Member_1 venv; transitive portable locks recorded |
| `.tools/member1-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: -r member1/requirements.txt --dest .tools/member1-wheels-macos` | All 15 distributions downloaded before lock hash authoring |
| `.tools/member1-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: -r member1/requirements.txt --dest .tools/member1-wheels-linux-arm64 --platform manylinux2014_aarch64 --platform manylinux_2_28_aarch64 --platform manylinux_2_17_aarch64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | Linux ARM64 wheels resolved; these hashes are in all relevant final locks |
| `.venv/bin/python -m venv .tools/registry-venv`; `.tools/registry-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements-registry.txt`; `.tools/registry-venv/bin/python -m pip check` | Clean isolated Registry installation; no Kafka dependency; pip check passes |
| `.venv/bin/python -m venv .tools/simulator-venv`; `.tools/simulator-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements-simulator.txt`; `.tools/simulator-venv/bin/python -m pip check` | Clean isolated simulator installation; no PostgreSQL/gRPC dependency; pip check passes |
| `.venv/bin/python -m venv .tools/member1-lock-check`; `.tools/member1-lock-check/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements.txt`; `.tools/member1-lock-check/bin/python -m pip check` | Clean combined test installation, all hashes verified, no broken requirements |
| `.tools/member1-venv/bin/python -m pip download --no-index --find-links .tools/member1-wheels-linux-amd64 --only-binary=:all: --require-hashes -r member1/requirements.txt --dest .tools/member1-hash-check-linux-amd64 --platform manylinux2014_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All 15 Linux x86_64 wheels hash-verified; this is resolution, not execution on that architecture |
| `make bindings`; `make contracts BASE_REF=HEAD` | **213 passed**; unchanged executable v1 definitions; new test validates real Member_1 telemetry/results/command fixtures with the frozen semantic validator |
| `make infra-up` | Existing Kafka/PostgreSQL healthy, eight canonical topics and all four database logins/queries pass; other stores/resources preserved |
| `make member1-test` | **14 passed** at this checkpoint; later event-ID and transaction-rollback checks are included in the final 23-test live run |
| `make member1-live MEMBER1_PYTHON=.tools/member1-lock-check/bin/python` | Final complete suite **23 passed**: deterministic scenarios, partition routing, output ACK/offset order, abrupt process exit, rollback on result-store failure, restart dedup/epochs, malformed/expired/conflicting/stale commands, Registry RPC/auth/version/pagination/deadline/concurrency/outage behavior and health/metrics |
| `make member1-build`; `make member1-up` | Separate ARM64 simulator/Registry images build from digest-pinned Python; hash-checked runtime installation; both containers healthy, UID 10001, read-only root filesystems, no host binds |
| `.tools/member1-venv/bin/python -m tests.member1.container_smoke` | All four Registry RPCs; real command/results and caused telemetry; container recreation retains Registry data and edge dedup/fence; duplicate, stale, expired and explicit restore outcomes pass; boot changes and both health/metrics endpoints verified |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed**; previous preparation/dependency behavior preserved |
| `git diff --check` | Passed |

### Resolved issues, decisions and limitations

- First sandboxed package/socket attempts were denied; approved isolated installs
  and local-network checks succeeded. No system/global pip was used.
- Host port 50051 was already occupied. Registry uses **127.0.0.1:15051** externally
  and 50051 inside its container; the unrelated listener was not stopped.
- A concurrently running demo produced indistinguishable extra results in the
  initial live test. Tests now own uniquely prefixed disposable topics and delete
  them in fixture teardown. A subsequent fresh-topic leader-metadata race was
  fixed with a bounded metadata wait. Three topics from the failed setup were
  removed explicitly. Final passing results supersede those earlier failures.
- The preliminary macOS/ARM64-only lock correctly rejected x86_64 wheels. Their
  exact versioned wheel hashes were added and the final cross-platform lock was
  verified offline. No library version was loosened.
- ADR-026 resolves the package layout and durable simulator boundary. The matrix's
  old gRPC runtime row now follows ADR-025's already accepted 1.85.3 runtime,
  keeping the compiler at 1.71.0. No executable contract was weakened.
- One simulator owns the fixture fleet. Horizontal scaling/device partition
  assignment, thousands-of-devices load, replicated Kafka availability,
  Kubernetes deployment/PVC/CNI gates, mTLS/production identity, Registry backup
  acceptance, shared monitoring/OTel exporters, scans/SBOMs and hosted CI execution
  remain later gates. Local-token authorization is tested; it is not mTLS.
- Telemetry has bounded native buffering/retries and visible delivery failure,
  not a durable telemetry spool. Result history is durable and not pruned; disk
  sizing/retention reconciliation is a later operational gate. No performance or
  full-platform acceptance claim is made.

Next recommended milestone: **Member_2's native ARM64 PyFlink packaging proof and
standalone aggregation slice**, using the frozen Member_1 topology/telemetry
fixtures. Keep Member_3 and Member_4 independent. Do not broaden this change into
full platform deployment or monitoring-stack installation.


Final local state: Kafka, PostgreSQL, Registry and simulator are healthy and left
running for inspection; dependency volumes and `smart-grid-local-dev_member1-edge`
are retained. No disposable `local-dev.member1-test.*` topics remain. Stop the
applications with `make member1-down`, then dependencies with `make infra-down`.
The previously occupied host port and existing kind cluster were not modified.

Final `docker inspect` assertions confirm health, UID `10001:10001`, read-only
root filesystems and absence of bind mounts. Running image IDs:

- Registry: `sha256:f0b35236fae7c3b571ca30932d098733f1a12398f66b3a1f7e5eb50fb51a9849`
- Simulator: `sha256:223d6006914a8ae9823dc1f8d1648d40957ad4aa9fa87d590964b0df1b3f7c54`

## Member_1 publication verification — 2026-11-22

The user explicitly authorized committing and pushing this milestone to `main`.
`git fetch origin main` succeeded; `git rev-list --left-right --count origin/main...HEAD`
returned `0 0`. Before publication, `make contracts BASE_REF=HEAD` passed **213**
checks, `make member1-live MEMBER1_PYTHON=.tools/member1-lock-check/bin/python`
passed **23** tests, and `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py`
passed **21** tests. `git diff --check` passed. These reruns do not claim hosted
CI execution. The normal push result is recorded in Git history; no force push
is authorized or required.

## Member_2 standalone aggregation — accepted 2026-11-22

Member_2 owns the new `member2` package, independent 56-distribution hash lock,
native ARM64 container, Compose Flink processes, golden tests and Kafka recovery
probe. Member_1 simulation/Registry/observability, Member_3 control/PostgreSQL/
Debezium and Member_4 projection/API/UI/identity/Copilot ownership is preserved.
No executable schema changed and no other component's internal code is imported.

### Executed commands and evidence

| Exact command | Result |
|---|---|
| `git status --short`; `rg --files`; reads of `AGENTS.md`, master/status/version guides, stream/event contracts, relevant accepted ADRs, design/testing plans and executable reference/fixtures | Initial working tree clean; existing implementations/contracts inspected before editing |
| `docker info --format '{{.Architecture}} {{.MemTotal}}'`; `docker images --format '{{.Repository}}:{{.Tag}}'` | Native aarch64, 6,210,510,848 guest bytes; existing images/services preserved. Initial socket denial resolved through authorized Docker access |
| `docker pull flink:2.2.1-scala_2.12-java17`; `docker buildx imagetools inspect flink:2.2.1-scala_2.12-java17` | Selected image includes native ARM64; immutable index digest recorded in matrix/Dockerfile |
| `docker build -f member2/Dockerfile -t smart-grid-local-dev/member2:v1 . > .tools/member2-build-final.log 2>&1`; `make member2-build` | Pass. Hash-verified published sources/wheels, native PyFlink source wheel, offline runtime install, `pip check`, verified connector checksum and ARM64 import |
| `make member2-test` | **28 passed**: all 15 stream golden scenarios plus invalid-input isolation, boot/latest ordering, finite-state limit, topology identity validation, superseded late-count behavior, audit dedup and cross-transformer watermark isolation |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pytest -q tests/member2/test_aggregation.py` | **28 passed** in the native runtime image |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python tools/member2_live.py --run acceptance02` | **PASS**, real Kafka/Flink job `a6c7cd84e5628384fd3e49ca6bf310f5`: checkpoint 2 completed; revision 1 observed in an open transaction before crash; SIGKILL/start of TaskManager restored one checkpoint; committed readers saw exactly revision 0 / 15 kW and revision 1 / 17 kW. Replayed inputs did not double count; invalid JSON reached DLQ and >30s reading reached late audit |
| `.venv/bin/python tools/member2_live.py --run guardcheck` | Expected exit 1: active-job guard refuses before any Compose mutation; current job preserved |
| `make contracts`; `make contracts BASE_REF=HEAD` | **213 passed** each; schema/OpenAPI/Protobuf and compatibility gate pass; executable v1 contracts unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed** |
| `.venv/bin/python -m pytest -q tests/member2/test_aggregation.py tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **49 passed** |
| `git diff --check` | Passed |

Earlier development attempts failed and were fixed: Docker's existing context
allowlist excluded Member_2; PyFlink 2.2 uses
`set_externalized_checkpoint_retention`; initial TaskManager memory fractions
exceeded the configured total; published sdist hashes must be checked before
compilation, while locally built wheels require their own manifest during offline
installation. Recovery also exposed an overly tight ten-second heartbeat under
simultaneous build load; the final profile uses sixty seconds. The final clean
acceptance above uses the corrected configuration and locked image.

### Decisions, limitations and inspection

[ADR-027](../decisions/ADR-027-member2-standalone-aggregation.md) records the
explicit managed-state event-time clock, inclusive `ingested_at` lateness,
selected-reading revision fingerprints, fixed topology and transactional sink
boundaries. The design's solar equation is corrected to meter + EV minus the
reported positive solar magnitude. Runtime replay of the historical fixture is
STALE/OLD_RESULT; golden tests use the frozen evaluation UTC for freshness cases.

This is one finite-run aggregation task, retaining all dedup/window state up to
100,000 unique valid IDs; at the limit it fails rather than corrupting loads.
Rescaling/throughput, native Flink watermark display, full-session HA/automatic
JobManager restore, kind/operator deployment, replicated Kafka availability,
security scans/SBOM, tracing and monitoring-stack integration are unverified or
deferred. Custom semantic watermark is exposed as an operator metric. Sink
transaction timeout is 300s versus a 60s checkpoint timeout: recovery must remain
inside that budget and consumers must use `read_committed`. Three output topics
do not have cross-topic atomic visibility. No platform-wide exactly-once claim
is made. Source-build tools/apt packages are not a bit-reproducible toolchain;
the runtime artifact inputs are hash checked and compiled hashes retained.

The live job and [Flink UI](http://127.0.0.1:18081) remain available for inspection;
its checkpoint volume and four topics per test run persist. Member_1 applications,
PostgreSQL, Kafka data and the existing kind cluster were not removed or reset.
Stop only Flink with `make member2-down`; rerun with a new run ID using
`make member2-live`. See [the runbook](../../member2/README.md) for all entry points.
No remaining blocker for this requested standalone slice. Next recommended work
is Member_3 standalone deterministic control against load fixtures. No commit or
push was made.

## Member_2 review and publication verification — 2026-11-22

The user authorized correctness/contract review, fixes, then commit and normal
push to `main` if checks pass. Review found and fixed these issues before
publication:

- The independently locked jsonschema runtime lacks its optional RFC3339 checker.
  Malformed event/ingestion/departure timestamps could be accepted. A local UTC
  date-time format checker now validates every schema timestamp without a new
  dependency; regression cases failed before the fix and pass afterward.
- Extreme but structurally valid timestamps could overflow datetime arithmetic
  and restart the job repeatedly. Difference-based comparisons preserve exact
  boundaries; an unrepresentable window end reaches DLQ before state mutation.
  Selected totals/utilization that overflow finite JSON numbers are likewise
  rejected before changing dedup/window state. Valid very-late finite readings
  remain audit events.
- The real recovery probe waited for a later checkpoint but did not actually
  rescan output. It now rescans committed output, asserts exactly one DLQ and
  late-audit event, and verifies the exact restored checkpoint ID.
- Required accepted-late count and watermark-delay gauges were absent. Added
  both and checked their values through the real recovered operator's REST
  metrics. REST discovery is asynchronous; the probe now waits with a deadline.
- The new Ubuntu CI job used the Mac-specific preparation lock. It now uses
  `member2/requirements-test.txt`, a portable subset of the same runtime pins.
  Clean Mac installation/execution and Linux x86_64 wheel resolution pass.

Executed review evidence:

| Exact command | Result |
|---|---|
| `make member2-test` | **43 passed**, including regression failures reproduced before fixes |
| `make contracts BASE_REF=HEAD`; `make contracts BASE_REF=origin/main` | **213 passed** each; executable v1 definitions unchanged |
| `.venv/bin/python -m pytest -q tests/member2/test_aggregation.py tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **64 passed** (43 aggregation + 21 repository checks) |
| `.venv/bin/python -m venv .tools/member2-test-check`; `.tools/member2-test-check/bin/python -m pip install --only-binary=:all: --require-hashes -r member2/requirements-test.txt`; `.tools/member2-test-check/bin/python -m pip check`; `.tools/member2-test-check/bin/python -m pytest -q tests/member2/test_aggregation.py` | Clean independent hash-checked installation, no broken requirements, **43 passed** |
| `.venv/bin/python -m pip download --only-binary=:all: --require-hashes -r member2/requirements-test.txt --dest .tools/member2-test-wheels-linux --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All ten pure-test dependencies resolve/hash-verify for Linux x86_64; not a Linux execution claim |
| `make member2-build > .tools/member2-review-final-build.log 2>&1`; `make member2-down`; `.venv/bin/python tools/member2_live.py --run review02` | Native ARM64 build and real Kafka/Flink acceptance pass. Job `f18edd78dfdd70ff0ff8c337fcd32e9f` restores checkpoint 2 after SIGKILL with revision 1 in an open transaction. Both output scans show only revisions 0/1 at 15/17 kW; exactly one DLQ and one too-late audit event |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pytest -q tests/member2/test_aggregation.py`; same prefix with `-m pip check` | **43 passed** in native runtime; no broken requirements |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python tests/member2/live.py metrics review02` | Required REST gauges visible: accepted late count **1**, semantic watermark **1794249005000 ms**, positive watermark delay. First immediate inspection saw no metrics before asynchronous discovery; bounded retry succeeds |
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; **0 0** before publication |
| `git diff --check` | Passed |

No unresolved correctness finding remains for the documented finite-run slice.
The single-task/run limit, transactional Kafka recovery conditions, lack of
cross-topic atomic visibility, and deferred kind/scaling/whole-cluster HA gates
remain unchanged. Hosted CI is authored, not claimed as locally executed. The
review02 job/UI and checkpoints remain available; unrelated services and kind
are preserved. Commit/push outcome is recorded in Git history; no force push is
authorized or needed. Next milestone remains Member_3 standalone control.
