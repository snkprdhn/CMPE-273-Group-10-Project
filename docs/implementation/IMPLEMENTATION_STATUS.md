# Implementation status

Updated **2026-11-22 (America/Los_Angeles)** after full acceptance of minimum local development infrastructure. Member_1 through Member_4 ownership is preserved. No application services have been scaffolded.

## Completed milestones

| Milestone | Result | Evidence |
|---|---|---|
| Repository/machine preparation | Accepted small Python/container stack, ADRs, manual prerequisites, resource profiles and root conventions | [Matrix](VERSION_MATRIX.md), [prerequisites](LOCAL_PREREQUISITES.md), [preparation evidence](PREPARATION_VERIFICATION.md) |
| Existing local prerequisites/empty cluster | Managed Python 3.12.9, isolated preparation venv, pinned native tools, Docker Desktop with built-in Kubernetes disabled, empty kind Kubernetes 1.34.0 | Read-only inventory reconfirmed below; earlier setup commands/digests retained in preparation evidence |
| Minimum local development infrastructure | One Kafka broker, PostgreSQL primary, on-demand Toxiproxy; isolated stores, real smoke checks, restart persistence, guarded reset | [Compose runbook](../../infra/compose/README.md), executed acceptance below |
| Executable contract bundle v1 | Eight Kafka events; canonical topology, REST/internal/SSE and seven agent-tool/evidence/output schemas; Registry proto; versioned examples and fixed golden scenarios; compatibility tests and Ubuntu CI workflow | [Bundle](../../schemas/README.md), [ADR-025](../decisions/ADR-025-executable-contract-freeze.md), `make contracts` |

## Current milestone and verification

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
- PyFlink native Linux ARM64 source build/import/checkpoint proof, real Kafka ordering/recovery, persistent device effects/fencing, actual Registry/OIDC authorization, SSE delivery, policy state machines, full-stack behavior, CNI allow/deny tests and PVC I/O/recovery remain unverified. The empty cluster does not establish them.
- Lightweight development remains 4 CPUs/6 GiB VM; staged review remains 8 CPUs/10 GiB on this 16 GiB host. Larger-profile fit, performance, security scans and application/chart/image artifact gates remain unmeasured. Licensing remains the owner's decision before public release.

## Next milestone

**Repository foundation and component packaging**, starting with native ARM64 PyFlink build/import/checkpoint proof and independently locked component environments. Add only the owning component's required dependencies, configuration, CI and fixtures; keep cloud and external model integrations deferred. Later components must run the frozen fixtures against real implementations. This does not claim the master guide's entire Phase 0/foundation or an application deployment complete.

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
