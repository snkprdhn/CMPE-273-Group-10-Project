# Architecture Decision Register

## Decision register

| ID | Decision | Choice | Status |
|---|---|---|---|
| ADR-001 | Event backbone | Apache Kafka | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-002 | Stateful processing | PyFlink 2.2.1, Python 3.12.9; JVM inside container | [Accepted](ADR-002-python-stream-processing.md) |
| ADR-003 | Operational persistence | PostgreSQL with time-indexed tables | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-004 | Main UI | NiceGUI Python; Locust and Playwright Python tests | [Accepted](ADR-004-python-dashboard.md) |
| ADR-005 | Live updates | API SSE; NiceGUI browser transport | Baseline accepted in [ADR-004](ADR-004-python-dashboard.md) |
| ADR-006 | Target runtime | Local kind; Docker Desktop (education); cloud deferred | [Accepted](ADR-006-local-kind.md) |
| ADR-007 | Command guarantee | At-least-once delivery with idempotent effects | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-008 | Internal RPC | gRPC/Protobuf Device Registry with deadlines | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-009 | Control coordination | Kubernetes Lease plus fencing epoch | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-010 | Atomic event publication | Transactional outbox and idempotent inbox | Proposed |
| ADR-011 | Database topology | One durable PostgreSQL primary with isolated logical stores and least-privilege roles | Proposed |
| ADR-012 | Observability | OpenTelemetry Collector with Prometheus, Grafana, traces, and structured logs | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-013 | Logging | Structured Kubernetes logs with trace/correlation fields; no separate log backend required | Proposed |
| ADR-014 | Agentic feature | Required bounded Grid Incident Investigator; read-only tools and no control-path dependency | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-015 | Kafka and Flink lifecycle | Strimzi and Apache Flink Kubernetes Operator | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-016 | Kafka schemas | Versioned JSON Schema and examples in Git with CI compatibility checks; Protobuf for gRPC only | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-017 | Outbox publication | Kafka Connect with Debezium Outbox Event Router | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-018 | PostgreSQL lifecycle | One persistent primary installed from a pinned Helm chart; no database operator or replica | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-019 | Packaging | Helm for pinned third-party stacks; Kustomize overlays for project workloads | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-020 | Model integration | Deterministic mock now; external provider deferred | Mock baseline accepted in [ADR-006](ADR-006-local-kind.md); external adapter deferred |
| ADR-021 | Agent authorization | Seven allowlisted read-only tools; no raw query, infrastructure, or write tools | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-022 | Data ownership | Registry, control, projection, and copilot-audit logical stores; no cross-service table reads | Proposed |
| ADR-023 | Independent development | Versioned contract bundle, golden fixtures, standalone profiles, then pairwise integration | Contract baseline accepted in [ADR-025](ADR-025-executable-contract-freeze.md); runtime unverified |
| ADR-024 | Pinned local baseline | Version matrix, component environments, minimal operators | [Accepted](ADR-024-pinned-local-baseline.md) |
| ADR-025 | Executable v1 contracts and resolved domain/auth defaults | Canonical JSON/Protobuf/OpenAPI, isolated tooling, golden fixtures and conservative compatibility | [Accepted](ADR-025-executable-contract-freeze.md) |

## Remaining decisions by milestone

Preparation choices are resolved in the accepted ADRs and [version matrix](../implementation/VERSION_MATRIX.md). No application code or deployment verification is implied.

- **Contract/schema milestone completed:** [ADR-025](ADR-025-executable-contract-freeze.md) freezes executable events/API/gRPC/agent schemas, fixtures, compatibility checks, control defaults and local identity names/roles. Runtime identity/bootstrap and service behavior remain component gates. Partition/replication/profile defaults remain in ADR-006.
- **Foundation/component packaging:** generate per-component hashed dependency locks; capture chart/JAR checksums and image digests; prove native ARM64 PyFlink source build; implement local CNI/security gates and pin rendered sidecars. These are execution gates, not unresolved language/deployment choices.
- **Before public release:** repository owner selects license; preserve upstream notices and produce SBOMs/scans.
- **Deferred:** cloud provider/region/budget/registry and external model/provider/credentials. Neither is a local prerequisite.

Each accepted ADR should record context, decision, alternatives, consequences, date, and approvers in a separate `ADR-NNN-title.md` file.

- [ADR-026 — Member_1 standalone state and acceptance boundary](ADR-026-member1-standalone-state.md) — accepted.
