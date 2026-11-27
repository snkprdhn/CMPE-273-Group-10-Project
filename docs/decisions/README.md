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
| ADR-007 | Command guarantee | At-least-once delivery with idempotent effects | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-008 | Internal RPC | gRPC/Protobuf Device Registry with deadlines | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-009 | Control coordination | Kubernetes Lease plus fencing epoch | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-010 | Atomic event publication | Transactional outbox and idempotent inbox | Accepted, implemented |
| ADR-011 | Database topology | One durable PostgreSQL primary with isolated logical stores and least-privilege roles | Accepted, implemented |
| ADR-012 | Observability | OpenTelemetry Collector with Prometheus, Grafana, traces, and structured logs | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-013 | Logging | Structured Kubernetes logs with trace/correlation fields; no separate log backend required | Accepted, implemented |
| ADR-014 | Agentic feature | Required bounded Grid Incident Investigator; read-only tools and no control-path dependency | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-015 | Kafka and Flink lifecycle | Strimzi and Apache Flink Kubernetes Operator | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-016 | Kafka schemas | Versioned JSON Schema and examples in Git with CI compatibility checks; Protobuf for gRPC only | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-017 | Outbox publication | Kafka Connect with Debezium Outbox Event Router | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-018 | PostgreSQL lifecycle | One persistent primary installed from a pinned Helm chart; no database operator or replica | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-019 | Packaging | Helm for pinned third-party stacks; Kustomize overlays for project workloads | Baseline accepted in [ADR-024](ADR-024-pinned-local-baseline.md) |
| ADR-020 | Model integration | Deterministic mock now; external provider deferred | Mock baseline accepted in [ADR-006](ADR-006-local-kind.md); external adapter deferred |
| ADR-021 | Agent authorization | Seven allowlisted read-only tools; no raw query, infrastructure, or write tools | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-022 | Data ownership | Registry, control, projection, and copilot-audit logical stores; no cross-service table reads | Accepted, implemented |
| ADR-023 | Independent development | Versioned contract bundle, golden fixtures, standalone profiles, then pairwise integration | Accepted; contract in [ADR-025](ADR-025-executable-contract-freeze.md), implemented and verified (see [status](../implementation/IMPLEMENTATION_STATUS.md)) |
| ADR-024 | Pinned local baseline | Version matrix, component environments, minimal operators | [Accepted](ADR-024-pinned-local-baseline.md) |
| ADR-025 | Executable v1 contracts and resolved domain/auth defaults | Canonical JSON/Protobuf/OpenAPI, isolated tooling, golden fixtures and conservative compatibility | [Accepted](ADR-025-executable-contract-freeze.md) |

## Later decisions

Each ADR records context, decision, alternatives, consequences and date in `ADR-NNN-title.md`.

- [ADR-026](ADR-026-devices-standalone-state.md): standalone device state and acceptance boundary.
- [ADR-027](ADR-027-standalone-aggregation.md): standalone aggregation and recovery.
- [ADR-028](ADR-028-action-domain.md): Action domain and durable saga.
- [ADR-029](ADR-029-outbox-and-fencing.md): outbox delivery and durable fencing.
- [ADR-030](ADR-030-projections-api.md): projections and operations API.
- [ADR-031](ADR-031-operator-dashboard.md): operator dashboard.
- [ADR-032](ADR-032-local-identity.md): local Keycloak identity and authorization.
- [ADR-033](ADR-033-live-component-integration.md): live component integration.
- [ADR-034](ADR-034-local-kind-deployment.md): local kind deployment (Helm and Kustomize profiles, Calico).
- [ADR-035](ADR-035-local-observability.md): local observability and collected incident evidence.
- [ADR-036](ADR-036-grid-incident-investigator.md): bounded deterministic Grid Incident Investigator.
- [ADR-037](ADR-037-continuous-fleet-and-rolling-retention.md): continuous fleet, bounded keyed state and rolling retention. Amends ADR-027 and ADR-030.

Deferred: cloud provider and budget beyond the AKS review, external model provider and credentials, repository license (choose before a public release, with SBOMs and scans).
