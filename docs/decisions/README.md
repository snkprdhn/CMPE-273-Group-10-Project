# Architecture Decision Register

## Proposed decisions

| ID | Decision | Proposed choice | Status |
|---|---|---|---|
| ADR-001 | Event backbone | Apache Kafka | Proposed |
| ADR-002 | Stateful processing | Apache Flink/PyFlink | Proposed |
| ADR-003 | Operational persistence | PostgreSQL with time-indexed tables | Proposed |
| ADR-004 | Main UI | React + TypeScript + Material UI + Recharts | Proposed |
| ADR-005 | Live browser updates | Server-Sent Events | Proposed |
| ADR-006 | Target runtime | Kubernetes using kind/minikube; Compose optional for development | Proposed |
| ADR-007 | Command guarantee | At-least-once delivery with idempotent effects | Proposed |
| ADR-008 | Internal RPC | gRPC/Protobuf Device Registry with deadlines | Proposed |
| ADR-009 | Control coordination | Kubernetes Lease plus fencing epoch | Proposed |
| ADR-010 | Atomic event publication | Transactional outbox and idempotent inbox | Proposed |
| ADR-011 | Database topology | One durable PostgreSQL primary; read replica is stretch only | Proposed |
| ADR-012 | Observability | OpenTelemetry Collector with Prometheus, Grafana, traces, and structured logs | Proposed |
| ADR-013 | Log aggregation | Kubernetes structured logs required; Loki stretch only | Proposed |
| ADR-014 | Agentic feature | Required bounded Grid Incident Investigator; read-only tools and no control-path dependency | Proposed |
| ADR-015 | Kafka and Flink lifecycle | Strimzi and Apache Flink Kubernetes Operator | Proposed |
| ADR-016 | Kafka schemas | JSON Schema stored in Apicurio Registry; Protobuf for gRPC only | Proposed |
| ADR-017 | Outbox publication | Kafka Connect with Debezium Outbox Event Router | Proposed |
| ADR-018 | PostgreSQL lifecycle | Single-instance CloudNativePG cluster; replica remains stretch | Proposed |
| ADR-019 | Packaging | Helm for pinned third-party stacks; Kustomize overlays for project workloads | Proposed |
| ADR-020 | Model integration | Provider-neutral adapter with one configured external provider and deterministic mock fallback | Proposed |
| ADR-021 | Agent authorization | Seven allowlisted read-only tools; no raw query, infrastructure, or write tools | Proposed |

## Decisions to close before coding

1. PyFlink versus a Java Flink job.
2. Kafka partition count and resource limits for the three-broker review profile.
3. Apicurio compatibility level and subject/artifact naming convention.
4. Window, watermark, allowed-lateness, cooldown, and recovery defaults.
5. Local Keycloak configuration and seeded role bootstrap.
6. Pinned compatible versions for Kubernetes, Strimzi, Kafka, Flink Operator, Flink, Debezium, Apicurio, and CloudNativePG.
7. License for the repository and third-party attribution/SBOM process.
8. External model provider, approved model identifier, request budget, retention, and class-demo credential process.

Each accepted ADR should record context, decision, alternatives, consequences, date, and approvers in a separate `ADR-NNN-title.md` file.
