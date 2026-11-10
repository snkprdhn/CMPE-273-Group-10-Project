# Open-Source Reuse and Build Boundary

## Guiding principle

The project will integrate proven open-source infrastructure instead of rebuilding commodity platform features. The team should spend its implementation time on the grid-specific decisions that make the system useful and on experiments that demonstrate distributed-systems behavior.

Using an operator or library does not remove the distributed-systems learning. The team must still configure partitioning, replication, delivery guarantees, recovery, resource limits, observability, and failure tests; explain the tradeoffs; and measure the resulting behavior.

## What the team will build

These components contain the project's domain value and must remain team-owned:

1. Deterministic EV charger, meter, and solar behavior plus repeatable overload and failure scenarios.
2. Transformer-aware telemetry partitioning and the Flink aggregation logic for event-time windows, watermarks, late revisions, and completeness.
3. The overload policy: warning/critical/recovery thresholds, hysteresis, cooldowns, charger eligibility, and fair load reduction.
4. The fenced Action Service state machine, command identity rules, reconciliation, and safe restore workflow.
5. Grid-specific query projections, REST/SSE behavior, and the operator dashboard.
6. Domain metrics, SLO definitions, acceptance fixtures, and evidence showing correctness during replay, delay, duplication, and failure.
7. The bounded incident-investigation workflow, provider adapter, evidence validator, and project-specific agent evaluations.

## What the team will reuse

| Need | Selected open-source project | How it will be used | What the team will not build |
|---|---|---|---|
| Container orchestration | Kubernetes with `kind` or `minikube` | Scheduling, Services, DNS, probes, reconciliation, RBAC, Secrets, NetworkPolicies, and Leases | A scheduler, service registry, or leader-election protocol |
| Kafka lifecycle | Strimzi Operator | Kafka clusters, topics, users, storage, rolling updates, and the three-broker review profile | Custom Kafka StatefulSets or lifecycle scripts |
| Stream-job lifecycle | Apache Flink Kubernetes Operator | Flink deployment, checkpoints/savepoints, upgrades, rollback, recovery, and job observability | A custom Flink controller |
| Event backbone | Apache Kafka | Partitioned telemetry, derived state, commands, replay, and consumer groups | A message broker or delivery protocol |
| Schema governance | Apicurio Registry | Store/version Kafka JSON Schemas and check compatibility; Protobuf remains the gRPC contract format | A schema registry or compatibility service |
| Reliable outbox publication | Kafka Connect with Debezium Outbox Event Router | Capture committed outbox rows and publish them to Kafka using stable event IDs | A polling outbox relay |
| Operational data | PostgreSQL managed by CloudNativePG | One required persistent primary with time-indexed history tables; backups/exports and health managed through Kubernetes resources | A database, replication protocol, or hand-written database StatefulSet |
| Identity | Keycloak | Local OIDC login, tokens, and operator/viewer roles | An identity provider or password system |
| Telemetry collection | OpenTelemetry SDKs, Operator, and Collector | Instrumentation, context propagation, collection, batching, and export | A telemetry agent or collector |
| Metrics and dashboards | Prometheus Operator, Prometheus, and Grafana | Service discovery, metric storage, alerts, and engineering dashboards | A monitoring database or dashboard framework |
| Tracing | Grafana Tempo | Store and query distributed traces from the OpenTelemetry Collector | A trace backend |
| UI foundation | React, Material UI, and Recharts | Accessible components, responsive layout, and charts | A component system or charting library |
| Agent integration | Official provider SDKs, JSON Schema/Pydantic validation, and OpenTelemetry | Model calls, typed tool/output validation, and traceable orchestration behind a small provider adapter | A general agent framework, model server, or custom schema validator |
| Load and browser testing | k6 and Playwright | API/SSE load, end-to-end user flows, and repeatable performance scenarios | A custom load-testing framework |
| Dependency fault injection | Toxiproxy plus Kubernetes-native pod/broker faults | Latency, connection loss, timeouts, and bounded recovery tests | A general-purpose chaos platform |
| Integration testing | Testcontainers | Real Kafka/PostgreSQL-compatible test dependencies where practical | In-memory substitutes for every integration test |
| Packaging | Helm for third-party stacks; Kustomize overlays for project workloads | Pin upstream charts/operators and maintain dev/review resource profiles | Forked vendor manifests or a custom deployment engine |

The exact versions will be pinned before implementation. Only stable, mutually compatible releases will be used; `latest` image tags are prohibited.

## Integration boundaries

The team still owns the configuration and correctness at every boundary:

- Strimzi manages Kafka, but Member_2 owns topic keys, partition counts, replication settings, retention, and failure evidence.
- The Flink Operator manages job lifecycle, but Member_2 owns event-time semantics, state, checkpoint policy, and recovery correctness.
- Debezium publishes outbox rows, but Member_3 owns the database transaction, stable IDs, event routing fields, and idempotent inbox consumers. Duplicate delivery remains possible and must be safe.
- CloudNativePG manages database lifecycle, but Member_3 owns schemas, constraints, concurrency rules, retention, and recovery verification.
- Keycloak issues identities, but Member_4 owns application authorization, role mapping, token validation, and negative tests.
- OpenTelemetry and Prometheus collect signals, but every component owner must define bounded-cardinality metrics and useful spans/logs.

## Deliberately avoided reinvention

The project will not implement custom consensus, Kafka controllers, database replication, schema governance, authentication, tracing storage, service discovery, deployment reconciliation, or generic retry/circuit-breaker frameworks. It will also avoid adding overlapping tools: no Consul beside Kubernetes DNS, no service mesh for the MVP, no second event broker, and no full chaos platform when Toxiproxy and Kubernetes faults cover the acceptance cases.

## Resource-aware deployment profiles

The local environment must remain demonstrable on student hardware.

| Profile | Purpose | Expected shape |
|---|---|---|
| Developer | Fast component work | One Kafka broker, reduced Flink workers, one database, optional observability backends |
| Integration | End-to-end verification | Full application path, Keycloak, metrics, tracing, and bounded simulator scale |
| Architecture review | Distributed failure evidence | Three Kafka brokers, two stateless replicas where relevant, two Action pods, checkpoints, and observability |

The project will not run every optional tool continuously. Loki, KEDA, a database replica, and second-model-provider comparison remain stretch items. The required agent thin slice can use its deterministic mock when external access is unavailable. If the review profile exceeds the agreed machine budget, the team will reduce simulated device count or run test phases separately before weakening correctness guarantees.

Plain PostgreSQL is sufficient for the proposed retention and class-scale query load. A time-series extension is intentionally excluded from the required stack; it can be reconsidered only if measurements show a real storage or query problem that indexed/partitioned PostgreSQL tables cannot address.

## Selection and maintenance rules

An open-source dependency is accepted only when it has a suitable license, active maintenance, clear documentation, Kubernetes compatibility, and a meaningful reduction in custom code. The repository will pin container/chart versions, record configuration in Git, retain upstream license notices where required, run dependency and image scans in CI, and generate a software bill of materials for final review.

Primary project documentation:

- [Strimzi Operator](https://strimzi.io/docs/operators/latest/overview/)
- [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [Apicurio Registry](https://www.apicur.io/registry/docs/apicurio-registry/3.0.x/index.html)
- [CloudNativePG](https://cloudnative-pg.io/documentation/current/)
- [OpenTelemetry Operator](https://opentelemetry.io/docs/platforms/kubernetes/operator/)
- [Prometheus Operator](https://prometheus-operator.dev/docs/getting-started/introduction/)
- [Keycloak](https://www.keycloak.org/documentation)
- [k6](https://grafana.com/docs/k6/latest/), [Toxiproxy](https://github.com/Shopify/toxiproxy), and [Testcontainers](https://testcontainers.com/)

