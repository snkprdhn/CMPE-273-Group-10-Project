# Kubernetes Deployment Design

## Purpose

The required deployment target is local kind on one computer. Use Docker Desktop tooling (free for education) with native ARM64 images; cloud/EKS deployment is deferred. Application and test code is Python. Infrastructure Java runtimes remain inside containers. The [local kind runbook](../../infra/kind/README.md) documents the deployed resources, profiles and inspection commands.

Each Member_1 through Member_4 slice remains independently runnable. Use the [preparation decisions](../decisions/ADR-006-local-kind.md), [pinned matrix](../implementation/VERSION_MATRIX.md), and [machine-sized profiles](../implementation/LOCAL_PREREQUISITES.md#resource-profiles). Stage larger review exercises rather than assuming every workload fits concurrently.

## Workload model

| Component | Resource or planned resource | Replicas/profile | Storage |
|---|---|---|---|
| NiceGUI Python UI | Deployment + Service | 1; two only after sticky-session/reconnect gate | Transient client sessions; reload from API |
| Projection/API | Separate consumer and API Deployments, API Service | 1 consumer; 1 dev/2 review API | Rebuildable projection/cache only |
| Copilot Service | Deployment + internal Service | 1 dev; 2 review | Bounded investigation/audit rows in PostgreSQL |
| Read-only Tool Gateway | Internal API module/Service route | Scales with Projection/API | No independent state |
| Device Registry gRPC | Deployment + Service | 1 dev/2 review | Durable registry database state |
| Action Service | Deployment + Lease | 1 dev/2 review active/standby | Durable saga/inbox/outbox state |
| Simulators | Scenario Job | One bounded deterministic run | Fenced gateway effects in its owned PostgreSQL schema |
| PyFlink | Apache Flink Kubernetes Operator resources | 1 manager/1 worker dev; 2 workers in stream review | Local checkpoint PVC |
| Kafka | Strimzi `Kafka` and `KafkaTopic` resources | 1-broker dev; 3-broker review profile | Persistent volumes |
| Kafka Connect/Debezium | Strimzi `KafkaConnect` resource | 1 required; 2 only if measured need permits | Connector offsets/config/status topics |
| PostgreSQL | Pinned Helm chart/StatefulSet | 1 primary with separate logical stores/roles | Persistent volume and documented backup/export |
| OTel Collector | Upstream Helm chart/Deployment; no OTel Operator | 1 | Bounded queues |
| Prometheus/Grafana/Tempo | Prometheus Operator/chart-managed workloads | 1 each for class environment | Persistent volumes |
| Keycloak OIDC provider | Deployment + Service | 1 class instance | Dedicated PostgreSQL logical database |

Kafka and Flink use their established Kubernetes operators because their distributed lifecycle and recovery behavior are part of the project. One PostgreSQL primary does not justify another operator; a pinned Helm chart plus persistent volume, probes, backup/export, and recovery test is sufficient. Kustomize supplies developer, integration, and review overlays for team-owned workloads. Reused charts/operators do not replace design work: the repository must still declare resources, durability, replication, probes, disruption behavior, and acceptance evidence.

## Workload ownership

Each component owner supplies their Deployment/operator resource, Service where needed, Kubernetes health signals, resources and policy. PodMonitors, dashboards and extended fault tests are follow-on work. Member_1 installs the shared observability stack; Member_4 owns Gateway, Keycloak, product SLO views, and the final overlay. Neither implements another member's component resources.

## Discovery and traffic

- Kubernetes Services and cluster DNS provide stable names; pod addresses are never application configuration.
- The local loopback port-forward exposes NiceGUI, the named realm’s public OIDC paths and Keycloak static assets. The dashboard reaches the query API by Service DNS. A temporary loopback API port-forward serves the direct REST/SSE test. Shared authenticated ingress and optional Grafana access remain later work.
- Kafka, databases, Flink management, gRPC registry, and telemetry backends remain cluster-internal.
- Kafka Connect, operator APIs, and database administration endpoints remain cluster-internal.
- The Copilot Service is reached through the authenticated API/Gateway path. The local deterministic mock requires no external model egress; the Tool Gateway remains cluster-internal.
- Readiness decides normal routing; liveness is reserved for locally unrecoverable process failure; startup probes protect slow initialization.
- A later review exercise will record endpoint propagation and connection-reuse behavior instead of assuming immediate or perfectly even balancing.
- Kubernetes-native discovery is chosen over adding Consul because both would solve overlapping membership problems in the class deployment. The architecture review will compare their registration, health, and failure models without operating two discovery planes.

## Reconciliation and shutdown

Deployments declare desired replicas and Kubernetes replaces failed pods. Applications still persist state before acknowledging work. Pods handle termination signals, stop accepting new work, finish or checkpoint bounded in-flight work, and exit within the termination grace period. PodDisruptionBudgets protect voluntary disruption where more than one ready replica is required.

## Scaling

- Projection/API scales on CPU, request rate, and active SSE pressure.
- Copilot Service concurrency is capped by provider quota and a bulkhead; adding replicas does not increase the configured global investigation budget.
- Simulators scale by scenario shard.
- Flink parallelism aligns with Kafka partitions and available task slots.
- Required scaling uses measured manual replica changes or a simple HPA where applicable; a separate event-driven autoscaling platform is excluded.
- Action Service replicas do not increase control concurrency unless ownership is partitioned and fenced.
- Resource requests/limits are mandatory so scheduling and overload behavior are visible.

## Security boundaries

Namespaces separate application, data, and observability workloads where practical. ServiceAccounts, RBAC, NetworkPolicies, secrets, non-root containers, read-only filesystems, and minimal exposed ports enforce least privilege. Kubernetes Secret encoding is not treated as encryption. The Copilot Service has no Kafka, database-administration, shell, or Kubernetes API permission; model credentials are mounted only into that workload and are never sent to the browser or Tool Gateway.

## Required runtime demonstrations

1. Resolve and call a service by stable Kubernetes DNS.
2. Route requests across two ready API replicas.
3. Fail readiness on one replica without restarting it and observe endpoint removal.
4. Delete a pod and measure reconciliation/recovery time.
5. Roll out a new stateless version without dropping all capacity.
6. Scale a consumer or projection component from measured demand.
7. Kill the Action leader and show lease transfer plus stale-epoch rejection.
8. Stop one Kafka broker in the replicated profile and document availability.
9. Delete a Copilot pod during an investigation and show a visible failed/partial result or bounded recovery without affecting grid processing.
10. Inject mock-provider unavailability and show bounded failure/mock behavior while the deterministic control loop remains healthy.

## Non-claims

A multi-node logical cluster on one laptop does not prove host, rack, zone, or region availability. Service discovery does not provide authorization, retries do not provide correctness, and Kubernetes restart does not recover state that was never made durable.

Serverless and multi-region deployment are excluded because the project specifically needs visible long-running Kafka/Flink state, partition behavior, and Kubernetes failure experiments. The tradeoff is documented rather than presenting Kubernetes as universally preferable.

## Local networking caveat

The default kind CNI is insufficient evidence of NetworkPolicy enforcement. The security-capable local profile must disable the default CNI and install the selected Calico policy-capable CNI before running isolation tests. The kind commands verify the Calico artifact and the smoke tests exercise allowed and denied connections. See [status](../implementation/IMPLEMENTATION_STATUS.md) for executed evidence. Initial empty-cluster smoke commands in the prerequisites guide use default networking and make no isolation claim.
