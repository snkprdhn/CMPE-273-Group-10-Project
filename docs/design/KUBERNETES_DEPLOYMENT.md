# Kubernetes Deployment Design

## Purpose

The final architecture review and demonstration will run in a local Kubernetes cluster such as `kind` or `minikube`. Docker Compose may support lightweight development, but it is not the final deployment target. This document defines the planned Kubernetes behavior; no manifests have been implemented yet.

## Workload model

| Component | Planned resource | Replicas/profile | Storage |
|---|---|---|---|
| React UI | Deployment + Service | 2 | None |
| Projection/API | Deployment + Service | 2+ | Rebuildable projection/cache only |
| Copilot Service | Deployment + internal Service | 1 dev; 2 review | Bounded investigation/audit rows in PostgreSQL |
| Read-only Tool Gateway | Internal API module/Service route | Scales with Projection/API | No independent state |
| Device Registry gRPC | Deployment + Service | 2 | Durable registry database state |
| Action Service | Deployment + Lease | 2 active/standby | Durable saga/inbox/outbox state |
| Simulators | Deployments/Jobs | Scaled by scenario | None beyond scenario state |
| Flink | Apache Flink Kubernetes Operator resources | 1 manager, 2+ workers | Checkpoint volume/object-compatible store |
| Kafka | Strimzi `Kafka` and `KafkaTopic` resources | 1-broker dev; 3-broker review profile | Persistent volumes |
| Kafka Connect/Debezium | Strimzi `KafkaConnect` resource | 1 required; 2 only if measured need permits | Connector offsets/config/status topics |
| Apicurio Registry | Deployment + Service | 1 class instance | PostgreSQL-backed registry state |
| PostgreSQL | CloudNativePG `Cluster` resource | 1 primary for required scope | Persistent volume and documented backup/export |
| OTel Collector | OpenTelemetry Operator resource | 1+ | Bounded queues |
| Prometheus/Grafana/Tempo | Prometheus Operator/chart-managed workloads | 1 each for class environment | Persistent volumes |
| Loki | Optional stretch workload | 1 if resources permit | Persistent volume |
| OIDC provider | Deployment + Service | 1 class instance | Persistent configuration |

Kafka, Flink, and PostgreSQL have nontrivial stateful lifecycle requirements. The project will use their established Kubernetes operators rather than maintain custom StatefulSets and recovery scripts. Helm installs pinned third-party charts/operators; Kustomize supplies developer, integration, and review overlays for team-owned workloads. Operator use does not replace design work: the repository must still declare resources, durability, replication, probes, disruption behavior, and acceptance evidence.

## Discovery and traffic

- Kubernetes Services and cluster DNS provide stable names; pod addresses are never application configuration.
- Gateway/Ingress exposes only the web UI, query API, OIDC endpoints, and optional Grafana access.
- Kafka, databases, Flink management, gRPC registry, and telemetry backends remain cluster-internal.
- Apicurio, Kafka Connect, operator APIs, and database administration endpoints remain cluster-internal.
- The Copilot Service is reached through the authenticated API/Gateway path. Only it receives restricted egress to the configured model endpoint; the Tool Gateway remains cluster-internal.
- Readiness decides normal routing; liveness is reserved for locally unrecoverable process failure; startup probes protect slow initialization.
- The project will record endpoint propagation and connection-reuse behavior instead of assuming immediate or perfectly even balancing.
- Kubernetes-native discovery is chosen over adding Consul because both would solve overlapping membership problems in the class deployment. The architecture review will compare their registration, health, and failure models without operating two discovery planes.

## Reconciliation and shutdown

Deployments declare desired replicas and Kubernetes replaces failed pods. Applications still persist state before acknowledging work. Pods handle termination signals, stop accepting new work, finish or checkpoint bounded in-flight work, and exit within the termination grace period. PodDisruptionBudgets protect voluntary disruption where more than one ready replica is required.

## Scaling

- Projection/API scales on CPU, request rate, and active SSE pressure.
- Copilot Service concurrency is capped by provider quota and a bulkhead; adding replicas does not increase the configured global investigation budget.
- Simulators scale by scenario shard.
- Flink parallelism aligns with Kafka partitions and available task slots.
- Kafka-lag-driven scaling with KEDA is a stretch goal after a baseline is measured; required scaling uses manual replicas or a simple HPA where applicable.
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
10. Disable the external model endpoint and show circuit-breaker/mock behavior while the deterministic control loop remains healthy.

## Non-claims

A multi-node logical cluster on one laptop does not prove host, rack, zone, or region availability. Service discovery does not provide authorization, retries do not provide correctness, and Kubernetes restart does not recover state that was never made durable.

Serverless and multi-region deployment are excluded because the project specifically needs visible long-running Kafka/Flink state, partition behavior, and Kubernetes failure experiments. The tradeoff is documented rather than presenting Kubernetes as universally preferable.
