# Infrastructure and Observability

**Status: design only; implementation not started.**

Planned Kubernetes runtime assets for local `kind`, including namespaces, workloads, Services, Gateway/Ingress, probes, resources, leases, persistent storage, RBAC, NetworkPolicies, and observability. Helm will install pinned third-party stacks such as Strimzi, the Flink Kubernetes Operator, PostgreSQL, Keycloak, and the observability stack. Kustomize overlays will configure project-owned workloads for developer, pairwise-integration, and architecture-review profiles. The team will not fork or recreate upstream operators.

Every member owns the Kubernetes and observability resources for their workloads. Member_2 owns Kafka/Flink platform resources, Member_3 owns PostgreSQL and Kafka Connect/Debezium, Member_1 owns the shared scenario/load/fault runner plus OTel/Prometheus/Grafana/Tempo installation, and Member_4 owns Gateway, Keycloak, product SLO views, and the final overlay. The runtime schema registry, database operator, Loki, KEDA, and database replicas are excluded.

A lightweight Docker Compose profile may support development, but Kubernetes is the architecture-review and final-demo target. See the [Kubernetes design](../docs/design/KUBERNETES_DEPLOYMENT.md) and [open-source reuse plan](../docs/design/OPEN_SOURCE_STACK.md).

The delivery model uses independent local slices and local kind for integration/review on one computer. Cloud/EKS is deferred; no cloud registry or model account is required. See the [accepted matrix](../docs/implementation/VERSION_MATRIX.md) and [manual prerequisites](../docs/implementation/LOCAL_PREREQUISITES.md). Docker Desktop and the empty CLI-managed `smart-grid-dev` kind cluster are now running. Built-in Desktop Kubernetes is disabled. No application or operator manifests were created/deployed. Deploy the OTel Collector directly by Helm without the OTel Operator; Flink webhooks are disabled in the local baseline to avoid cert-manager. Python code and model mocks are the default.

The planned Copilot Service is a separate restricted Deployment with an internal Service, read-only Tool Gateway access, deterministic mock mode with no external provider egress or credentials, and no Kafka or Kubernetes API permission. The deterministic mock-provider profile is required for CI and local demonstrations; no manifests or provider integration have been implemented yet.
