# Infrastructure and Observability

**Status: design only; implementation not started.**

Planned Kubernetes runtime assets for `kind` or `minikube`, including namespaces, workloads, Services, Gateway/Ingress, probes, resources, leases, persistent storage, RBAC, NetworkPolicies, and observability. Helm will install pinned third-party stacks such as Strimzi, the Flink Kubernetes Operator, PostgreSQL, Keycloak, and the observability stack. Kustomize overlays will configure project-owned workloads for developer, pairwise-integration, and architecture-review profiles. The team will not fork or recreate upstream operators.

Every member owns the Kubernetes and observability resources for their workloads. Member_2 owns Kafka/Flink platform resources, Member_3 owns PostgreSQL and Kafka Connect/Debezium, Member_1 owns the shared scenario/load/fault runner plus OTel/Prometheus/Grafana/Tempo installation, and Member_4 owns Gateway, Keycloak, product SLO views, and the final overlay. The runtime schema registry, database operator, Loki, KEDA, and database replicas are excluded.

A lightweight Docker Compose profile may support development, but Kubernetes is the architecture-review and final-demo target. See the [Kubernetes design](../docs/design/KUBERNETES_DEPLOYMENT.md) and [open-source reuse plan](../docs/design/OPEN_SOURCE_STACK.md).

The planned Copilot Service is a separate restricted Deployment with an internal Service, read-only Tool Gateway access, model-provider-only egress, provider credentials from a Kubernetes Secret, and no Kafka or Kubernetes API permission. A deterministic mock-provider profile supports CI and offline demonstrations; no manifests or provider integration have been implemented yet.
