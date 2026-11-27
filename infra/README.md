# Infrastructure

- [kind](kind/README.md): the local developer and three-broker review profiles on Docker Desktop. Helm installs Strimzi, the Flink Kubernetes Operator and PostgreSQL. Kustomize declares the workloads and policies. Calico enforces NetworkPolicy.
- [aks](aks/README.md) and the [AKS runbook](aks/AGENT_RUNBOOK.md): the hosted review overlay (managed disks, Cilium, Caddy HTTPS, three-hour Kafka retention) and how to roll out by image digest.
- [hosted](hosted/README.md): settings shared by the hosted overlays (identity, retention).
- [observability](observability/README.md): OpenTelemetry Collector, Prometheus, Grafana and Tempo.
- [compose](compose/README.md): lightweight Kafka and PostgreSQL for component tests and CI.
- [connect](connect/README.md): Kafka Connect and Debezium image, plus the standalone outbox and fencing profile.

See the [master guide](../docs/MASTER_GUIDE.md) for the architecture and the [version matrix](../docs/implementation/VERSION_MATRIX.md) for pins.
