# Infrastructure and Observability

The [local kind runbook](kind/README.md) covers the developer and three-broker
review profiles on Docker Desktop. Helm installs Strimzi, the Flink Kubernetes
Operator and persistent PostgreSQL. Kustomize declares the owned workloads and
policies. Calico provides NetworkPolicy enforcement.

Member_1 owns simulation, Registry and observability. Member_2 owns Kafka and
Flink. Member_3 owns PostgreSQL, Connect, Debezium and Action. Member_4 owns
projections, API, UI, Keycloak and the local routing profile. Copilot and the
full observability stack are follow-on work.

The [Compose runbook](compose/README.md) covers independent developer
dependencies, health, persistence and the optional Toxiproxy profile. The
[Member_3 runbook](member3/README.md) covers independent outbox and fencing
verification. See the [deployment design](../docs/design/KUBERNETES_DEPLOYMENT.md)
and [version matrix](../docs/implementation/VERSION_MATRIX.md) for scope and
pins.
