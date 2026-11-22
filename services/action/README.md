# Action Service — Member_3

The standalone Action domain and durable PostgreSQL saga are implemented in
[member3](../../member3/README.md), with load/result fixtures, a fake Registry,
transactional inbox/outbox, optimistic versions and restart acceptance.
See [ADR-028](../../docs/decisions/ADR-028-member3-action-domain.md).

Kafka consumption, Debezium publication, Kubernetes Lease/fencing allocation,
live charger observations, operator compensation API and deployment/operations
remain later milestones. Member_1 owns simulation/Registry/observability,
Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium, and Member_4
projection/API/UI/identity/Copilot. No custom polling relay is used.
