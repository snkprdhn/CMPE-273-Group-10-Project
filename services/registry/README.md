# Device Registry — Member_1

Implemented all four frozen gRPC RPCs with owned PostgreSQL persistence, operation
idempotency, capability-version conflicts, heartbeat freshness, bounded eligibility
pagination, local service tokens and deadlines. Generated bindings come only from
`make bindings`. The owning implementation is `member1/registry.py`; its separate
runtime lock/venv and image exclude Kafka libraries.

See the [standalone runbook](../../member1/README.md) for local startup, fixture
clients, exact failure/restart semantics, health/metrics and tests. Host gRPC uses
loopback port **15051**; container internal gRPC stays 50051. Member_3/Member_4 use
typed clients and contracts, never Registry tables. Kubernetes routing/mTLS and
replicated load/backup gates remain unverified.
