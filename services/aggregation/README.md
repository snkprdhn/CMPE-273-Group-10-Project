# Aggregation Service — Member_2

The standalone Python/Kafka/Flink slice is implemented in [member2](../../member2/README.md).
It publishes transformer loads, DLQ and late audit events using the frozen v1
contracts and never writes PostgreSQL. Pure golden checks and real native ARM64
TaskManager/checkpoint/replay acceptance run independently of simulators and other
members' applications. See [ADR-027](../../docs/decisions/ADR-027-member2-standalone-aggregation.md).

Local kind/operator deployment, distributed scaling and whole-cluster HA remain
future integration gates; this slice uses the lightweight Compose profile.
