# Action Service

**Status: design only; implementation not started.**

Planned home of transformer state transitions, hysteresis, cooldown, charger selection, alert generation, idempotent command lifecycle management, and durable control-state recovery. It consumes calculated loads rather than raw telemetry. Domain changes and outbox rows commit together in the owned `control` logical store; Debezium/Kafka Connect publishes alert, action, and command events, so this service will not contain a custom polling relay. A typed internal control endpoint accepts optimistic policy changes without exposing tables. The service is tested independently with load/result fixtures and a fake Registry. See the [system design](../../docs/design/SYSTEM_DESIGN.md), [transaction design](../../docs/design/TRANSACTIONS_AND_SAGAS.md), and [Member_3's work plan](../../docs/team/03_MEMBER_3_CONTROL.md).
