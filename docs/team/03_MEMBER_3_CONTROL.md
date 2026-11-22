# Member_3 — Distributed Control and Durable State

## Distributed-systems ownership

Member_3 owns the distributed action processor, Kubernetes leader lease and fencing epochs, durable control saga, outbox/inbox, PostgreSQL state, and the at-least-once command lifecycle. The main challenge is creating one logical control effect when leaders fail or events are replayed concurrently.

## Implemented standalone domain

[Member_3 runbook](../../member3/README.md) and
[ADR-028](../decisions/ADR-028-member3-action-domain.md) cover the fixture-driven
PostgreSQL domain/saga, transactional inbox/outbox, versions and recovery.
[ADR-029](../decisions/ADR-029-outbox-and-fencing.md) and the
[delivery runbook](../../infra/member3/README.md) complete local CDC and durable
Lease fencing. Live observation acquisition, operator compensation API and full
operational deployment remain later gates. The domain profile needs only PostgreSQL plus
load/result fixtures and a fake Registry with explicit charger observations.

## Responsibilities (remaining integration gates included)

1. Consume transformer loads through a coordinated consumer group.
2. Implement deterministic transformer state transitions, hysteresis, and cooldowns.
3. Select eligible chargers while preventing concurrent plans from overcorrecting one transformer.
4. Create stable action and command IDs, write domain state and outbox rows atomically, and configure Debezium routing fields.
5. Track acknowledgement, timeout, bounded retry, completion, and compensation/restore behavior.
6. Use unique constraints and idempotent upserts for repeated events.
7. Recover active plans and command status after restart.
8. Publish alerts and control events with enough context for real-time projections and auditing.
9. Demonstrate leader failover and stale-epoch rejection across two Action Service pods.
10. Enforce optimistic versions for concurrent policy/state changes and maintain a documented backup/export procedure.
11. Own the pinned PostgreSQL Helm deployment, `control` schema/role, backup/export procedure, and recovery verification without implementing a database controller.
12. Publish bounded action-timeline and command-result evidence fixtures without granting the agent control or write access; Member_4 owns investigation audit persistence.
13. Configure Kafka Connect/Debezium and package/instrument Member_3 workloads with their own Kubernetes resources, RBAC/NetworkPolicy, metrics/traces/logs, dashboard panel, and leader/database fault experiment.

## Boundaries

Member_3 consumes calculated transformer loads rather than independently aggregating raw telemetry. It owns only the `control` logical store and never requires Member_4 to read its tables; alerts, actions, and results cross the boundary as events. Read-oriented projections, agent audit storage, and SSE delivery belong to Member_4.

## Independent development profile

Use transformer-load and command-result fixtures, a fake Device Registry, local Kafka, PostgreSQL, and Debezium. Member_3 can validate policies, saga/outbox/inbox behavior, fencing, timeouts, compensation, and recovery without live simulators, Flink, UI, or the agent.

## Independent distributed demonstration

Replay and concurrently deliver critical load events, restart during an active plan, and prove that the system recovers one plan, one logical command per target, and an explainable final state. Verify that agent evidence exposes the trigger and outcome without permitting policy or command mutation.

## Done when

Threshold behavior is deterministic, duplicates do not create duplicate effects, concurrent events preserve invariants, and durable history reconstructs every decision and outcome.
