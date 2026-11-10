# Member_3 — Distributed Control and Durable State

## Distributed-systems ownership

Member_3 owns the distributed action processor, Kubernetes leader lease and fencing epochs, durable control saga, outbox/inbox, PostgreSQL state, and the at-least-once command lifecycle. The main challenge is creating one logical control effect when leaders fail or events are replayed concurrently.

## Planned responsibilities

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
11. Configure the required single-instance CloudNativePG resource and verify persistence/recovery without implementing a database controller.
12. Provide bounded action-timeline and command-result evidence, plus investigation audit persistence and retention, without granting the agent control or write access.

## Boundaries

Member_3 consumes calculated transformer loads rather than independently aggregating raw telemetry. Read-oriented UI projections and SSE delivery belong to Member_4.

## Independent distributed demonstration

Replay and concurrently deliver critical load events, restart during an active plan, and prove that the system recovers one plan, one logical command per target, and an explainable final state. Verify that agent evidence exposes the trigger and outcome without permitting policy or command mutation.

## Done when

Threshold behavior is deterministic, duplicates do not create duplicate effects, concurrent events preserve invariants, and durable history reconstructs every decision and outcome.
