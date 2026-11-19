# Team Work Allocation

## Working agreement

The project is divided into four independently testable workstreams with clear source and runtime ownership. Shared contracts are agreed on before implementation, and no teammate changes another teammate's component without review. Each member builds against versioned schemas, golden fixtures, and local test dependencies rather than waiting for another service. Every workstream provides an isolated demonstration, automated tests, Kubernetes resources, metrics/traces/logs, a dashboard panel, a fault experiment, and documentation before end-to-end integration.

## Ownership

| Member | Distributed component | Owns | Distributed-systems focus |
|---|---|---|---|
| Member_1 | Distributed telemetry, internal RPC, reliability tooling, and observability platform | `simulators/`, gRPC registry, producer reliability, command consumers, contract/scenario fixtures, CI/k6/Toxiproxy runner, and OTel/Prometheus/Grafana/Tempo installation | Partition-aware publishing, deadlines, retries, backpressure, duplicates, out-of-order delivery, observable faults, and evidence collection |
| Member_2 | Messaging and stateful stream processing | Strimzi Kafka topology, JSON Schema compatibility checks, Flink Operator configuration, and `services/aggregation/` | Partitioning, event time, distributed state, checkpoints, replay, replication, and scaling |
| Member_3 | Distributed control, coordination, and durable state | `services/action/`, PostgreSQL deployment/control schema, saga/inbox, Debezium/Kafka Connect, and leader lease/fencing | Idempotent effects, stale-worker rejection, transaction failure windows, concurrency, and recovery |
| Member_4 | Distributed read operations, UI, identity/Gateway, and agent orchestration | `services/api/`, `services/copilot/`, `dashboard/`, Gateway, Keycloak, product SLO views, and final overlay | Replica routing, SSE recovery, bounded tool use, provider failure, authorization, and operator experience |

`docs/contracts/`, shared configuration, and root-level files require a reviewed pull request. Member_4 coordinates the final overlay, but does not implement other members' manifests or instrumentation. Contract decisions require the affected producer and consumer to approve them.

## Independence rules

1. Members depend on contracts and fixtures, not a teammate's running service.
2. No service imports another service's internal package or reads another service's owned database schema.
3. Every member owns the Deployment/operator resource, Service, probes, resources, ServiceAccount/RBAC, NetworkPolicy, instrumentation, and fault test for their workloads.
4. One PostgreSQL instance may be shared for hardware efficiency, but `registry`, `control`, `projection`, and `copilot_audit` use separate ownership and credentials.
5. Flink publishes load events only; the projection consumer owned by Member_4 persists load history.
6. The Copilot Service reads Member_4's projections and bounded observability adapters rather than calling Members 1–3 services directly.

## Workload balance

Equality is measured by complete, defensible deliverables rather than line counts. Every member owns one substantial distributed slice, one shared platform contribution, one standalone profile, Kubernetes/security/observability for that slice, pairwise integration, failure evidence, and a final business outcome.

| Member | Primary distributed challenge | Shared contribution | Business proof |
|---|---|---|---|
| Member_1 | Concurrent producers, gRPC deadlines, backpressure, command idempotency | Contract/scenario fixtures, reusable load/fault runner, and shared observability installation | Realistic devices respond once and the team can measure the resulting demand change |
| Member_2 | Partitioned event-time state, late data, checkpoints, replay, Kafka replication | Messaging platform and schema-compatibility CI | Trustworthy transformer load is produced despite delay and restart |
| Member_3 | Fenced control, saga/outbox/inbox, concurrency, durable recovery | PostgreSQL and Debezium/Kafka Connect platform | Overload produces one safe, explainable throttle/restore workflow |
| Member_4 | Replay-safe projections, SSE recovery, authorization, bounded agent tools | Gateway, identity, product SLO views, and final overlay | Operator sees the outcome live and can investigate it using evidence |

## Milestones

| Checkpoint | Member_1 | Member_2 | Member_3 | Member_4 |
|---|---|---|---|---|
| A — Contracts | Device/command events, gRPC, retry rules, and scenario fixtures | Topics, keys, windows, load schema, replication, and health evidence | Load/command expectations, saga/fencing, outbox, and action evidence | Projection/SSE/OpenAPI, identity, agent tools/output, and UI fixtures |
| B — Standalone | Edge profile plus CI scenario/fault runner passes without other services | Streaming profile produces golden loads and restores checkpoints | Control profile recovers fenced sagas and publishes outbox events | Operations profile converges projections and passes mock-agent/UI tests |
| C — Pairwise | Telemetry→load and command→device gates | Telemetry→load and load→action gates | Load→action and command→device gates | Recorded domain events→projection/UI/agent gate |
| D — Acceptance | Scale, backpressure, device failure, and shared harness evidence | Broker/task restart, event-time correctness, and replay evidence | Leader failover, saga/outbox, stale epoch, and recovery evidence | Kubernetes routing, auth, SLOs, agent evaluation, UI, and final business story |

## Shared contract freeze

Before coding, the team must approve:

1. Topic names, partition keys, and schema versions.
2. Power units, timestamp format, and the sign convention for solar generation.
3. Transformer window size, watermark, and allowed-lateness policy.
4. Warning, critical, and recovery thresholds.
5. Command lifecycle and idempotency key.
6. REST paths, JSON field names, error shape, pagination, and SSE event types.
7. Kubernetes workload/probe/resource contracts and the final cluster profile.
8. Proposed performance/SLO targets and the machine profile used to measure them.
9. Pinned, compatible open-source versions and ownership of each integration boundary.
10. Agent tool schemas, evidence/output schema, provider budget, authorization, retention, and deterministic mock behavior.
11. Service-owned database schemas/roles, component-local profiles, and golden fixture locations.

## Required collaborative tests

1. **Telemetry-to-load:** Member_1 and Member_2 verify that mixed device readings produce the expected net transformer load.
2. **Load-to-action:** Member_2 and Member_3 verify that a critical load produces one logical throttle plan and that recovery produces a restore plan.
3. **Action-to-device:** Member_3 and Member_1 verify duplicate-safe command delivery and command-result reporting.
4. **Event-to-view:** Member_3 and Member_4 verify projection consistency, snapshot loading, live updates, errors, and cursor-based reconnect behavior.
5. **End to end:** All four members demonstrate the overload and recovery scenario together.
6. **Evidence-grounded investigation:** Members 1–3 contribute incident fixtures and observable signals; Member_4 builds projections/tool adapters from those stable contracts and verifies tool choice, citations, uncertainty, and write-tool denial.

## Branch and review process

- Use one branch per feature or document change.
- Open a pull request into `main`; do not use `main` as a shared working branch.
- Require at least one reviewer, and require both sides of a contract change to review it.
- Keep pull requests scoped to one workstream where possible.
- Use mock events and mock HTTP responses while dependent components are unavailable.
- A pull request for one workstream must not require another member's unpublished branch or manually running service.
- Integration defects are captured as shared fixtures and contract tests before the fix is merged.
- Do not report a planned feature as complete until its acceptance evidence is linked.

## Definition of done

The team can run a reproducible Kubernetes demonstration in which simulated devices publish telemetry, transformer loads are calculated correctly, an overload produces a fenced duplicate-safe control saga, the UI updates live, Kubernetes and stream failures recover, and the platform remains explainable through correlated history, metrics, traces, and logs. A bounded read-only agent can investigate one seeded incident using valid evidence, rejects unavailable/write tools, and fails independently of the control loop.
