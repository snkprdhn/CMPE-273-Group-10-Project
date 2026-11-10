# Team Work Allocation

## Working agreement

The project is divided into four workstreams with clear source ownership. Shared contracts are agreed on before implementation, and no teammate changes another teammate's component without review. Each workstream must provide an isolated demonstration, automated tests, metrics, and documentation before end-to-end integration.

## Ownership

| Member | Distributed component | Owns | Distributed-systems focus |
|---|---|---|---|
| Member_1 | Distributed telemetry and internal RPC | `simulators/`, gRPC registry, producer reliability, command consumers, and fixtures | Partition-aware publishing, deadlines, retries, backpressure, duplicates, and out-of-order delivery |
| Member_2 | Messaging and stateful stream processing | Strimzi Kafka topology, Flink Operator configuration, Apicurio integration, and `services/aggregation/` | Partitioning, event time, distributed state, checkpoints, replay, and scaling |
| Member_3 | Distributed control, coordination, and durable state | `services/action/`, saga/inbox, Debezium outbox integration, leader lease/fencing, and CloudNativePG persistence | Idempotent effects, stale-worker rejection, replication consistency, concurrency, and recovery |
| Member_4 | Kubernetes platform, real-time operations, and agent orchestration | `services/api/`, `services/copilot/`, `dashboard/`, `infra/`, Keycloak, OpenTelemetry/Prometheus Operators, and fault harness | Discovery, replica routing, SSE recovery, bounded tool use, provider failure, SLOs, observability, and Kubernetes behavior |

`docs/contracts/`, shared configuration, and root-level files require a reviewed pull request. Member_4 coordinates integration, but contract decisions require the affected producer and consumer to approve them.

## Milestones

| Checkpoint | Member_1 | Member_2 | Member_3 | Member_4 |
|---|---|---|---|---|
| A — Contracts | Device events, gRPC, producer guarantees, and device incident fixtures | Topics, replication, keys, windows, load output, and stream-health evidence | Commands, saga, fencing, consistency, and action evidence | Kubernetes workload, projection, SSE, OTel, UI, tool schemas, and agent output contracts |
| B — Standalone | Multi-producer, RPC deadline, command-consumer faults, and seeded device cases | Aggregate fixture streams, recover checkpoints, and expose bounded health summaries | Repeated loads create one fenced durable saga and explainable action timeline | Kubernetes discovery/probes, mock projections, mock-provider agent, and telemetry correlation |
| C — Integration | Register devices, publish to Kafka, apply commands, and verify device evidence | Connect replicated Kafka/Flink, recovery, and agent stream-health tools | Connect lease, outbox/inbox, commands, results, DB, and action-history tools | Deploy integrated workloads, agent/tool gateway, SSE, OTel, OIDC, UI, and dashboards |
| D — Acceptance | Scale, backpressure, delayed-device, and agent fixture demo | Broker/task restart, correctness, and stream-evidence demo | Leader failover, stale epoch, saga, conflicting-update, and action-evidence demo | Kubernetes faults, agent evaluation, SLO/security evidence, UI, and end-to-end demo |

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

## Required collaborative tests

1. **Telemetry-to-load:** Member_1 and Member_2 verify that mixed device readings produce the expected net transformer load.
2. **Load-to-action:** Member_2 and Member_3 verify that a critical load produces one logical throttle plan and that recovery produces a restore plan.
3. **Action-to-device:** Member_3 and Member_1 verify duplicate-safe command delivery and command-result reporting.
4. **Event-to-view:** Member_3 and Member_4 verify projection consistency, snapshot loading, live updates, errors, and cursor-based reconnect behavior.
5. **End to end:** All four members demonstrate the overload and recovery scenario together.
6. **Evidence-grounded investigation:** All four members contribute one incident fixture; Member_4 runs the agent against their bounded evidence tools and verifies tool choice, citations, uncertainty, and write-tool denial.

## Branch and review process

- Use one branch per feature or document change.
- Open a pull request into `main`; do not use `main` as a shared working branch.
- Require at least one reviewer, and require both sides of a contract change to review it.
- Keep pull requests scoped to one workstream where possible.
- Use mock events and mock HTTP responses while dependent components are unavailable.
- Do not report a planned feature as complete until its acceptance evidence is linked.

## Definition of done

The team can run a reproducible Kubernetes demonstration in which simulated devices publish telemetry, transformer loads are calculated correctly, an overload produces a fenced duplicate-safe control saga, the UI updates live, Kubernetes and stream failures recover, and the platform remains explainable through correlated history, metrics, traces, and logs. A bounded read-only agent can investigate one seeded incident using valid evidence, rejects unavailable/write tools, and fails independently of the control loop.
