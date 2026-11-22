# Independent Component Testing Plan

## Principle

Every member must be able to build, run, test, and demonstrate their workstream using contracts, fixtures, and local open-source dependencies. The full project is an integration target, not a development prerequisite.

## Shared fixture layout

```text
tests/contracts/
├── telemetry/          # valid, invalid, duplicate, delayed, out-of-order
├── transformer-load/   # normal, warning, critical, corrected, stale
├── commands/           # throttle, restore, duplicate, expired, stale epoch
├── command-results/    # applied, already applied, rejected, timeout
├── projections/        # expected snapshot and SSE changes
├── agent-evidence/     # grounded, stale, partial, injected instructions
└── scenarios/          # complete seeded overload-to-recovery stories
```

Each case contains input, expected output, expected durable state, expected metrics, and expected failure/DLQ behavior. Fixtures are versioned with their schema and use synthetic IDs and UTC timestamps.

## Member_1 profile: edge

Run the simulator and Registry with a local Kafka dependency. A fixture producer publishes charger commands; a mock gRPC client/server exercises deadlines and degraded behavior.

Required standalone tests:

- Telemetry validates against JSON Schema and uses the expected partition key.
- Broker delay produces bounded buffering/retry behavior.
- Duplicate and expired commands produce one or zero logical effects as specified.
- Registry replicas return contract-compatible responses and enforce deadlines.
- Seeded scenarios reproduce the same logical event sequence.
- Locust/Toxiproxy and CI scenario commands are usable by the other members.

## Member_2 profile: streaming

Run Kafka and the Flink job with telemetry fixtures; Member_1 services are not required.

Required standalone tests:

- Golden meter/EV/solar inputs produce exact window totals.
- Duplicate readings are not double counted according to the frozen rule.
- Delayed readings create the expected revision; too-late records follow the audit policy.
- Invalid events reach the DLQ while healthy partitions continue.
- Checkpoint restart and replay preserve expected results.
- Partition/parallelism changes preserve per-transformer ordering and state.

## Member_3 profile: control

The implemented domain profile runs PostgreSQL with load/result JSON fixtures and a fake Registry, including explicit fresh charger observations. See [Member_3](../../member3/README.md). Kafka Connect/Debezium, fixture topics and live leadership belong to the next integration gates; Member_1 and Member_2 services are not required.

Required standalone tests:

- Critical and recovery load fixtures create the expected plan and commands.
- Duplicate/revised loads create only permitted state transitions.
- Stable IDs, inbox constraints, and command-result replay prevent duplicate effects.
- Crash-before/after outbox publication recovers without losing the logical event.
- Leader failover increments fencing state and stale commands are rejected.
- Partial acknowledgements and timeouts preserve an explainable final state.

## Member_4 profile: operations

Replay recorded load, alert, action, and result events into the projection consumer. Use mock OIDC identities and deterministic model/tool responses. Members 1–3 services are not required.

Required standalone tests:

- Duplicate and reordered events converge to the same read model.
- Snapshot and SSE cursors recover after disconnect and gap.
- UI states cover loading, stale, partial, permission denied, and unavailable.
- Agent tool inputs are bounded and output evidence IDs resolve.
- Prompt injection and write-tool requests are denied.
- Provider timeout/mock mode does not affect API, projection, or UI health.

## Contract CI

Every pull request runs fast checks for JSON Schema fixtures, Protobuf compatibility/generation, OpenAPI validation, agent schemas, and golden examples. Component-owned jobs then run independently. Full Kubernetes tests run on scheduled integration branches or milestones rather than blocking every local change.

No component may import another component's internal package or read another service's owned database schema. A failing integration becomes a reproducible fixture and contract test before the fix is merged.

## Pairwise integration gates

| Gate | Participants | Evidence |
|---|---|---|
| Telemetry to load | Members 1 and 2 | Live events match golden load fixtures |
| Load to action | Members 2 and 3 | Live critical/recovery events match plan fixtures |
| Action to device | Members 3 and 1 | Duplicate-safe command and result lifecycle |
| Events to operations | Members 2/3 and 4 | Projection snapshots/SSE match recorded expectations |
| Full system | All | Kubernetes overload-to-recovery, failure, and agent investigation demo |

Pairwise gates verify integration without making one member responsible for debugging or deploying another member's component.

