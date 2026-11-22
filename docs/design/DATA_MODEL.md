# Data Model

## 1. Conceptual relationships

```mermaid
erDiagram
    TRANSFORMER ||--o{ DEVICE : supplies
    TRANSFORMER ||--o{ TRANSFORMER_LOAD : has
    TRANSFORMER ||--o{ ALERT : raises
    TRANSFORMER ||--o{ ACTION_PLAN : triggers
    DEVICE ||--o{ TELEMETRY_READING : emits
    ACTION_PLAN ||--o{ CHARGER_COMMAND : contains
    DEVICE ||--o{ CHARGER_COMMAND : receives
    CHARGER_COMMAND ||--o{ COMMAND_RESULT : reports
    POLICY_VERSION ||--o{ ACTION_PLAN : governs
    TRANSFORMER ||--o{ INVESTIGATION : scopes
    INVESTIGATION ||--o{ TOOL_CALL : records
    INVESTIGATION ||--o{ EVIDENCE_REFERENCE : cites
```

## 2. Core records

| Record | Key fields | Purpose |
|---|---|---|
| Transformer | ID, name, capacity kW, region, thresholds, enabled | Grid asset being protected |
| Device | ID, transformer ID, type, controllable, priority, status | Simulated field endpoint |
| Telemetry reading | Event ID, device ID, event time, ingest time, power kW, quality | Raw time-series input |
| Transformer load | Window ID, revision, totals, utilization, completeness | Aggregated grid state |
| Alert | ID, transformer ID, severity, status, reason, timestamps | Operator-visible condition |
| Action plan | ID, trigger load revision, policy version, reduction target, status | Explainable control decision |
| Charger command | ID, action ID, device ID, type, rate, expiry, status | Requested device change |
| Command result | Event ID, command ID, outcome, applied rate, reason | Device acknowledgement |
| Policy version | ID, thresholds, cooldown, selection rules, effective time | Versioned decision configuration |
| Scenario run | ID, scenario type, seed, scale, status, timestamps | Reproducible simulation run |
| Audit event | ID, actor, action, entity, before/after summary, time | Configuration and operator history |
| Investigation | ID, actor, entity/time scope, prompt hash, provider mode, status, timestamps | Bounded agent workflow and final structured result |
| Tool call | ID, investigation ID, tool name, arguments hash, status, latency, result hash | Auditable read-only agent execution without storing unrestricted payloads |
| Evidence reference | ID, investigation ID, source type, entity/correlation reference, observed time, freshness, deep link | Verifiable support for an agent claim |

## 3. Logical store ownership

One PostgreSQL primary is shared only to fit student hardware; services do not share tables or credentials. The fenced simulated gateway
adds the narrow same-database isolated-schema boundary in
[ADR-029](../decisions/ADR-029-outbox-and-fencing.md): Member_1 owns
`member1_gateway` in the control database and calls only a typed authorization
function, so its simulated effect and epoch check can commit atomically.

| Logical store | Owner | Records | Rebuildability |
|---|---|---|---|
| `registry` | Member_1 / Device Registry | Device registration, assignment, capabilities, heartbeat summary | Restored from registry backup/re-registration |
| `control` | Member_3 / Action Service | Policies, action plans, commands, inbox/outbox, fencing and audit state | Authoritative; requires backup and recovery |
| `projection` | Member_4 / Projection API | Load history, alerts, action/result views, UI cursors | Rebuildable from Kafka topics within retention |
| `copilot_audit` | Member_4 / Copilot Service | Investigations, tool calls, evidence references | Non-authoritative, retention bounded |

Member_2 writes no PostgreSQL data. Flink publishes transformer-load events, and the projection consumer persists load history. Configuration changes cross a typed internal service API rather than allowing the Projection API to write `control` tables.

## 4. State machines

### Transformer

```mermaid
stateDiagram-v2
    [*] --> NORMAL
    NORMAL --> WARNING: utilization >= warning
    WARNING --> NORMAL: utilization < warning
    WARNING --> CRITICAL: utilization >= critical
    NORMAL --> CRITICAL: utilization >= critical
    CRITICAL --> RECOVERING: corrective plan active
    RECOVERING --> NORMAL: below recovery for required windows
    RECOVERING --> CRITICAL: load rises or plan fails
```

### Command

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> SENT
    SENT --> APPLIED: positive result
    SENT --> REJECTED: device rejects command
    SENT --> TIMED_OUT: acknowledgement deadline
    TIMED_OUT --> SENT: bounded retry
    APPLIED --> RESTORED: restore confirmed
    REJECTED --> [*]
    RESTORED --> [*]
```

### Investigation

```mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> RUNNING
    RUNNING --> COMPLETED
    RUNNING --> PARTIAL: deadline or evidence gap
    RUNNING --> FAILED: provider or validation failure
    QUEUED --> CANCELLED
    RUNNING --> CANCELLED
    COMPLETED --> [*]
    PARTIAL --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

## 5. Time and units

- IDs are UUIDs unless a deterministic command ID is required.
- Timestamps use UTC ISO 8601 in JSON and timezone-aware database types.
- Power uses kilowatts; energy, if added later, uses kilowatt-hours.
- Solar generation is negative power; consumption is positive.
- Percentages are stored as numeric values from 0 through values above 100, not fractions.

## 6. Retention proposal

Raw telemetry is retained for the short duration needed for replay and the final demonstration. Aggregated load, alerts, actions, command results, configuration, and authoritative audit records are retained for the life of the project. Agent investigations have a shorter configurable retention and may be deleted without changing grid history. Retention must be configurable so load tests do not fill local disks.

## 7. Integrity rules

- Device IDs reference an existing transformer.
- A command targets one controllable EV charger.
- A load revision is unique by transformer, window, and revision number.
- Event IDs, action IDs, and command IDs are unique.
- Stored actions are append-only except for explicit lifecycle status updates.
- Historical actions are never deleted when corrected telemetry arrives.
- Every displayed agent evidence ID belongs to the same investigation and resolves to a recorded tool result/deep link.
- Investigation/tool records cannot create or mutate policies, action plans, commands, or scenario runs.
- Database roles cannot read or write another service's logical store; shared facts cross boundaries through events or typed service APIs.
