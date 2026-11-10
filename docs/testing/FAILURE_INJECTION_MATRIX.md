# Failure-Injection Matrix

| Experiment | Invariant | Evidence |
|---|---|---|
| API pod readiness failure | Other ready replicas continue; stale UI is explicit | Endpoint membership, request metrics, trace |
| API pod deletion | Kubernetes reconciles desired replicas | Recovery time and transient errors |
| Action leader loss | One new leader; no valid stale command | Lease/epoch logs and rejected command |
| Kafka broker loss | Critical topics follow configured replication guarantees | ISR, lag, publication outcome |
| Flink task restart | Committed state restores without double count | Checkpoint and expected totals |
| Duplicate telemetry/load | One logical state transition | Inbox/dedup metric |
| Poison message | Healthy partitions continue; record reaches DLQ | DLQ reason and lag |
| Conflicting policy update | One accepted version; stale writer is rejected | Requested/current versions and audit event |
| gRPC latency/outage | Deadline, breaker, and safe degraded behavior | Attempts, breaker state, trace |
| Command response lost | Status reconciliation avoids second effect | Command ID and device count |
| SSE disconnect/cursor gap | Client reconnects or reloads authoritative snapshot | Cursor and UI state |
| Telemetry backend loss | Business path continues within bounded buffers | Drop/backlog metrics and alert |
| Model provider timeout/rate limit | Agent degrades explicitly; grid path remains healthy | Breaker/tool trace, investigation status, control SLI |
| Copilot pod deletion | Investigation is visibly interrupted/recovered; no domain mutation | Pod events, agent trace, unchanged action history |
| Agent tool failure | Completed evidence is retained and missing source is labeled | Partial response and tool audit |
| Prompt injection/write-tool request | No unavailable tool executes and no authorization widens | Denial audit and zero domain changes |

Each test records hypothesis, blast radius, start/end timestamps, expected and observed behavior, affected SLI, logs/trace IDs, recovery, and cleanup. Toxiproxy provides repeatable dependency latency and connection faults; Kubernetes and the selected operators provide pod, task, and broker disruption; seeded fixtures provide duplication, delay, and poison events. The team will not build a chaos framework or add a full chaos platform unless these tools cannot express a required acceptance case. Faults are rehearsed, reversible, and stopped if containment or recovery differs from the plan.
