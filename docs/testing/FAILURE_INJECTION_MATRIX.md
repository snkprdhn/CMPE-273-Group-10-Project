# Failure-Injection Matrix

| Owner | Experiment | Invariant | Evidence |
|---|---|---|---|
| Member_4 | API readiness/pod loss | Other ready replicas continue; stale UI is explicit; Kubernetes reconciles | Endpoint membership, recovery time, request metrics, trace |
| Member_3 | Action leader loss | One new leader; no valid stale command | Lease/epoch logs and rejected command |
| Member_2 | Kafka broker loss | Critical topics follow configured replication guarantees | ISR, lag, publication outcome |
| Member_2 | Flink task restart | Committed state restores without double count | Checkpoint and expected totals |
| Members 2/3 | Duplicate telemetry/load | One logical state transition | Dedup/inbox metric |
| Member_2 | Poison message | Healthy partitions continue; record reaches DLQ | DLQ reason and lag |
| Member_3 | Conflicting policy update | One accepted version; stale writer is rejected | Requested/current versions and audit event |
| Member_1 | gRPC latency/outage | Deadline, breaker, and safe degraded behavior | Attempts, breaker state, trace |
| Members 1/3 | Command response lost | Status reconciliation avoids a second device effect | Command ID and device count |
| Member_4 | SSE disconnect/cursor gap | Client reconnects or reloads authoritative snapshot | Cursor and UI state |
| Each owner | Telemetry export loss | Business path continues within bounded buffers | Drop/backlog metrics and alert |
| Member_4 | Model provider timeout/rate limit | Agent degrades explicitly; grid path remains healthy | Breaker/tool trace, investigation status, control SLI |
| Member_4 | Copilot pod/tool failure | Investigation is partial/failed without domain mutation | Pod events, agent trace, tool audit, unchanged action history |
| Member_4 | Prompt injection/write-tool request | No unavailable tool executes and no authorization widens | Denial audit and zero domain changes |

Each owner defines expected behavior and evidence for their component. Member_1 maintains the reusable Toxiproxy/scenario runner but does not own another component's assertions or fixes. Tests record hypothesis, blast radius, timestamps, expected and observed behavior, affected SLI, logs/trace IDs, recovery, and cleanup. Kubernetes/operator faults and seeded fixtures cover the remaining cases; no full chaos platform is required.


The observability milestone automates a bounded projection metadata SQL-lock
incident; see the [runbook](../../infra/observability/README.md) and
[execution status](../implementation/IMPLEMENTATION_STATUS.md). Its collected
metric/trace/log packet measures actual query failure and recovery. It does not
complete the separate broker-loss or Action leader-handover gates above.
