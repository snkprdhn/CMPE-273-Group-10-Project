# CMPE 273 Curriculum Alignment

This matrix maps course concepts to planned project behavior and evidence. Canvas remains the authoritative grading source; the matrix must be updated when the official rubric is released.

| Course concept | Planned project feature | Required evidence |
|---|---|---|
| Distributed boundaries and partial failure | Independent simulator, registry, Kafka/Flink, action, projection, UI, and data services | Failure-domain diagram and component outage behavior |
| Containers and runtime isolation | Separate images/workloads, state/config boundaries, health endpoints | Kubernetes workload inventory and probe results |
| REST and gRPC | REST/SSE browser boundary; gRPC Device Registry | Contract comparison, deadline and failure trace |
| Architecture styles | Event-driven core, CQRS projections, owned data | Architecture decision and integration-boundary diagram |
| Messaging | Partitioned topics, consumer groups, DLQ, replay, outbox/inbox | Duplicate/replay and poison-message evidence |
| Discovery and load balancing | Kubernetes Services, DNS, ready endpoints, multiple replicas | Readiness and pod-replacement experiment |
| Time and ordering | Event time, watermarks, sequence gaps, revisions, fencing epochs | Late-event and stale-leader tests |
| Coordination/consensus | Kubernetes Lease and consensus-backed fencing | Leader failover and old-epoch rejection |
| Replication/consistency | Kafka replication, optimistic database versions, and per-operation policy | Broker-loss and concurrent-policy-update experiment; database replica is stretch |
| Distributed transactions | Control saga, transactional outbox, idempotent inbox | Crash-window recovery experiments |
| Fault tolerance | Deadlines, bounded retries, breaker, bulkheads, backpressure | Before/during/after fault evidence |
| Cloud-native runtime | Kubernetes reconciliation, probes, scaling, security policies | Deployment and scaling demonstration |
| Observability | OTel traces, metrics, logs, SLIs/SLOs | Correlated incident investigation and error budget |
| Security | OIDC/RBAC, ServiceAccounts, NetworkPolicies, Secrets, and server-side authorization | Positive and negative authorization tests; Kafka ACL/TLS is stretch hardening |
| Agentic systems | Read-only Grid Incident Investigator with typed tools, provider adapter, bounded state, evidence validation, audit trail, and human verification | Tool-choice/evidence evaluation, prompt-injection denial, provider-outage behavior, and one live investigation; never part of grid control correctness |
| Architecture defense | ADRs, tradeoffs, non-goals, measured limitations | Each member defends one decision and one failure result |

## Final-review checklist

The review package must identify what was implemented versus proposed, include reproducible commands, name the measurement environment, show individual contributions, and avoid claiming high availability, exactly-once effects, or production security beyond the evidence.
