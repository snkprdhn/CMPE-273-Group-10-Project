# Acceptance and Final Demonstration Plan

## Proposed success criteria

The final submission must distinguish proposed targets from measured results. Measurements include the demo machine, configuration, event rate, duration, and raw evidence location.

| Area | Acceptance evidence |
|---|---|
| Correctness | Known fixture produces expected transformer totals and revisions |
| Scale | 10,000 simulated devices and 5,000 events/second for 10 minutes on the agreed machine |
| Detection | P95 telemetry-to-critical-state latency under 8 seconds |
| Control | One logical throttle effect per command ID and documented acknowledgement |
| Recovery | Aggregator restart resumes from checkpoint without double counting |
| Late data | In-policy late event corrects a revision; very-late event is audited only |
| UI | Live overload and recovery visible without refresh; stale/reconnect state works |
| Explainability | Action detail shows trigger, policy, candidates, commands, and outcomes |
| Kubernetes | Service discovery, readiness removal, pod reconciliation, and multi-replica routing are demonstrated |
| Coordination | Leader failover completes and an old fencing epoch is rejected |
| Consistency | Kafka replication behavior and optimistic policy-version rejection are demonstrated |
| Observability | One incident is explained with a metric, trace, and correlated log sequence |
| Security | Required positive and negative role/service-identity tests pass |
| Agentic investigation | Seeded incident selects appropriate read-only tools, cites valid evidence, labels unknowns, and remains isolated from control |
| Agent resilience | Provider timeout/mock mode is explicit and does not affect the deterministic grid workflow |

## Demo sequence

1. Show Kubernetes workloads, ready endpoints, service discovery, observability, and the UI in a normal state.
2. Start the deterministic evening-spike scenario.
3. Watch selected transformers move from normal to warning and critical.
4. Open a critical transformer and explain its load breakdown.
5. Show the action plan, selected chargers, commands, and acknowledgements.
6. Confirm reduced EV demand and transition through recovering to normal.
7. Inject a late event and show the revised historical window.
8. Kill the Action leader, show lease transfer, and reject a delayed old-epoch command.
9. Restart stream processing and show checkpoint recovery and UI staleness/recovery.
10. Fail readiness or delete an API pod and observe Kubernetes routing/reconciliation.
11. Submit a stale policy version and show the conflicting update is rejected.
12. Replay a command and show that the charger reports `ALREADY_APPLIED`.
13. Diagnose one injected fault using a metric, trace, and correlated structured logs.
14. Ask the Grid Incident Investigator why the injected incident occurred and open its cited load, action, metric, and trace evidence.
15. Attempt an unauthorized/write tool request and show that the tool gateway denies it.
16. Disable the external provider, show explicit agent degradation or mock mode, and confirm the core control loop remains healthy.
17. Show SLO/error-budget calculations, agent evaluation results, and the final measured-results table.

## Required artifacts

- Reproducible scenario configuration and seed
- Expected fixture totals
- Automated test report
- Load-test report and machine specifications
- Screenshots or short recording
- CI result
- Kubernetes resource/health evidence
- One correlated metric/trace/log incident packet
- Kafka replication, optimistic-consistency, and leader-failover evidence
- Known limitations and deviations from design
- Agent evaluation report, tool-call audit, cited evidence packet, prompt-injection test, and mock-provider result

## Presentation ownership

Member_1 explains distributed producers, gRPC behavior, backpressure, command consumers, and device-related agent fixtures. Member_2 explains Kafka/Flink, event time, replication, checkpoint recovery, and stream-health evidence. Member_3 explains leader fencing, saga/outbox/inbox, consistency, idempotent effects, and action-history evidence. Member_4 explains Kubernetes discovery/reconciliation, projections, the Copilot Service, OpenTelemetry/SLOs, security integration, the UI, and final results.
