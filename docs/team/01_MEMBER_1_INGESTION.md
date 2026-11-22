# Member_1 — Distributed Telemetry Ingestion and Simulation

## Distributed-systems ownership

Member_1 owns the distributed edge and reusable test orchestration: concurrent device simulators, the typed gRPC Device Registry, partition-aware Kafka producers, charger command consumers, deterministic fault scenarios, shared contract fixtures, and the CI scenario/load/fault runner. This covers synchronous deadlines as well as producer reliability, backpressure, retry behavior, ordering limits, consumer idempotency, and reproducible integration inputs.

## Planned responsibilities

1. Model EV chargers, smart meters, and solar systems distributed across multiple transformers and publish the versioned topology/capacity fixture used by all standalone profiles.
2. Run thousands of concurrent producers with configurable sampling, batching, and pacing.
3. Route telemetry by `transformer_id` and document ordering guarantees within and across partitions.
4. Handle broker delays with bounded buffering, retry backoff, and visible delivery failures.
5. Generate normal, spike, solar-drop, delayed, duplicate, out-of-order, and offline-device scenarios.
6. Consume commands as distributed device groups and deduplicate repeated command IDs.
7. Publish command results and follow-up telemetry after a rate change.
8. Provide deterministic seeds and metrics for published, retried, dropped, and acknowledged events.
9. Define the Protobuf registry contract, service identity, deadlines, error codes, and safe degraded behavior.
10. Deploy simulator shards and registry replicas as Kubernetes workloads with health probes and resource limits.
11. Provide seeded device-outage, delayed-telemetry, duplicate-command, and missing-acknowledgement fixtures with expected evidence for agent evaluation.
12. Own shared scenario fixtures plus the Locust/Toxiproxy and CI runner that invokes component-owned test commands without implementing their internals.
13. Package and instrument Member_1 workloads with their own Kubernetes resources, RBAC/NetworkPolicy, metrics/traces/logs, dashboard panel, and fault experiment.
14. Install and document the shared OpenTelemetry Collector, Prometheus, Grafana, and Tempo stack; component owners still supply their own instrumentation and panels.

## Boundaries

Member_1 does not calculate transformer totals, select chargers for throttling, or debug/deploy other members' components. The shared runner orchestrates published component commands only. Contract changes require review from the downstream owner rather than private producer assumptions.

## Independent development profile

Use a local Kafka dependency, command fixtures, and mock Registry clients. Member_1 can validate telemetry, Registry deadlines, command idempotency, scenarios, and fault/load tooling without Flink, Action, API, UI, or the agent running.

## Independent distributed demonstration

Run concurrent producers across several partitions, introduce broker delay and duplicate/out-of-order delivery, show bounded backpressure behavior, prove that repeated commands produce one logical device effect, and verify that the device incident fixture exposes only the expected redacted evidence.

## Done when

Outputs validate against shared schemas, scenarios are reproducible, partition routing is correct, failures are observable, and command consumption remains idempotent under replay.
