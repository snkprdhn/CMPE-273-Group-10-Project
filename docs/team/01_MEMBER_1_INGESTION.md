# Member_1 — Distributed Telemetry Ingestion and Simulation

## Distributed-systems ownership

Member_1 owns the distributed edge of the platform: concurrent device simulators, the typed gRPC Device Registry, partition-aware Kafka producers, charger command consumers, and fault scenarios. This covers synchronous deadlines as well as producer reliability, backpressure, retry behavior, ordering limits, and consumer idempotency.

## Planned responsibilities

1. Model EV chargers, smart meters, and solar systems distributed across multiple transformers.
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

## Boundaries

Member_1 does not calculate transformer totals or select chargers for throttling. Contract changes require review from the downstream owner rather than private producer assumptions.

## Independent distributed demonstration

Run concurrent producers across several partitions, introduce broker delay and duplicate/out-of-order delivery, show bounded backpressure behavior, prove that repeated commands produce one logical device effect, and verify that the device incident fixture exposes only the expected redacted evidence.

## Done when

Outputs validate against shared schemas, scenarios are reproducible, partition routing is correct, failures are observable, and command consumption remains idempotent under replay.
