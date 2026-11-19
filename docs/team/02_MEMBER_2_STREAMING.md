# Member_2 — Kafka and Stateful Stream Processing

## Distributed-systems ownership

Member_2 owns Kafka topic topology and the distributed Flink aggregation job, including parallel keyed state, event-time processing, checkpoints, replay, and scaling behavior.

## Planned responsibilities

1. Define topic partitions, replication assumptions, retention, and consumer groups.
2. Validate telemetry and route invalid records without blocking healthy partitions.
3. Repartition by transformer and calculate net load using parallel keyed state.
4. Load the versioned topology/capacity fixture locally; do not call another member's service for window calculation.
5. Handle out-of-order events with event timestamps, watermarks, allowed lateness, and revisions.
6. Publish versioned transformer-load events with completeness metadata.
7. Configure checkpoints and restore state and offsets following task failure.
8. Measure throughput, consumer lag, late-event rate, checkpoint duration, and end-to-end latency.
9. Demonstrate rescaling or explain the state redistribution plan if the local environment cannot support it.
10. Configure Strimzi and the Flink Kubernetes Operator, and own JSON Schema/fixture compatibility checks without operating a separate schema registry.
11. Define bounded stream-health evidence for the agent: lag, watermark, checkpoint, backpressure, freshness, and truncation metadata without arbitrary PromQL access.
12. Package and instrument Member_2 workloads with their own Kubernetes resources, RBAC/NetworkPolicy, metrics/traces/logs, dashboard panel, and broker/task fault experiment.

## Boundaries

Member_2 reports grid load and data quality but does not select chargers, create control commands, or write the shared PostgreSQL database. Load history is persisted by Member_4's event projection. The output contract remains independent of Flink internals.

## Independent development profile

Use telemetry JSON fixtures, a local Kafka dependency, and the Flink test/runtime profile. Member_2 can validate window totals, duplicates, late revisions, DLQ behavior, checkpoints, replay, and rescaling without Member_1 simulators or Members 3–4 services.

## Independent distributed demonstration

Process a partitioned fixture in parallel, inject late records, verify corrected windows, restart a task from checkpoint, show that replay does not double-count accepted readings, and verify that the agent-facing summary distinguishes grid overload from processing staleness.

## Done when

Expected totals match, event-time policy is verified, recovery works from a committed checkpoint, partition lag is visible, and consumers receive stable load contracts.
