# Member_2 — Kafka and Stateful Stream Processing

## Distributed-systems ownership

Member_2 owns Kafka topic topology and the distributed Flink aggregation job, including parallel keyed state, event-time processing, checkpoints, replay, and scaling behavior.

## Planned responsibilities

1. Define topic partitions, replication assumptions, retention, and consumer groups.
2. Validate telemetry and route invalid records without blocking healthy partitions.
3. Repartition by transformer and calculate net load using parallel keyed state.
4. Handle out-of-order events with event timestamps, watermarks, allowed lateness, and revisions.
5. Publish versioned transformer-load events with completeness metadata.
6. Configure checkpoints and restore state and offsets following task failure.
7. Measure throughput, consumer lag, late-event rate, checkpoint duration, and end-to-end latency.
8. Demonstrate rescaling or explain the state redistribution plan if the local environment cannot support it.
9. Configure Strimzi, the Flink Kubernetes Operator, and Apicurio integration; own their distributed settings without modifying or recreating the operators.
10. Define bounded stream-health evidence for the agent: lag, watermark, checkpoint, backpressure, freshness, and truncation metadata without arbitrary PromQL access.

## Boundaries

Member_2 reports grid load and data quality but does not select chargers or create control commands. The output contract remains independent of Flink internals.

## Independent distributed demonstration

Process a partitioned fixture in parallel, inject late records, verify corrected windows, restart a task from checkpoint, show that replay does not double-count accepted readings, and verify that the agent-facing summary distinguishes grid overload from processing staleness.

## Done when

Expected totals match, event-time policy is verified, recovery works from a committed checkpoint, partition lag is visible, and consumers receive stable load contracts.
