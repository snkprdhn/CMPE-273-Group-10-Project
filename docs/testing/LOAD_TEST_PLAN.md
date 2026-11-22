# Load and Scalability Test Plan

## Proposed workload

Start with 10 devices/one transformer in development and 100 devices/three transformers in the larger review profile. The earlier 1,000–10,000-device and 5,000-events/second goals are optional measured experiments, not prerequisites on the inspected 16 GiB laptop. Record the exact cluster, CPU, memory, Kubernetes version, partitions, replicas, Flink parallelism, payload size, and duration.

Locust supplies Python-authored HTTP load and reporting. Streaming scenarios must explicitly measure SSE first-event delay, event latency, cursor continuity, reconnects, and disconnect outcomes; ordinary HTTP request duration is not sufficient. Locust supplies the load runner, while the project simulators supply domain-correct Kafka telemetry. Testcontainers supplies reproducible integration dependencies where a full Kubernetes test is unnecessary. Member_1 owns the reusable runner and scenario parameters; every component owner supplies their command, metrics, limits, assertions, and bottleneck analysis. The team will write scenarios and assertions, not a new load runner or container harness.

## Measurements

Measure accepted/published rate, Kafka lag, watermark delay, checkpoint duration, processing latency, decision latency, database writes, API p50/p95/p99, SSE delivery delay, resource saturation, errors, retries, and dropped work. For the agent, separately record concurrent investigations, model/tool latency, provider throttling, token use, completion/partial/error outcomes, and whether agent load changes control-path SLIs. Compare equal-duration baseline, steady-state, overload, and recovery windows.

## Scaling experiments

1. Increase producers without changing consumers.
2. Increase Kafka partitions and Flink parallelism together.
3. Add projection/API replicas and observe connection/request distribution.
4. Scale from Kafka lag only after documenting stabilization delay.
5. Identify the first bottleneck and confirm it with a second signal.
6. Run bounded concurrent mock-provider investigations and verify that their bulkhead prevents API, projection, or control starvation.

Targets in the README remain proposed until this report supplies measured results. Percentiles are computed from compatible distributions, not averages of per-instance percentiles.
