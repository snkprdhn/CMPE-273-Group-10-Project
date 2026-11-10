# Load and Scalability Test Plan

## Proposed workload

Start with 1,000 devices, then increase toward 10,000 devices and 5,000 events/second across at least 20 transformers. Record the exact cluster, CPU, memory, Kubernetes version, partitions, replicas, Flink parallelism, payload size, and duration.

k6 supplies API/SSE load and threshold reporting, while the project simulators supply domain-correct Kafka telemetry. Testcontainers supplies reproducible integration dependencies where a full Kubernetes test is unnecessary. The team will write scenarios and assertions, not a new load runner or container harness.

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
