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

## Execution status — 2026-11-23

See [the current report](LOAD_TEST_REPORT.md). The gradual telemetry experiment and separate HTTP/SSE baselines ran;
**the concurrent experiment failed on harness token expiry**. That failed result
is retained; a full concurrent rerun of the corrected harness remains unverified. Do not interpret that packet or prior
smoke results as sustained throughput evidence. ADR-027/ADR-034 fix current
aggregation to one bounded task; partition/parallelism scale-out in this plan is
not an implemented runtime capability. Start rate experiments on the existing
three-device topology, and use fresh consistent topology/run state for device
count experiments. Locust and opt-in simulator rate scheduling are now implemented below.

## Implemented bounded rate experiment

`tools.load_test` reuses the existing Member_1 domain simulator and preserves
its actual control/edge/aggregation assertions. `GRID_LOAD_STAGES=1` opts the
one-shot kind scenario into three 60-second reporting stages: 1, 3, and 10
samples/device/second, or 3, 9, and 30 events/second for the fixed three-device
topology. The first stage includes the existing normal/overload/intervention/
recovery sequence; later stages increase reporting rate on the restored system.
Thus these are equal rate-stage windows, **not equal business-phase windows**.
The producer records actual sample times after delivery drain, counts and encoded
payload sizes. Slow producers do not silently claim the scheduled rate. Business
outcomes remain checked by `tools.integration.evaluate`.

Locust 2.33.2 runs three real Viewer clients via PKCE-authenticated loopback
port-forwards. Snapshot requests and bounded SSE/reconnect tasks produce CSV
latency distributions and failures. SSE event age uses projection publication
time; it does not measure browser rendering. Cursor regression and disconnect
exceptions are failures. Raw kubelet summaries, Flink checkpoints and selected
Prometheus signals accompany the scenario evidence. Missing signals are unknown.

From a prepared, freshly deployed **dedicated** development cluster:

```sh
.venv/bin/python -m venv .tools/load-venv
.tools/load-venv/bin/python -m pip install -r requirements-load.txt
.tools/member3-venv/bin/python -m tools.load_test --cluster smart-grid-kind-dev-load01 --run load01
.venv/bin/python -m json.tool .tools/load-test/load01/report.json
```

The requirements file pins the resolved local macOS environment; hashed
cross-platform artifact locking remains unverified. The command refuses an
existing run directory and existing simulator Job. Do not clear authoritative
history to rerun it. Use a fresh dedicated cluster; retain failed evidence.
It requires the separately prepared dashboard test venv/Chromium for existing
PKCE login tooling. Credentials travel in a mode-0600 temporary file removed
on exit, never in CSV or command arguments. Existing global context is unchanged.
`BUSINESS_PASS_PARTIAL_MEASUREMENTS` is a bounded slice result, not completion
of all scaling/agent experiments or an achieved production SLO.

The client renews its real Viewer login every 60 seconds and atomically reloads
the token per request. Its 20-second idle SSE read timeout accommodates the
implemented 15-second heartbeat. `--http-only` runs a separate 160-second
populated read baseline with no telemetry or capacity claim. See the report for
retained token-expiry/client-timeout failures and the corrected baseline result.
