# Local load experiment — 2026-11-23

## Verdict

**Executed, with failed acceptance retained.** The gradual telemetry experiment
passed the existing aggregation, Action, fenced device, recovery and projection
assertions. Its concurrent HTTP/SSE workload **failed** because the harness reused
a Viewer token beyond the realm's fixed 120-second lifetime. That failed result
has not been overwritten or promoted to PASS. Token renewal and idle SSE handling
were corrected and checked separately; a new concurrent telemetry run with those
fixes remains unverified. This report establishes bounded measurements, not a
maximum sustainable rate, production SLO or all scalability-plan acceptance.

Devices owns scenarios/measurement/Registry/observability; Aggregation Kafka/PyFlink;
Action control/PostgreSQL/Debezium; Projection projection/API/SSE/UI/identity/Copilot.
All observations came from one computer, local free infrastructure, simulated
devices and deterministic mock configuration. No cloud/model-provider calls.

## Configuration and reproducibility

- Host: Apple M5, ARM64, 10 logical CPUs, 17,179,869,184 bytes RAM; Docker Desktop
  engine 29.9.3. Built-in Kubernetes remained disabled.
- Initial VM: 4 CPUs, 6,144 MiB configured, 6,210,510,848 usable bytes. After
  retaining startup failures: 8 CPUs, 10,240 MiB configured, 10,416,844,800 usable
  bytes. Competing kind nodes automatically resumed by the VM restart were
  stopped before measurement. Standalone containers were staged separately.
- Cluster: `smart-grid-kind-dev-load01`, one kind node, Kubernetes v1.34.0,
  Calico v3.31.0, developer topology. The larger VM did **not** turn this into the
  three-broker review topology. Single instances of broker, Connect, PostgreSQL,
  Registry, Action, projection, API, UI, Keycloak and mock Copilot; one JobManager,
  one TaskManager, one aggregation task. No host-loss or replica-scaling claim.
- Domain topics: three partitions, replication factor 1; Debezium heartbeat
  topic: one partition, RF 1. Flink parallelism 1, checkpoint interval 1,000 ms,
  fixed three-device `tx-001` topology, capacity 20 kW, seed 273. All telemetry
  shares the contractual transformer key, so this does not spread one
  transformer's processing over partitions.
- Scheduled reporting: 60 seconds each at 1, 3, 10 samples/device/second
  (3, 9, 30 total events/second). The first stage includes the existing normal,
  overload, intervention and recovery sequence; later stages raise rate on the
  restored system. Business-phase windows are therefore not equal-duration.
- Encoded payloads: **415–536 bytes**, mean **449.692 bytes**, excluding Kafka
  key/headers/protocol overhead, measured from all 1,980 retained readings.
- Locust 2.33.2: three Viewer users, spawn rate 1 user/second, snapshot/SSE tasks,
  200-second configured ceiling; actual report spans also include scenario
  startup and convergence. HTTP tokens were acquired via real local PKCE login.

The ignored private evidence root is `.tools/load-test/`. `preflight-large.json`
records machine/allocation/node-image identity. `load02/nodes.json`, `topics.json`,
`flink.json`, `pods.json` and `runtime-all-pods.json` contain observed versions,
partition/replica counts, image IDs, and container CPU/memory requests/limits.
`load02/report.json` records the Git revision, source SHA-256 values, exact query
commands, kubelet samples, checkpoints and raw Prometheus vectors. Source files
for that failed experiment are preserved in `load02/source/`; later fixes do not
change which code produced its measurements. These private artifacts stay out of
Git. The committed report records the synthetic results without credentials.

Locally built runtime image IDs (not registry-published release digests):

| Image | Observed SHA-256 |
|---|---|
| acceptance | `0a542e8518312856096ba54a89cab7a4f2c500b789c007ff3928332fd60f54da` |
| Registry | `907c04a8f940bf7bfb2055487154b2b73c4cb9400203007fa33cd5fe69af76e7` |
| aggregation | `b17cf7cc9b5b74a34c0604de56c9f9a903d983b8c5cd410b2594cd88d1379879` |
| Action | `ba8186278057472423410e36930978003e84322b8cab76127fcaf6e59e11035c` |
| API/projection | `61559b2ed0b55b96e05a93f7dc8f602c0608f1ba1b096638083491ef84595d1c` |
| UI | `89b4782239b1679709d5df70d1126f0e9d2750abafc325f93f9a6a466d1cc596` |
| Connect | `41bbcf953cf5a509cf1705c488b7c920af70c8cde419864fd99bae53a72b1df0` |
| Copilot | `7937eee8cd8740861f43040a408eee50efa4c1c96d1b343a258857420713ab7f` |

Kafka's observed registry digest is
`sha256:5d96fe7c68db643f4916146176a7587aaf9f91c32fa9da7300f013e3e79298d8`;
PostgreSQL's is `sha256:304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad`;
Keycloak's is `sha256:044a457e04987e1fff756be3d2fa325a4ef420fa356b7034ecc9f1b693c32761`.
All other infrastructure identities and exact requests/limits are in the private
runtime inventory. The simulator uses a 100m CPU/96Mi request and 500m CPU/256Mi
limit. The observed VM allocation is distinct from container limits.

## Measured gradual load and business result

| Stage | Scheduled rate | Delivered readings | Achieved rate over 60 s |
|---|---:|---:|---:|
| 0–60 s | 3 events/s | 174 | 2.9 events/s |
| 60–120 s | 9 events/s | 432 | 7.2 events/s |
| 120–180 s | 30 events/s | 1,374 | 22.9 events/s |

The counts follow successful producer delivery drain; enqueue alone is not counted
as delivery. **The scheduled targets were not achieved.** Overall delivery was
1,980 / 180 = 11 events/s. The projection journal convergence assertion passed.
Thirty-four committed window outputs were collected; event-time tail windows
require later telemetry to advance the watermark and are not claimed closed.

The existing assertions passed without weakening their thresholds:

- Normal load 13.2 kW; overload **20.2 kW** on a **20 kW** transformer (101%).
- Actual simulated EV demand **7.2 → approximately 2.0 kW**, reduction **5.2 kW**;
  complete post-effect load windows show **15.0 kW** (75%), rather than merely
  claiming the command's requested target was applied.
- First overload reading to first observed committed critical load:
  **16.229725 s**. First overload reading to observed PLAN_CREATED:
  **17.311231 s**. First observed critical load to first observed safe post-effect
  load: **14.024140 s**. These include event-time/window/checkpoint/consumer delay;
  they are not pure service-processing times or proposed SLOs.
- Applied-result timestamp minus issued-command timestamp: throttle
  **0.741878 s**, restore **0.789168 s** (two samples, no percentile claim).
- Recovery demand **8.0 kW** while throttled; after restore **13.2 kW**, EV **7.2 kW**.
  Action reached COMPLETED; duplicate command produced ALREADY_APPLIED and exactly
  **two** durable gateway effects remained. Projection replay equality passed.

The original summary used exact equality for a 2.000000000000001 kW sample and
incorrectly left two business measurements null. The raw evidence and original
FAIL report are preserved. `analysis-corrected.json` derives the values above
using the executable contract's floating-point tolerance, with a regression test.
No business gate was altered.

## Streaming, latency, errors and saturation

Twenty kubelet/Flink/Prometheus samples were retained over the scenario/convergence
period; these are point samples, not continuous peaks or a long-duration soak.

| Measurement | Actual observation |
|---|---|
| Observed load delay from window end | 34 samples; p50 10.833629 s, p95 11.759641 s, p99 11.875314 s, nearest-rank |
| Semantic watermark delay | 19 samples, 10,088–11,004 ms |
| Flink per-partition exporter lag | raw -1–30 records; -1 is an exporter limitation, not negative queue depth |
| Control / projection exporter lag | raw -1–4 / -1–1 records |
| Flink backpressure | zero in 40 sampled task series; no downstream saturation demonstrated |
| Completed checkpoints | 445 → 640, **+195** during collection |
| Failed checkpoints | 11 → 11, **no additional failures**; prior startup/recovery failures retained |
| Last-completed checkpoint durations | sampled 16–425 ms; these are snapshots of last completion, not every-checkpoint percentiles |
| Node CPU | sampled 0.711–2.457 cores of 8 |
| Node working set | sampled 6,453–6,748 MiB (6.30–6.59 GiB) |
| Simulator CPU | sampled maximum 0.080 cores, below its 0.5-core limit |

The generator's serial Registry heartbeats, schema validation, delivery drain,
and two up-to-50-ms consumer polls share the reporting loop. Achieved rate below
schedule, low generator CPU, small lag and zero sampled Flink backpressure are
consistent with producer pacing/round-trip overhead, **not a measured Kafka/Flink
capacity ceiling**. Profiling that loop is the next bottleneck experiment. Do not
raise scale claims from 22.9 delivered events/s.

The failed concurrent Locust result is retained separately for each distribution;
HTTP latency includes fast authorization failures and cannot be advertised as a
successful-query SLO:

| Distribution | Count | Failures | p50 / p95 / p99 (ms, Locust buckets) |
|---|---:|---:|---|
| GET overview | 766 | **470** | 6 / 18 / 50 |
| SSE connection request | 249 | **155** | 7 / 21 / 71 |
| SSE first event, successful observations | 94 | 0 | 13 / 72 / 1,800 |
| SSE projection-publication age, delivered events | 1,990 | 0 | 200 / 3,100 / 6,300 |

The 625 failed HTTP/SSE requests resulted from the harness's expired bearer.
SSE event age is publication-to-client receipt and includes replay; it is not
browser-render latency. Early overview responses had no Registry context because
the existing acceptance harness refreshes that projection context at final
convergence. That further limits comparison with populated production queries.
No percentiles from unlike distributions are pooled into a business latency.

The first separate 160-second HTTP baseline (`http-renewal`) renewed authentication
at 61.60 and 123.31 seconds: **267 overview requests, zero failures**. It still
failed overall with 114 SSE read timeouts because the test's three-second read
budget contradicted the API's implemented 15-second idle heartbeat. The client
was corrected to a 20-second idle read budget; no server deadline, authorization,
heartbeat, or assertion was weakened. The corrected separate baseline is retained
under `http-renewal-fixed/`. The corrected 160-second baseline exited 0 with status HTTP_BASELINE_ONLY:
81 populated overview requests and 28 SSE connections had zero failures;
27 heartbeat-only reconnects had zero failures. Authentication renewed at
61.270 and 122.829 seconds. Overview p50/p95/p99 were 13/35/120 ms; SSE connection
p50/p95/p99 were 12/35/78 ms. Only one first-event sample (26 ms) and one delivered
event-age sample (15 ms) occurred; those do not support delivery percentile claims.
Heartbeat-only durations are approximately 15 seconds and are not event latency.
Neither HTTP-only baseline retroactively passes the concurrent telemetry experiment.

## Failed small-budget attempt

The 4-CPU/6-GiB startup was stopped after Keycloak failed to reach readiness and
Flink/entity-operator probes timed out. This was a deliberately interrupted
startup, **not** a completed load experiment or an observed maximum event rate.
The preserved kubelet sample reports 3.015 cores used; CPU "some" PSI avg10
75.03%, memory "some" PSI avg10 22.02%, memory "full" PSI avg10 4.93%, and
2,129,896 cumulative major page faults. These independent pressure/readiness
signals identify local VM resource contention during startup. Major faults are
cumulative and do not alone identify a swap rate or one responsible service.
The larger-allocation run later used more working-set memory than the smaller
VM's usable budget, supporting resource-fit limitations. CPU and memory were
increased together, so this is not a controlled single-variable attribution.

Evidence: `deploy.log`, `startup-events.json`, `startup-pods.json`,
`pressure-resources.json`, `startup-resources.json`, `preflight-load01.json`.
The first larger-budget recovery also returned the earlier Keycloak progress
failure; a subsequent unchanged readiness check passed. Logs remain retained.

## Commands and inspection

Executed commands (from repository root; local-network/Docker authorization was
used where required):

```sh
docker exec smart-grid-local-dev-jobmanager-1 flink list -r
docker exec smart-grid-local-dev-jobmanager-1 flink stop --savepointPath file:///checkpoints/load-preserved 0789faed407bb71676d9185d79626e80
make KIND_NAME=smart-grid-kind-dev-load01 kind-create kind-install kind-deploy
.venv/bin/python -m tools.load_preflight --cluster smart-grid-kind-dev-load01 --output .tools/load-test/preflight-load01.json
.venv/bin/python -m venv .tools/load-venv
.tools/load-venv/bin/python -m pip install locust==2.33.2
.tools/load-venv/bin/python -m pip check
.tools/load-venv/bin/locust -f tools/load_http.py --list
make KIND_NAME=smart-grid-kind-dev-load01 kind-create
.venv/bin/python -c "from tools.kind_local import Cluster; Cluster('dev','smart-grid-kind-dev-load01').apps()"
.venv/bin/python -m tools.load_preflight --cluster smart-grid-kind-dev-load01 --output .tools/load-test/preflight-large.json
.tools/action-venv/bin/python -m tools.load_test --cluster smart-grid-kind-dev-load01 --run load01
.tools/action-venv/bin/python -m tools.load_test --cluster smart-grid-kind-dev-load01 --run load02
.tools/action-venv/bin/python -m tools.load_test --cluster smart-grid-kind-dev-load01 --run http-renewal --http-only
.tools/action-venv/bin/python -m tools.load_test --cluster smart-grid-kind-dev-load01 --run http-renewal-fixed --http-only
```

The savepoint completed at
`file:/checkpoints/load-preserved/savepoint-0789fa-f7200b393ee3`.
The first deployment was terminated at the retained readiness failure; the
larger-profile apps command first failed on the historical progress deadline,
then passed unchanged. `load01` failed before Job creation on adjacent kubectl
JSON documents; the parser was fixed with a regression test. `load02` exited 1
with the business checks passing and HTTP failures retained. The first HTTP-only
run exited 1 on idle SSE errors. Dependency checks and Locust user discovery passed.
Docker lifecycle commands used supported `docker desktop stop/start` and local
settings changed only Cpus/MemoryMiB while preserving KubernetesEnabled=false;
the original allocation values are retained in `docker-allocation-before.json`.

Inspect retained evidence without printing credentials:

```sh
.venv/bin/python -m json.tool .tools/load-test/load02/report.json
.venv/bin/python -m json.tool .tools/load-test/load02/analysis-corrected.json
.venv/bin/python -m json.tool .tools/load-test/http-renewal-fixed/report.json
```

For a new **concurrent** experiment, use the method in this report with a fresh dedicated
cluster and a new output directory. Do not delete the completed Job/control/edge
history to manufacture a rerun. The final revised concurrent runner has not yet
been demonstrated end to end. requirements-load.txt pins the resolved macOS
Python environment; hashed cross-platform dependency verification remains open.

## Limitations and next experiment

No 10/100-device topology, partition/parallelism scale-out, API-replica scale-out,
autoscaling, broker loss, leader handover, mock-agent concurrency, provider outage,
long soak, host loss, or browser-render load acceptance is claimed. ADR-027/034
explicitly fix the aggregation to one task and 100,000 unique inputs. Changing
that topology or global semantics is separate work, not a load-test shortcut.
No unobserved error/DLQ/retry/loss metric is asserted zero. Native lag includes
negative exporter artifacts, and the finite 5-second/10-second event-time tail is
not force-closed. Authentication and client bugs are distinguished from service
saturation. Next: a fresh concurrent run of the corrected runner, plus producer
pacing profiling before raising the offered rate or claiming consumer capacity.

Final local checks: `.tools/action-venv/bin/python -m pytest -q tests/test_kind_local.py tests/test_integration_evidence.py tests/test_load_test.py tests/test_load_preflight.py`
reported 15 passed, 2 skipped. Skips are existing opt-in kind fixture checks in that
environment, not load successes. `git diff --check` passed. No contracts changed.

## Final restoration

Executed `make KIND_NAME=smart-grid-kind-dev-load01 kind-stop` successfully,
retaining its node/PVCs and evidence. Used `docker desktop stop --timeout 60`,
restored the saved Cpus=4/MemoryMiB=6144/KubernetesEnabled=false settings, then
`docker desktop start --timeout 60`. Automatically resumed acceptance nodes were
stopped again with `docker stop --timeout 10` before restoring the standalone
stack. Executed `bash .tools/load-test/restore.sh`: exit 0. It restores from the
newly captured canonical savepoint rather than an older checkpoint. Flink reported
`aggregation-review02` RUNNING as `c0a248ed31c5a5f55b4d5f5dd83d9d1f`.
`restored-allocation.txt` confirms **4 CPUs / 6,210,510,848 usable bytes**;
`standalone-restored.log` retains the actual result. The original standalone
containers and `smart-grid-dev` node are restored; the load-test node is stopped.
No cluster, PVC, database, or retained result was deleted. A separate rerun must
stage the standalone stack again and deliberately select the measured larger VM
allocation. `tests/test_preparation_docs.py`: 5 passed on final documentation.
