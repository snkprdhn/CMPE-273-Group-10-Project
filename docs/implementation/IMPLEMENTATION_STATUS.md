# Implementation status

Updated **2026-11-23 (America/Los_Angeles)** for the local observability milestone. Member_1 simulation/Registry, Member_2 aggregation, Member_3 standalone Action domain, the local CDC/Lease/fenced gateway boundary and Member_4 standalone projections/API and NiceGUI dashboard plus local OIDC are implemented. Member_1 retains simulation/Registry/observability, Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium and Member_4 projection/API/UI/identity/Copilot ownership. Historical evidence is preserved below.

## Current milestone — local observability

The Collector, Prometheus, Grafana, and Tempo ran on local Docker Desktop kind.
Five dashboards are provisioned. Explicit instrumentation covers Kafka, gRPC,
HTTP, SQL, Action/Lease, and SSE, with durable transport context for CDC and
projection events. Public v1 contracts remain unchanged. See
[ADR-035](../decisions/ADR-035-local-observability.md) and the
[runbook](../../infra/observability/README.md).

The seeded incident gate passed using the independent loopback API, local
Compose PostgreSQL, and kind collection. An exclusive lock on the run-owned
projection metadata caused one operator snapshot request to return HTTP 503.
Prometheus recorded the error. Tempo recorded the SQL and HTTP error spans,
and stderr contained the matching trace and span IDs. Three sub-second reads
confirmed recovery 3.330 seconds after injection. Trace
`90c170be453d4ca2b544cb045e53131d` and the evidence packet are retained under
`.tools/observability-incidents/obs01/`.

The repeat run `obs04` passed against kind PostgreSQL and kind collection after
monitoring settled. The independent loopback API returned one HTTP 503.
Prometheus recorded one error and nine successful queries. Tempo and the
structured logs matched trace `e473510bd1ee4ee3bf735c1eef4d1f5d`. Recovery took
**3.338 seconds** from injection and **0.074 seconds** after rollback. The
three recovery reads took 28, 22 and 24 ms. The packet and collection image IDs
are retained under `.tools/observability-incidents/obs04/`.

| Executed command | Result |
|---|---|
| `.venv/bin/python -m tools.observability install --context kind-smart-grid-dev` | All four stack components rolled out and five dashboards were provisioned. The first Collector startup exposed invalid retry settings, corrected in the chart values. |
| `PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .venv/bin/python -m tools.observability incident --context kind-smart-grid-dev --run obs01` | PASS, real collected metric, trace, correlated logs, HTTP 503 business impact and measured recovery. |
| `.venv/bin/python -m tools.observability incident --context kind-smart-grid-kind-dev-obs --kubeconfig .tools/kind-dev.kubeconfig --run obs04` | PASS with kind PostgreSQL and the Collector, Prometheus and Tempo in kind. The earlier `obs02` and `obs03` attempts failed on port-forward readiness and evidence availability, respectively, while the monitoring pods were restarting. |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/dashboard/test_client.py tests/dashboard/test_presentation.py tests/test_kind_local.py tests/test_integration_evidence.py tests/test_preparation_docs.py` | 33 passed on the resumed verification. |
| `make observability-test observability-render` | 8 tests passed and all three pinned charts rendered. |
| `make contracts BASE_REF=HEAD` | 214 passed. |
| `make member1-test member2-test` | 16 and 43 passed respectively. |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | 86 passed, three opt-in checks skipped. |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_api.py tests/member4/test_projection.py tests/member4/test_review.py tests/member4/test_dashboard_boundary.py` | 36 passed. |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/dashboard/test_client.py tests/dashboard/test_presentation.py tests/test_kind_local.py` | 25 passed. |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/test_kind_local.py tests/test_integration_evidence.py tests/test_preparation_docs.py` | 16 passed. |
| `make KIND_NAME=smart-grid-kind-dev-obs kind-create kind-install kind-deploy` | First attempt installed the observability stack and started PostgreSQL, Kafka/exporter, CDC and Flink. Application rollout timed out with 3975m CPU reserved on a 4-core node. The developer overlay now lowers Flink requests while retaining its CPU limits. |

The retry with `make KIND_NAME=smart-grid-kind-dev-obs kind-install kind-deploy kind-smoke`
built and loaded the instrumented images, applied database migrations and
confirmed Debezium and Flink were running. Both Flink pods scheduled with the
new CPU requests. Kubernetes admitted the six app pods, but container startup
stalled while the VM and Kubernetes API were slow. The Registry readiness
gate prevented the smoke test from running. The Prometheus scrape check
found that Strimzi exposes the exporter as a pod without a Service. The chart
now discovers exporter pods on port 9404. Grafana provisioning sidecars also
hit their 64 MiB limits, now revised to 192 MiB. Final live Kafka/Flink panel
population and the instrumented Kafka-to-SSE closed loop remain unverified.
The final Helm retry for Kafka discovery and the Grafana sidecar adjustment
also timed out waiting for readiness. See the private deployment and incident
logs under `.tools/`.

`make KIND_NAME=smart-grid-kind-dev-obs kind-stop` stopped the acceptance node
using its Docker fallback after the Kubernetes shutdown timed out. The
`bash .tools/observability-restore-standalone.sh` restore restarted the
previous containers and returned Flink job `member2-review02` to RUNNING
as `b9a26e187ac5c6574dc39c9f4eb539e3` from canonical savepoint
`file:/checkpoints/observability-preserved/savepoint-68bf12-b7078937035d`.
The acceptance volumes were retained.

The proposed query SLO is displayed as a measured good fraction. Other SLOs,
sustained capacity, broker loss and leader handover require separate runs.
The next recommended milestone is the review-profile broker-loss and Action
leader handover gate, followed by the bounded deterministic Copilot.

### Readiness follow-up — 2026-11-23

Reopened the preserved developer cluster with
`make KIND_NAME=smart-grid-kind-dev-obs kind-create`. Recovered Helm's
interrupted monitoring revision with `helm rollback monitoring 3` in the
`observability` namespace, then ran
`.venv/bin/python -m tools.observability install --context kind-smart-grid-kind-dev-obs --kubeconfig .tools/kind-dev.kubeconfig`.
Monitoring deployed revision 7. Grafana reported 3/3 ready with zero restarts
at inspection and its authenticated dashboard API returned all five dashboards.

Changed deploy to apply and wait for application deployments in dependency
order. All six deployments rolled out using
`.venv/bin/python -c "from tools.kind_local import Cluster; Cluster('dev','smart-grid-kind-dev-obs').apps()"`.
Registry initially timed out connecting to PostgreSQL. A probe using the
Registry NetworkPolicy labels resolved and connected to the PostgreSQL Service,
and the Registry recovered on restart. The smoke Flink readiness wait is now
180 seconds to allow the actual job to recover after a node restart.

`make KIND_NAME=smart-grid-kind-dev-obs kind-smoke` first timed out at the old
15-second Flink gate. The retry passed Debezium, Flink, application readiness,
and Lease RBAC. It failed at the 150-second readiness wait for the scheduled
NetworkPolicy probe pod `grid-policy-outside` and did not launch the simulator
through the full smoke harness. The probe was cleaned up. The official report
is `.tools/kind-dev/report.json` and the retry log is
`.tools/observability-resume-smoke.log`.

A separate diagnostic `kubectl apply -k infra/kind/dev -l grid-stage=smoke`
created the simulator Job with the existing platform policies in effect.
The subsequent Job/log requests failed with control-plane TLS or watch errors,
so this attempt supplies no Kafka-to-SSE result. Kubelet logged housekeeping
delays up to 14.5 seconds and exec probe timeouts during this run.

Prometheus's active target API reported the Kafka exporter, both Flink
endpoints and both Collector endpoints up. Grafana's authenticated datasource
query returned an empty consumer-lag vector before scenario data. The final
Grafana datasource request returned HTTP 502. Live panel
population and the full smoke remain open. Local checks passed:
`make observability-test observability-render` (8 tests plus chart render),
`.tools/dashboard-test-venv/bin/python -m pytest -q tests/test_kind_local.py`
(8 tests), and `git diff --check`.

The acceptance node was stopped using the Docker fallback after Kubernetes
shutdown timed out. `bash .tools/observability-resume-restore.sh` restarted the
standalone containers. Flink restored from
`file:/checkpoints/observability-resume/savepoint-b9a26e-79d37c0d34ac`
and reported `member2-review02` RUNNING as
`7c3792adc192073d77d4d7b11f1c0831`. The next verification should use the
documented larger local review VM budget to isolate the recurring node
startup stalls from application behavior.

### Larger-budget acceptance — 2026-11-23

**Full Kafka-to-SSE smoke passed** on the one-broker developer topology with
the documented review VM allocation of 8 CPUs and 10,240 MiB. Docker reported
8 CPUs and 10,416,844,800 usable bytes. This run used a fresh kind cluster
named `smart-grid-kind-dev-obsnew` with no competing nodes.

The preserved cluster's first simulation exposed a real PyFlink worker
failure. Beam's replacement for Python `print` rejected the logger's
`flush=True` argument. Structured logging now writes and flushes stderr
directly. The harness also compresses and chunks its final evidence so
container logs can carry the whole report. Existing clusters refresh the
bounded Lease API egress allowlist when the node restarts.

| Executed command | Result |
|---|---|
| `make KIND_NAME=smart-grid-kind-dev-obsnew kind-create kind-install kind-deploy kind-smoke` | Exit 0. All four member gates passed, as did Calico ingress/egress, Action Lease RBAC, Keycloak Viewer login, and matching REST, SSE, and UI action assertions. |
| `PYTHONPATH=. .venv/bin/python .tools/observability-large-panels.py` | Authenticated Grafana API returned all five dashboard definitions and live samples for all six provisioned streaming queries. Grafana was 3/3 ready with zero restarts on the fresh rollout. |
| `make observability-test` | 8 passed. |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/test_kind_local.py tests/test_integration_evidence.py` | 11 passed. |
| `git diff --check` | Passed. |

The smoke finished at 14:46:52 PST. It retained 14 load samples, 342 completed
checkpoints, matching projection state, and two device effects. Commands
throttled the charger to 2 kW, confirmed that replay did not apply a second
effect, and restored 7.2 kW. The report is
`.tools/observability-large-fresh-report.json` and the deployment and smoke
log is `.tools/observability-large-fresh.log`.

Grafana samples taken during simulation included Flink consumer lag of one
record, semantic watermark delay of 11,032 ms, 285 completed and six failed
checkpoints, a 190 ms last checkpoint, and zero backpressure in the sampled
series. The exporter also returned negative lag aggregates for Action and
Projection. Those raw values are recorded as an exporter limitation and should
not be interpreted as queue depth. The queries and raw samples are retained in
`.tools/observability-large-fresh-panels.log`. These are point samples from a
short run. Sustained capacity, broker-loss and leadership-failover verification
remain separate gates.

After capture, `make KIND_NAME=smart-grid-kind-dev-obsnew kind-stop` stopped
the acceptance node with volumes retained. Docker Desktop returned to 4 CPUs
and 6,144 MiB configured, and
`bash .tools/observability-large-restore.sh` restored the original services.
`member2-review02` reported RUNNING as `0789faed407bb71676d9185d79626e80`
from canonical savepoint
`file:/checkpoints/observability-large/savepoint-7c3792-507019903243`.

## Completed milestones

| Milestone | Result | Evidence |
|---|---|---|
| Local component integration | Four boundary assertions and deterministic closed loop pass on host/Compose with the kind Lease | [Runbook](LOCAL_INTEGRATION.md), executed evidence below |
| Repository/machine preparation | Accepted small Python/container stack, ADRs, manual prerequisites, resource profiles and root conventions | [Matrix](VERSION_MATRIX.md), [prerequisites](LOCAL_PREREQUISITES.md), [preparation evidence](PREPARATION_VERIFICATION.md) |
| Existing local prerequisites/empty cluster | Managed Python 3.12.9, isolated preparation venv, pinned native tools, Docker Desktop with built-in Kubernetes disabled, empty kind Kubernetes 1.34.0 | Read-only inventory reconfirmed below; earlier setup commands/digests retained in preparation evidence |
| Minimum local development infrastructure | One Kafka broker, PostgreSQL primary, on-demand Toxiproxy; isolated stores, real smoke checks, restart persistence, guarded reset | [Compose runbook](../../infra/compose/README.md), executed acceptance below |
| Executable contract bundle v1 | Eight Kafka events; canonical topology, REST/internal/SSE and seven agent-tool/evidence/output schemas; Registry proto; versioned examples and fixed golden scenarios; compatibility tests and Ubuntu CI workflow | [Bundle](../../schemas/README.md), [ADR-025](../decisions/ADR-025-executable-contract-freeze.md), `make contracts` |
| Member_1 standalone edge and Registry | Deterministic three-device scenario; Kafka telemetry/commands/results; durable simulated effects/dedup/fences; PostgreSQL gRPC Registry; operational endpoints and independent ARM64 images | [Runbook](../../member1/README.md), [ADR-026](../decisions/ADR-026-member1-standalone-state.md), executed evidence below |
| Member_2 standalone Kafka/PyFlink aggregation | Native ARM64 source-built runtime, 56-package hash lock; fixed golden telemetry, transactional load/DLQ/late output; checkpoint/replay proof with an open transaction at TaskManager crash | [Runbook](../../member2/README.md), [ADR-027](../decisions/ADR-027-member2-standalone-aggregation.md), evidence below |
| Outbox delivery and fencing | Real Debezium CDC, kind Lease and durable epochs, atomic fenced simulated gateway | [ADR-029](../decisions/ADR-029-outbox-and-fencing.md), [runbook](../../infra/member3/README.md), failure evidence below |
| Member_3 Action domain and durable saga | PostgreSQL transactional state/inbox/outbox, deterministic fixture allocation, retry/timeout/hold/restore, optimistic versions and restart recovery | [Runbook](../../member3/README.md), [ADR-028](../decisions/ADR-028-member3-action-domain.md), 42 PostgreSQL tests and demo below |
| Member_4 local OIDC / authorization | Keycloak bootstrap/recreation, PKCE browser sessions/renewal/logout, API/tool role checks and audit | [Identity runbook](../../identity/README.md), [ADR-032](../decisions/ADR-032-member4-local-identity.md), executed evidence below |
| Member_4 operator dashboard | Responsive NiceGUI REST/SSE console, contract-checked fixture mode, accessible states, confirmations and scoped investigation entry points | [Runbook](../../dashboard/README.md), [ADR-031](../decisions/ADR-031-member4-operator-dashboard.md), executed acceptance below |
| Member_4 projections and API | Replay-safe Kafka/SQL projections, migrations/rebuild, REST snapshots/filters/pages, durable cross-replica SSE/reset, freshness and internal contract mocks | [Runbook](../../member4/README.md), [ADR-030](../decisions/ADR-030-member4-projections-api.md), executed evidence below |

## Previous milestone — local kind deployment

**The reproducible local kind deployment milestone is complete.** Both developer
and three-broker review profiles passed all four closed-loop gates, live Calico
ingress/egress enforcement, scoped Action Lease RBAC, Keycloak Viewer PKCE login,
matching REST/SSE payloads and the Chromium dashboard assertion.

| Profile | VM budget | Node working set, six samples | CPU, six samples | Checkpoints | Final charger |
|---|---|---|---|---|---|
| Developer | 4 CPUs / 6144 MiB configured | 4680–4857 MiB | 656–3162m | 165 | 7.2 kW, two effects |
| Review | 8 CPUs / 10240 MiB configured | 6956–7296 MiB | 1084–1614m | 178 | 7.2 kW, two effects |

These are sampled node working sets and CPU utilization during acceptance,
including Kubernetes and operator processes. They do not establish sustained
capacity or physical host availability.

### Executed commands and results

The commands ran from `.tools/kind-finish/checkout`, a fresh local clone of
`da2848557ba88d89f53ad93a919e3236102f0670` with the uncommitted milestone source
changes copied in. Generated bindings, credentials, application data and
kubeconfigs started fresh. The checkout reused preparation binaries, locked
contract/browser environments and verified download caches. This verifies the
proposed source tree before committing it.

| Command | Result |
|---|---|
| `make KIND_NAME=smart-grid-kind-dev-clean kind-create kind-install kind-deploy` | PASS. Fresh one-broker KRaft, nine managed topics, persistent PostgreSQL, Debezium streaming task, operator-managed PyFlink, Registry, Action, projection, API, UI and Keycloak rolled out. |
| `make KIND_NAME=smart-grid-kind-dev-clean kind-smoke` | PASS at `2026-11-23T04:23:48.390275Z`. Action `1f07468a-b166-51e7-ae07-635b3c5d150c` completed. |
| `make KIND_NAME=smart-grid-kind-dev-clean kind-measure kind-inspect` | PASS. Complete dev measurement recorded before review. |
| `make KIND_NAME=smart-grid-kind-dev-clean kind-stop` | Stopped the dev node, retaining volumes. |
| `make PROFILE=review kind-create kind-install kind-deploy` | PASS under the documented larger VM budget. Three brokers, RF3/minISR2 topics and internal state, two TaskManagers, two Action/API/Registry replicas. |
| `make PROFILE=review kind-smoke` | PASS at `2026-11-23T06:46:47.128227Z`. Action `6cf4c5d7-0d8c-5f3a-aace-6d1f1c5510db` completed. |
| `make PROFILE=review kind-measure kind-inspect` | PASS. Every deployed workload has its Member ownership and health signal. |
| `make contracts BASE_REF=HEAD` | 214 passed. Executable contracts unchanged. |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/test_kind_local.py tests/test_integration_evidence.py tests/test_preparation_docs.py` | 16 passed, including rendered profiles, bounded ordered shutdown, every-partition evidence reading and Debezium task assignment/failure. |
| `.tools/member4-venv/bin/python -m pytest -q tests/identity/test_local_routes.py tests/identity/test_auth.py tests/identity/test_browser.py` | 31 passed. |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | 85 passed, three opt-in checks skipped in the earlier regression run. |
| `make identity-api-test` | 38 passed, including live mutation failure behavior and chronological classification fixtures. |
| `git diff --check`; `.venv/bin/python -m compileall -q member2 member3 member4 dashboard identity tools` | Passed. |

Reports, simulator logs, browser screenshots and resource samples are retained
under `.tools/kind-finish/checkout/.tools/kind-dev` and `kind-review`.
Lifecycle logs are under `.tools/kind-finish`. Credentials remain private.

### Decisions and resolved failures

Member_1 owns Registry, simulation and observability. Member_2 owns Kafka,
PyFlink and their operators. Member_3 owns PostgreSQL, Action, Lease fencing
and CDC. Member_4 owns projection/API/UI, identity and Copilot.

The earlier partial run competed with existing workloads in the small VM.
After saving and stopping the standalone runtimes, Debezium completed its
snapshot and streamed. Strimzi Ready can precede task assignment. Deploy and
smoke now explicitly require the connector and assigned task to be RUNNING,
fail on a FAILED task and bound their waits. The full smoke verifies the
resulting CDC commands and effects. A first fresh-copy build also exposed that
Docker does not include an external directory symlink in its build context.
The retry used the hash-verified plugin archive as a regular local build input.

The existing standalone Flink job was stopped with the canonical savepoint
`file:/checkpoints/savepoint-f18edd-252790095f6d`. Existing volume mounts were
recorded before shutdown. The review temporarily used 8 CPUs/10240 MiB with
built-in Kubernetes disabled. Docker Desktop was restored to 4 CPUs/6144 MiB and all original standalone
containers were restarted. The saved `member2-review02` job was resubmitted
from that savepoint as job `68bf12d96cab5a0740c1a3191fcccdde`. Flink REST
confirmed savepoint checkpoint 1999 restored and three new checkpoints completed. See `.tools/kind-finish/standalone-restored.log`.
Task-owned dev/review/probe nodes are stopped with their volumes retained.

The profiles share one kind node and its local-path checkpoint PVC. Browser
access uses loopback UI and the selected realm's public OIDC paths. API and
identity backchannel calls use Service DNS. Live scenario/policy mutation
adapters return 503 until their separate milestone. Full observability-stack
packaging, Copilot, broker-loss and Action-leader failure exercises remain
subsequent milestones. No cloud resources, paid calls, commit or push occurred.

**Next recommended milestone** is the review failure-injection gate for broker
loss, Action leader handover and reconciliation, followed by observability and
the bounded deterministic Copilot.

## Previous milestone — local component integration

**Local component integration and the deterministic closed loop pass in the
host/Compose profile.** The Action worker acquires its Lease from local kind.
Member_1 owns simulation/Registry/observability, Member_2 Kafka/PyFlink,
Member_3 control/PostgreSQL/Debezium, and Member_4 projection/API/UI/identity/Copilot.
See the [runbook](LOCAL_INTEGRATION.md) and [ADR-033](../decisions/ADR-033-live-component-integration.md).

### Executed closed-loop evidence

Run `loop03` started at `2026-11-23T03:11:24.103932Z` and finished at
`2026-11-23T03:13:07.785648Z`. Its report and browser capture are under
`.tools/integration/loop03/`. Seed 273 and 276 emitted input readings are
recorded in the report. The 20 kW topology yielded 16 COMPLETE loads and
89 completed checkpoints.

| Phase | Expected kW | Observed kW |
|---|---:|---:|
| Normal | 13.2 | 13.2 |
| Critical overload | 20.2 | 20.2 |
| Throttled, original meter demand | 15 | 15 |
| Recovery while throttled | 8 | 8 |
| After safe restore | 13.2 | 13.2 |

- Action `254170d5-e9b5-5d25-9e2c-6aa3adeb0f8f` reached COMPLETED
  in seven transitions at Lease epoch 1. The confirmed reduction was 5.2 kW.
- Throttle `557dc3d5-5e91-5609-b929-d379c55cad77` applied 2 kW.
  Replaying it produced ALREADY_APPLIED without another effect.
- Restore `9a9d36ed-d76c-5753-b92a-e34f17fcaae8` applied 7.2 kW after
  two consecutive fresh recovery windows. The charger finished at 7.2 kW
  with exactly two durable effects.
- Kafka journal replay reproduced current projection rows. REST and SSE matched
  the completed Action payload. Chromium verified the Action ID, COMPLETED
  status and applied result in the real dashboard. The harness used its local
  configured Viewer identity. All four gate assertions ran separately over
  the shared live capture. Independent live deployments per gate were not run.

| Exact command | Result |
|---|---|
| `.tools/member3-venv/bin/python -m tools.integration_setup --run loop03` | Created run-owned SQL state and CDC connector |
| `PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/member3-venv/bin/python -m tools.integration --run loop03 --config .tools/integration-config/loop03.json --start-flink` | Four gates and host/Compose closed loop PASS, no cleanup errors |
| `.tools/member3-venv/bin/python -m tools.integration --check-gate "$gate" --report .tools/integration/loop03/report.json` for `member1-member2`, `member2-member3`, `member3-member1`, `members23-member4` | Each invocation PASS |
| `make contracts BASE_REF=HEAD` | 214 passed |
| `make member1-live` | 23 passed, plus binding generation and 214 contract checks |
| `make member2-test` | 43 passed |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | 85 passed, three opt-in checks skipped |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control MEMBER3_KIND_TEST=1 MEMBER3_CDC_TEST=1 .tools/member3-venv/bin/python -m pytest -q tests/member3/test_cdc_live.py::test_worker_graceful_loss_and_restart_reconciliation tests/member3/test_worker.py` | 4 passed |
| `make identity-api-test` | 37 passed including the classification regression |
| `make dashboard-test` | 29 passed |
| `.venv/bin/python -m pytest -q tests/test_integration_evidence.py tests/test_preparation_docs.py` | 7 passed |
| `.venv/bin/python -m compileall -q member2 member3 member4 tools` and `git diff --check` | Passed |

`loop01` found that a missing Debezium heartbeat topic blocked outbox delivery.
`loop02` verified throttle and duplicate acknowledgement, but a timer reset
the recovery streak while the next load was still behind the Flink watermark.
The timer now marks aged loads held while a fresh contiguous load is required
to advance the streak. Both failures are retained in their run reports. A fresh setup attempt
(`loop05`) found a transient Connect status 404 during assignment. Setup now
waits for the assigned task. In `loop06`, a heartbeat could update the Registry
just after Action captured the evaluation timestamp, leaving the latest
heartbeat too new for that query. The runner now reports the completed sample
interval with a two-second observation lag, within the 15-second freshness limit.

The repeat run `loop04` passed all four gates and the full host/Compose loop
at `2026-11-23T03:16:27.898119Z`. It recorded 387 input readings, 24 COMPLETE
windows, 126 completed checkpoints and a final 13.2 kW load at 66% utilization.
Action `8f6f155d-f3ab-56a1-9626-303d3d5cc3b7` completed. Throttle
`978c5f2e-49aa-5347-9168-6ca9e60e1495` returned APPLIED and ALREADY_APPLIED.
Restore `a00bf720-04c5-55c3-9bd7-f76fe802abc6` returned APPLIED. The final
charger rate was 7.2 kW with two effects. The report includes the matching REST
and SSE payloads and the Chromium assertion under `.tools/integration/loop04/`.

- `PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/member3-venv/bin/python -m tools.integration --run loop04 --config .tools/integration-config/loop04.json --start-flink` passed after restoring connector capacity and handling already provisioned topics. The first setup attempt hit the four replication sender limit. The failed attempts were preserved. The four `--check-gate` invocations above each passed again with `loop04/report.json`.
- `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection MEMBER4_TEST_KAFKA=127.0.0.1:19092 .tools/member4-venv/bin/python -m pytest -q tests/member4/test_kafka_live.py tests/member4/test_stream_live.py` passed all six tests.
- The run-owned CDC connectors and inactive replication slots were removed after retaining SQL/Kafka evidence. The existing `member3-outbox` connector and `member2-review02` job remained running.

The final verification run `loop07` went from `2026-11-23T03:19:49.713870Z`
to `2026-11-23T03:21:28.528690Z`. It passed fresh provisioning, all four
boundary assertions, the closed loop, and cleanup. Seed 273 produced 258
recorded input readings and 15 committed load windows with 72 completed
checkpoints. The final load was 13.2 kW. Action
`004d6640-d9f4-50b9-a645-2cfef2f7c590` completed. Throttle
`e3cecdc1-81b1-5e6d-b621-5bbb60d73408` applied 2 kW and replay returned
ALREADY_APPLIED. Restore `266bac56-c746-564a-99f3-5a678cbd174a` applied
7.2 kW, with exactly two total charger effects. The report, matching REST
and SSE payloads, and Chromium screenshot are under `.tools/integration/loop07/`.

| Exact command | Result |
|---|---|
| `.tools/member3-venv/bin/python -m tools.integration_setup --run loop07` | Provisioned fresh topics, schemas, publication and running connector |
| `PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/member3-venv/bin/python -m tools.integration --run loop07 --config .tools/integration-config/loop07.json --start-flink` | Exit 0. All four gates and full loop PASS. Flink job CANCELED during teardown and no cleanup errors. |
| `.tools/member3-venv/bin/python -m tools.integration --check-gate "$gate" --report .tools/integration/loop07/report.json` for `member1-member2`, `member2-member3`, `member3-member1`, `members23-member4` | Four independent assertion invocations PASS |

**Next recommended milestone:** package and run the closed loop in the local
kind integration namespace, then verify the kind deployment gates. Live
operator policy and scenario mutation delivery and Copilot remain separate.
No cloud resources, paid calls, commits or pushes were made.

## Previous milestone — Member_4 local identity

**Member_4 local Keycloak/OIDC identity and application authorization are implemented
in the standalone local profile.** See [ADR-032](../decisions/ADR-032-member4-local-identity.md)
and the [identity runbook](../../identity/README.md). Member_1 retains simulation/
Registry/observability, Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium,
and Member_4 projection/API/UI/identity/Copilot. Initial Git status was clean;
no commit/push, cloud resource, paid call, system software or external model was used.

- Reproducible pinned ARM64 Keycloak realm/client/audience/subject/role bootstrap,
  random safe local users, own database/login in the existing PostgreSQL primary,
  ignored mode-0600 import/config/password files and scoped identity reset.
- Normal API/UI runtime selects OIDC; explicit fixture/mock mode stays independent.
  RS256 signature/JWKS, exact issuer/audience, expiry/issued-at/subject, token type,
  authorized client and signed realm-role mapping fail closed. Spoofed actor/role
  headers and direct browser-bypassing requests cannot grant access.
- Browser code+PKCE S256 with state/nonce; opaque HttpOnly SameSite sessions;
  server-held tokens, serialized renewal/rotation and logout/session invalidation.
  API SSE closes at expiry. Callback access logging is disabled and pages use
  no-store/no-referrer. Successful changes atomically audit subject, role,
  authorization timestamp, request/trace/idempotency context without credentials.
- Executable contracts retain Viewer reads, Operator scenarios/acknowledgements,
  **Administrator policy writes**. The tested tool guard revalidates Operator and
  accepts only seven frozen read-only names. No tool executor or Copilot stub was
  added; scoped dispatch/evidence/service identity belong to the next milestone.
- No new LAN/public broker/database/admin binding. Existing local developer ports
  remain loopback-only; no unrelated container, volume or service data was removed.

### Identity executed verification — 2026-11-22

| Exact command | Result |
|---|---|
| `.tools/member4-venv/bin/python -m pip index versions PyJWT`; `.tools/member4-venv/bin/python -m pip index versions cryptography` | Selected available 2.15.1 / 50.0.2; existing libraries had no JWT verifier |
| `.tools/member4-venv/bin/python -m pip --cache-dir .tools/pip-cache install --report .tools/identity-install.json 'PyJWT[crypto]==2.15.1' 'cryptography==50.0.2'` | Installed four pinned distributions in independent API venv; report hashes generated identity lock |
| `.tools/dashboard-test-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r identity/requirements.txt`; `.tools/dashboard-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r identity/requirements.txt` | Both isolated dashboard environments installed the four exact hashes |
| `.tools/member4-venv/bin/python -m identity.bootstrap`; `docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml config --quiet` | Own Keycloak role/database and ignored random config created; merged config valid |
| `docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak` | Selected ARM64 Keycloak 26.1.4 built/started; PostgreSQL realm import succeeds |
| `docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml stop keycloak`; `.tools/member4-venv/bin/python -m identity.bootstrap --reset`; `docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak` | Recreated only identity database and imported users/client/roles; unrelated state preserved |
| `curl -fsS http://127.0.0.1:18080/realms/smart-grid/.well-known/openid-configuration -o .tools/identity/discovery.json` | Real discovery succeeds with exact loopback issuer |
| `make identity-test` | **16 passed**: claims/signature/role failures, tool role denial, direct API bypass, state/nonce/replay/expiry, logout CSRF, serialized refresh/failure and API bearer renewal hook |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_api.py tests/member4/test_oidc.py tests/member4/test_review.py tests/member4/test_projection.py tests/member4/test_dashboard_boundary.py` | **36 passed**, no skips: disposable real PostgreSQL, API authorization/audit, expiry of open SSE and existing regression boundaries |
| `make identity-live-test` | **1 passed**, no skips: real Chromium and Keycloak for all three roles, PKCE login/logout, real refresh, tool-role enforcement, then a subprocess direct API suite with actual Keycloak JWTs and disposable SQL schema (**1 passed**); transient test token file removed |
| `make dashboard-test` | **29 passed**, no skips; existing fixture/client/Chromium suite preserved |
| `make contracts BASE_REF=HEAD` | **214 passed**; no executable v1 changes |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py tests/test_local_infra.py` | **21 passed** |
| `.tools/member4-venv/bin/python -m pip check`; `.tools/dashboard-test-venv/bin/python -m pip check`; `.tools/dashboard-venv/bin/python -m pip check` | No broken requirements; sandbox pip-cache warning only |
| `.tools/member4-venv/bin/python -m compileall -q identity member4 dashboard tests/identity tests/member4/test_oidc.py tests/member4/test_keycloak_live.py`; `git diff --check` | Passed |

Earlier iterations are not acceptance evidence: real Keycloak required complete
synthetic user profiles and an explicit basic subject mapper; corrected bootstrap
was imported after scoped database recreation. Browser test-only callback capture
also required observing redirected requests instead of routing a redirect target.
An initial identity-unit invocation in the dashboard test venv collected an API
module without psycopg; API tests now run in their owned Member_4 environment.
The selected Starlette emits its existing HTTPX TestClient deprecation warning.
No passwords/tokens are committed; logs/config/test files remain ignored locally.

### Identity prepublication correctness/security review — 2026-11-22

The user authorized review, fixes, commit and a normal push to `main`. Initial
uncommitted files were the identity milestone above; all changes in this review
remain within that slice. `git fetch origin main` succeeded and
`git rev-list --left-right --count origin/main...HEAD` returned `0 0` before
publication. Existing Member_1/2/3 services and their data were preserved.

Regression tests reproduced **3 failures before fixes**: a numeric-string JWT
expiry passed validation and could break stream/renewal comparisons; a refresh
could return a bearer after concurrent local logout; and a malformed provider
JSON array raised an uncaught TypeError. Fixes and further review cover:

- Require finite NumericDate values and recheck local session presence/expiry
  after awaited token renewal. Reject malformed/missing token response fields.
- Validate newly returned ID tokens on refresh, including signature, issuer,
  audience, subject and authorized client. Failed renewal invalidates the session.
- Clear open-tab data and dialogs, stop background tasks and navigate to a local
  signed-out page after invalidation, including while keyboard reading holds a
  panel. Real Keycloak tests verify a second open tab after logout for all roles.
- Deny non-string tool names as well as unavailable/write tools; never accept
  caller/model role headers. All API responses explicitly use no-store.
- Bootstrap changes permissions only on its five generated files; an idempotent
  rerun preserves credentials and unrelated files in the ignored identity folder.
  No database reset or unrelated container/volume cleanup was needed for review.

Final review verification supersedes initial identity counts above:

| Exact command | Result |
|---|---|
| `.tools/member4-venv/bin/python -m pytest -q tests/identity/test_auth.py tests/identity/test_browser.py -k review --tb=short` | Initial regression run **3 failed, 1 passed before fixes** |
| `make identity-test` | **22 passed**, no skips; regression coverage includes logout/expiry during refresh, malformed provider payload, renewed ID token validation and malformed tool names |
| `make identity-api-test` | **36 passed**, no skips; actual SQL schemas, permitted/forbidden direct API changes and transactional audit |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_stream_live.py` | **1 passed**; real TCP replay/reconnect/cross-replica/heartbeat/reset regression |
| `make identity-live-test` | **1 passed**, no skips; all three real Keycloak roles, refresh, cross-tab logout, tool guard; subprocess direct API and bootstrap preservation checks **2 passed** |
| `make dashboard-test` | **29 passed**, no skips |
| `make contracts BASE_REF=origin/main` | **214 passed**; no executable contract changes |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py tests/test_local_infra.py` | **21 passed** |
| `.tools/member4-venv/bin/python -m pip --cache-dir .tools/pip-cache check`; `.tools/dashboard-venv/bin/python -m pip --cache-dir .tools/pip-cache check`; `.tools/dashboard-test-venv/bin/python -m pip --cache-dir .tools/pip-cache check` | No broken requirements in all three independent environments |
| `docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml config --quiet` | Valid; local container inventory confirms all published developer ports still bind loopback |
| `.tools/member4-venv/bin/python -m compileall -q identity member4 dashboard tests/identity tests/member4/test_oidc.py tests/member4/test_keycloak_live.py`; `git diff --check` | Passed |

No remaining actionable finding blocks the standalone identity slice. Existing
Starlette/HTTPX TestClient deprecation warning remains; no dependency pin changed.
Keycloak is left running for local inspection; test servers/schemas/transient
tokens are cleaned up. TLS, production hardening/dependency scans, kind/Calico
isolation, persistent UI sessions and live internal control delivery retain their
existing later-gate status. Publication is recorded in Git history/remote state;
hosted CI is a separate result.

### Identity limits and next milestone

- **No remaining blocker for standalone identity.** Keycloak remains running on
  loopback 18080 for inspection. Browser/API test processes stop and temporary SQL
  schemas/token files are removed. Normal startup preserves the imported realm.
- HTTP loopback and random synthetic users are for the trusted local computer.
  API tokens already issued remain usable until their two-minute expiry after
  logout. UI sessions are in memory, capped at 100 and 30 minutes; a UI restart
  requires login. No production TLS or UI replica/session failover claim.
- Policy/scenario effects remain the existing explicit contract mocks. Live
  Member_1/3 service authentication/delivery, Linux application locks/images,
  kind/Calico isolation tests, security scans and full-stack closed-loop acceptance
  remain later gates. Private loopback bindings are not NetworkPolicy evidence.
- **Next recommended milestone:** Member_4 typed read-only Tool Gateway and
  deterministic MOCK Copilot with entity/evidence scope, budgets and audit;
  preserve independent member ownership and later live/kind integration gates.

## Previous milestone — Member_4 standalone operator dashboard

**Member_4 standalone operator dashboard is implemented.** This requested slice
adds the operations console; Member_1 retains simulation/Registry/observability,
Member_2 Kafka/PyFlink, Member_3 control/PostgreSQL/Debezium and Member_4
projection/API/UI/identity/Copilot ownership. No other member's runtime changed.

The operator can follow normal → overload → throttle → recovery/restore without
refreshing, inspect the trigger/policy/action and command outcomes, acknowledge
alerts, and confirm scenario operations. REST/SSE responses are checked against
the executable v1 bundle. Reconnect, cursor reset, duplicate/older events,
periodic freshness refresh, retained values on failure and mutation idempotency
are implemented. Navigation/search and logical button focus survive updates;
status changes announce critical counts politely. Charts have corresponding
value tables and disable animation. Desktop/tablet/mobile screenshots were
rendered and visually inspected; skip-link overlap, drawer contrast, crowded
secondary device columns and threshold-label spacing were corrected.

### Dashboard prepublication review — 2026-11-22

The user authorized review, fixes, commit and a normal push to `main`. The
uncommitted starting changes were the dashboard milestone above; unrelated
Member_1/2/3 implementations and existing service data were preserved.

Review reproduced three initial client failures before fixes: a disconnected
stream could become falsely Live after failed/healthy REST refreshes; an
uncertain mutation lost its retry key when another operation intervened; and
partially completed multi-endpoint reads leaked into displayed state. All three
regressions passed after stream-state separation, per-operation keys and staged
snapshot publication. Additional review corrections and checks cover:

- Open action explanations now follow current action/command results. Dialogs
  retain a persistent parent across live redraws and transient dialogs clean up.
- Timestamp fractions sort by instant; recent lists sort before truncation.
  Table sorting also compares fractional instants and numeric power values.
  Negative net export is plotted; non-finite/overflow and overly nested JSON
  fail visibly rather than killing refresh tasks or showing invalid numbers.
- History results carry both entity and time-range scope. SSE reset clears old
  health records that have no REST source. Empty fixture configuration now
  succeeds instead of indexing an absent transformer.
- Table headers expose columnheader/scope/sort state and real keyboard buttons.
  The previous autofocus/rebuild approach is superseded by a visible keyboard
  reading hold, retaining focus, table sort/page state and expansions; move focus
  outside the panel to apply current values. Header critical/status updates stay
  live and do not repeatedly announce snapshot timestamps.
- The investigator fits mobile width, disables its unavailable question input,
  traps Tab on its sole enabled control, and supports Escape/focus return.
  Native dialog transitions are disabled; tests wait for viewport visibility.
- Noncompleted plans are labeled Open actions, not asserted physical throttles.
  No v1 shape, dependency pin, domain state logic or service ownership changed.

Final review verification (supersedes initial 19-check acceptance below):

| Exact command | Result |
|---|---|
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; `0 0` before publication |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/dashboard/test_client.py -k review --tb=short` | Initial three client regressions **3 failed before fixes**; then passed in the expanded client suite |
| `make dashboard-test` | **29 passed**, no skips (12 browser cases, 14 client/fixture checks, 3 presentation regressions) |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_dashboard_boundary.py --tb=short` | **1 passed**, no skip; actual PostgreSQL/API through HTTPX ASGI transport, snapshot validation, historical stale data, Viewer denial, start/stop/acknowledgement; temporary schema removed |
| `make contracts BASE_REF=origin/main` | **214 passed**; executable contracts unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/dashboard-test-venv/bin/python -m pip check`; `.tools/dashboard-venv/bin/python -m pip check` | No broken requirements; selected versions unchanged |
| `.tools/dashboard-test-venv/bin/python -m compileall -q dashboard tests/dashboard tests/member4/test_dashboard_boundary.py`; `git diff --check` | Passed |

Browser review uses real Chromium and TCP fixture REST/SSE. Rendered review
screenshots are ignored in `.tools/dashboard-evidence/`; no evidence/cache files
are published. Keyboard/ARIA assertions and visual inspection are verified;
a manual screen-reader audit is not claimed. Full live control/Kafka, identity,
Copilot execution, Linux/kind packaging and multi-UI failover remain the documented
later gates. The real-API check above is not a new end-to-end closed-loop claim.
No blocker remains for this review scope after the final checks. Git history and
remote state record the authorized commit/push; hosted CI is a separate result.

### Dashboard executed verification — 2026-11-22

Initial Git status was clean. All downloads and Python environments are local
and ignored; selected dependency versions were reused. The runtime hash lock
contains 53 distributions; the separate test lock adds 14. Network download,
loopback servers and Chromium execution required sandbox escalation. Disposable
browser-test servers own ports 18185/18186 and stop after tests; manual previews
use 18085/18086 (`.tools/dashboard-venv/bin/python -m dashboard.fixture_api` and
`.tools/dashboard-venv/bin/python -m dashboard --fixture`), and were stopped with
`kill -INT 34743 34794` after inspection. No commits, pushes, cloud resources or model calls.

| Exact command | Result |
|---|---|
| `.venv/bin/python -m venv .tools/dashboard-venv` | Created independent dashboard environment, CPython 3.12.9 |
| `.tools/dashboard-venv/bin/python -m pip --cache-dir .tools/pip-cache install --report .tools/dashboard-install.json 'nicegui==3.19.0' 'fastapi==0.165.0' 'starlette==1.10.0' 'uvicorn==0.35.0' 'pydantic==2.14.4' 'pydantic-core==2.50.5' 'httpx==0.28.1' 'pytest==8.3.5' 'playwright==1.51.0' 'pytest-playwright==0.7.0'` | Approved pins resolved/installed; report supplies exact artifact hashes; initial sandbox DNS failure, escalated retry succeeded |
| `.tools/dashboard-venv/bin/python -m pip --cache-dir .tools/pip-cache install --no-index --find-links .tools/member4-wheels-macos --report .tools/dashboard-schema-install.json 'jsonschema==4.23.0'` | Reused selected schema validator/wheels; report hashes included in runtime lock |
| `.venv/bin/python -m venv .tools/dashboard-runtime-check`; `.tools/dashboard-runtime-check/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r dashboard/requirements.txt` | Clean runtime environment/hash install passed for original 49 runtime dependencies |
| `.tools/dashboard-runtime-check/bin/python -m pip --cache-dir .tools/pip-cache install --no-index --find-links .tools/member4-wheels-macos --require-hashes -r dashboard/requirements.txt` | Final 53-package runtime lock satisfied; four schema distributions added offline with hashes |
| `.venv/bin/python -m venv .tools/dashboard-test-venv`; `.tools/dashboard-test-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r dashboard/requirements-test.txt` | Clean independent test environment installed all 67 hash-locked distributions |
| `PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-venv/bin/python -m playwright install chromium` | Chromium 134.0.6998.35/build 1161 plus its headless shell/FFmpeg installed under ignored `.tools`; initial sandbox DNS failure, escalated retry passed |
| `make dashboard-test` | **19 passed** (10 Chromium browser cases, 9 client/fixture checks); no skips |
| `make contracts BASE_REF=HEAD` | **214 passed**; executable v1 unchanged |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py` | **10 passed** |
| `.tools/dashboard-venv/bin/python -m pip check`; `.tools/dashboard-test-venv/bin/python -m pip check`; `.tools/dashboard-runtime-check/bin/python -m pip check` | No broken requirements |
| `.tools/dashboard-test-venv/bin/python -m compileall -q dashboard tests/dashboard`; `git diff --check` | Passed |

Earlier nonpassing iterations are not acceptance evidence: fixture liveness
initially required authentication; NiceGUI theme configuration initially created
a global UI element; subprocesses inherited NiceGUI's unrelated internal pytest
mode; browser locators needed dialog scoping; the tablet drawer overlapped its
header toggle; one landmark was duplicated; and a focused data-panel button was
removed by rendering. Tests reproduced these issues and fixes were verified.
History queries now publish their transformer scope atomically with their results,
so a failed switch cannot relabel another transformer's history.
A mutation notification also outlived its rendered parent; use the persistent
client context. Native Quasar autofocus now preserves the same logical action
button, with no project JavaScript. Client unit checks run their event loops in
an isolated thread so Playwright's running loop cannot interfere.

Screenshots/logs are ignored in `.tools/dashboard-evidence/`: overload, recovery,
reconnect, loading/state failures, tablet scenarios, mobile overload and the
investigator drawer. Browser tests run real TCP REST/SSE with schema-checked
fixtures, not in-browser response interception. Unit checks cover pagination
loops, invalid success payloads, history-scope failure, stale/duplicate SSE versions, permission denial,
uncertain mutation keys and fixture electrical accounting/schema agreement.

### Dashboard decisions, limits and next milestone

- [ADR-031](../decisions/ADR-031-member4-operator-dashboard.md) resolves conceptual
  UI/API differences explicitly; v1 shapes/enums remain unchanged. The trend is
  a named highest-risk transformer's history; overview demand remains the API
  total. Scenario cards/fields use only the four approved v1 inputs.
- Candidate lists, complete command envelopes, every historical saga transition,
  telemetry arrival timestamps and component metrics are not supplied by v1.
  Display bounded published results and label unknown coverage; do not fabricate
  healthy state or control decisions. Recovery remains an Action lifecycle.
- The fixture story is controlled synthetic UI evidence (72 → 96 → 75 → 72 kW),
  not a new simulator/control implementation. Scenario operations remain backend
  contract mocks. Live mode connects to the existing independent API; this
  milestone does not claim new Kafka/SQL/control closed-loop acceptance.
- One loopback UI process with a server-configured local mock identity. OIDC,
  per-user sessions, Linux lock/image/kind packaging, observability metrics and
  UI replica failover remain later gates. No production identity claim.
- Investigation entry points preserve transformer/action/alert/time context and
  explicitly report unavailable. Copilot execution, stages, provider errors and
  evidence links belong to the Tool Gateway/Copilot milestone, not fake answers.
- **Remaining blocker for the standalone dashboard: none.**
- **Next recommended milestone:** Member_4 Keycloak identity integration, followed
  by Tool Gateway/Copilot and complete live context/internal adapters; retain the
  independent Member_2 → Member_3 → Member_1 and kind integration gates.

## Previous milestone — Member_4 projections and API

**Member_4 projections and API are complete in the standalone local profile.**
Only the requested backend milestone is implemented. Member_1 retains
simulation/Registry/observability, Member_2 Kafka/PyFlink, Member_3
control/PostgreSQL/Debezium, and Member_4 projection/API/UI/identity/Copilot.

Owned PostgreSQL transactions persist event dedup, highest entity versions,
Kafka source positions and SSE publication atomically. Recorded events converge
to a fixed full expected snapshot under duplication, reordering and rebuild.
SQL-backed replay/pages/mutation idempotency work across API instances, and real
HTTP clients reconnect or reload snapshots on cursor gaps. Policy/scenario
operations use explicit contract mocks with validation, conflicts and failures.
No Registry/Control tables or application internals are read.

### Member_4 prepublication review — 2026-11-22

The user authorized correctness/failure review, fixes, commit and a normal push
to `main`. Review reproduced and fixed the following boundaries:

- SSE checked only the earliest retained row. An interior/tail hole could be
  silently skipped (including retention with replica clock skew). Verify every
  sequence through the committed high-water mark; otherwise require a snapshot.
- Snapshot/expiry/publication clocks were sampled before waiting for the SQL
  lock. Sample after acquisition so waits do not preserve false freshness or
  replay an expired cursor. Frozen pages expose their original `X-Snapshot-At`.
- Events for unconfigured entities were journaled but omitted from inventory
  while overview/inventory could report FRESH. Signal partial coverage and emit
  degraded projection health without inventing metadata or domain decisions.
- Equal-version events could disagree with bootstrap fixture loads and disappear
  behind the fixture. Reject the conflict atomically in either loading order.
  Reject excessively future-dated event/load times rather than keeping them fresh.
- Initial SSE database failure returned 200 and immediately closed. Check before
  headers and return a safe 503. A later outage still closes the established
  stream; real HTTP tests prove cursor-based recovery after the dependency returns.
- ISO parsing accepted non-RFC3339 forms; non-ASCII bearer input raised TypeError;
  sufficiently nested JSON raised an uncaught RecursionError. Enforce the frozen
  UTC timestamp form and return bounded authentication/validation errors.
- A subscription exception could bypass consumer cleanup. Validate source binding
  before constructing the consumer, and include subscribe in the cleanup scope.

Eleven initial regression cases failed before fixes, then all eleven passed.
Additional cases cover future inputs, reverse fixture/event loading, established
SSE outage recovery, and rollback after an SSE insert. A deeply nested JSON case
also failed before its parser guard; the earlier shallower variant correctly
returned a domain error and was increased to exercise actual parser exhaustion.
The first expanded full run had one failure because the new fixture conflict
check masked the more specific reused-event-ID check; identity validation now
runs first. These intermediate failures are not acceptance evidence.

Final review verification (supersedes the initial 22-test acceptance below):

| Exact command | Result |
|---|---|
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; `0 0` before publication |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_review.py --tb=short` | **11 failed before fixes**, then **11 passed** for the initial regression set |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_review.py tests/member4/test_projection.py --tb=short` | **23 passed** before the final two request/rollback cases were added |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection MEMBER4_TEST_KAFKA=127.0.0.1:19092 make member4-test` | **39 passed**, no skips; real PostgreSQL/Kafka, killed/restarted consumer, two TCP API instances, reconnects/heartbeats, stream outage and cursor gaps |
| `make contracts BASE_REF=origin/main` | **214 passed**; executable v1 unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/member4-venv/bin/python -m pip check`; `.tools/member4-venv/bin/python -m compileall -q member4 tests/member4`; `git diff --check` | Passed; no broken dependencies, compilation or whitespace errors |

One selected Starlette TestClient/HTTPX deprecation warning remains; no pin was
changed. This publication includes only the requested Member_4 implementation,
its review fixes/tests and affected documentation. Existing Member_1/2/3
services/data were preserved; no model calls or cloud resources were used.
No blocker remains for this scope. Git history/remote state record the authorized
commit and normal push; hosted CI is a separate, unclaimed result.

### Member_4 executed verification — 2026-11-22

All credentials below are disposable local fixtures. Local PostgreSQL/Kafka and
TCP HTTP checks required sandbox escalation. All temporary schemas/topics are
removed after acceptance; existing other-member services/data were preserved.
The CLI smoke leaves only the new Member_4 fixture journal/context/views in the
owned projection database; its temporary API process was gracefully stopped.

| Exact command | Result |
|---|---|
| `git status --short`; `rg --files`; reads of AGENTS, master/status/version matrix, relevant design/contracts and accepted ADRs | Initially clean; scoped implementation added without unrelated edits |
| `.venv/bin/python -m venv .tools/member4-venv` | Independent CPython 3.12.9 environment created |
| `.tools/member4-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --require-hashes -r member3/requirements.txt` | Existing selected Kafka/SQL/validation/test pins installed offline |
| `.tools/member4-venv/bin/python -m pip --cache-dir .tools/pip-cache install --report .tools/member4-install.json 'fastapi==0.165.0' 'starlette==1.10.0' 'uvicorn==0.35.0' 'pydantic==2.14.4' 'pydantic-core==2.50.5' 'httpx==0.28.1'` | Selected API pins and transitives resolved/installed; report hashes used for Member_4 lock |
| `.tools/member4-venv/bin/python -m pip --cache-dir .tools/pip-cache download --require-hashes -r member4/requirements.txt -d .tools/member4-wheels-macos` | All 29 distributions downloaded with verified lock hashes |
| `.tools/member4-venv/bin/python -m pip install --no-index --find-links .tools/member4-wheels-macos --require-hashes -r member4/requirements.txt` | Offline locked environment satisfied |
| `MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection MEMBER4_TEST_KAFKA=127.0.0.1:19092 make member4-test` | **22 passed**, no skips; real PostgreSQL/Kafka and two TCP HTTP/SSE instances; one selected Starlette TestClient/HTTPX deprecation warning |
| `make contracts BASE_REF=HEAD` | **214 passed**, frozen executable v1 unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m member4 migrate` | Owned migration completed |
| `PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m member4 replay` | Recorded fixtures replayed |
| `PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m member4 rebuild` | Views rebuilt, cursor generation rotated |
| `PROJECTION_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection MEMBER4_MOCK_IDENTITIES='{"local-admin-token":{"actor":"local-admin","role":"Administrator"}}' .tools/member4-venv/bin/python -m member4 serve` | CLI started on loopback 18084; HTTPX smoke below passed; gracefully stopped with `kill -INT 33005` after observation of that PID |
| `.tools/member4-venv/bin/python -m pip check`; `.tools/member4-venv/bin/python -m compileall -q member4 tests/member4`; `git diff --check` | No broken requirements, compilation/whitespace passed |

Exact manual HTTP smoke executed using `.tools/member4-venv/bin/python -`:

```python
import httpx
headers={'Authorization':'Bearer local-admin-token'}
with httpx.Client(base_url='http://127.0.0.1:18084/api/v1',timeout=5) as client:
 for path in ['/health','/ready','/overview','/transformers?state=STALE&limit=1']:
  r=client.get(path,headers=headers);r.raise_for_status();print(path,r.status_code,r.json())
 with client.stream('GET','/events',headers=headers) as r:
  r.raise_for_status()
  for line in r.iter_lines():
   if line.startswith('event:'):
    print('SSE',r.status_code,line);assert line=='event: snapshot.required';break
```

All five requests returned 200; overview showed one transformer, 96 kW and
explicit STALE freshness for the historical recording. Tests also check the
complete expected fresh snapshot at its fixed historical evaluation instant.

Earlier nonpassing evidence is not acceptance: the first projection tests found
an invalid test status and an assumed fixture ordering, both corrected to the
executable contracts; API tests found unresolved relative OpenAPI refs, fixed
with local bundle resolution. The initial real Kafka restart hit the default
45-second group timeout; the local consumer now uses six seconds, and the
retested SIGKILL/restart passes. Six leftover random test topics from the first
cleanup failure were removed; final cleanup bounds shutdown and kills a stuck
test worker before deleting its owned topics. No existing component was killed.
One manual-server attempt was rejected because automatic approval review was
out of workspace credits, not because it determined the action unsafe; after
the user's instruction to continue, the reviewed retry and smoke passed.

### Decisions, limitations and next milestone

- [ADR-030](../decisions/ADR-030-member4-projections-api.md) resolves the missing
  context feeds, alert version ordering, frozen response/history shapes, and
  conceptual RECOVERING mismatch. Unknown context is not invented. This is a
  standalone fixture boundary; full inventory/policy/state delivery needs an
  owning-service boundary before live completeness can be claimed.
- The API never calculates control thresholds, allocation or recovery. Local
  alert acknowledgements are independent audited annotations; policy/scenario
  adapters are explicit deterministic contract mocks, with no live effects.
- REST `X-Event-ID` and shared durable generations close snapshot/subscribe and
  replica recovery gaps. Page snapshots expire after five minutes; SSE retains
  1000 events/five minutes. Payload freshness is as of publication; reconnect
  and periodic snapshot refresh provide current age during event silence.
- One PostgreSQL primary and one local Kafka broker; finite-profile global SQL
  lock, materialized pages and project-lifetime journal. Monitor disk; no scale,
  replicated availability or incomplete-retention disaster recovery claim.
- Mock bearer identity binds loopback; OIDC/TLS, real service authentication and
  network deadlines, live context feeds, packaged Linux native wheels, images,
  kind workloads/NetworkPolicies, observability and security scans remain later
  gates. Two real HTTP API instances and consumer process restart were tested;
  Kubernetes replica failover and database-server crash were not exercised.
- **Remaining blocker for this milestone: none.** No cloud resources, model calls,
  host tooling installation, commits or pushes were performed.
- **Next recommended milestone:** Member_4 NiceGUI dashboard and Keycloak identity
  against this snapshot/SSE boundary; then complete live context/internal adapters
  and pairwise events-to-operations acceptance. Existing live observation and
  Member_2 → Member_3 → Member_1 closed-loop gates remain independent prerequisites
  for full platform acceptance.

## Previous current milestone — outbox delivery and fencing

Outbox delivery and fencing completed in the small local profile under
[ADR-029](../decisions/ADR-029-outbox-and-fencing.md). Member_3 owns Connect,
PostgreSQL authority and kind Lease; Member_1 owns the atomic fenced simulated
gateway. Historical verification and publication evidence follows unchanged.

## Outbox prepublication review — 2026-11-22

The user authorized correctness/failure review, fixes, commit and a normal push
to `main`. Review found and fixed four boundary issues:

- A Lease GET/renew response could cross the previous local deadline and revive
  the same incarnation. Check the prior deadline after GET and after CAS; an
  uncertain/late successful renewal still loses local authority.
- Falsy empty Kafka records could bypass parsing and later be skipped by a higher
  committed offset. Handle every non-None message; malformed inputs stop visibly
  without committing an offset.
- Graceful shutdown could close the consumer before revoking authority; failure
  to revoke could skip consumer cleanup. Revoke first and always close the
  consumer in a nested finally, including database failures.
- The gateway evaluated expiry using arrival time before database waits. Count
  monotonic elapsed work, and recheck command expiry and SQL authorization after
  effect/output writes. If either budget expires, the entire effect/dedup/output
  transaction rolls back. Already-applied duplicates still cause no new effect.

Seven regression cases failed before their fixes (two Lease, three worker, two
gateway wait cases). Two additional cases cover final authorization latency and
server-clock authority expiry. Final verification:

| Exact command | Result |
|---|---|
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; `0 0` before publication |
| `.tools/member3-venv/bin/python -m pytest -q tests/member3/test_lease.py tests/member3/test_worker.py` | **5 failed before fixes**, then **5 passed** |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3/test_delivery.py -k expiry_counts_database_waits` | **2 failed before fixes**; covered by final expanded suite |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control MEMBER3_CDC_TEST=1 MEMBER3_KIND_TEST=1 make member3-test` | **67 passed in 40.20s**, no skips; real CDC crashes, kind leader death, worker restarts and PostgreSQL effects |
| `make contracts BASE_REF=origin/main` | **214 passed**; frozen contracts unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/member1-venv/bin/python -m pytest -q tests/member1/test_edge.py tests/member1/test_operations.py` | **16 passed** |
| `.tools/member3-venv/bin/python -m pip check`; `.tools/member3-venv/bin/python -m compileall -q member1/fencing.py member3 tests/member3 tools/member3_setup.py`; `git diff --check` | Passed; no broken requirements or compilation/whitespace errors |

Only the requested outbox/fencing files and review fixes are included. Existing
Member_1/2 containers and data remain intact. No blocker remains for this scope;
limits and next milestone above remain unchanged. Git history and remote state
record publication; hosted CI is a separate, unclaimed result.

## Outbox delivery and fencing — executed 2026-11-22

Use the environment values in the runbook; all passwords below are disposable
local fixtures. Docker, loopback PostgreSQL/Kafka and kind access required sandbox
escalation after the initial read-only checks were denied. Those initial denials
were environment restrictions, not passing verification.

| Exact command / check | Result |
|---|---|
| `git status --short`; `rg --files`; reads of root AGENTS, master/status/version matrix, relevant design/contracts and ADR-025/026/028 | Initially clean worktree; existing implementation traced before edits |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml ps --all`; `.venv/bin/kubectl --context kind-smart-grid-dev get nodes` | Existing dependencies healthy; existing kind node Ready; unrelated services preserved |
| `curl -fL --max-time 60 -o .tools/downloads/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.0.8.Final/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz`; `shasum -a 256 .tools/downloads/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz` | Selected plugin downloaded to ignored cache; SHA-256 `600806875b810abb96b3774f789d9ed33dbe4ac687d3ed9454bc0caa76b7279c`; build enforces checksum |
| `.tools/member3-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --require-hashes -r member3/requirements.txt` | Existing selected confluent-kafka 2.8.2 installed offline; other locked packages reused |
| `MEMBER3_ADMIN_DSN=postgresql://local_admin:local_admin_disposable@127.0.0.1:15432/local_dev_control CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m tools.member3_setup` | Isolated schema/publication, restricted CDC/gateway roles and isolated local-m3 topics created; repeated setup passes |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member3.yaml up -d --build connect` | ARM64 pinned Connect/plugin image builds and starts; no unrelated services restarted |
| `.tools/member3-venv/bin/python -m member3.cdc configure`; `.tools/member3-venv/bin/python -m member3.cdc status` | Real task 0 RUNNING after configuration settles; canonical unwrapped payload routing |
| `.venv/bin/kubectl --context kind-smart-grid-dev apply -f infra/member3/lease.yaml` | Scoped namespace, precreated Lease, ServiceAccount and get/update-only Role/Binding applied |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3` | Original **42 passed**; expanded pre-live run **48 passed, 1 explicit kind skip**; not the final acceptance command |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control MEMBER3_CDC_TEST=1 MEMBER3_KIND_TEST=1 make member3-test` | **58 passed in 40.44s**, no skips; real PostgreSQL, Kafka Connect and kind |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control MEMBER3_CDC_TEST=1 MEMBER3_KIND_TEST=1 .tools/member3-venv/bin/python -m pytest -q tests/member3/test_cdc_live.py` | **2 passed in 29.45s** after unused trial metadata cleanup; final-schema CDC and worker restart reverified |
| `make contracts BASE_REF=HEAD` | **214 passed**; no executable wire shape changes |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/member1-venv/bin/python -m pytest -q tests/member1/test_edge.py tests/member1/test_operations.py` | **16 passed** |
| `.venv/bin/kubectl --context kind-smart-grid-dev -n smart-grid-control auth can-i get lease/action --as=system:serviceaccount:smart-grid-control:action`; same command with `delete` | `yes` for get; `no` for delete (expected exit 1) |
| `.tools/member3-venv/bin/python` inline gateway-role connection with `search_path=member1_gateway,member3_delivery`, `FencedEdgeState` creation/current read, and attempted SELECT of `member3_delivery.action_commands` | Own gateway state/interface works; Control table SELECT denied |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py` | **10 passed** after documentation updates |
| `.tools/member3-venv/bin/python -m pip check`; `.tools/member3-venv/bin/python -m compileall -q member3 member1/fencing.py tests/member3 tools/member3_setup.py`; `git diff --check` | No broken requirements; compilation and whitespace pass |

Initial live CDC testing failed because pgoutput 17 omits generated columns.
Resolved by retaining the existing physical row `id` in the router header and
using immutable payload `event_id`/`command_id` for logical dedup. No slot or
Connect-offset reset was used. Unused trial column/trigger metadata was removed
without removing any outbox payloads; the final schema matches the checked-in SQL. A subsequent assertion incorrectly expected no
batch duplicates; it now waits for all unique expected events and verifies exact
payloads. Setup's empty topic-creation list was also corrected. The paused-session test
initially expected the generic OperationalError; PostgreSQL reports its specific
IdleInTransactionSessionTimeout class, now asserted explicitly. These failures
are not counted as passing evidence; final acceptance exercises the fixes.

### Current decisions, limits and next milestone

- Lease acquisition and SQL authority are separate transactions. Before serving,
  reverify Lease ownership and commit a monotonically increasing epoch. Domain
  transactions recheck leadership before and after work. Graceful stop revokes
  authority before releasing the Lease. Paused SQL sessions are bounded by idle
  transaction timeout; session loss rolls back simulated effects and frees locks.
- PostgreSQL owns the fenced gateway simulated rate, not a mirror of SQLite.
  Own schema/login plus typed authorization prevents shared-table access. Shared
  effect locks and exclusive epoch locks give a single durable ordering. Physical
  hardware effects remain outside this simulator guarantee.
- Restart holds every old active saga, retains partial results and unknown effects,
  and never rewrites command IDs/epochs/expiry. First authority activation fences
  historical standalone epochs; supplied-epoch callers cannot bypass authority.
- One local control shard, one broker and one PostgreSQL primary; no replicated
  broker availability claim. Local host workers use explicit kind kubeconfig;
  in-pod image/deployment and ServiceAccount credential integration are unverified.
  Lease deletion/recreation fails closed and requires operator recovery.
- Run the CDC failure gate serially: it kills only Member_3 Connect and retains
  synthetic outbox evidence. All test schemas/temporary Leases are removed.
  Static observation fixtures are explicit and become stale; they are not live
  charger observations. Slot/WAL retention and backups require operational work.
- **Next recommended milestone:** live observation adapter and the Member_2 →
  Member_3 → Member_1 pairwise closed loop, using this fenced delivery boundary,
  before full kind packaging and the Member_4 projection/API/UI/identity slice.

## Member_3 Action domain — executed 2026-11-22

| Exact command / check | Result |
|---|---|
| `.venv/bin/python -m venv .tools/member3-venv` | Independent CPython 3.12.9 component environment created |
| `.tools/member3-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --require-hashes -r member3/requirements.txt` | 12 locked packages installed offline; accepted psycopg 3.2.9/jsonschema 4.23.0/pytest 8.3.5 pins and hashes reused |
| `.tools/member3-venv/bin/python -m pip check` | No broken requirements |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml ps --all` | Existing PostgreSQL healthy on loopback 15432; existing unrelated component containers preserved. Docker/host TCP checks required sandbox escalation; no infra restart/reset was performed |
| `.tools/member3-venv/bin/python -m pytest -q tests/member3` (before final added cases, DSN absent) | 2 pure checks passed; then 20 database checks explicitly skipped. Not acceptance evidence |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3` | Initial 22, expanded 36 and final **37 passed** against real PostgreSQL; isolated random schemas removed after each test |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | Initial **37 passed**; post-review **42 passed**, including fixed fixtures, concurrent deliveries, atomic rollback after an outbox insert, optimistic conflicts, stale epochs, restore safety, cooldown and separate-process recovery |
| `.tools/member3-venv/bin/python -m member3 cases` | Lists all 14 fixture scenarios |
| `CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m member3 replay --schema member3_demo --case 'safe restore and restart' > .tools/member3-safe-restore.json` | Durable demo completes; synthetic `member3_demo` schema intentionally retained for user inspection |
| `CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m member3 inspect --schema member3_demo > .tools/member3-inspected.json` | Fresh process returns identical committed state |
| `.tools/contracts-venv/bin/python` inline validation of those two JSON files using `tools.contracts.validate` on every outbox event/key, plus state equality/count assertions | COMPLETED, four commands, 13 semantically valid outbox rows; exact fresh-process equality |
| `make contracts BASE_REF=HEAD` | **214 passed**; frozen schema/OpenAPI/Protobuf compatibility and canonical Member_3 fixture validation pass; no contract shapes changed |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed**; existing infrastructure/preparation/aggregation behavior and documentation links preserved |
| `git diff --check` | Passed |

### Member_3 prepublication review — 2026-11-22

The user authorized review, fixes, commit and a normal push to `main`. Review
found and fixed three issues: a same-window correction could overwrite visible
review hold with NORMAL; delayed results could bypass exhausted deadlines when
no timer ran during downtime; and extremely small/large finite loads could
produce an empty ACKS_PENDING plan (allocation resolution/intermediate overflow).
Four new cases reproduced the failures before the fix. A fifth regression checks
exhaustion of the final retry before TTL. Deadline exhaustion is shared by result
handling and the timer; late effects are recorded while the saga remains held.
Inspection now reads one consistent, read-only PostgreSQL snapshot.

| Exact command / check | Final result |
|---|---|
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; `0 0` before creating the Action commit |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control .tools/member3-venv/bin/python -m pytest -q tests/member3 -k 'review_hold_survives or late_results_cannot or extreme_finite'` | **4 failed before fixes**, proving the regressions |
| `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test` | **42 passed**, no skipped database tests |
| `make contracts BASE_REF=origin/main` | **214 passed**; canonical schemas, inputs and compatibility verified |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py tests/member2/test_aggregation.py` | **64 passed** |
| `.tools/member1-venv/bin/python -m pytest -q tests/member1/test_edge.py tests/member1/test_operations.py` | **16 passed** with localhost permission; initial sandbox run had 15 pass/1 socket-bind permission failure, resolved by rerunning with host access |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py` | **10 passed** after documentation updates |
| `.tools/member3-venv/bin/python -m pip check`; `.tools/member3-venv/bin/python -m compileall -q member3 tests/member3`; `git diff --check` | All pass; no broken requirements or compilation/whitespace errors |

Only Action milestone files and related documentation/test-target changes are
included. Existing Member_1/Member_2 services and local data are preserved.
Publication result is recorded by Git history/remote state; hosted CI is a
separate check, not claimed by these local results.

### Member_3 decisions and limitations

Historical standalone checkpoint; ADR-029 above supersedes its CDC/Lease
future-work items. Other integration/operations limits still apply.

- PostgreSQL is authoritative; inbox, domain state, commands, saga progress and
  outbox commit atomically. A bounded global advisory lock serializes the small
  standalone store; database versions/unique constraints enforce conflicts and
  one active saga per transformer. No SQLite or success-returning relay stub.
- Registry v1 lacks current-rate/online/charging observations. Explicit fresh
  fixture observations supplement canonical capability records; no rate is
  inferred from maximum capability or missing telemetry. This gap and the
  outbox-commit meaning of dispatch states are resolved in affected design/docs.
- Insufficient capacity records review/hold without issuing an inadequate plan.
  Corrections preserve action history; retries preserve command/event IDs and
  original expiry, with at most two total attempts. Physical outbox retry rows
  have distinct delivery IDs. Partial/time-out/late results remain held until
  explicit operator reconciliation; no automatic restore from unknown state.
- Restore requires distinct consecutive post-confirmation recovery windows,
  current matching capabilities/rates and projected restored load below critical.
  Missing/stale state never means zero load or safe restoration.
- Supplied positive epochs are stored/compared. Distributed Lease acquisition and
  durable cluster epoch allocation, Kafka consumers/offsets, Debezium routing,
  live observation adapters, authenticated operator compensation, kind packaging,
  metrics and backup/export remain unimplemented here. Database-server restart,
  actual CDC delivery and end-to-end physical effects are not claimed. Mac ARM64
  offline install is verified; Linux runtime/security scans are future gates.

## Historical minimum-infrastructure verification

**Minimum local development infrastructure is complete and accepted locally.** Clean startup, Kafka publish/consume, four-store persistence/authentication/isolation, container-recreation retention, Toxiproxy outage/recovery, shutdown and disposable reset all pass. No blocker remains in this milestone. See [local dependency runbook](../../infra/compose/README.md). The preparation and executable-contract work is complete locally. No system software was installed, Docker settings/cluster resources were not changed, and no cloud resources or external model calls were made. Contract authoring made no commits or pushes; the user subsequently authorized verification, committing and pushing these changes to main. Only pinned Python contract tools were installed into ignored component venvs. The existing `.venv` preparation tools remain intact.

Executed evidence:

| Exact command / check | Result |
|---|---|
| `uname -m`; `sysctl -n hw.memsize hw.ncpu machdep.cpu.brand_string`; `df -h .`; `memory_pressure` | ARM64 Apple M5, 10 cores, 17,179,869,184 bytes RAM; 258 GiB disk available; 36% free-memory snapshot. sysctl required approved host access after sandbox denial. |
| `git --version`; `.venv/bin/python --version`; `.venv/bin/kind version`; `.venv/bin/kubectl version --client`; `.venv/bin/helm version --short` | Git 2.54.0; CPython 3.12.9; kind 0.30.0; kubectl 1.34.0/Kustomize 5.7.1; Helm 3.19.0 |
| `python3 --version`; `command -v node`; `node --version`; `command -v java`; `java -version`; `colima version`; `colima status` | Global Python 3.14.7 preserved; Node absent; Java launcher present but no runtime; Colima 0.10.3 installed/stopped. Node/Java/Colima are not selected prerequisites. |
| `docker version`; `docker info --format '{{.Architecture}} {{.NCPU}} {{.MemTotal}}'`; `docker desktop kubernetes status`; `.venv/bin/kubectl --context kind-smart-grid-dev get nodes` | Desktop 4.99.0, CLI 29.8.0, Engine 29.9.3; Linux ARM64, 4 CPUs, 6,210,510,848 usable guest bytes; built-in Kubernetes disabled; existing project node Ready/v1.34.0. Docker socket checks required approved host access after sandbox denial. |
| `.tools/contracts-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: --require-hashes -r requirements-contracts.txt --dest .tools/contract-wheels-macos` | All 27 locked distributions have hash-verified compatible wheels |
| Same download with `--dest .tools/contract-wheels-linux --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All 27 Linux x86_64 wheel artifacts resolved and verified; not a Linux execution claim |
| `.venv/bin/python -m venv .tools/contracts-lock-check`; `.tools/contracts-lock-check/bin/python -m pip install --no-index --find-links .tools/contract-wheels-macos --only-binary=:all: --require-hashes -r requirements-contracts.txt`; `.tools/contracts-lock-check/bin/python -m pip check`; `.tools/contracts-lock-check/bin/python tools/contracts.py` | Clean offline hash-checked installation and pip check passed; full contract suite passed at that checkpoint (195 checks before final additional cases) |
| `make contracts`; `make bindings`; `make contracts BASE_REF=HEAD` | Final suite: **212 checks pass**; OpenAPI/schema validation, Protobuf compilation and generated capability consistency pass. Git HEAD has no executable v1 baseline: explicitly reported as initial freeze; deliberate incompatible JSON, OpenAPI and Protobuf changes are detected. |
| `.tools/contracts-venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py`; `git diff --check` | **10 checks pass**; whitespace check passes |

The bundle contains **77 valid JSON examples, 76 invalid JSON examples with precise expected failure reasons, six valid/four invalid Registry examples, 17 stream/control golden scenarios and five agent authorization/evidence scenarios**. It covers stable UUID identities, UTC times, kW signs, partition keys, duplicates, out-of-order samples, exact window/watermark/lateness/freshness/completeness boundaries, corrected histories, checkpoint-output replay, stale/missing capacity, expiry, epochs, scoped read-only tools and evidence grounding. The reference replay is an executable specification, not production aggregation/control code.

## Decisions and limitations

- [ADR-002](../decisions/ADR-002-python-stream-processing.md), [ADR-004](../decisions/ADR-004-python-dashboard.md), [ADR-006](../decisions/ADR-006-local-kind.md) and [ADR-024](../decisions/ADR-024-pinned-local-baseline.md) remain accepted: PyFlink 2.2.1/Python 3.12.9, NiceGUI, container-only JVMs, Docker Desktop/local kind. Upstream Flink/Helm compatibility was rechecked; full application metadata was not re-resolved. No host Java/Maven/Node/npm is required.
- [ADR-025](../decisions/ADR-025-executable-contract-freeze.md) fixes Registry pagination/errors/deadlines, stable window/load UUIDs, corrected-window semantics, command expiry/fencing, control policy defaults, OIDC realm/client/role names, REST/internal errors/idempotency, SSE replay, and agent scope/budgets. The system-design recovery proposal is now accepted at **75% for two fresh COMPLETE windows**. No blocking choice remains in this contract milestone.
- The new independent compiler/validator environment has a portable hashed lock. Application packages remain independently scoped; no PyFlink/Beam dependencies were merged into it.
- CI workflow is authored for standard GitHub Ubuntu runners and its local command sequence passes. Hosted CI results are tracked by GitHub Actions for the published commit; local checks are not a hosted-run claim. The compatibility gate deliberately rejects some safe changes and cannot detect arbitrary changes in business meaning; producer/consumer review remains required.
- Member_1 standalone Kafka routing, durable simulated effects/fencing, Registry local-token authorization and process/container restart proofs now pass (evidence below). PyFlink ARM64 import/checkpoint proof now passes under ADR-027; replicated Kafka recovery, Registry mTLS/production identity, OIDC, SSE, policy state machines, full-stack behavior, CNI allow/deny and Kubernetes PVC I/O/recovery remain unverified. The empty cluster does not establish them.
- Lightweight development remains 4 CPUs/6 GiB VM; staged review remains 8 CPUs/10 GiB on this 16 GiB host. Larger-profile fit, performance, security scans and application/chart/image artifact gates remain unmeasured. Licensing remains the owner's decision before public release.

## Next milestone

**Member_3 standalone control delivery/integration:** implement Kafka fixture
consumption and Debezium outbox publication with duplicate-delivery proof, then
explicit live charger observations and Lease/fencing allocation before pairwise
control acceptance. Retain Member_1 simulation/Registry/observability, Member_2
Kafka/PyFlink and Member_4 projection/API/UI/identity/Copilot boundaries. Operator
compensation and operational packaging remain explicit follow-on work. Member_2
kind/operator deployment and distributed rescaling remain later gates.

## Prepublication verification — 2026-11-21

The user authorized verification and a normal push to `main`. `git fetch origin main` succeeded and `git rev-list --left-right --count origin/main...HEAD` returned `0 0` before creating the contract commit. Review found and fixed two validator edge cases: compatibility now compares JSON value types recursively (including booleans inside arrays), and load/unit checks enforce the documented absolute 1e-9 tolerance without relative tolerance. Four regression cases failed before the fixes and pass afterward.

Executed `make contracts BASE_REF=origin/main` (**212 passed**), `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py` (**10 passed**), `.tools/contracts-venv/bin/python -m pip check` (no broken requirements), and `git diff --check` (passed). Publication state is recorded in Git history and GitHub Actions; no force push is authorized or needed.

## Minimum local development infrastructure — accepted 2026-11-22

Member_2 owns Kafka/topics; Member_3 owns PostgreSQL/control; Member_1 owns Registry
and fault tooling; Member_4 owns projection and Copilot audit. One Kafka 3.9.2 KRaft
broker and PostgreSQL 17.4 primary run in the fixed `smart-grid-local-dev` Compose
project. Toxiproxy 2.12.0 runs only on demand. All bases retain their accepted digest
pins; no runtime/library versions changed. Four service-owned databases have separate
nonprivileged roles, SCRAM host authentication and no cross-store CONNECT. Topics
come from executable `x-events`, with `local-dev.` physical prefixes, three partitions,
RF=1/minISR=1 and 24-hour retention. Consumer-group conventions and safe application
connection examples are documented in the [runbook](../../infra/compose/README.md).

### Resolved startup failure and implementation decisions

Earlier runs on 2026-11-21 stalled in Created. Repeated restarts and API 1.51 did not
fix it. Local-only Docker diagnostics identified a macOS FSEvents watcher stuck in
`EventStreamCreate`/`AddWatch`, with `VolumeShare.Add` and container-start rewrites
waiting for `recomputeWatchesSync`. The earlier inference that every new container
was independently broken was too broad: a volume-free Toxiproxy check passed before
Compose triggered the shared watcher stall, then unrelated starts queued behind it.

The fixed profile has **no runtime host bind mounts**. A small multi-target Dockerfile
copies config into local images from the same pinned upstream bases; `up --build`
uses cache and includes configuration edits. Named volumes still persist data.
One authorized Docker Desktop force stop/start cleared old stuck requests; no
factory reset, software update, storage-engine change or unrelated deletion occurred.
Local diagnostics were not uploaded and their temporary bundle was deleted.

Once startup was unblocked, real Kafka startup exposed missing/wrongly owned data
and runtime-log directories. The derived image prepares `/var/lib/kafka/data` and
`/opt/kafka/logs` for UID 1001, and Kafka runs as that non-root user. Regression checks
cover no host binds/loopback ports and rebuilding configuration images. No full
monitoring stack, application services, cloud or external model integration was added.

### Executed acceptance evidence

| Exact command / check | Result |
|---|---|
| `make infra-reset CONFIRM=smart-grid-local-dev`; `make infra-up` | Clean volumes initialized; Kafka and PostgreSQL healthy; eight canonical domain topics created; all four database logins/queries pass |
| `make infra-health`; `make infra-smoke` | Kafka key/value publish (`acks=all`) and consume pass; writes/reads in four databases pass; four bad-password attempts and all 12 cross-store connections rejected |
| `make infra-verify` | Same Kafka record and all four database rows survive full `down`/`up` container removal/recreation; auth/isolation checks pass again |
| `make infra-faults-up`; `make infra-fault-smoke`; `make infra-faults-down` | Proxy API ready; PostgreSQL query forwarded, rejected during disabled-proxy outage, and forwarded after recovery; proxy removed |
| `.venv/bin/python` inline configuration/endpoint assertions | Compose JSON has no host binds and only loopback ports; host TCP 19092/15432 reachable; exactly eight domain topics after excluding Kafka internal names; `kafka id -u` returns 1001 |
| [Disposable reset acceptance proof](../../infra/compose/README.md#disposable-reset-acceptance-proof) (Python seed, `make infra-down`, volume inspection, confirmed reset, `make infra-up`, Python absence assertions, `make infra-smoke`, `make infra-down`) | Probe topic and rows seeded in all four stores; normal down retains exactly two volumes; reset removes both; startup recreates eight contract topics and probes/smoke tables are absent in all four stores; fresh smoke passes |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml --profile faults ps --all`; `docker volume ls --filter label=com.docker.compose.project=smart-grid-local-dev` | Final shutdown leaves no project containers and retains exactly `smart-grid-local-dev_kafka-data` / `smart-grid-local-dev_postgres-data` |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed**, including reset guard, failure propagation, timeout-byte regression, fault cleanup, image rebuild, no host bind mounts and loopback bindings |
| `make infra-health` after final shutdown; `make infra-reset` without confirmation | Both correctly return nonzero (make exit 2): stopped dependencies reported; destructive reset rejected before Docker |
| `.venv/bin/kubectl --context kind-smart-grid-dev get nodes`; `docker desktop kubernetes status`; `docker ps --all --format '{{.Names}} {{.Status}}'` | Existing kind Ready/v1.34.0; built-in Kubernetes disabled; project containers absent and unrelated resources preserved |
| `make contracts` | **212 passed**; executable contracts unchanged |
| `git diff --check` | Passed |

### Acceptance boundary and remaining work

This requested infrastructure milestone is complete. Full component packaging,
PyFlink native ARM64 proof, domain migrations, CDC, Kafka ACLs, application client
integration, replicated availability, backup/restore, crash recovery, resource/load
measurements and security scans remain later component gates. Host TCP reachability
is proven; independently locked host application client environments are not created
here. Toxiproxy's live fault proof covers PostgreSQL; a Kafka proxy still needs
advertised-listener routing for its owning future fault scenario.

Docker Desktop's built-in Kubernetes remains disabled; the existing kind cluster
is preserved. No system software was installed and no commit or push was made.
Final local state: dependencies shut down, persistent project volumes retained,
Toxiproxy removed. Run `make infra-up` to inspect/reuse them. Next recommended
milestone remains repository foundation/component packaging, not full Kubernetes
or monitoring deployment.

## Infrastructure publication verification — 2026-11-22

The user explicitly authorized committing and pushing this milestone to `main`.
`git fetch origin main` succeeded and `git rev-list --left-right --count origin/main...HEAD`
returned `0 0` before the infrastructure commit. Final publication checks:
`.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py`
(**21 passed**), `make contracts BASE_REF=HEAD` (**212 passed**) and `git diff --check`
(passed). Full live acceptance evidence above remains valid; these checks did not
restart dependencies or alter retained volumes. Publication results are recorded in
Git history; no force push is required.


## Member_1 standalone acceptance — 2026-11-22

### What is implemented

Member_1 owns `member1/`, simulator/Registry images, fixture clients and standalone
tests. Member_2/3/4 applications remain untouched. This is one 20 kW transformer
with one EV, meter and solar device, one owning simulator shard and the existing
local Kafka/PostgreSQL dependencies. Fixed seed/start/run/config reproduce exact
offline JSON and event IDs; live runs use fresh boot IDs and actual UTC. Sampling
defaults to one second, with normal/overload/recovery thirds. The finite waveform
ends after 30 ticks by default; the fixture command consumer stays running.

The edge SQLite WAL/FULL transaction owns the simulated effect, greatest accepted
epoch, command payload/result, and durable result/follow-up outbox. IDs are kept
for the state volume's lifetime. New boots never clear dedup/fencing/rates.
Applied duplicates return ALREADY_APPLIED even after expiry/new leadership;
expired/stale/conflicting/invalid commands cannot repeat an effect. Output replay
retains event IDs and trace headers. Kafka offsets follow acknowledged output;
publication failure leaves pending outputs and offsets replayable. At-least-once
outputs still require downstream event deduplication. Wiping/rolling back the
edge volume voids the restart-safety claim; this is not real-hardware atomicity.

The Registry persists capabilities/registrations, monotonic heartbeats and
operation-ID responses in its own PostgreSQL database. All four generated gRPC
RPCs enforce validation/version conflicts, bounded pagination, freshness,
departure eligibility and service permissions. Client deadlines/retries share a
two-second budget; database/lock waits are bounded. No read returns fabricated
capabilities on dependency failure. The simulator and Registry have separate
runtime locks/venvs and Docker targets; tests have a combined Member_1-only lock.

### Executed commands and results

All commands below were executed in this repository on the existing Python
3.12.9/macOS ARM64/Docker Desktop local profile. Loopback/Docker/network checks
used approved sandbox access; no host software was installed.

| Exact command / check | Result |
|---|---|
| `git status --short`; `pwd`; repository instruction/design/contract/ADR reads | Initially clean working tree; existing contracts take precedence; no unrelated changes present |
| `.venv/bin/python -m venv .tools/member1-venv`; `.tools/member1-venv/bin/python -m pip install --cache-dir .tools/pip-cache 'confluent-kafka==2.8.2' 'psycopg[binary]==3.2.9' 'grpcio==1.85.3' 'protobuf==5.29.6' 'jsonschema==4.23.0' 'pytest==8.3.5' --report .tools/member1-install.json` | Pinned selected libraries resolved/installed only in ignored Member_1 venv; transitive portable locks recorded |
| `.tools/member1-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: -r member1/requirements.txt --dest .tools/member1-wheels-macos` | All 15 distributions downloaded before lock hash authoring |
| `.tools/member1-venv/bin/python -m pip download --cache-dir .tools/pip-cache --only-binary=:all: -r member1/requirements.txt --dest .tools/member1-wheels-linux-arm64 --platform manylinux2014_aarch64 --platform manylinux_2_28_aarch64 --platform manylinux_2_17_aarch64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | Linux ARM64 wheels resolved; these hashes are in all relevant final locks |
| `.venv/bin/python -m venv .tools/registry-venv`; `.tools/registry-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements-registry.txt`; `.tools/registry-venv/bin/python -m pip check` | Clean isolated Registry installation; no Kafka dependency; pip check passes |
| `.venv/bin/python -m venv .tools/simulator-venv`; `.tools/simulator-venv/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements-simulator.txt`; `.tools/simulator-venv/bin/python -m pip check` | Clean isolated simulator installation; no PostgreSQL/gRPC dependency; pip check passes |
| `.venv/bin/python -m venv .tools/member1-lock-check`; `.tools/member1-lock-check/bin/python -m pip install --no-index --find-links .tools/member1-wheels-macos --only-binary=:all: --require-hashes -r member1/requirements.txt`; `.tools/member1-lock-check/bin/python -m pip check` | Clean combined test installation, all hashes verified, no broken requirements |
| `.tools/member1-venv/bin/python -m pip download --no-index --find-links .tools/member1-wheels-linux-amd64 --only-binary=:all: --require-hashes -r member1/requirements.txt --dest .tools/member1-hash-check-linux-amd64 --platform manylinux2014_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All 15 Linux x86_64 wheels hash-verified; this is resolution, not execution on that architecture |
| `make bindings`; `make contracts BASE_REF=HEAD` | **213 passed**; unchanged executable v1 definitions; new test validates real Member_1 telemetry/results/command fixtures with the frozen semantic validator |
| `make infra-up` | Existing Kafka/PostgreSQL healthy, eight canonical topics and all four database logins/queries pass; other stores/resources preserved |
| `make member1-test` | **14 passed** at this checkpoint; later event-ID and transaction-rollback checks are included in the final 23-test live run |
| `make member1-live MEMBER1_PYTHON=.tools/member1-lock-check/bin/python` | Final complete suite **23 passed**: deterministic scenarios, partition routing, output ACK/offset order, abrupt process exit, rollback on result-store failure, restart dedup/epochs, malformed/expired/conflicting/stale commands, Registry RPC/auth/version/pagination/deadline/concurrency/outage behavior and health/metrics |
| `make member1-build`; `make member1-up` | Separate ARM64 simulator/Registry images build from digest-pinned Python; hash-checked runtime installation; both containers healthy, UID 10001, read-only root filesystems, no host binds |
| `.tools/member1-venv/bin/python -m tests.member1.container_smoke` | All four Registry RPCs; real command/results and caused telemetry; container recreation retains Registry data and edge dedup/fence; duplicate, stale, expired and explicit restore outcomes pass; boot changes and both health/metrics endpoints verified |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed**; previous preparation/dependency behavior preserved |
| `git diff --check` | Passed |

### Resolved issues, decisions and limitations

- First sandboxed package/socket attempts were denied; approved isolated installs
  and local-network checks succeeded. No system/global pip was used.
- Host port 50051 was already occupied. Registry uses **127.0.0.1:15051** externally
  and 50051 inside its container; the unrelated listener was not stopped.
- A concurrently running demo produced indistinguishable extra results in the
  initial live test. Tests now own uniquely prefixed disposable topics and delete
  them in fixture teardown. A subsequent fresh-topic leader-metadata race was
  fixed with a bounded metadata wait. Three topics from the failed setup were
  removed explicitly. Final passing results supersede those earlier failures.
- The preliminary macOS/ARM64-only lock correctly rejected x86_64 wheels. Their
  exact versioned wheel hashes were added and the final cross-platform lock was
  verified offline. No library version was loosened.
- ADR-026 resolves the package layout and durable simulator boundary. The matrix's
  old gRPC runtime row now follows ADR-025's already accepted 1.85.3 runtime,
  keeping the compiler at 1.71.0. No executable contract was weakened.
- One simulator owns the fixture fleet. Horizontal scaling/device partition
  assignment, thousands-of-devices load, replicated Kafka availability,
  Kubernetes deployment/PVC/CNI gates, mTLS/production identity, Registry backup
  acceptance, shared monitoring/OTel exporters, scans/SBOMs and hosted CI execution
  remain later gates. Local-token authorization is tested; it is not mTLS.
- Telemetry has bounded native buffering/retries and visible delivery failure,
  not a durable telemetry spool. Result history is durable and not pruned; disk
  sizing/retention reconciliation is a later operational gate. No performance or
  full-platform acceptance claim is made.

Next recommended milestone: **Member_2's native ARM64 PyFlink packaging proof and
standalone aggregation slice**, using the frozen Member_1 topology/telemetry
fixtures. Keep Member_3 and Member_4 independent. Do not broaden this change into
full platform deployment or monitoring-stack installation.


Final local state: Kafka, PostgreSQL, Registry and simulator are healthy and left
running for inspection; dependency volumes and `smart-grid-local-dev_member1-edge`
are retained. No disposable `local-dev.member1-test.*` topics remain. Stop the
applications with `make member1-down`, then dependencies with `make infra-down`.
The previously occupied host port and existing kind cluster were not modified.

Final `docker inspect` assertions confirm health, UID `10001:10001`, read-only
root filesystems and absence of bind mounts. Running image IDs:

- Registry: `sha256:f0b35236fae7c3b571ca30932d098733f1a12398f66b3a1f7e5eb50fb51a9849`
- Simulator: `sha256:223d6006914a8ae9823dc1f8d1648d40957ad4aa9fa87d590964b0df1b3f7c54`

## Member_1 publication verification — 2026-11-22

The user explicitly authorized committing and pushing this milestone to `main`.
`git fetch origin main` succeeded; `git rev-list --left-right --count origin/main...HEAD`
returned `0 0`. Before publication, `make contracts BASE_REF=HEAD` passed **213**
checks, `make member1-live MEMBER1_PYTHON=.tools/member1-lock-check/bin/python`
passed **23** tests, and `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py`
passed **21** tests. `git diff --check` passed. These reruns do not claim hosted
CI execution. The normal push result is recorded in Git history; no force push
is authorized or required.

## Member_2 standalone aggregation — accepted 2026-11-22

Member_2 owns the new `member2` package, independent 56-distribution hash lock,
native ARM64 container, Compose Flink processes, golden tests and Kafka recovery
probe. Member_1 simulation/Registry/observability, Member_3 control/PostgreSQL/
Debezium and Member_4 projection/API/UI/identity/Copilot ownership is preserved.
No executable schema changed and no other component's internal code is imported.

### Executed commands and evidence

| Exact command | Result |
|---|---|
| `git status --short`; `rg --files`; reads of `AGENTS.md`, master/status/version guides, stream/event contracts, relevant accepted ADRs, design/testing plans and executable reference/fixtures | Initial working tree clean; existing implementations/contracts inspected before editing |
| `docker info --format '{{.Architecture}} {{.MemTotal}}'`; `docker images --format '{{.Repository}}:{{.Tag}}'` | Native aarch64, 6,210,510,848 guest bytes; existing images/services preserved. Initial socket denial resolved through authorized Docker access |
| `docker pull flink:2.2.1-scala_2.12-java17`; `docker buildx imagetools inspect flink:2.2.1-scala_2.12-java17` | Selected image includes native ARM64; immutable index digest recorded in matrix/Dockerfile |
| `docker build -f member2/Dockerfile -t smart-grid-local-dev/member2:v1 . > .tools/member2-build-final.log 2>&1`; `make member2-build` | Pass. Hash-verified published sources/wheels, native PyFlink source wheel, offline runtime install, `pip check`, verified connector checksum and ARM64 import |
| `make member2-test` | **28 passed**: all 15 stream golden scenarios plus invalid-input isolation, boot/latest ordering, finite-state limit, topology identity validation, superseded late-count behavior, audit dedup and cross-transformer watermark isolation |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pytest -q tests/member2/test_aggregation.py` | **28 passed** in the native runtime image |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pip check` | No broken requirements |
| `.venv/bin/python tools/member2_live.py --run acceptance02` | **PASS**, real Kafka/Flink job `a6c7cd84e5628384fd3e49ca6bf310f5`: checkpoint 2 completed; revision 1 observed in an open transaction before crash; SIGKILL/start of TaskManager restored one checkpoint; committed readers saw exactly revision 0 / 15 kW and revision 1 / 17 kW. Replayed inputs did not double count; invalid JSON reached DLQ and >30s reading reached late audit |
| `.venv/bin/python tools/member2_live.py --run guardcheck` | Expected exit 1: active-job guard refuses before any Compose mutation; current job preserved |
| `make contracts`; `make contracts BASE_REF=HEAD` | **213 passed** each; schema/OpenAPI/Protobuf and compatibility gate pass; executable v1 contracts unchanged |
| `.venv/bin/python -m pytest -q tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **21 passed** |
| `.venv/bin/python -m pytest -q tests/member2/test_aggregation.py tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **49 passed** |
| `git diff --check` | Passed |

Earlier development attempts failed and were fixed: Docker's existing context
allowlist excluded Member_2; PyFlink 2.2 uses
`set_externalized_checkpoint_retention`; initial TaskManager memory fractions
exceeded the configured total; published sdist hashes must be checked before
compilation, while locally built wheels require their own manifest during offline
installation. Recovery also exposed an overly tight ten-second heartbeat under
simultaneous build load; the final profile uses sixty seconds. The final clean
acceptance above uses the corrected configuration and locked image.

### Decisions, limitations and inspection

[ADR-027](../decisions/ADR-027-member2-standalone-aggregation.md) records the
explicit managed-state event-time clock, inclusive `ingested_at` lateness,
selected-reading revision fingerprints, fixed topology and transactional sink
boundaries. The design's solar equation is corrected to meter + EV minus the
reported positive solar magnitude. Runtime replay of the historical fixture is
STALE/OLD_RESULT; golden tests use the frozen evaluation UTC for freshness cases.

This is one finite-run aggregation task, retaining all dedup/window state up to
100,000 unique valid IDs; at the limit it fails rather than corrupting loads.
Rescaling/throughput, native Flink watermark display, full-session HA/automatic
JobManager restore, kind/operator deployment, replicated Kafka availability,
security scans/SBOM, tracing and monitoring-stack integration are unverified or
deferred. Custom semantic watermark is exposed as an operator metric. Sink
transaction timeout is 300s versus a 60s checkpoint timeout: recovery must remain
inside that budget and consumers must use `read_committed`. Three output topics
do not have cross-topic atomic visibility. No platform-wide exactly-once claim
is made. Source-build tools/apt packages are not a bit-reproducible toolchain;
the runtime artifact inputs are hash checked and compiled hashes retained.

The live job and [Flink UI](http://127.0.0.1:18081) remain available for inspection;
its checkpoint volume and four topics per test run persist. Member_1 applications,
PostgreSQL, Kafka data and the existing kind cluster were not removed or reset.
Stop only Flink with `make member2-down`; rerun with a new run ID using
`make member2-live`. See [the runbook](../../member2/README.md) for all entry points.
No remaining blocker for this requested standalone slice. Next recommended work
is Member_3 standalone deterministic control against load fixtures. No commit or
push was made.

## Member_2 review and publication verification — 2026-11-22

The user authorized correctness/contract review, fixes, then commit and normal
push to `main` if checks pass. Review found and fixed these issues before
publication:

- The independently locked jsonschema runtime lacks its optional RFC3339 checker.
  Malformed event/ingestion/departure timestamps could be accepted. A local UTC
  date-time format checker now validates every schema timestamp without a new
  dependency; regression cases failed before the fix and pass afterward.
- Extreme but structurally valid timestamps could overflow datetime arithmetic
  and restart the job repeatedly. Difference-based comparisons preserve exact
  boundaries; an unrepresentable window end reaches DLQ before state mutation.
  Selected totals/utilization that overflow finite JSON numbers are likewise
  rejected before changing dedup/window state. Valid very-late finite readings
  remain audit events.
- The real recovery probe waited for a later checkpoint but did not actually
  rescan output. It now rescans committed output, asserts exactly one DLQ and
  late-audit event, and verifies the exact restored checkpoint ID.
- Required accepted-late count and watermark-delay gauges were absent. Added
  both and checked their values through the real recovered operator's REST
  metrics. REST discovery is asynchronous; the probe now waits with a deadline.
- The new Ubuntu CI job used the Mac-specific preparation lock. It now uses
  `member2/requirements-test.txt`, a portable subset of the same runtime pins.
  Clean Mac installation/execution and Linux x86_64 wheel resolution pass.

Executed review evidence:

| Exact command | Result |
|---|---|
| `make member2-test` | **43 passed**, including regression failures reproduced before fixes |
| `make contracts BASE_REF=HEAD`; `make contracts BASE_REF=origin/main` | **213 passed** each; executable v1 definitions unchanged |
| `.venv/bin/python -m pytest -q tests/member2/test_aggregation.py tests/test_local_infra.py tests/test_preparation_docs.py tests/test_preparation_environment.py` | **64 passed** (43 aggregation + 21 repository checks) |
| `.venv/bin/python -m venv .tools/member2-test-check`; `.tools/member2-test-check/bin/python -m pip install --only-binary=:all: --require-hashes -r member2/requirements-test.txt`; `.tools/member2-test-check/bin/python -m pip check`; `.tools/member2-test-check/bin/python -m pytest -q tests/member2/test_aggregation.py` | Clean independent hash-checked installation, no broken requirements, **43 passed** |
| `.venv/bin/python -m pip download --only-binary=:all: --require-hashes -r member2/requirements-test.txt --dest .tools/member2-test-wheels-linux --platform manylinux_2_17_x86_64 --platform manylinux_2_28_x86_64 --implementation cp --python-version 3.12 --abi cp312 --abi abi3 --abi none` | All ten pure-test dependencies resolve/hash-verify for Linux x86_64; not a Linux execution claim |
| `make member2-build > .tools/member2-review-final-build.log 2>&1`; `make member2-down`; `.venv/bin/python tools/member2_live.py --run review02` | Native ARM64 build and real Kafka/Flink acceptance pass. Job `f18edd78dfdd70ff0ff8c337fcd32e9f` restores checkpoint 2 after SIGKILL with revision 1 in an open transaction. Both output scans show only revisions 0/1 at 15/17 kW; exactly one DLQ and one too-late audit event |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pytest -q tests/member2/test_aggregation.py`; same prefix with `-m pip check` | **43 passed** in native runtime; no broken requirements |
| `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python tests/member2/live.py metrics review02` | Required REST gauges visible: accepted late count **1**, semantic watermark **1794249005000 ms**, positive watermark delay. First immediate inspection saw no metrics before asynchronous discovery; bounded retry succeeds |
| `git fetch origin main`; `git rev-list --left-right --count origin/main...HEAD` | Fetch succeeded; **0 0** before publication |
| `git diff --check` | Passed |

No unresolved correctness finding remains for the documented finite-run slice.
The single-task/run limit, transactional Kafka recovery conditions, lack of
cross-topic atomic visibility, and deferred kind/scaling/whole-cluster HA gates
remain unchanged. Hosted CI is authored, not claimed as locally executed. The
review02 job/UI and checkpoints remain available; unrelated services and kind
are preserved. Commit/push outcome is recorded in Git history; no force push is
authorized or needed. Next milestone remains Member_3 standalone control.
