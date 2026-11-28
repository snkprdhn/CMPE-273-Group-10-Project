# Implementation status

Updated **2026-11-27 (America/Los_Angeles)**. The system is deployed on the AKS review cluster and in soak toward the Mon 2026-11-30 demonstration. Read the [master guide](../MASTER_GUIDE.md) and [demo UI plan](DEMO_UI_PLAN.md) for the architecture, deferred Phase 3 scope, and demo script. Nothing is committed or pushed. The detailed per-milestone logs from before this cleanup are kept privately in `.tools/backup/`.

## Current milestone: Live demo audit fixes (`ui` & Grafana) rolled out to AKS — 2026-11-27

Rolled out UI and Grafana fixes on top of the Phase 1/2 AKS deployment after auditing live `/live`, `/operator`, and `/grafana/` against the PRD and [demo UI plan](DEMO_UI_PLAN.md) (Phase 3 remains explicitly deferred).

- **`/live` (`ui`)**: `Late, accepted` switched to 5-minute window matching injected delays (`565` vs `568` live); added `Watermark delay` row (`10.5 s` vs `10 s` target), `Rejected (DLQ)` `0` fallback when Flink is live, and per-partition/subtask readouts (`P0–P2`, `S0–S1`); mapped `'integration-kind.projection-telemetry'` in the lag bar chart and excluded historical `grid.telemetry.dlq.v1` lag; stopped `STALE` fixture `ev-001` from showing as `Spared`; tuned `1440×900` right-rail heights to clear `.cr-dock`.
- **`/operator` (`ui`)**: parallelized `Operations.snapshot()`, coalesced high-rate SSE bursts (`sse_min_interval=2.0`), bumped `revision` only when `data`/`errors` change, and replaced `0.3s` full `json.dumps` diffs with `current_fingerprint()`. Fixed page-navigation freezes and WebSocket `Connection lost` disconnects.
- **Grafana (`observability/demo_dashboards.py`)**: changed `Online partitions` from `kafka_topic_partition_leader == 1` to `>= 0` (fixes `0` → `24`), excluded `grid.telemetry.dlq.v1` from `SERVICE_LAG`, and shortened stat titles (`demo-grid-control-room`, `demo-grid-pipeline`, `demo-grid-kafka` updated; `demo-grid-platform` & `demo-grid-storage` unchanged).

### Deployed AKS image digests

| Deployment | Image | Rollback template |
|---|---|---|
| `Deployment/projection` & `Deployment/api` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/api@sha256:3bcb2e51e6b9eecd8ab9ce541a6e4e8a0d6cf1981fa58f30f96780e18da4a825` | `.tools/aks-review/phase1-2/{projection,api}-before.json` |
| `Deployment/ui` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/ui@sha256:29cd425fa71b13ec93defab3c4bb315fe7ca6e21fb84ac6a5988feff346f7397` (`ui-fix-20261128060831`) | `.tools/aks-demo/ui-previous-image.txt` (`ui@sha256:1b4b6361…`) & `.tools/aks-review/phase1-2/ui-before-fix.json` |
| `Deployment/fleet` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/fleet@sha256:cc81a1355dcbfc2d87c9314635bc835fa1c79148c59142c90bc994d9dfd8ceb2` | `.tools/aks-review/phase1-2/fleet-before.json` |
| Grafana `demo-grid-*` ConfigMaps (`observability`) | Updated `demo-grid-{control-room,pipeline,kafka}` | `.tools/aks-review/phase1-2/demo-dashboards-before.json` |

### Executed verification

| Target | Command / check | Executed result |
|---|---|---|
| Contracts & manifests | `make contracts && make delivery-render` | `214` contract checks passed (`0.23s`); all 6 Helm charts & compose config `PASS` |
| Observability & dashboards | `make observability-test && make dashboard-test` | `8` observability tests (`0.13s`) and `128` Chromium Playwright dashboard tests (`135.23s`) passed; `observability.demo_dashboards` YAML generation passed |
| AKS health & endpoints | `/healthz`, `/readyz`, `.venv/bin/python -m tools.demo_check` | All checks `PASS` (`output freshness 4.5 s`, `flink consumer lag 162`, `flink busy 22.5%`, `12.1 completed in 10 min`, `0` held plans, `ui-7b46fd678f-zn26q` `READY=true`, `RESTARTS=0`) |
| AKS live browser audit | Playwright Chromium on `/live`, `/operator`, `/grafana/d/{grid-kafka,grid-pipeline,grid-control-room}` | `/live`: `0` errors, `1440×900` fits above dock, `Late, accepted 565`, `Rejected (DLQ) 0`, `Watermark delay 10.5 s`, `P0–P2` + `S0–S1` live, `ev-001` `Stale`/`Normal`. `/operator`: loads in `19.5s` and navigates across all views (`0` disconnects). Grafana: `Online partitions 24`, `integration-kind.projection` lag `4` (no `3.33 Mil`), `0` console errors. |

Limitations / unverified:
- `ev-001` legacy fixture entry remains in the `/devices` DB (`Stale` / `Normal` in `/live` Devices table).
- `/operator` initial load takes ~`19.5s` while paging ~3,600 retained `/alerts`, and logs one NiceGUI `echart.js` `beforeUnmount` error when leaving `Overview` (`0` errors on `/live`).
- `make aggregation-test` (`65 passed`), `make devices-test` (`214 + 35 passed`), and `make copilot-test` (`12 passed`) ran before the UI/Grafana edits cleanly (`dashboard/` and `observability/` changes do not touch those services). Phase 3 PRD features remain deferred per [DEMO_UI_PLAN.md](DEMO_UI_PLAN.md).

## Previous milestone: Phase 1 projection/UI catch-up and Phase 2 fleet controls on AKS — 2026-11-27

Phase 1 (`/live` closed-loop control room, `COPILOT_URL=""`, projection topic split + shared read-lock/per-entity update optimization) and Phase 2 (`devices_fleet_unmanaged_kw{transformer}` Prometheus gauge and `/inject kind=cloud_cover`) are deployed and verified on the AKS review cluster (`smart-grid-aks-review` in `rg-smart-grid-professor-review`, `$85` approved ceiling, spending limit `On`).

### Deployed AKS image digests

| Deployment | Image | Rollback template |
|---|---|---|
| `Deployment/projection` & `Deployment/api` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/api@sha256:3bcb2e51e6b9eecd8ab9ce541a6e4e8a0d6cf1981fa58f30f96780e18da4a825` | `.tools/aks-review/phase1-2/{projection,api}-before.json` |
| `Deployment/ui` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/ui@sha256:1b4b6361bf0d3b211af62cbc937565e977fe5b67c2a9a049f12582984913cdf3` | `.tools/aks-review/phase1-2/ui-before.json` |
| `Deployment/fleet` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/fleet@sha256:cc81a1355dcbfc2d87c9314635bc835fa1c79148c59142c90bc994d9dfd8ceb2` | `.tools/aks-review/phase1-2/fleet-before.json` |

### Executed verification

| Target | Command / check | Executed result |
|---|---|---|
| Contracts & `devices` Phase 2 | `make devices-test` | `214` contract checks passed (`0.24s`); `35` device/fleet tests passed (`11.64s`), covering `fleet_unmanaged_kw` and `cloud_cover` |
| Projection lock, ordering & freshness | `PROJECTION_TEST_DSN=... pytest -q tests/projection/test_api.py test_projection.py test_review.py test_dashboard_boundary.py` + `test_review.py test_kafka_live.py -k "not test_real_kafka"` | `45 passed` (`test_api.py`, `test_projection.py`, `test_dashboard_boundary.py`; 1 transient cold DB connect timeout in `test_review.py` re-verified: `23 passed in 32.77s`); earlier full run also passed `test_real_kafka_crash_restart_replay` (`57 passed`, `1` live Keycloak skipped before test pattern fix) |
| Dashboard Phase 1/1b & manifests | `make dashboard-test`, `test_live.py`, `make delivery-render` | `126 passed`, `17 passed` in Playwright Chromium, `PASS` across 6 Helm charts |
| AKS Phase 1 catch-up & `/live` | `tools.demo_check`, Kafka group lag, `/api/v1/{actions,overview}`, browser check (`21:11–21:12 PST`) | `tools.demo_check` `PASS` (`output freshness 1.9 s`); `integration-kind.projection` lag `0–3`, `integration-kind.projection-telemetry` lag `36–41`; `/actions` & `/overview` `FRESH`; `/live` Decision Log live without `feed stale: projection behind`, active saga stepper, live Decisions and Devices (`FRESH`) |
| AKS Phase 2 gauge & `cloud_cover` | `deploy/fleet` `/metrics`, Prometheus `devices_fleet_unmanaged_kw`, `/live` Chaos Dock, 3 × `10 s` `POST /inject` `cloud_cover` + `tools.demo_check` (`21:12–21:13 PST`) | `25` `devices_fleet_unmanaged_kw` series in Prometheus; `Cloud cover` dock button visible and `"Prometheus history · Without throttling uses fleet offered demand"` active on `/live`; 3 `cloud_cover` injections succeeded, post-run `tools.demo_check` `PASS` (`output freshness 1.4 s`, `plans held for review: 0`) |

Limitations / unverified:
- `ev-001` legacy fixture entry remains in `/devices` alongside the 70 live chargers (`Stale` in the Devices table).
- Phase 2 `cloud_cover` was verified on AKS; local kind `cloud_cover` test and a separate post-Phase-2 1-hour continuous soak were not run. `/ponytail-review` slash command was not invoked.

## Isolate raw telemetry in the projection — 2026-11-27

The existing `integration-kind.projection` group now handles the control and load topics. A second container in the same Pod consumes raw readings under `integration-kind.projection-telemetry`. Both groups resume from existing topic-partition SQL checkpoints. The load worker owns pruning. The Deployment uses Recreate for the transition and the AKS overlay splits the existing CPU and memory limits across workers. The lag panels switch to the new group's telemetry offsets as they appear.

Local verification passed on the dedicated `smart-grid-kind-dev-projection` cluster on Docker Desktop. AKS remains paused for visual inspection and separate rollout authorization. Hosted verification still needs to measure how quickly `/actions` and `/loads` catch up and whether telemetry keeps pace with ingestion. See [the projection runbook](../../projection/README.md#kafka-and-rebuild).

| Command | Executed result |
|---|---|
| `PROJECTION_TEST_DSN=<local projection DSN> PROJECTION_TEST_KAFKA=127.0.0.1:19092 make projection-test` | 53 passed, one live Keycloak test skipped. The control worker projected a revised load and a new action with 5,001 telemetry inputs queued. The telemetry worker then reused the old SQL checkpoint and advanced past the queue, including one logged poison record. |
| `make delivery-render` | PASS. The dev, review, hosted and AKS renders cover all six topics exactly once across two groups. |
| `make observability-test` | 8 passed. |
| `make dashboard-test` | 125 passed after the consumer-lag query change. |
| `make kind-create KIND_NAME=smart-grid-kind-dev-projection` and `make kind-install KIND_NAME=smart-grid-kind-dev-projection` | Passed on the dedicated Docker Desktop cluster. |
| `make kind-deploy KIND_NAME=smart-grid-kind-dev-projection` | PASS. During first boot the Docker node briefly went NotReady and several components restarted on probe timeouts. Deployment recovered after temporarily scaling down the optional monitoring workloads in this cluster. |
| `make kind-smoke KIND_NAME=smart-grid-kind-dev-projection` | Full closed loop PASS, including real throttle and restore, SQL replay equality, action equality across Kafka, REST, SSE and Chromium, RBAC, and network policy. The optional monitoring workloads were paused for the smoke and their replica settings were restored afterward. |
| `make kind-stop KIND_NAME=smart-grid-kind-dev-projection` | Stopped after readiness became unstable again on monitoring restart. The Kubernetes drain timed out and the command used its Docker shutdown fallback. The node is stopped with its volumes retained. The separate fixture preview at `http://127.0.0.1:18191/live` still returned HTTP 200. |
| `kubectl exec` with Kafka `AdminClient.describe_consumer_groups` | Both deployed groups stable. The control group owns the five control/load topics and the telemetry group owns only raw telemetry. The running image's consumer source hash matched the checkout. |
| Prometheus `/api/v1/query` via local port-forward | Both revised lag expressions parsed successfully before the simulator emitted data. |
| Local `Job/projection-lag-check` on kind | Passed. Isolated Kafka topics and SQL schema, 70 managed fixture chargers among 1,000 IDs, 50,000 records initially queued and 201 new readings/s for 36 batches. The cold-start action appeared in PostgreSQL in 4.08 s. The eight later transitions appeared within 1.12 s. Telemetry lag reached 42 records at the 20 s sample and all 57,236 records were checkpointed. The first attempt hit a Decimal-to-JSON error in the temporary reporting script. The corrected run and its inputs are retained in `.tools/kind-dev-projection/`. |

## `/live` closed-loop control room — 2026-11-27

**Status: paused before AKS rollout for local inspection and projection lag.** The Phase 1 changes cover the UI, its tests and documentation, and the hosted `COPILOT_URL` setting. See [DEMO_UI_PLAN.md](DEMO_UI_PLAN.md).

### What changed

| Area | Change |
|---|---|
| Audit fixes | Topology effect `trailLength` 0 (blank chart after a tab switch); Prometheus selectors skip the projection copy of the gauges; Decision Log shows the utilization of the action's trigger window; charts use UTC; monitor-only transformers never request `/loads`; the dock's EV rush picks the least loaded quiet hero; legend and Correctness layout fixes |
| Closed loop | Four-column topology (Grid + Solar, transformers, chargers, control saga) with telemetry, command and acknowledgment lines; a seven-step saga stepper; outbox / CDC and fenced gateway on the pipeline strip with a return rail |
| Asset explorer | Tabs Grid operations, Transformers (25), Devices (1,000), Pipeline health. All 25 transformers with row detail. 70 managed chargers with Throttled / Spared / Normal, polled every 10 s only while the Devices tab is open |
| Data | `tx_history` per transformer (live and backfilled), trigger-window cache, loads fall back to Prometheus when the API has nothing recent |
| Follow-up audit | The saga skips steps without evidence, Spared starts only after confirmed reduction, and an actions API outage freezes the KPIs and shows an amber status. The rush dock retains the accepted target during its active rush, asset detail scrolls into view, and monitor-only transformers omit the shadow comparison. Hosted `/operator` hides Flow Map, Scenarios and Investigate. |

### Executed verification

| Command | Result |
|---|---|
| `make dashboard-test` | 125 passed in the follow-up audit run (includes `/live` tab switching and node selection, outage recovery, hosted-style `/operator` navigation and layout fit at 1440×900 and 1280×720) |
| `.tools/dashboard-test-venv/bin/python -m pytest -q tests/dashboard/test_control_room.py` | 67 passed, including the 60 s selection-cache renewal and insufficient-capacity saga checks |
| `make delivery-render` | PASS |
| `curl -fsS http://127.0.0.1:18191/live` | HTTP 200 for the local visual-inspection preview against the disposable backend |
| Live read-only checks through the Grafana proxy | `max by (transformer) (grid_business_kw{signal="net_load_kw",exported_job!~"projection.*"})` returns 25 series; the unfiltered query also returns 25 here because the projection only exports 5 |
| Live read-only checks through `kubectl port-forward svc/api` | `GET /devices?device_type=EV_CHARGER&limit=200` returns one page (`next_cursor` null) of 71 devices, and the new `FleetData` code parses it and classifies the chargers against real actions |
| Live `Deployment/ui` | `smartgridevreview9e9.azurecr.io/smart-grid-kind/ui@sha256:2d49fbd80363e405535fccaaa7f0b7fe816f6e50242754443fb103a6dab08d3a`, rollback target |

Not run: a kind run, the AKS rollout, `tools.demo_check` for this build, the one-hour soak.

### Limitations found on the live cluster

- The follow-up read-only Grafana audit reported telemetry lag growing from 1,158,655 to 1,684,135 and action lag from 102 to 153 in one hour. It measured about 46 records/s consumed and 201 telemetry records/s arriving, and estimated an ensuing 8–9 minute action delay. The audit used the `integration-kind.projection` consumer group. The current UI changes leave that consumer behavior in place.
- The API's `freshness` is `STALE` for every device and for `/transformers/{id}/loads`. The projection consumer group lags (about 8.9k records on the load topic at 01:10 UTC), and `/loads` held only 5 to 9 windows per 10 minutes for the managed transformers, the newest about nine minutes old. The detail chart therefore falls back to Prometheus history ("loads API behind") and the trigger percentage in the Decision Log is omitted when its window is missing. The cause is in the projection, outside this change.
- The device list has 71 entries: the 70 fleet chargers (`ev-00N-01..14`) plus a leftover `ev-001` from an earlier end-to-end run. The Devices tab shows all 71.
- Live chargers carry no `departure_time`, so the Departure column shows "–". "Spared" cannot use the API's per-device freshness (always `STALE`), so it requires GOOD or ESTIMATED telemetry, charging and controllable.
- Layout fit was measured in Chromium at 1440x900 and 1280x720 against the test backend; the compact layout is chosen once at page load from the viewport height.

## Previous milestone: repository cleanup and naming — 2026-11-27

**Status: done locally and rolled out to AKS. Behavior is unchanged.**

### What changed

| Area | Change |
|---|---|
| Names | The four ownership labels are gone from code, tests, manifests, CI, Make targets and docs. Packages are `devices/`, `aggregation/`, `action/`, `projection/` (tests under `tests/<package>/`). `infra/connect/` holds the Kafka Connect and Debezium material. Compose overlays are `devices.yaml`, `aggregation.yaml`, `aggregation-scale.yaml`, `connect.yaml`. `AGENTS.md` no longer asks to preserve person labels. |
| Runtime identifiers | PostgreSQL tables `devices`, `operations` (Registry) and `edge_state`, `edge_commands`, `edge_outbox` (gateway). Roles `cdc` and `gateway`. Metrics prefix `devices_`. Consumer groups `integration-kind.action.control.v1`, `integration-kind.devices.edge.v1`, `agg-<run>-aggregation-v1`. Flink job name `aggregation-<run>`. Env vars `AUTH_MODE`, `MOCK_IDENTITIES`, `<COMPONENT>_TEST_DSN`. The Kubernetes `grid-owner` labels are removed. |
| Module move | `aggregation/aggregation.py` is now `aggregation/engine.py`. Run as a script by Flink (`python /app/aggregation/job.py`), the old file name shadowed the package. This was found on the first AKS rollout. |
| Removed | `services/` and `simulators/` placeholder READMEs, `tools/e2e_benchmark.py`, `tools/vm_review.py` (no references), the retired Azure VM text in `infra/hosted/README.md`, and the docs listed below. |
| Docs | One [master guide](../MASTER_GUIDE.md) replaces the team, demo, design, runbook, security, course and planning documents. Kept: contracts, ADRs, version matrix, local prerequisites, load report, three design deep dives (data model, UI, Investigator), the agent evaluation plan, component READMEs and runbooks. This file is condensed. |
| Layers | `infra/aks/Dockerfile.layers` and `build-layer.sh` gained a `registry` layer and set the new entrypoints for `action`, `api` and `registry`. |

Deleted documents: `docs/team/*`, `docs/demo/*` (final architecture, final demo, design audit, acceptance plan, design review, Nov 30 plan), `docs/course/*`, `docs/security/*`, `docs/runbooks/*`, `docs/design/` (system design, data flow, failure handling, security, Kubernetes, communication, transactions, coordination, replication, observability, scope, open-source stack, dependency minimization), `docs/testing/` (load-test plan, failure-injection matrix, independent component testing), `docs/implementation/` (master implementation guide, preparation verification, local integration). Their content is condensed into the master guide.

### Executed verification

| Command | Result |
|---|---|
| `make infra-up`, then `ACTION_TEST_DSN=... .tools/action-venv/bin/python -m pytest -q tests/action` | 103 passed, 3 skipped (real PostgreSQL) |
| `PROJECTION_TEST_DSN=... PROJECTION_TEST_KAFKA=... .tools/projection-venv/bin/python -m pytest -q tests/projection` | 51 passed, 1 skipped (real PostgreSQL and Kafka, includes the retention-resume test) |
| `DEVICES_TEST_DSN=... DEVICES_TEST_KAFKA=... .tools/devices-venv/bin/python -m pytest -q tests/devices` | 40 passed |
| `make aggregation-test` | 65 passed |
| `make dashboard-test` | 46 passed |
| `make copilot-test`, `make observability-test` | 12 and 8 passed |
| `make identity-test` (without the live Keycloak case) | 25 passed, 1 skipped |
| `make contracts` | 214 passed, "Schemas, OpenAPI and compatibility gate passed" |
| `.venv/bin/python -m pytest -q tests/test_*.py`, `tests/test_backend_scale.py` in the projection venv | 30 passed, 3 skipped, and 2 passed |
| `make delivery-render` | Manifests rendered and checked |
| Image smoke: import every entry module inside each `clean` layer on `linux/amd64`, and run `job.py --help` | Pass |

Not run: the three-broker kind acceptance (`make local-acceptance`) and `make aggregation-live`. The rename does not touch their code paths beyond identifiers covered above.

### AKS rollout (executed with the spending limit On)

Layers `clean-1` (registry, fleet, action, ui, api) and `clean-2` (aggregation) were built `linux/amd64` over the previous digests and pushed to ACR. Digests are in `.tools/aks-demo/images.json`. The previous digests are in `.tools/aks-demo/pre-clean-digests.txt` and `images-before-clean.json`.

| Digest | Workload |
|---|---|
| `registry@sha256:4db01f91…` | `Deployment/registry` |
| `fleet@sha256:05d41a41…` | `Deployment/fleet` |
| `aggregation@sha256:b35b0d76…` | `FlinkDeployment/aggregation` |
| `action@sha256:6f0f651b…` | `Deployment/action` |
| `ui@sha256:2d49fbd8…` | `Deployment/ui` |
| `api@sha256:79b69a76…` | `Deployment/api`, `Deployment/projection` |

Order and live migrations, all done with the fleet and Action stopped so no message was lost or doubled:

1. Added `MOCK_IDENTITIES` to Secret `mock-identity-runtime` (the old key stays for rollback). Widened the Kafka exporter `groupRegex`.
2. Scaled `fleet`, `action` and `registry` to zero and suspended the Flink job once its consumer lag was 0.
3. Copied committed offsets to the renamed consumer groups with `kafka-consumer-groups.sh --reset-offsets --from-file` (Action control group, device edge group, Flink source group).
4. Renamed the five tables, renamed the two prefixed database roles to `cdc` and `gateway` (SCRAM passwords survive a rename), updated the `EDGE_DSN` Secret and the KafkaConnector `database.user`. The Debezium connector and task stayed `RUNNING`.
5. Applied registry, action, api, projection, ui, fleet and the Flink job by digest.
6. Re-applied the five Grafana dashboard ConfigMaps (new metric prefix).

Findings during the rollout:

- The first Flink start failed with `No module named 'aggregation.aggregation'; 'aggregation' is not a package` (script directory on `sys.path`). Fixed by the module rename and the `clean-2` layer.
- After suspend and resume the operator left the JobManager without its `flink-config-aggregation` ConfigMap. Fix: set `state: suspended`, wait for `lifecycleState: SUSPENDED`, then set `running`.
- Restarting Action while the gateway was down used up three compensation attempts on two plans (`NEEDS_OPERATOR_REVIEW`, `compensations=3`), the same failure as on 2026-11-27 morning. Fix as before: plan `COMPLETED`, `active=false`, `version+1` in the column and in the JSON, chargers back to 7.2 kW. Order for the next restart: bring the gateway (`fleet`) up before Action, or restart Action only while no plan is active.

`tools.demo_check` after the rollout: all PASS (public endpoints, freshness 6 s, Flink lag 179, Flink busy 24%, all pods 0 restarts, Flink `STABLE/RUNNING`, Debezium connector and task `RUNNING`, slot lag 4 MB, volumes 10% / 40% / 6%). At 12:28 PST, after the two held plans were closed, every check passed: 3 actions completed in the last 10 minutes (the window includes the restart), 0 plans held, Flink lag 165, slot lag 10 MB.

Limits of this change: the base layers (published digests) still contain the old source directories under their previous names. They are not imported. A full image rebuild would remove them. The live Kafka CR, StatefulSets and pods keep their old `grid-owner` labels until the next full apply, which would restart Kafka and PostgreSQL pods, so it is deferred. No Azure resource was created.

## Continuous fleet, live control room and rolling retention — 2026-11-26

**Status: deployed on the AKS review cluster and running.** Design record: [ADR-037](../decisions/ADR-037-continuous-fleet-and-rolling-retention.md).

### What changed

| Component | Change |
|---|---|
| Devices | `devices/fleet.py`: continuous fleet of 25 transformers x 40 devices at 200 events/s, five managed transformers with 70 registered chargers following real Action commands, rotating incidents, fault injection and a token-protected control port. `fleet-topology.json` is the single topology source for the simulator and the Flink image. Grafana dashboards in `observability/demo_dashboards.py`. |
| Aggregation | Flink keyed by transformer with bounded per-window state (`aggregation/keyed.py`), `--parallelism`, `--startup`, checkpoint tolerance, sampled tracing and logging. The run-wide ID limit and the global key are gone. Kafka topics keep three hours with size caps. The broker volume is 8 GiB. |
| Action | Skips malformed charger readings, re-emitted load revisions and duplicate results instead of crash-looping. Scopes work to `CONTROL_TRANSFORMERS`. Retries held plans with approved compensation. Debezium heartbeat row so the replication slot advances (it had retained 600 MB of WAL). PostgreSQL WAL bounds. |
| Projection and dashboard | The projection stores only the managed inventory, paces device readings and prunes a 30-minute window. NiceGUI live control room at `/live` (landing page on AKS), shared Prometheus and API poller, fault and incident controls. Grafana read only at `/grafana/`. |

### Defects found and fixed on the live cluster

- Action crash-looped 140 times on one non-UUID telemetry record. Flink was `FAILED` (checkpoint expiry).
- The old 1M benchmark could not pass: its topology file had one transformer, the producer's device IDs matched none (everything went to the DLQ), and the scripts passed flags that did not exist. The scripts were deleted.
- Flink parallelism 3 was a no-op (`set_parallelism(1)` and one global key).
- The Debezium slot lagged 599 MB because only one table is published. A heartbeat row in the same database fixes it.
- PostgreSQL kept 1 GB of WAL for replicas that do not exist (`wal_keep_size`).
- Grafana could not reach Prometheus (a network policy allowed only the UI).
- Prometheus and Grafana URLs in the UI environment named services that do not exist.
- The cluster is at 6 of 6 regional vCPUs (DS3_v2 plus a D2_v2 Flink node), so no node can be added.

### Measured on AKS

| Measurement | Result |
|---|---|
| Steady load | 1,000 devices, 200 events/s, Flink busy about 20%, consumer lag under 500, checkpoint median about 120 ms, state about 3.4 MiB and flat |
| Window to Kafka | 10.4 to 12.6 s |
| Backlog drain | Flink suspended 300 s (108,496 records behind), job `RUNNING` after about 110 s, lag 108,496 to 202 in 93 s, about 1,100 records/s net of live input. 100k records drain in under 5 minutes including the restart. |
| Closed loop | Heroes cycle overload, plan, throttle, confirmed reduction (about 35 kW per transformer), recovery and restore to `COMPLETED` about every four minutes each |
| Fault drills | Duplicate burst 60 s: dropped per minute 118 to 3,048. Out-of-order burst 60 s: corrections per minute 58 to 1,082. TaskManager deleted: job `RUNNING` in 33 s, freshness back under 5 s about 12 s later, no restart of Action, projection or fleet |
| Freshness | `NORMAL`/`FRESH` sampled every 2 s for 20 s, no flicker (before: `STALE` about 40% of samples) |
| Rolling deletion | 40,000 records of 500 B into a topic with a 4 MiB cap and 1 MiB segments: directory 22 MiB to 4.7 MiB, earliest offset 0 to 31,522 |
| Storage after one hour | PostgreSQL 14%, Kafka 5% of 8 GiB, checkpoints 5%, WAL 128 MB, slot lag 4 MB |

### Kafka broker OOM incident — 2026-11-27, and recovery

The broker JVM (`-Xmx384m`) threw `OutOfMemoryError` at 13:10 UTC and hung, so Kubernetes never restarted it. Projection crash-looped and Flink restarted in a loop.

| Change | Evidence |
|---|---|
| `infra/aks/kustomization.yaml`: broker heap 768m, limit 1536Mi, `-XX:+ExitOnOutOfMemoryError` (applied with `tools.aks_demo apply KafkaNodePool/brokers`, hung pod deleted) | `grid-brokers-0` 1/1 with the new limits |
| `projection/consumer.py`: in rolling-scope mode a checkpoint older than Kafka retention resumes at the oldest retained offset and logs `projection_checkpoint_expired`. Full-history mode still raises | Test in `tests/projection/test_kafka_live.py` passes against live Kafka |
| `tools/demo_check.py`: an empty Grafana frame no longer raises `IndexError` | `demo_check` passes |

Follow-ups the same day: the Debezium replication slot was invalidated (`wal_status=lost`) during the outage. Dropping the slot and restarting the task restored alerts and commands. Four plans used up their three compensation attempts while Debezium was down. They were closed by hand and 15 throttled chargers were restored (do not reset `compensations` to 0: it re-issues attempt IDs and crashes Action). A scale-up to 75 x 40 (600 events/s) ran at about 50% Flink busy and was reverted because the Flink node memory is at 87%.

### Limitations

- One Kafka broker with replication factor 1 on AKS. Three-broker evidence is local only.
- The projection and API cover the five managed transformers. The other 20 appear through Prometheus.
- A stateless Flink restart re-emits revisions for open windows. Action keeps the first copy.
- Compensation restores simulated chargers automatically. Real equipment would need an approval policy.
- `tools.demo_check` does not yet check the replication slot's `wal_status`.

### Next

Soak. Re-run `tools.demo_check` periodically. At the Sun 2026-11-29 18:00 freeze record the running digests and allow only rollbacks. Rehearse the demo twice from a cold browser and record a backup video. Ask before committing, extending the review window or tearing anything down.

## Completed milestones

| Milestone | Result | Evidence |
|---|---|---|
| Repository and machine preparation | Small Python and container stack, ADRs, prerequisites, resource profiles, root conventions | [Version matrix](VERSION_MATRIX.md), [prerequisites](LOCAL_PREREQUISITES.md) |
| Local infrastructure | One Kafka broker, PostgreSQL primary, optional Toxiproxy, isolated stores, restart persistence, guarded reset | [Compose runbook](../../infra/compose/README.md) |
| Executable contracts v1 | Eight Kafka events, REST, internal and SSE schemas, seven agent-tool schemas, Registry proto, examples, golden scenarios, compatibility gate | [Schemas](../../schemas/README.md), [ADR-025](../decisions/ADR-025-executable-contract-freeze.md) |
| Devices | Deterministic scenario, durable simulated effects, dedup and fences, PostgreSQL gRPC Registry | [devices](../../devices/README.md), [ADR-026](../decisions/ADR-026-devices-standalone-state.md) |
| Aggregation | Source-built ARM64 runtime, hash locks, golden telemetry, transactional load, DLQ and late output, checkpoint and replay proof | [aggregation](../../aggregation/README.md), [ADR-027](../decisions/ADR-027-standalone-aggregation.md) |
| Action domain | PostgreSQL state, inbox and outbox, deterministic allocation, retry, timeout, hold and restore, optimistic versions | [action](../../action/README.md), [ADR-028](../decisions/ADR-028-action-domain.md) |
| Outbox delivery and fencing | Debezium CDC, kind Lease, durable epochs, atomic fenced gateway | [ADR-029](../decisions/ADR-029-outbox-and-fencing.md), [connect](../../infra/connect/README.md) |
| Projection and API | Replay-safe projections, rebuild, REST, durable SSE, freshness, contract mocks | [projection](../../projection/README.md), [ADR-030](../decisions/ADR-030-projections-api.md) |
| Operator dashboard | NiceGUI console, fixture mode, accessible states, scoped investigation entry | [dashboard](../../dashboard/README.md), [ADR-031](../decisions/ADR-031-operator-dashboard.md) |
| Local identity | Keycloak realm, PKCE sessions, role checks, audit | [identity](../../identity/README.md), [ADR-032](../decisions/ADR-032-local-identity.md) |
| Component integration | Four boundary assertions and a deterministic closed loop on Compose with the kind Lease | [ADR-033](../decisions/ADR-033-live-component-integration.md) |
| Local kind deployment | Helm and Kustomize profiles, Calico, closed-loop acceptance, replay, recovery, backup and restore | [kind](../../infra/kind/README.md), [ADR-034](../decisions/ADR-034-local-kind-deployment.md) |
| Observability | OpenTelemetry, Prometheus, Grafana, Tempo, SQL-lock incident packet | [observability](../../infra/observability/README.md), [ADR-035](../decisions/ADR-035-local-observability.md) |
| Grid Incident Investigator | Seven read-only tools, mock orchestration, audit, grounded UI | [Investigator](../../copilot/README.md), [ADR-036](../decisions/ADR-036-grid-incident-investigator.md) |
| Local load experiment | Gradual measurements and the failed concurrent run retained | [Load report](../testing/LOAD_TEST_REPORT.md) |
| AKS review | One-worker AKS Free cluster in Denmark East, Caddy HTTPS, persistent volumes, Cilium, scoped digest rollouts | [AKS runbook](../../infra/aks/AGENT_RUNBOOK.md) |
| Continuous fleet, control room, retention | Section above | [ADR-037](../decisions/ADR-037-continuous-fleet-and-rolling-retention.md) |
