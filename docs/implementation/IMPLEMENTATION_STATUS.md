# Implementation status

Updated **2026-11-27 (America/Los_Angeles)**. The system is deployed on the AKS review cluster and in soak toward the Mon 2026-11-30 demonstration. Read the [master guide](../MASTER_GUIDE.md) for the architecture and the demo script. Nothing is committed or pushed. The detailed per-milestone logs from before this cleanup are kept privately in `.tools/backup/`.

## Current milestone: repository cleanup and naming — 2026-11-27

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
