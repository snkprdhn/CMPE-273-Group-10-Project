# Smart Grid / EV Fleet Energy Balancing Platform

A local Kubernetes demonstration of a simulated transformer overload. EV, meter, and solar readings flow through Kafka and PyFlink. The Action service commits a throttle plan and outbox records to PostgreSQL. Debezium publishes the commands, the fenced simulated gateway applies them, and measured EV demand falls before charging is restored. FastAPI and NiceGUI show the resulting state and action history. Operators can open a read-only investigation backed by a deterministic mock.

Start with the [master guide](docs/MASTER_GUIDE.md): architecture, data flow, design decisions, the hosted AKS demo, operations and the demo script.

## Reproduce the local demo

Use Docker Desktop on Apple Silicon with its built-in Kubernetes disabled. The documented demo budget is **8 CPUs and 10 GiB** for Docker Desktop. Stop competing profiles and keep the host awake during the run. Follow [local prerequisites](docs/implementation/LOCAL_PREREQUISITES.md#recreating-the-environment) for Python 3.12.9, kind 0.30.0, kubectl 1.34.0, and Helm 3.19.0. Run these commands from the repository root after creating `.venv` and installing those tools.

```sh
.venv/bin/python -m venv .tools/contracts-venv
.tools/contracts-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r requirements-contracts.txt
.venv/bin/python -m venv .tools/dashboard-test-venv
.tools/dashboard-test-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r dashboard/requirements-test.txt
PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m playwright install chromium
.venv/bin/python -m venv .tools/copilot-venv
.tools/copilot-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r copilot/requirements.txt
.venv/bin/python -m venv .tools/projection-venv
.tools/projection-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r projection/requirements.txt -r observability/requirements.txt

export KIND_NAME=smart-grid-kind-dev-demo01
make kind-create
make kind-install
make kind-deploy
make local-acceptance
make kind-inspect
make kind-open-ui
```

Open [GridBalance](http://127.0.0.1:18086). Sign in as `operator` with the password in `.tools/kind-dev-demo01/local.json`. Open **Alerts & actions**, select the completed action, and choose **Investigate action**. The one-shot scenario runs under `make local-acceptance`. Scenario and policy writes in the live console return 503.
Once the simulator stops reporting, the dashboard can mark current telemetry stale while retaining the completed action and its history.

Acceptance checks the closed loop, duplicate-safe effects, projection replay, live REST/SSE and browser sign-in, NetworkPolicy and RBAC, grounded mock investigation, Flink TaskManager recovery, PostgreSQL export and restore, and a correlated SQL timeout incident. Results and screenshots are under `.tools/kind-dev-demo01/`. Keep that directory private. The [master guide](docs/MASTER_GUIDE.md) explain what to show and where to find it. The [implementation status](docs/implementation/IMPLEMENTATION_STATUS.md) records what ran and any open gates.

Press Ctrl+C to close the UI port-forward. Stop the cluster to retain its storage, or reset after keeping the evidence and backups you need.

```sh
make kind-stop
make kind-reset CONFIRM="$KIND_NAME"
```

Reset deletes the named node and its PVCs, removes the rehearsal's image tags, and keeps local evidence. Use a new suffix for a new acceptance run. See [kind troubleshooting](infra/kind/README.md) and [recovery drills](infra/kind/README.md#recovery-drills).

## Development and design

- Read the [master guide](docs/MASTER_GUIDE.md) for the architecture, data flow, design decisions, demo script and operations. The [documentation index](docs/README.md) lists the rest.
- Run `make contracts` for offline schema, fixture and compatibility checks ([contract bundle](schemas/README.md)).
- Run `make delivery-render`, `make delivery-images` and `make delivery-scan` for packaging checks. GitHub Actions runs contract, component, Kafka and PostgreSQL, browser, image and manifest checks. The full suite uses local Docker Desktop.
- Run a single component's tests with the targets in the [test guide](tests/README.md), or use the [three-broker review profile](infra/kind/README.md#three-broker-review-profile).
- Pins are in the [version matrix](docs/implementation/VERSION_MATRIX.md). Executed evidence is in the [implementation status](docs/implementation/IMPLEMENTATION_STATUS.md).

The hosted demo runs on one AKS review cluster. The [AKS runbook](infra/aks/AGENT_RUNBOOK.md) covers rollout by image digest, checks and rollback.

The demo uses synthetic devices. Real utility equipment and external model calls are out of scope. The [measured load report](docs/testing/LOAD_TEST_REPORT.md) retains the failed concurrent experiment and its corrected standalone HTTP baseline.
