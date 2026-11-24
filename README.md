# Smart Grid / EV Fleet Energy Balancing Platform

A local Kubernetes demonstration of a simulated transformer overload. EV, meter, and solar readings flow through Kafka and PyFlink. The Action service commits a throttle plan and outbox records to PostgreSQL. Debezium publishes the commands, the fenced simulated gateway applies them, and measured EV demand falls before charging is restored. FastAPI and NiceGUI show the resulting state and action history. Operators can open a read-only investigation backed by a deterministic mock.

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
.venv/bin/python -m venv .tools/member4-venv
.tools/member4-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r member4/requirements.txt -r observability/requirements.txt

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

Acceptance checks the closed loop, duplicate-safe effects, projection replay, live REST/SSE and browser sign-in, NetworkPolicy and RBAC, grounded mock investigation, Flink TaskManager recovery, PostgreSQL export and restore, and a correlated SQL timeout incident. Results and screenshots are under `.tools/kind-dev-demo01/`. Keep that directory private. The [demo script and evidence index](docs/demo/FINAL_DEMO.md) explain what to show and where to find it. The [implementation status](docs/implementation/IMPLEMENTATION_STATUS.md) records what ran and any open gates.

Press Ctrl+C to close the UI port-forward. Stop the cluster to retain its storage, or reset after keeping the evidence and backups you need.

```sh
make kind-stop
make kind-reset CONFIRM="$KIND_NAME"
```

Reset deletes the named node and its PVCs, removes the rehearsal's image tags, and keeps local evidence. Use a new suffix for a new acceptance run. See [kind troubleshooting](infra/kind/README.md) and [recovery](docs/runbooks/LOCAL_RECOVERY.md).

## Development and design

- Run `make contracts` for offline schema, fixture, and compatibility checks. See the [contract bundle](schemas/README.md).
- Run `make delivery-render`, `make delivery-images`, and `make delivery-scan` for the packaging checks. GitHub Actions runs contract, component, lightweight Kafka/PostgreSQL, browser, image and manifest checks on standard runners. The full suite uses local Docker Desktop.
- Run a selected [independent component](docs/testing/INDEPENDENT_COMPONENT_TESTING.md) or use the [three-broker review profile](infra/kind/README.md#three-broker-review-profile).
- See the [implemented architecture and ownership](docs/demo/FINAL_ARCHITECTURE.md) and the [design audit](docs/demo/DESIGN_AUDIT.md) for supported behavior and remaining gates. The [version matrix](docs/implementation/VERSION_MATRIX.md) records the selected pins.

The demo uses synthetic devices on one computer. Cloud deployment, external model calls, and real utility equipment are deferred. The [measured load report](docs/testing/LOAD_TEST_REPORT.md) retains the failed concurrent performance experiment and its corrected standalone HTTP baseline.
