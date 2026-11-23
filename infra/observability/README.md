# Local observability — Member_1

Member_1 owns collection/Registry/simulation, Member_2 Kafka/PyFlink,
Member_3 control/PostgreSQL/Debezium, Member_4 projection/API/UI/identity/Copilot.
See [ADR-035](../../docs/decisions/ADR-035-local-observability.md) for exact
trace and metric semantics and [status](../../docs/implementation/IMPLEMENTATION_STATUS.md)
for executed evidence. All commands run from the repository root.

## Run and inspect

The existing kind deployment flow installs the stack before application
workloads. Use one Docker Desktop VM/profile at a time; built-in Kubernetes
stays disabled. Preserve existing standalone Flink work with a canonical
savepoint before stopping competing runtimes. No command here deletes PVCs.

```sh
make KIND_NAME=smart-grid-kind-dev-obs kind-create kind-install kind-deploy
make KIND_NAME=smart-grid-kind-dev-obs kind-smoke kind-measure
make OBSERVABILITY_CONTEXT=kind-smart-grid-kind-dev-obs observability-inspect
```

To add/reconcile only observability on an existing developer kind cluster:

```sh
make OBSERVABILITY_CONTEXT=kind-smart-grid-kind-dev-obs observability-install
```

Archives are cached under ignored `.tools/kind-downloads` and verified on every
use. `make observability-render` renders all three pinned charts offline once
cached. The stack uses a local 2 GiB Prometheus PVC, 2 GiB Tempo PVC and 1 GiB
Grafana PVC, with finite resource limits and 24-hour metric/trace retention.
These are storage requests, not preallocated host RAM. Grafana credentials are
Helm-generated in the local Kubernetes Secret; do not paste them into evidence.

```sh
.venv/bin/python -m tools.observability open-grafana \
  --context kind-smart-grid-kind-dev-obs --kubeconfig .tools/kind-dev.kubeconfig
```

Open <http://127.0.0.1:13000>. In a separate terminal, obtain the local admin
password for your own login with the following command (do not save its output):

```sh
.venv/bin/kubectl --kubeconfig .tools/kind-dev.kubeconfig \
  --context kind-smart-grid-kind-dev-obs -n observability \
  get secret monitoring-grafana -o jsonpath='{.data.admin-password}' | base64 -d
```

The five provisioned dashboards are **Transformer load and demand reduction**,
**Kafka lag and Flink health**, **Action outcomes and leadership**, **API and SSE
health**, and **Resource saturation and proposed SLOs**. Grafana's Tempo data
source opens collected traces. Inspect correlated JSON with `kubectl logs` for
the relevant platform pod, bounded by `--since`, `--tail` and trace ID. There is
no separate log backend. Missing panels are unknown, never zero/healthy.

## Seeded incident packet

Install the additive SDK lock in the existing independent Member_4 venv, then
run against a deployed kind PostgreSQL/Collector/Prometheus/Tempo:

```sh
.tools/member4-venv/bin/python -m pip install --require-hashes -r observability/requirements.txt
.venv/bin/python -m tools.observability incident \
  --context kind-smart-grid-kind-dev-obs --kubeconfig .tools/kind-dev.kubeconfig --run incident01
```

This command manages loopback port-forwards (14318/19090/13200/15434), obtains
only the projection role credentials in memory, and starts the real independent
API on loopback port 18094 with an explicit local Viewer mock. It creates a
new `incident_<run>` schema, replays the fixed contract fixtures, samples five
baseline queries, locks only that schema's metadata table, observes a real
three-second database timeout/HTTP 503, rolls back, and requires three successful
sub-second reads. It queries the real metrics/trace backends and rejects missing
or mismatched evidence. The synthetic fixtures are labeled; this incident does
not claim a concurrent physical-control effect or sustained load baseline.

The private packet and correlated JSON log are under
`.tools/observability-incidents/<run>/`. The packet includes image IDs, collection
context, commit/dirty state, technical observations, business snapshot impact,
and measured recovery intervals. Use a fresh run name; it refuses to overwrite
an existing packet or schema. Run-owned schemas/evidence remain for inspection;
cleanup is deliberate after retaining evidence. Existing component stores are
never truncated. For a separately running local PostgreSQL, set `PROJECTION_DSN`
in your environment; collection still runs in kind and that mixed runtime is
labeled explicitly in the packet.

## Independent checks and instrumentation

```sh
.venv/bin/python -m venv .tools/observability-venv
.tools/observability-venv/bin/python -m pip install --require-hashes -r requirements-dev.txt -r observability/requirements.txt
make observability-test observability-render
```

For a standalone service, install the same additive lock in **its** venv and
set `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:14318` while forwarding the local
Collector. Without the endpoint, component tests/domains remain independent of
the SDK. In kind, `telemetry-runtime` and the Flink pod template set the internal
endpoint. Exports have bounded queues and timeouts; collection failure cannot
make a control operation succeed or justify retrying an uncertain effect.

The proposed query SLO counts each non-health/non-SSE GET once; a good response
is <400 **and** has response-start latency <=1 second. Eligibility includes
invalid/unauthorized queries as bad; there are no hidden exclusions. The panel
uses the joint success/latency histogram numerator and all eligible queries as
denominator, so slow failures are counted once. The window is five minutes;
zero traffic/missing samples yield unknown. Target: 99.5%, proposed only.
SSE duration is excluded. Business gauges show the last observed accepted
projection/load; use stream freshness and query time to interpret them. Domain
outcome counters count observed projection changes and are not an authoritative
persistent count of all command attempts. Other SLOs await their own baselines.
