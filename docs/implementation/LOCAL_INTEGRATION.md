# Local component integration

Member_1 owns simulation, Registry and observability. Member_2 owns Kafka and
PyFlink. Member_3 owns Action, PostgreSQL and Debezium. Member_4 owns projections,
API, UI, identity and Copilot.

Start the existing local profiles for Kafka/PostgreSQL, Registry, Flink and
Kafka Connect. The runner uses the existing kind cluster for the Action Lease
and starts host Python processes and an additional Compose TaskManager for
this run. The [kind runbook](../../infra/kind/README.md) deploys the same loop as Kubernetes workloads with real Keycloak browser authentication. The read-only
browser check uses an explicit local Viewer identity with the real API and
NiceGUI. The Keycloak suite remains independently runnable.

Install the Member_3 lock and Member_4 optional live-context lock into their
existing environments and regenerate the shared Registry bindings:

```sh
.tools/member3-venv/bin/python -m pip install --require-hashes -r member3/requirements.txt
.tools/member4-venv/bin/python -m pip install --require-hashes -r member4/requirements-live-context.txt
make bindings
```

Choose a new lowercase run identifier each time. Prepare the initial roles
using the delivery runbook before the first integration run.

```sh
.tools/member3-venv/bin/python -m tools.integration_setup --run demo01
PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/member3-venv/bin/python -m tools.integration --run demo01 --config .tools/integration-config/demo01.json --start-flink
```

Inspect `.tools/integration/demo01/report.json` and the adjacent process logs
and Chromium screenshot. The config file contains disposable credentials and
is stored separately under ignored `.tools/integration-config`. Sample power
values are fixed. Seed 273 is part of the generated telemetry identities. The
report retains the emitted input events and actual timestamps, load and saga
events, duplicate acknowledgement, expected totals and final gateway state.

Run each boundary assertion separately against the live capture with:

```sh
for gate in member1-member2 member2-member3 member3-member1 members23-member4; do
  .tools/member3-venv/bin/python -m tools.integration --check-gate "$gate" --report .tools/integration/demo01/report.json
done
```

Those commands recheck distinct boundaries in a shared live capture. They
do not start four independent deployments. The standalone component suites
remain available for isolated diagnosis.

The runner stops its workers and its own Flink job and TaskManager. It preserves
SQL and Kafka evidence. Each connector consumes one of the local PostgreSQL
replication slots. After retaining the report, stop and remove only that
run's connector and release its inactive slot to free capacity. Do not remove
the shared `member3_outbox` slot. On the disposable profile, for run `demo01`:

```sh
curl -fsS -X DELETE http://127.0.0.1:18083/connectors/integration_demo01
docker exec smart-grid-local-dev-postgres-1 psql -U local_admin -d local_dev_control -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='integration_demo01' AND NOT active"
```

If the connector is still disconnecting, wait until the run slot is inactive
before releasing it. Keep the isolated schemas, publication and topics while
inspecting the evidence.
