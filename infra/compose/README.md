# Lightweight local dependencies

Kafka/topics, PostgreSQL, fault tooling, and the projection and Copilot audit stores run here. This is the standalone development
milestone, not the Kubernetes deployment or full foundation milestone. No operators,
monitoring, identity, Connect, Flink, applications or model providers are started.

From the repository root, with Docker Desktop running (built-in Kubernetes disabled)
and the existing preparation `.venv` available:

```sh
make infra-up
make infra-health
make infra-smoke
make infra-verify
make infra-down
```

`up` waits for healthy dependencies, creates missing contract topics and checks all
four database logins. It is repeatable and never reformats an existing Kafka log.
`health` fails for stopped/missing/unhealthy dependencies. `smoke` publishes with
`acks=all`, consumes the same key/value, writes/reads each database and verifies bad
password and cross-database rejection. `verify` additionally removes/recreates
containers with `down`/`up` between writes and reads to prove volume persistence.
It interrupts this development environment; stop component processes first.
Synthetic smoke rows and the unique smoke topic are removed afterward; the empty
`infra_smoke` table remains in each store. This infrastructure probe is not domain
schema validation, application migrations, a throughput test or crash recovery proof.

`down` removes project containers/network, retaining the two named volumes:
`smart-grid-local-dev_kafka-data` and `smart-grid-local-dev_postgres-data`.
To **destroy all data in those two project volumes only**, then start clean:

```sh
make infra-reset CONFIRM=smart-grid-local-dev
make infra-up
```

An absent/wrong confirmation fails before Docker is invoked. No global prune,
external volume, unrelated container, kind cluster, or image is removed. The fixed
project name prevents environment variables or the checkout directory from silently
changing the reset target. Use one checkout at a time for this shared local profile.

## Configuration and isolation

Configuration is deliberately fixed for this disposable profile. The Compose
[Dockerfile](Dockerfile) copies configuration into three local images from the
digest-pinned bases in the accepted version matrix. `up` builds with cache before
starting, so configuration edits take effect without runtime host bind mounts.
This avoids the Docker Desktop macOS file-watcher deadlock observed on this host.
Kafka data and runtime log directories are prepared for UID 1001; the broker still
runs as that upstream non-root user. `.env.example` supplies safe host application
connection examples; Compose does not load application `.env` settings. Passwords
are public disposable placeholders, never reuse them for real data. PostgreSQL uses
SCRAM host authentication. Ports bind only to IPv4 loopback. Ports in use cause
startup failure; stop the conflicting dependency rather than silently selecting a
new endpoint. SQL initialization runs only when the PostgreSQL volume is empty;
future schema changes require owning-service migrations or an explicit reset.

| Setting | Type/default and allowed values | Component | Classification |
|---|---|---|---|
| Environment/project | fixed `local-dev` / `smart-grid-local-dev` | Shared | Non-secret |
| Host Kafka | `127.0.0.1:19092`; container clients `kafka:9092` | Kafka | Non-secret |
| Topic prefix | fixed `local-dev.` | Kafka | Non-secret |
| Topic topology | 3 partitions, RF=1, minISR=1, retention 86400000 ms | Kafka | Non-secret |
| Producer acknowledgement | `all` required | Producer owner | Non-secret |
| Host PostgreSQL | `127.0.0.1:15432`; container clients `postgres:5432` | PostgreSQL | Non-secret |
| Store/role | table below; no cross-store CONNECT | Store owner | Non-secret |
| Password | `<role>_disposable`; admin `local_admin_disposable` | Store owner / PostgreSQL | Disposable local secret placeholder |
| Fault API / DB proxy | `127.0.0.1:18474` / `127.0.0.1:15433` | Fault tooling | Non-secret |
| Provider mode | `MOCK` only | Investigator | Non-secret |

| Database | Login role | Component |
|---|---|---|
| `local_dev_registry` | `registry` | Devices |
| `local_dev_control` | `control` | Action |
| `local_dev_projection` | `projection` | Projection |
| `local_dev_copilot_audit` | `copilot_audit` | Investigator |

Roles own only their database, have no superuser/create-role/create-database/replication
privileges, and have no cross-membership. Each service runs its own migrations.
`local_admin` is bootstrap administration only; never give it to services. Aggregation
has no database role. Logical WAL is enabled for the planned Debezium stack, but
no CDC identity, connector, publication or replication slot is created yet.

## Topics and consumer groups

The runner reads canonical topic names from `schemas/v1/bundle.json` `x-events`.
The physical name is `local-dev.<canonical-topic>`; payload event types and partition
keys remain unchanged. Auto topic creation is disabled. Domain topics start with
three partitions, RF=1/minISR=1 and 24-hour retention including DLQ/late audit, matching
ADR-006's development profile. Startup creates missing topics and does not alter
existing topics/partition counts during ordered scenarios. Use reset for clean defaults.

| Canonical topics | Producer | Consumer group convention |
|---|---|---|
| `grid.telemetry.v1` | Simulators | `local-dev.aggregation.windows.v1` (aggregation) |
| `grid.transformer-load.v1` | Aggregation | `local-dev.control.loads.v1` (Action), `local-dev.projection.events.v1` (projection) |
| `grid.alert.v1`, `grid.action.v1` | Action | `local-dev.projection.events.v1` (projection) |
| `grid.charger-command.v1` | Action | `local-dev.simulator.<device-id>.commands.v1` (devices; filter own device) |
| `grid.command-result.v1` | Simulators | `local-dev.control.results.v1` (Action), `local-dev.projection.events.v1` (projection) |
| `grid.telemetry.late.v1`, `grid.telemetry.dlq.v1` | Aggregation | `local-dev.diagnostics.<run-id>` (devices) |

Registry has no event consumer group in v1. Copilot has no Kafka credentials/group;
it reads approved projections through the future Tool Gateway and writes only its
audit store. Groups follow `<environment>.<component>.<purpose>.v1`; replicas of the
same logical consumer share a group, independent consumers use different groups.
Smoke topics/groups use `local-dev.smoke.<unique-run-id>`. Kafka conventions provide
naming isolation, not ACL enforcement. PLAINTEXT/no ACLs is restricted to disposable
local development; Kubernetes identity/policy and replicated review proof remain later gates.

## On-demand fault proxy

```sh
make infra-faults-up
make infra-fault-smoke
make infra-faults-down
```

The default PostgreSQL proxy forwards to `postgres:5432`. Startup waits for the proxy API to respond.
`fault-smoke` proves a query passes, fails when the proxy is disabled, and succeeds
after recovery; a `finally` block re-enables it on assertion failure. Configure only
the component under test with DB port 15433. Direct database health remains on 15432.
The API supports latency toxics for later component-specific tests. No Kafka proxy
is preconfigured: Kafka's advertised broker addresses require a dedicated routing
configuration before a Kafka fault test can claim coverage. Proxy configuration and
faults are disposable; removing/recreating Toxiproxy reloads the clean JSON file.

Inspect locally:

```sh
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml --profile faults ps --all
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml logs --tail 50 kafka postgres
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml exec postgres psql -U local_admin -d postgres -c '\l'
.venv/bin/python -m pytest -q tests/test_local_infra.py
```

On failure, commands exit nonzero; inspect project logs and rerun `health`. No automatic
reset hides failures. Single-broker availability, single-host loss and PostgreSQL
backup/restore are outside this milestone. Normal restart persistence is the gate.
Upstream references: [Kafka KRaft](https://kafka.apache.org/39/operations/kraft/),
[Compose readiness](https://docs.docker.com/compose/how-tos/startup-order/),
[Toxiproxy API](https://github.com/Shopify/toxiproxy/tree/v2.12.0).

## Disposable reset acceptance proof

This optional check creates synthetic probes, then destroys **all state in this
project's two volumes**. Stop component processes first. Run against a healthy
`make infra-up` environment:

```sh
.venv/bin/python - <<'PY'
from tools import local_infra as infra
infra.create_topic('local-dev.reset-proof')
for store in infra.STORES:
    infra.sql(store, 'CREATE TABLE infra_reset_probe (id integer PRIMARY KEY); INSERT INTO infra_reset_probe VALUES (1);')
    assert infra.sql(store, 'SELECT id FROM infra_reset_probe;').stdout.strip() == '1'
print('PASS: reset probes seeded')
PY
make infra-down
docker volume ls --filter label=com.docker.compose.project=smart-grid-local-dev
make infra-reset CONFIRM=smart-grid-local-dev
docker volume ls --filter label=com.docker.compose.project=smart-grid-local-dev
make infra-up
.venv/bin/python - <<'PY'
from tools import local_infra as infra
assert set(infra.kafka('topics', '--list').stdout.splitlines()) == set(infra.topics())
for store in infra.STORES:
    assert not infra.sql(store, "SELECT to_regclass('public.infra_reset_probe');").stdout.strip()
    assert not infra.sql(store, "SELECT to_regclass('public.infra_smoke');").stdout.strip()
print('PASS: reset erased probes and recreated only contract topics')
PY
make infra-smoke
make infra-down
```

After normal `down`, the volume listing contains exactly the two project volumes;
after reset it is empty. The final `down` retains fresh volumes for the next start.
Kafka may also create its internal `__consumer_offsets` topic after consumer checks;
it is not a domain topic. Compare domain inventories excluding names starting `__`.
