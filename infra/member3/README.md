# Local outbox and fencing — Member_3 / Member_1 boundary

This profile runs only the new Connect container against the existing local
Kafka/PostgreSQL and uses the existing `kind-smart-grid-dev` API for Leases.
Member_1 owns gateway simulated effects; Member_2 Kafka/PyFlink; Member_3
control/PostgreSQL/Debezium; Member_4 projection/API/UI/identity/Copilot.
The [transaction/ownership decision](../../docs/decisions/ADR-029-outbox-and-fencing.md)
defines the guarantees and remaining integration gates.

## Prepare and verify

From the repository root with the existing dependencies running:

```sh
.tools/member3-venv/bin/python -m pip install --require-hashes -r member3/requirements.txt
make bindings
curl -fL -o .tools/downloads/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.0.8.Final/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz
shasum -a 256 .tools/downloads/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz
export CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control
export MEMBER3_ADMIN_DSN=postgresql://local_admin:local_admin_disposable@127.0.0.1:15432/local_dev_control
.tools/member3-venv/bin/python -m tools.member3_setup
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member3.yaml up -d --build connect
.tools/member3-venv/bin/python -m member3.cdc configure
.venv/bin/kubectl --context kind-smart-grid-dev apply -f infra/member3/lease.yaml
MEMBER3_TEST_DSN="$CONTROL_DSN" MEMBER3_KIND_TEST=1 MEMBER3_CDC_TEST=1 make member3-test
```

Expected archive SHA-256:
`600806875b810abb96b3774f789d9ed33dbe4ac687d3ed9454bc0caa76b7279c`.
The image build enforces it. Setup is additive/idempotent: creates the isolated
`member3_delivery` schema, restricted `member3_cdc` replication role/publication,
`member1_gateway` role/schema and six `local-m3.grid.*` topics plus heartbeat.
Disposable local credentials are versioned examples, never deployment secrets.
Do not run `compose down`, `--remove-orphans`, reset volumes, delete replication
slots/internal topics, or replace a Lease UID as a recovery shortcut.

The live test kills/restarts **only Connect**, appends synthetic outbox evidence,
and creates/drops random PostgreSQL schemas and temporary kind Leases. It proves
rollback exclusion, commit-before-publication recovery, publication-before-crash
replay, explicit duplicate CDC inserts, stable topic/key/envelope routing,
command dedup after lost responses and process restart. Other tests kill actual
leader/gateway processes and prove effect/epoch serialization, partial-result
hold and stale work rejection. Review regressions also cover delayed Lease responses,
empty Kafka records, revocation failure and expiry during database waits. Without opt-in variables these checks skip
explicitly; a skipped run is insufficient acceptance.

## Run and inspect

```sh
.tools/member3-venv/bin/python -m member3.cdc status
.venv/bin/kubectl --context kind-smart-grid-dev -n smart-grid-control get lease action -o yaml
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member3.yaml logs --tail 50 connect
CONTROL_DSN="$CONTROL_DSN" .tools/member3-venv/bin/python -m member3 inspect --schema member3_delivery
```

Both connector and task 0 must report RUNNING. Status can briefly show transitional
states immediately after configuration. Task failures stop publication visibly;
retain the SQL rows and offsets, repair the cause, then use Connect's
`POST /connectors/member3-outbox/restart?includeTasks=true&onlyFailed=true`.
The header `id` identifies a physical row; dedup by JSON `event_id`/`command_id`.
Action projections additionally compare `saga_version`.

The following long-running commands are available for manual integration;
startup/shutdown/restart of the worker is covered by the live test. Use separate terminals for two workers. One holds leadership. The worker defaults
to live Registry capabilities and Kafka telemetry. Set `REGISTRY_ACTION_TOKEN`
for the configured Registry service identity. Set `CONTROL_SNAPSHOTS` to a JSON
list to opt into the standalone fixture adapter. See the
[isolated closed-loop runbook](../../docs/implementation/LOCAL_INTEGRATION.md)
for a fresh run without historical input records.

```sh
export CONTROL_DSN='postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control?options=-csearch_path%3Dmember3_delivery'
.tools/member3-venv/bin/python -m member3.worker
```

Worker settings: `CONTROL_DSN` required secret; `KAFKA_BOOTSTRAP` defaults to
loopback `127.0.0.1:19092`; `KAFKA_PREFIX=local-m3.`; `LEASE_NAMESPACE=smart-grid-control`;
`LEASE_NAME=action`; optional local fixture path `CONTROL_SNAPSHOTS`.
It uses the repository's `.venv/bin/kubectl` and explicit kind context. SIGTERM
stops work, revokes SQL authority and releases the Lease. Failed renewals exit;
restart as a fresh process. Unprocessable input exits without committing its
offset; inspect/fix it rather than silently dropping it.

Member_1's existing simulator command selects the fenced profile when `EDGE_DSN`
is supplied (install the updated Member_1 simulator lock first). A manual
integration invocation, **not a full closed-loop acceptance claim**, is:

```sh
export EDGE_DSN='postgresql://member1_gateway:member1_gateway_disposable@127.0.0.1:15432/local_dev_control?options=-csearch_path%3Dmember1_gateway%2Cmember3_delivery'
.tools/member1-venv/bin/python -m member1 simulate --prefix local-m3. --group local-m3.member1.edge.v1
```

The role can execute `authorize_command` and manage only its own tables; it
cannot SELECT Control tables. Its PostgreSQL device rate is the simulated effect,
not a mirror of the standalone SQLite file. Do not switch state stores during an
active scenario or discard either history. The fixture fleet can contain multiple
devices for component tests; the existing live simulator still owns one EV shard.
Stopping this profile only: `docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member3.yaml stop connect`.
