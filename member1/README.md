# Member_1 standalone edge and Registry

Member_1 owns this package, simulator/Registry images, fixtures and tests. Member_2
still owns Kafka/PyFlink, Member_3 owns control/PostgreSQL/Debezium, and Member_4
owns projection/API/UI/identity/Copilot. This slice uses only the accepted local
Kafka and Registry database; it does not require their application services.

## Run and inspect locally

From the repository root with the existing Python 3.12.9 preparation/contract
venvs, create independent application environments (first-time setup):

```sh
.venv/bin/python -m venv .tools/registry-venv
.tools/registry-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r member1/requirements-registry.txt
.venv/bin/python -m venv .tools/simulator-venv
.tools/simulator-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r member1/requirements-simulator.txt
.venv/bin/python -m venv .tools/member1-venv
.tools/member1-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r member1/requirements.txt
make bindings
make member1-test
make infra-up
make member1-live
make member1-up
```

`member1-up` builds both non-root, read-only images and waits for readiness. Kafka
and PostgreSQL retain their existing dependency volumes; the simulator adds the
owned `smart-grid-local-dev_member1-edge` named volume. No host bind mounts,
cluster changes, cloud resources, Flink, Action, UI, or model calls are involved.
Local image tags name build outputs, not published releases; upstream bases use
the accepted digest. Do not publish these images as a measured deployment gate.

Inspect JSON health and Prometheus text at:

- Registry: [readiness](http://127.0.0.1:8001/readyz), [metrics](http://127.0.0.1:8001/metrics).
- Simulator: [readiness](http://127.0.0.1:8002/readyz), [metrics](http://127.0.0.1:8002/metrics).
- `/healthz` is process liveness; `/readyz` checks the owned PostgreSQL tables or
  Kafka metadata/delivery health. Readiness is not evidence of fresh load data.

```sh
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member1.yaml logs --tail=30 registry simulator
.tools/member1-venv/bin/python -m tests.member1.container_smoke
```

The smoke client registers unique synthetic devices, applies a command,
**recreates both application containers**, verifies Registry retention, replays
the command, rejects an older epoch, and checks both operational endpoints. It
leaves its owned synthetic registrations and command history for inspection.
It uses canonical topics and requires the Member_1 simulator running. The live
pytest suite instead creates/deletes three uniquely named temporary topics and
cleans its own PostgreSQL rows, so it can run alongside the demo.

Stop applications with `make member1-down` (retains state). Then `make infra-down`
stops/removes dependencies while retaining their volumes. **Do not run the
infrastructure reset to restart a simulator.** Removing/restoring an old edge
volume removes/rolls back fencing and deduplication; do not reconnect that state
to retained commands without reconciliation. No automatic destructive reset is
provided for application state.

## Reproducible scenarios and command fixtures

```sh
.tools/simulator-venv/bin/python -m member1 scenario --seed 273 --steps 30 --interval 1 --topology /tmp/member1-topology.json > /tmp/member1-telemetry.jsonl
.tools/simulator-venv/bin/python -m member1 command-fixture --live --run-id manual-001 --case throttle > /tmp/member1-command.json
.tools/simulator-venv/bin/python -m member1 command --fixture /tmp/member1-command.json
.tools/simulator-venv/bin/python -m member1 command --fixture /tmp/member1-command.json
```

Repeat `scenario` with identical arguments for identical bytes, event IDs, boots,
sequence numbers, power values and UTC times. The versioned fixture topology is
`fixtures/topology.json`: one 20 kW transformer, one EV, one meter, one solar unit.
Samples default to one second, with equal normal/overload/recovery thirds. Normal
and recovery net demand are below 75% capacity; overload exceeds 95%. The seed
controls bounded meter/solar variation. Solar power stays negative. These are
synthetic waveforms, not an aggregation or control implementation.

`command-fixture` defaults to a fixed historical start for reproducible tests;
`--live` uses current UTC and a ten-second TTL. Publish the saved file again to
test duplicates, including after expiry. **Do not regenerate a command with the
same run ID/case and a new timestamp**: changing a payload under the same ID is a
conflict. Choose a new run ID for each new experiment. `--case expired` expires
exactly at creation; `--case stale` uses epoch 41 after a valid epoch 42 command;
`--case restore` explicitly sets 7.2 kW. Fixed fixtures and expected outcomes live
in `fixtures/commands.json`; the executable contract tests check real outputs.

The live simulator rebases timestamps to actual UTC, creates new boots on every
process start, and emits the seeded waveform once (30 ticks by default). It stays
alive afterward to accept fixture commands and publish their results/follow-up
telemetry until SIGTERM/SIGINT. Use `--steps`/`--interval` for a longer scenario;
restart for another run. Offered EV demand is capped by the durable commanded
rate. Missing telemetry and the end of a scenario never restore a charger.

## Registry without containers

Start only PostgreSQL (`make infra-up` also starts Kafka), then in one terminal:

```sh
export REGISTRY_DSN=postgresql://registry:registry_disposable@127.0.0.1:15432/local_dev_registry
export REGISTRY_SIMULATOR_TOKEN=simulator_disposable
export REGISTRY_ACTION_TOKEN=action_disposable
export REGISTRY_API_TOKEN=api_disposable
export REGISTRY_CURSOR_KEY=local-disposable-cursor-key-273
.tools/registry-venv/bin/python -m member1 registry
```

In another terminal, register the shared fixed capabilities using the test client:

```sh
REGISTRY_SIMULATOR_TOKEN=simulator_disposable .tools/registry-venv/bin/python -m member1 register
```

Registry listens on `127.0.0.1:15051` on the host (container internal port 50051).
Port 50051 was already occupied on this machine; its existing listener is preserved.
The simulator can run independently with
`.tools/simulator-venv/bin/python -m member1 simulate`; stop the container simulator
first to avoid two consumers for the same logical charger. Registration/heartbeats
are explicit typed client operations, not a hidden simulator startup dependency.
All four RPCs are exercised by `make member1-live`; use `member1.registry.Client`
with generated `pb` request messages for further fixtures.

## Guarantees and failure behavior

- SQLite commits the simulated effect, greatest accepted epoch, immutable command
  payload/result, result event, and follow-up reading in **one transaction**.
  IDs are retained for the volume's lifetime. Process restarts get a new boot but
  retain rates, epochs, results and pending event IDs/trace headers. This is durable
  simulated-device safety, not a transaction with real charging hardware.
- Applied duplicates return `ALREADY_APPLIED` before checking expiry/new epochs.
  A reused ID with another payload is `REJECTED/COMMAND_ID_CONFLICT`. Unseen expired,
  stale-epoch, future-issued, wrong-assignment or out-of-range commands have no
  effect. Only applied commands advance the fence. Restore requires a new command.
- Results/follow-up telemetry remain in a bounded 1000-entry durable outbox until
  Kafka acknowledges them. Offsets commit afterward. Crashes between broker ACK
  and local deletion may replay the **same event ID**. Downstream consumers still
  deduplicate events. A crash after effect commit cannot repeat that effect.
- Telemetry uses transformer keys; commands/results use device keys. Ordering is
  per partition only. Native producer idempotence, `acks=all`, three retries,
  100–1000 ms retry backoff, a 10-second delivery deadline, a 1000-message/1 MiB
  queue, and a 64 KiB application message cap bound buffering/retry. Queue pressure
  blocks only to a bounded deadline; exhaustion raises/logs failure and exits.
  Unconfirmed telemetry is reported, not silently claimed durable across restart.
  Pending command outputs survive restart. Kafka RF=1 is not replicated safety.
- Malformed, mismatched-key and unknown-shard commands stop visibly without
  offset commit; repair the fixture/assignment before restart. The telemetry DLQ
  is not repurposed for commands. This profile has exactly one simulator shard
  owning the fixture fleet; do not horizontally scale it or share its group with
  disjoint device sets. Partition assignment/shard coordination is a later gate.
- Registry uses its nonprivileged owned database, bounded connections/statements,
  transactional operation IDs/capability versions and monotonic heartbeats.
  Eligible EVs are controllable, at most 15 seconds old (inclusive), not future,
  and depart more than five minutes after evaluation. Cursors bind transformer,
  policy version, evaluation instant, page size and lexical device position using
  HMAC; concurrent changes between pages are not a snapshot isolation promise.
- The Registry client defaults to a two-second total deadline and limit 50;
  UNAVAILABLE gets at most two retries with jitter inside that same budget.
  Other failures are surfaced; deadline cancellation cannot undo committed work.
  Failed database/lock/deadline operations never return fabricated capabilities.
- Explicit service tokens allow simulator writes and action/api reads. Transport
  is plaintext **only on loopback/private disposable Compose networking**. Tokens
  in Compose are public development values, not production credentials. mTLS,
  Kubernetes identity/network policies, replica/load/backup acceptance, OTel
  exporters and the shared monitoring stack are outside this standalone gate.
- JSON logs correlate events/commands/operations; metrics use bounded names,
  never device IDs as labels. Metrics reset on process restart. Inspect native
  Kafka retry counts, bounded queue use, delivery failures, command outcomes,
  Registry errors and committed offsets. No measured throughput/SLO claim is made.

## Configuration

Owner of all settings below: Member_1. CLI has precedence over safe defaults;
required Registry environment secrets fail startup when absent. Compose overrides
are explicit local values. Bootstrap/prefix/group must identify this local test
environment, never production infrastructure.

| Setting | Default / allowed | Secret |
|---|---|---|
| `--seed`, `--run-id` | 273; contract ID `member1-001` | No |
| `--steps`, `--interval` | 30 (at least 3); 1 second (positive finite) | No |
| `--start` | `2026-11-22T18:00:00Z`, UTC; offline/register fixtures | No |
| `--bootstrap`, `--prefix`, `--group` | `127.0.0.1:19092`, `local-dev.`, `local-dev.member1.edge.v1` | No |
| `--state` | `data/member1-edge.sqlite`; retained writable path | No |
| `--registry` | `127.0.0.1:15051`; server bind/client target | No |
| `--bind`, `--metrics-port` | loopback; 8001 Registry / 8002 simulator | No |
| `REGISTRY_DSN` | required; Registry-owned PostgreSQL only | Yes |
| `REGISTRY_{SIMULATOR,ACTION,API}_TOKEN` | required nonempty, separate service tokens | Yes |
| `REGISTRY_CURSOR_KEY` | required, at least 16 characters; preserve across replicas/restarts | Yes |

Separate runtime locks/venvs keep Registry independent of Kafka libraries and
simulation independent of PostgreSQL/gRPC. The combined test lock is not a
Member_2/3/4 environment. Linux ARM64 images execute locally; Linux x86_64 wheels
resolve for CI, which is authored but not claimed executed on GitHub.
