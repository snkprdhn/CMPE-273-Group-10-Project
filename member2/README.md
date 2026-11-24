# Member_2 standalone Kafka/PyFlink aggregation

Implemented: fixed topology, event-time latest-per-device loads, revisions,
deduplication, validation/DLQ, inclusive late correction/audit, transactional Kafka
output and real TaskManager checkpoint recovery. No simulator, Registry,
PostgreSQL or other member's application is needed.

From the repository root, with Docker Desktop running and its built-in Kubernetes
still disabled:

```sh
make member2-test
make member2-build
make member2-live
```

The pure tests can use the preparation `.venv` or an independent venv installed
from `member2/requirements-test.txt` (the portable subset used by CI); all PyFlink execution and Kafka client
probes use `/opt/venv` inside the independently locked native image. The first
build downloads pinned artifacts and builds PyFlink on ARM64. No host JDK, Maven,
Node/npm, cloud service or model call is involved. Linux amd64 builds are not yet
verified. The source build's isolated build tools/OS apt packages are not a
bit-for-bit reproducible toolchain; published runtime inputs are hash checked and
compiled wheel hashes are recorded in `/opt/member2-wheel-sha256.txt`.

The live command creates a fresh `m2-<run>.` topic prefix, starts only Kafka and
Flink, submits fixtures, observes committed output/checkpoints, observes a pending
transaction, crashes/restarts the TaskManager, then verifies restored state,
replayed IDs, correction totals/revisions and audit routing. An existing active
Flink job causes an explicit failure so its state can be inspected first. To
repeat, run `make member2-down`, then `make member2-live`. Checkpoints and Kafka
records persist. There is no destructive reset in this workflow.

Inspect [the Flink UI](http://127.0.0.1:18081) for job graph, checkpoints, exception
history, task restarts, backpressure and connector metrics. Historical golden
telemetry intentionally produces STALE/OLD_RESULT loads when evaluated now.
The engine's tests use frozen evaluation times and check all fresh quality states.

```sh
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml logs --tail=100 taskmanager
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager /opt/venv/bin/python -m pip check
make member2-down
```

`tools/member2_live.py --run <new-id>` accepts a chosen run ID when invoked with
`.venv/bin/python`. All topic keys are preserved; each run has four topics with
three partitions. The default `topology.json` is the frozen `topology-001` shared
contract fixture. Submit a different fixture with the job's `--topology` argument
inside a rebuilt image and a **new** run ID. Capacity is immutable for that run.
Do not change topic partitions during a run.

For manual checkpoint restore, inspect the UI's completed checkpoint external
path, then submit the identical image/config/run with:

```sh
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml exec -T jobmanager flink run -d -s file:///checkpoints/<job>/<checkpoint> -py /app/member2/job.py --run <same-run>
```

That whole-session restore command is documented, not an executed HA proof.
The tested crash is TaskManager SIGKILL with the JobManager alive.

The finite-run ceiling is 100,000 unique valid IDs, including very-late ones;
state is never silently evicted. One task enforces run-wide identity and watermark
semantics across Kafka partitions; throughput/rescaling are unmeasured. Custom
operator metrics expose duplicates, DLQ, very-late, corrections, input/output,
semantic watermark/delay, accepted late count and latest output. Flink supplies checkpoint duration/failures,
restart/backpressure and Kafka source metrics. The standalone slice needs no monitoring stack. The subsequent [observability milestone](../infra/observability/README.md) enables the bundled native Prometheus reporter and explicit Kafka header/span propagation in kind.

The three Kafka sinks select transactional exactly-once delivery, with successful
checkpoints, durable checkpoint files, recovery within a 300s transaction timeout,
unique run prefixes and `read_committed` consumers. It is not a platform-wide
guarantee, cross-topic atomic visibility or RF=1 broker availability. See
[ADR-027](../docs/decisions/ADR-027-member2-standalone-aggregation.md) for precise
semantics, simplifications and remaining gates. Member_1 retains simulation,
Registry and observability; Member_3 owns control/PostgreSQL/Debezium; Member_4
owns projection/API/UI/identity/Copilot.

### Local 100,000-record scale acceptance

The scale fixture models 25 transformers with 100 devices each, including
meters, EV chargers and solar. It submits 40 accelerated event-time rounds at
a configurable Kafka delivery target. Readings arrive out of timestamp order.
One message repeats an event ID and one valid message exceeds its lateness
deadline. The terminal round advances the watermark so the 39 preceding
rounds can be compared with independently calculated totals. Its own trailing
windows remain open.

With the local Kafka dependency running and the Member_2 image built, run:

```sh
docker compose -p grid-scale -f infra/compose/member2-scale.yaml up -d
docker compose -p grid-scale -f infra/compose/member2-scale.yaml exec -T jobmanager \
  /opt/venv/bin/python -m tests.member2.scale scale100k01 --rate 2500
```

Use a fresh run ID each time. The probe writes checkpoint and lag samples
and a final report under `/checkpoints/<run>` in the separate Compose volume.
It uses isolated Kafka topics on the existing local broker. This profile
measures Kafka and Flink on one machine. The deployed control loop has its
own acceptance scenario.

The Flink job keeps the exact finite-run ID budget. It stores IDs and window
snapshots separately in managed MapState and updates only the active window
and newly eligible windows. It retains older windows to preserve valid
corrections based on envelope ingestion time even after a Kafka replay.
Long-running retention and horizontal aggregation remain future work.

For total memory including the Python worker, sample Docker working-set usage
during the run with `docker stats --no-stream grid-scale-taskmanager-1
grid-scale-jobmanager-1 smart-grid-local-dev-kafka-1`. The probe's JSONL also
contains Flink JVM memory and per-partition checkpointed lag. Flink lag is the
broker end offset minus the consumer group's committed checkpoint offset, so
it includes work processed since the last completed checkpoint.
