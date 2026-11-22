# ADR-027 — Member_2 standalone aggregation and recovery

- Status: Accepted, 2026-11-22, within the authorized standalone milestone
- Owner: Member_2 Kafka/PyFlink; Member_1 topology/telemetry, Member_3 control,
  Member_4 projection/API/UI/identity/Copilot retain their boundaries

## Decision

Use the selected PyFlink/Flink 2.2.1, Python 3.12.9 and Kafka connector
5.0.0-2.2. The independently hash-locked native ARM64 image compiles the
published PyFlink source distribution. Java/compiler tools stay in the build
container. The runtime uses a Python venv, an immutable Flink base and a
SHA-256-verified upstream connector JAR; there is no authored JVM code.

The standalone job reads Kafka raw key/value plus partition/offset through the
Table connector, uses a Python DataStream keyed process function for aggregation,
and publishes through three transactional Table Kafka sinks. Raw formats let
malformed JSON and wrong/null keys reach the DLQ. Member_2 imports neither
Member_1 internals nor contract test tooling and never accesses PostgreSQL.

The contract clock is implemented explicitly in checkpointed state: half-open
five-second event-time buckets and watermark = maximum unique valid, non-audited
accepted event time minus exactly ten seconds. It emits once watermark >= end;
accepted corrections emit immediately once that condition holds. This is a
semantic watermark, not the Table source's native watermark. Native bounded
out-of-order watermarks use a different boundary/partition-minimum policy, and
native window cleanup could discard an event still admissible by v1 ingestion
time. Thus the job deliberately uses managed state rather than Flink's default
window operator or its default allowed-lateness cleanup.

Arrival is the validated envelope's `ingested_at`, not the time Kafka is replayed.
The inclusive correction deadline is end + 30 seconds. Load freshness is evaluated
against actual processing UTC; replaying historical fixtures today correctly
marks them OLD_RESULT. Golden domain tests supply the fixture's evaluation UTC.
The runtime explicitly validates every schema date-time field as RFC3339 UTC;
this does not depend on optional jsonschema format packages. Timestamp arithmetic
uses differences to avoid datetime range overflow. An unrepresentable UTC window
end or selected total/utilization is invalid and goes to the DLQ before changing
identity/window state; a valid very-late finite reading still goes to audit.

Wall-clock staleness alone never creates revisions. A selected-reading ID change
creates a revision; a superseded accepted late sample increments the window's
late count but does not by itself emit another revision. This follows the frozen
executable reference. No-reading windows are not synthesized.

One aggregation task retains global event IDs and windows for a fixed run
(maximum 100,000 unique valid input IDs by default, including very-late IDs).
This preserves global maximum/dedup semantics across the three Kafka partitions.
At the limit it fails visibly instead of evicting identity or emitting incorrect
loads. State and full-window scans grow with run length; this profile is for
bounded deterministic runs, not production throughput. Rescaling and distributed
watermark coordination are deferred. Restore rejects a changed topology or run
limit. Use a new run/topic/group/transaction prefix for changed topology/capacity;
never run two jobs with the same run ID. The default topology is copied from the
shared frozen golden scenario, not fetched from a service.

## Delivery boundaries

All three sinks select `sink.delivery-guarantee=exactly-once`, rather than the
connector's default. Flink checkpoints source offsets, managed aggregation state
and sink transactions. Checkpoints default to two seconds, timeout after 60s,
and allow one concurrent checkpoint. Kafka transaction timeout is 300s. Consumers
must set `isolation.level=read_committed`; each live job/sink has a unique transaction
prefix. Recovery must finish within the transaction timeout and Kafka retention;
expired transactions, lost checkpoint storage or a fresh submission without restore
are outside that guarantee. Three sinks do not expose one cross-topic atomic read.

The real acceptance test sees an uncommitted correction using `read_uncommitted`,
SIGKILLs the TaskManager before the next completed checkpoint, and verifies that
restore/replay yields only revisions 0/1 (15/17 kW) to `read_committed` consumers.
Consumers must still retain highest window revision and ignore duplicate/lower
versions. Restart of a whole session cluster requires explicit submission with
`flink run -s <checkpoint-path>` and identical run/configuration; this milestone
proves TaskManager loss, not automatic JobManager HA.

The one-broker RF=1/minISR=1 local profile cannot establish replicated availability
or host-loss durability. No guarantee extends to Member_3 database transactions,
Member_4 projections or simulated physical effects. There is no platform-wide
exactly-once claim. See [Apache's connector guarantees](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/kafka/#consistency-guarantees)
and [transaction timeout guidance](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/#fault-tolerance).

## Resolved design contradictions

- `DATA_FLOW.md` previously added solar's positive reported magnitude to demand.
  Executable contracts require meter + EV **minus** solar magnitude. Corrected.
- Prose describing lateness only relative to a runtime watermark was too broad:
  v1 fixtures/ADR-025 use `ingested_at` and an inclusive end+30s deadline.
- The earlier blanket plan for exactly-once output is now scoped to selected
  transactional Kafka sinks, successful checkpoints and committed readers.

Kind/operator packaging, distributed rescaling, broker failover, dashboards/traces,
security policies and whole-cluster HA remain later milestones. Existing kind,
Docker settings, Member_1 services and all unrelated stores are preserved.
