# ADR-026 — Standalone device state and acceptance boundary

- Status: Accepted, 2026-11-22, under the requested Devices standalone milestone

Use one Devices-owned Python package, `devices/`, for the small standalone slice.
Its README links the separate commands, runtime locks, images and venvs. This
avoids another shared package or service-internal imports; no other service
imports these internals.

Registry owns `devices` and `operations` in its existing isolated
PostgreSQL database/role. Transactional advisory locking serializes this small
Registry across instances; operation IDs and registration versions are atomic.
Heartbeats never lower liveness or change capabilities. Register/Get/Heartbeat and
paginated eligibility use the frozen proto, generated only by `make bindings`.
Client defaults remain 2 seconds, 50 entries, two transient retries maximum.

The simulated edge owns a SQLite WAL/FULL file on a retained volume. Rate effect,
accepted fencing epoch, command-ID payload/result, result event and follow-up
reading commit together, before Kafka offset advancement. Pending publications
have a 1000-entry cap; command history is retained for the state volume's entire
lifetime (no time-based pruning). Restart creates a fresh telemetry boot only;
effects, fences, dedup state and pending output IDs/trace headers survive. A
previously applied command returns ALREADY_APPLIED even after expiry/new epochs;
changed payload under an existing ID is rejected. Rejected/expired attempts do
not advance the fence. Wiping or rolling back this file invalidates that safety
claim. This atomic effect is valid for a simulated charger only; real hardware
would need its own reconciliation/transaction boundary.

Use the selected confluent-kafka 2.8.2 and psycopg 3.2.9. The matrix's older gRPC
1.71.0 runtime row contradicted ADR-025's accepted installed runtime 1.85.3;
Devices follows the latter, keeping grpcio-tools 1.71.0/protobuf 5.29.6 and the
compiler venv separate. Runtime locks include verified macOS ARM64, Linux ARM64
and Linux x86_64 wheel hashes; no host compiler, JVM or JavaScript tooling is added.

This profile has one transformer with one EV, meter and solar device and a single
owning simulator shard. Live timestamp/boot identity differs across process runs;
offline fixtures are byte-repeatable for a given seed/start/run/boot. The live
waveform is finite and the consumer remains available for fixture commands after
it ends. Registry registration/heartbeats use explicit typed clients, so simulator
startup does not depend on a Registry connection. Kafka telemetry may fail visibly
after its bounded retry/buffering budget; it is not a durable telemetry spool.
Command outputs are durable and replay-safe by event ID; consumer offsets follow
acknowledged output. This is at-least-once output, not cross-Kafka/SQLite exactly-once.

Basic JSON logs, bounded-name metrics, HTTP liveness/readiness, local tokens and
non-root container builds satisfy this standalone gate. The local Registry host
port is 15051 because 50051 is already occupied; preserve that unrelated listener.
Plaintext is limited to loopback/private disposable Compose networking. Kubernetes
deployment, mTLS/identity, enforced network policies, Registry replica/load/backup
acceptance, large-scale sharding, OTel collection and the full monitoring stack
remain later gates. Do not describe this milestone as full Phase 0/1/2 completion.
