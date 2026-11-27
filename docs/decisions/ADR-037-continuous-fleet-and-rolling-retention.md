# ADR-037 — Continuous fleet, bounded keyed state and rolling retention

- Status: Accepted for the hosted demonstration, 2026-11-26. Amends ADR-027 (finite run), ADR-030 (projection scope) and
  the deduplication note in `STREAM_SEMANTICS_V1`. No v1 wire contract changes.

## Context

The hosted site had to show a live system for days on one AKS worker and one 2-vCPU Flink node under the student
quota (6 regional vCPUs, no node can be added). The finite-run design stops after its event-ID budget, one
Flink subtask did all the work, and nothing deleted old data.

## Decisions

1. **Fleet.** A `fleet` Deployment publishes 25 transformers x 40 devices (1,000 devices, one reading each every five seconds,
   200 events/s) to the real topics. Five managed (hero) transformers have 70 registered chargers whose rate follows the real
   Action commands. Incidents rotate across them. Late, duplicate, dropped and very-late readings are injected at small rates
   and can be raised on demand through a token-protected control port. The fleet size is configuration, not a design limit.
2. **Keyed, bounded state.** Flink keys by transformer. Each key holds one entry per five-second window (the window and the
   event IDs seen for it) and drops entries older than 120 s behind its own watermark. The run-wide ID limit is gone. A
   replay older than that horizon is audited as late and never recounted. A duplicate older than the horizon is therefore
   reported as `very_late` instead of `duplicates`; duplicates inside it are counted as before. The watermark is per
   transformer, which is sufficient because a transformer's readings share one Kafka partition and key.
3. **Rolling retention.** Topics keep three hours with a per-partition size cap and ten-minute segments (Kafka only deletes closed
   segments). The broker volume is 8 GiB. The projection stores only the managed inventory, keeps one telemetry row per device
   and rolling windows per kind (loads 15 minutes, alerts 60 minutes, commands and results 3 hours, actions 24 hours), and prunes every five
   minutes. Freshness is 30 s and the previous window's classification is carried for 8 s (`PROJECTION_FRESH_SECONDS`,
   `PROJECTION_ALERT_GRACE_SECONDS`; defaults 15 s and 0 keep v1 behavior), because a window closes about 11 to 16 s after it ends
   and its alert trails its load. PostgreSQL bounds WAL (`wal_keep_size=0`,
   `max_wal_size=512MB`, `max_slot_wal_keep_size=768MB`) and Debezium writes a heartbeat row so its slot advances although other
   databases in the instance are busy.
4. **Control scope.** Action processes loads and charger readings only for `CONTROL_TRANSFORMERS`. Readings of other transformers
   cannot lead to a command, so they are skipped before decoding.
5. **Failure handling for unattended runs.** Malformed charger readings and conflicting re-emitted load revisions or duplicate
   results are logged and skipped; stale saga versions still stop the worker. Plans held for review (restart, timeout) get an
   approved compensation: the throttle is lifted with new command IDs, up to three attempts, then the plan stays held for an
   operator. Enabled by `ACTION_AUTO_COMPENSATE=1`.
6. **Observability.** Per-transformer load, capacity, device coverage and fleet counters are Prometheus series. Grafana is served
   read-only under `/grafana/` on the existing HTTPS host with five provisioned dashboards. The live control room is a NiceGUI
   page fed by one shared poller (Prometheus plus the REST API), not by per-session snapshots.

## Consequences

- Measured on AKS (2026-11-26): Flink busy about 20% at 200 events/s; a backlog of 108,496 records drained in 93 s of running
  time after a 110 s restart (about 1,100 records/s net of the live 290/s input, roughly 1,400/s processed).
- The projection and API no longer describe unmanaged transformers; they are visible through metrics and the control room.
- A stateless Flink restart can re-emit window revisions with different contents; Action keeps the first copy and logs the rest.
- Compensation restores chargers after a controller restart without an operator. This is acceptable for simulated chargers and
  would need an explicit approval policy for real equipment.
