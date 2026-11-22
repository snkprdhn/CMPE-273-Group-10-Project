# ADR-029 — Outbox delivery and durable fencing

- Status: Accepted, 2026-11-22, requested outbox/fencing milestone
- Owners: Member_3 control/PostgreSQL/Debezium; Member_1 simulation/Registry/observability;
  Member_2 Kafka/PyFlink; Member_4 projection/API/UI/identity/Copilot

## Decision and resolved contradictions

Use the selected Strimzi Kafka 3.9.2 Connect runtime with the pinned Debezium
3.0.8.Final PostgreSQL plugin and Outbox Event Router. PostgreSQL 17 pgoutput
captures committed inserts. Route `aggregatetype` to `local-m3.${routedByValue}`,
key by `aggregateid`, and emit the stored JSON envelope without wrapping it.
`id` in the Kafka header is the **physical outbox row ID**. The immutable JSON
`event_id` and `command_id` are the logical consumer identities. Retransmissions
have new physical IDs and unchanged logical envelopes. PostgreSQL 17 pgoutput
omits generated columns: do not use a generated logical-ID column as router ID.
No application published flag, polling relay, or end-to-end exactly-once claim.
Retain rows, replication slot, Connect internal topics and Kafka data on restart.

The Lease controls candidacy; it **does not allocate epochs**. CAS updates use
resourceVersion, unique process-incarnation holder IDs and leaseTransitions.
Observe an unchanged renewal record for its full duration before takeover;
use monotonic local time, bounded requests and a conservative local deadline.
Every failed/uncertain renewal stops the worker. A renewal response arriving
after the previous local deadline cannot revive that incarnation. A fresh process must reacquire.
The `action` Lease is precreated and worker RBAC permits only get/update of it.
The local host runner uses the existing explicit kind kubeconfig, not that
ServiceAccount; in-pod packaging/credential verification remains a later gate.

After acquisition, lock the control shard in PostgreSQL, reverify the Lease,
advance the durable epoch, and commit the holder/generation/expiry. Renewals of
the same incarnation retain its epoch. First activation fences all historical
supplied epochs. Older generations cannot activate; a changed Lease UID fails
closed and needs explicit operator recovery. Never reset epochs independently
of retained command/device history. Database authorization expires no later than
the conservative local Lease budget. Final transaction checks reject lost
leadership and roll back inbox/domain/commands/outbox together.

A device-only greatest-seen epoch is insufficient: an unseen old command could
arrive before the first new-leader command. The fenced Member_1 gateway therefore
uses PostgreSQL for its **simulated effect**, command dedup and result outbox.
Its own `member1_gateway` schema/role has no Control table privileges; the typed
SECURITY DEFINER `authorize_command(UUID,BIGINT,JSONB)` function validates current
unexpired authority and an exact committed command envelope. The function's
shared transaction advisory lock is retained through the simulated effect commit;
leadership advancement takes the exclusive lock. Paused transactions have a
bounded idle timeout, so a disconnected worker cannot retain that lock forever. PostgreSQL session death rolls
back both the effect and its output. There is no PostgreSQL/SQLite dual write.
SQLite remains the independently runnable **standalone** edge profile, with its
original greatest-seen fencing guarantee. Distributed acceptance uses the fenced
PostgreSQL profile. Real charger hardware needs its own durable effect boundary.

This is a narrow addition to logical-store isolation: Member_1 owns an isolated
schema and login in the same PostgreSQL database so the typed authorization call
and simulated effect share one transaction. It does not read Member_3 tables or
use Member_3 credentials. Registry/projection/audit databases stay isolated.

## Transaction boundaries

| Boundary | Atomic contents / consequence |
|---|---|
| Kubernetes API | CAS Lease acquisition/renewal only; no SQL/Kafka transaction |
| Durable authority | Exclusive shard lock, verified Lease generation, increasing epoch and conservative expiry; commit before work |
| Domain input | Same exclusive lock and before/after authority checks; inbox, state, command and outbox commit together; Kafka input offset follows SQL commit |
| CDC | Only committed rows; publication and Connect offset durability are separate, so publication can repeat |
| Simulated device | Shared authority lock, exact command authorization, per-device row lock, rate effect, greatest epoch, command dedup, result/telemetry outbox in one SQL transaction |
| Result output | Kafka acknowledgement precedes deleting the Member_1 result outbox row; input offset follows draining results; lost responses safely replay |
| Leadership loss | Stop local work, revoke durable authority, then CAS-release Lease on graceful exit; a crash relies on expiry/takeover |
| Restart reconciliation | Hold every active previous-epoch saga, retain applied results, time out unknown pending work; never re-epoch or automatically restore an old command |

An effect authorized while an epoch-advancement transaction waits commits before
that advancement. After advancement commits, an unseen old command is rejected,
even on a device which has never seen the new epoch. An already applied duplicate
returns ALREADY_APPLIED without another effect. Gateway command expiry includes
monotonic time spent waiting for database work; after effect/output writes,
command expiry and SQL authority are checked again before committing. Exhaustion
rolls back the effect, dedup record and outputs for safe retry. Worker shutdown
revokes SQL authority before group departure and closes its consumer even if
revocation fails. Empty Kafka records are parsed and fail without offset commit.
Lease loss and database/authority
unavailability fail closed. Timeouts retain unknown outcomes for operator review.

## Scope and consequences

One shard and a bounded global transaction lock are sufficient for the local
profile. No new SDK, broker client version, consensus implementation or model is
needed. CDC uses its own restricted replication login/publication. Topics use
`local-m3.` to preserve running Member_1/2 standalone consumers. Source offsets,
replication-slot disk growth, backup/restore, full kind workload packaging,
NetworkPolicies, TLS/identity, metrics and real observation acquisition remain
separate integration/operations gates. Never delete the slot/internal topics to
repair a task; fix the configuration and restart it with retained offsets.

See the [local runbook](../../infra/member3/README.md) and executed evidence in
[implementation status](../implementation/IMPLEMENTATION_STATUS.md).
