# Coordination, Leases, and Fencing

## Ownership model

Multiple Action Service pods exist for availability, but only one leader may issue commands for a control shard. Leadership uses a Kubernetes Lease or another consensus-backed lease mechanism; the application does not implement Raft or Paxos. The architecture defense explains that Kubernetes etcd uses Raft-style consensus internally and that consensus availability depends on a quorum, while the project focuses on the application-visible lease and fencing consequences.

## Why a lease is insufficient

A paused or partitioned leader can continue running after its lease expires. Every leadership acquisition therefore obtains a monotonically increasing fencing epoch. Commands, outbox events, and active saga state carry that epoch. The command consumer or gateway remembers the greatest accepted epoch and rejects older epochs.

```mermaid
sequenceDiagram
    participant A as Action A epoch 41
    participant Lease as Kubernetes Lease
    participant DB as Durable authority
    participant B as Action B
    participant Device
    A-xLease: Renewal fails during partition
    B->>Lease: Acquire leadership
    Lease-->>B: Ownership generation
    B->>DB: Verify Lease and advance epoch under SQL lock
    DB-->>B: Committed epoch 42
    B->>Device: Command epoch 42
    Device-->>B: Applied; greatest epoch = 42
    A->>Device: Delayed command epoch 41
    Device-->>A: Rejected stale epoch
```

## Ordering rules

Physical timestamps support event-time windows but do not establish distributed causality. Kafka ordering is scoped to a partition. Device sequence numbers detect gaps; window revision orders corrections; saga version prevents stale transitions; fencing epoch orders leaders. A small Lamport-clock experiment may illustrate happens-before. Vector clocks can detect concurrent updates but are not adopted for the single-owner control saga because explicit versions and fenced ownership provide a simpler invariant.

## Failover demonstration

Kill or isolate the leader, observe lease expiry and new leadership, issue a valid new-epoch plan, deliver a delayed old-epoch command, and prove rejection. Record failover duration, duplicate deliveries, availability impact, and whether a control decision was safely delayed.

## Implemented local boundary

[ADR-029](../decisions/ADR-029-outbox-and-fencing.md) resolves the distinction:
Kubernetes allocates Lease ownership generations; PostgreSQL allocates fencing
epochs. A device-only greatest-seen fence cannot reject an unseen old command
before a new device command arrives. The fenced simulated gateway therefore uses
typed SQL authorization in the same transaction as its PostgreSQL effect, dedup
and result outbox. Its shared lock serializes against durable epoch advancement;
no cross-store effect transaction is claimed. Old active sagas enter review on
new ownership, retaining partial acknowledgements and never re-epoching commands.
See the ADR for complete transaction/expiry/recovery boundaries and the
[runbook](../../infra/member3/README.md) for local Lease/CDC failure tests.
