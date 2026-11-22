# ADR-028 — Member_3 standalone Action domain and durable saga

- Status: Accepted, 2026-11-22
- Authority: Requested Action domain/state-machine milestone; routine decisions
- Owners: Member_3 control/PostgreSQL/Debezium; Member_1 Registry/observability;
  Member_2 Kafka/PyFlink; Member_4 projection/API/UI/identity/Copilot

## Decision

Implement [Member_3](../../member3/README.md) as an independent Python package
with the existing psycopg 3.2.9/jsonschema 4.23.0/pytest 8.3.5 pins and a separate
hash-locked venv. Persist state, commands, inbox and router-shaped outbox rows in
the control PostgreSQL database. Use real transactions, constraints, optimistic
versions and a bounded advisory lock. No SQLite substitute or in-memory durable
store is introduced. Keep the saga executable without Kafka, simulators or Flink.

ADR-025 remains the policy and wire authority. Resolve the following gaps without
changing executable v1 contracts:

1. Registry's canonical Device has no current rate, charging or online state.
   Require explicit fresh observations in fake snapshots. Missing observations
   never mean maximum rate, zero load or restoration safety. A live observation
   source must be implemented before pairwise automatic control is accepted.
2. Dispatch states denote outbox commitment, not broker/device acknowledgement.
   Actual Debezium routing/publication is a separate requested milestone.
3. Select at most the needed reduction. Insufficient flexible capacity creates a
   reviewed hold plan without issuing an under-sized automatic correction.
4. Allow one retransmission within the original TTL; preserve the entire logical
   event, command and expiry. Give the physical outbox delivery a distinct row ID.
   Never renew TTL or replay old-epoch commands as new commands.
5. Timeout/partial outcomes remain held for explicit operator reconciliation.
   Late results are durable evidence and cannot auto-release a held plan. Result
   handling enforces exhausted deadlines even if the timer did not run during
   downtime. Corrected windows preserve both review status and visible safe hold.
6. Count distinct consecutive post-confirmation recovery windows. Require fresh
   current device/capability state plus projected restored load below critical.
   These conservative guards resolve safe restoration beyond merely observing
   two low aggregate values; missing telemetry cannot justify restoration.

## Consequences and alternatives

A single store lock keeps this finite standalone slice easy to verify across
processes/threads; replace with transformer-scoped locking only when throughput
requires it. One active saga per transformer is stronger than per-policy-epoch
uniqueness and prevents concurrent policy updates from overcorrecting a device.
Corrections keep history and cannot create another plan for a previously acted
window. Existing active plans pin their full policy snapshot.

Fixtures assert durable transition sequences and outbox contents after restart.
No success-returning publisher/lease/operator stub is supplied. Live Registry
observation acquisition, Kafka consumers, CDC, distributed leader allocation,
operator compensation API, kind packaging and operational gates remain explicit
future work. No model is required for deterministic control.

The later requested CDC/leadership gate is now implemented in
[ADR-029](ADR-029-outbox-and-fencing.md); the future-work list above records the
original standalone milestone boundary.
