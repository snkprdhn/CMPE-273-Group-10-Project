# ADR-033 — Live component integration

- Status: Accepted implementation decisions for the requested integration milestone;
  acceptance depends on executed evidence in the implementation status.
- Owners: Member_1 simulation/Registry/observability; Member_2 Kafka/PyFlink;
  Member_3 control/PostgreSQL/Debezium; Member_4 projection/API/UI/identity/Copilot.

## Decision

Keep the existing independently runnable component profiles and their fixtures.
Integration uses real local dependencies and explicit topic namespaces. Member_2
accepts an optional shared namespace; its standalone namespace and unique Flink
transaction identity remain unchanged. Capacity comes from the same versioned
topology as the producer, rather than the unrelated 100 kW golden-test topology.

Registry v1 supplies capabilities only. Control must join those capabilities to
actual validated charger telemetry; a heartbeat never supplies a current rate or
proves restoration safety. Missing or stale observations retain the conservative
hold behavior. Registry and projection clients use generated RPC bindings, not
another member's implementation or database tables. Inventory IDs and transformer
configuration are explicit run inputs because Registry v1 has no inventory-list
or transformer-policy RPC.

## Resolved scenario mismatch

The Member_1 standalone waveform's overload is approximately 23.2 kW on a 20 kW
transformer. Its single charger offers only 6.2 kW flexibility, insufficient to
reach the frozen 15 kW recovery target. A reviewed hold is correct for that input;
changing control thresholds to force success would violate the accepted policy.
The integrated success scenario instead specifies meter 15 kW, EV 7.2 kW and
solar -2 kW: net 20.2 kW, required reduction 5.2 kW, commanded EV rate 2 kW and
post-throttle net 15 kW. Recovery meter demand of 8 kW gives net 8 kW while
throttled and 13.2 kW after restoring 7.2 kW. Restore still requires two distinct
fresh post-confirmation windows and current verified capabilities/rates.

## Acceptance boundary

Run each pairwise gate independently and preserve failures as evidence. A full
loop requires measured follow-up loads, stable action/command identities,
acknowledgements, and final restored state; publishing a command or receiving an
HTTP success does not establish those outcomes. Record relative scenario phases
and the actual UTC start so event-time IDs can be reproduced from the manifest.

Local dependency/host integration evidence does not establish the required final
kind workload deployment, NetworkPolicies, Kubernetes service identities or HA.
Those claims require their own executed deployment checks. No cloud or model
provider is involved.

The live Action worker publishes classification evidence using the existing
alert envelope. OPEN events classify WARNING or CRITICAL for an exact load
window and revision. A RESOLVED event with the explicit owner classification
message provides positive NORMAL evidence for that same window and revision.
Generic resolved alerts do not classify load. An aged load enters safe hold.
The timer preserves the prior recovery streak until the next load arrives.
The next load must itself be fresh and contiguous before it can count toward
restore. A missing or unsafe window cannot trigger restore.
