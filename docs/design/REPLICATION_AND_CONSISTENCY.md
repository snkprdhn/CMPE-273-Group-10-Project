# Replication and Consistency Decisions

## Required replicated systems

The architecture plans a three-broker Kafka review profile, while Kubernetes control state is backed by its consensus store. Kafka KRaft and Kubernetes/etcd rely on majority quorum availability for their respective metadata/control decisions. PostgreSQL uses one durable primary in the required scope. These systems provide different guarantees and must not be described with one universal CAP label.

## Operation-specific guarantees

| Operation | Guarantee and route | Partition behavior |
|---|---|---|
| Create/advance control saga | Primary transaction with optimistic version | Prefer consistency; fail rather than create competing plans |
| Validate fencing epoch | Consensus-backed lease/current durable epoch | Reject or wait when current epoch cannot be verified |
| Change transformer policy | Primary write plus read-your-writes | Return unavailable/conflict rather than accept an uncertain update |
| Historical chart | Primary database read with response freshness | Temporary unavailability is preferable to fabricated current data |
| Current critical state | Primary/materialized live projection with freshness | Mark stale; do not present old state as current |
| Kafka event durability | Replicated topic, intentional acknowledgements and ISR policy | Availability depends on remaining in-sync replicas |
| Agent explanation | Bounded snapshot may be stale and must carry freshness/version evidence | Agent cannot execute, mutate, or present stale context as current |

## Required experiment

Stop one Kafka broker in the replicated profile and record topic availability, in-sync replicas, consumer behavior, and recovery. Demonstrate optimistic version rejection for one concurrent policy update and explain why a single durable database primary was selected for the project timeline.

## Stretch experiment

If all required scope is stable, add a PostgreSQL read replica, pause replay, compare primary and replica versions, label the stale historical view, resume replay, and measure convergence. Automatic failover remains excluded.

## CAP and PACELC statement

During a partition, critical control writes choose consistency over availability. Historical reads may remain available with explicit staleness. During normal operation, remote acknowledgement and replication improve durability/consistency at a latency cost. The final report will record actual acknowledgement points and measured latency rather than assign a product-wide label.
