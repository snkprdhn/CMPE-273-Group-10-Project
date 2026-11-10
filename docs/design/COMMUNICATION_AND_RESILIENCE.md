# Communication, Deadlines, and Resilience

## Communication choices

| Boundary | Style | Reason |
|---|---|---|
| Browser to API | REST | Resource queries, configuration, standard browser tooling |
| API to browser | SSE | Ordered one-way live updates with cursor recovery |
| Simulator/action to Device Registry | gRPC | Typed internal capability contract and explicit deadlines |
| Domain workflow | Kafka | Durable asynchronous events, independent consumers, replay |
| Browser/API to Copilot Service | REST plus bounded SSE progress | Authenticated investigation creation, status, cancellation, and incremental UI feedback |
| Copilot Service to Tool Gateway | Internal REST with typed schemas | Read-only evidence retrieval, explicit authorization, deadlines, and bounded results |
| Copilot Service to model provider | Provider HTTPS API | Tool selection and structured explanation outside the deterministic control path |

## Deadline and retry policy

Every synchronous request has an end-to-end deadline. Downstream calls receive the remaining budget rather than a new full timeout. One layer owns retries, attempts are bounded, exponential backoff uses jitter, and only safe/idempotent operations are retried automatically. Cancellation does not undo a committed write.

| Dependency | Retry owner | Protection |
|---|---|---|
| API → Registry gRPC | Calling API/action service | Deadline, circuit breaker, concurrency bulkhead |
| Action → database | Database adapter | Transaction timeout, bounded pool, no blind transaction replay |
| Debezium outbox → Kafka | Kafka Connect | Stable event ID, connector backoff, bounded backlog alert |
| UI → API | Browser client | Bounded reconnect, jitter, authoritative refresh |
| SSE client | Projection service/client | Heartbeat, cursor, bounded per-client buffer |
| Copilot → model provider | Copilot Service | End-to-end deadline, safe transient retry, breaker, concurrency/rate-limit bulkhead |
| Copilot → Tool Gateway | Copilot Service | Per-tool deadline, no blind retry of partial multi-tool workflows |

## Circuit breaker and overload

Breakers use closed/open/half-open states with measured thresholds and limited probes. Bulkheads isolate registry, database, Kafka publication, and SSE client capacity. Bounded queues reject or defer work visibly rather than growing without limit. Backpressure and retry amplification are first-class metrics.

No fallback may claim a successful charger action when its result is unknown. When authoritative capability or fresh load state is unavailable, new automatic control plans pause and an operator-visible data-quality alert is raised.

Agent degradation is isolated: provider unavailability opens only the Copilot breaker and produces an explicit unavailable/partial result. It cannot fail Action, aggregation, projection, or SSE readiness. The mock provider is a test/demo fallback and must be labeled rather than presented as a live-model answer.

## Required experiments

Slow gRPC beyond its deadline; exhaust a bounded pool; open and recover a breaker; disconnect SSE; delay Kafka publication; time out the model provider; fail one agent tool; and verify exactly one retry owner. Record attempts per logical operation, remaining budget, queue age, error ratio, and recovery.
