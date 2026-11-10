# Device Registry gRPC Contract

## Purpose

The internal Device Registry provides typed, synchronous access to relatively stable device capabilities. High-volume telemetry and control outcomes remain Kafka events. The contract is planned; no `.proto` file or server has been implemented.

## Planned service

| RPC | Shape | Semantics |
|---|---|---|
| `RegisterDevice` | Unary | Idempotently create/update a device registration by device/version |
| `Heartbeat` | Unary | Update liveness and current capability version |
| `GetDevice` | Unary | Return authoritative capability and transformer assignment |
| `ListEligibleChargers` | Server streaming or paginated unary | Return bounded candidates for one transformer/policy version |

Messages include device ID, transformer ID, type, controllability, min/max rate, priority, departure constraint, capability version, and update time. Mutating calls carry an operation/idempotency key.

## Deadlines and failures

Clients set an end-to-end deadline and propagate remaining time. `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`, `UNAVAILABLE`, and `DEADLINE_EXCEEDED` have explicit meanings. Calls are not retried after deadline exhaustion. Registration/heartbeat may be retried with the same operation ID; a control plan must not proceed from unverifiable or stale capabilities.

## Security and observability

Only simulator identities register/heartbeat. Action/API identities may read required capabilities. The registry cannot publish charger commands. Trace context, request/operation ID, caller identity, deadline, outcome, and redacted error category are recorded.
