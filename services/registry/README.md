# Device Registry

**Status: design only; implementation not started.**

Planned replicated gRPC service for device registration, heartbeat, transformer assignment, and charger capabilities. It exists to demonstrate typed RPC, deadlines, service discovery, replica routing, and circuit-breaker behavior; telemetry and commands remain asynchronous Kafka contracts. See the [gRPC contract](../../docs/contracts/GRPC_CONTRACTS.md).
