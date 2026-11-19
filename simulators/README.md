# Device Simulators

**Status: design only; implementation not started.**

Planned home of EV charger, smart meter, solar, and scenario simulators. This component publishes `grid.telemetry.v1`, consumes `grid.charger-command.v1`, and publishes `grid.command-result.v1`. It owns partition-aware producers, retries, bounded buffering, reproducible seeds, delays, duplicates, outages, command deduplication, and the shared scenario/load/fault runner. Command fixtures and local Kafka make the edge profile independently testable. See the [event contract](../docs/contracts/EVENT_CONTRACTS.md), [independent testing plan](../docs/testing/INDEPENDENT_COMPONENT_TESTING.md), and [Member_1's work plan](../docs/team/01_MEMBER_1_INGESTION.md).
