# Device Simulators

**Status: design only; implementation not started.**

Planned home of EV charger, smart meter, solar, and scenario simulators. This component publishes `grid.telemetry.v1`, consumes `grid.charger-command.v1`, and publishes `grid.command-result.v1`. It owns partition-aware producers, retries, bounded buffering, reproducible seeds, delays, duplicates, outages, and command deduplication. See the [event contract](../docs/contracts/EVENT_CONTRACTS.md) and [Member_1's work plan](../docs/team/01_MEMBER_1_INGESTION.md).
