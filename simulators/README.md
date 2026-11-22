# Device simulators — Member_1

Implemented standalone deterministic EV, meter and solar simulation, normal /
overload / recovery scenarios, bounded Kafka publication, durable charger command
handling and results. The small owning package and complete runbook are in
[member1](../member1/README.md); its independent simulator runtime lock excludes
PostgreSQL/gRPC. Use `make member1-test`, `make member1-live`, `make member1-up`.

The topology and command fixtures are versioned under `member1/fixtures` and
validate against the shared executable contracts. SQLite persistence retains
command-ID deduplication, effects, greatest accepted epoch and pending results
across process/container restart; a new boot ID scopes new telemetry sequences.
This is one standalone shard, not a horizontally scaled fleet acceptance claim.
