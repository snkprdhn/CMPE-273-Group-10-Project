# Stream Semantics v1

Executable v1 definitions and fixtures are in [schemas](../../schemas/README.md). [ADR-025](../decisions/ADR-025-executable-contract-freeze.md) resolves exact IDs, defaults, compatibility, deadlines and auth rules. Devices telemetry/command and Aggregation standalone aggregation runtimes are implemented; the Action fixture-driven Action domain is implemented under [ADR-028](../decisions/ADR-028-action-domain.md), and the Projection standalone projection/API runtime is implemented under [ADR-030](../decisions/ADR-030-projections-api.md); see [local component integration](../decisions/ADR-033-live-component-integration.md) for the live profile. Aggregation runtime boundaries are in [ADR-027](../decisions/ADR-027-standalone-aggregation.md). See [implementation status](../implementation/IMPLEMENTATION_STATUS.md).

## Status and purpose

This document is the v1 implementation baseline for telemetry and transformer-load behavior. It resolves the cross-service questions that would otherwise block independent work. A change requires review from the affected producer and consumer plus updated schemas and golden fixtures.

## Device reporting and identity

- Simulated devices report once per second by default; scenario configuration may change the interval while recording it in the run manifest.
- `event_id` is created once and remains unchanged when a producer retries the same logical reading.
- `sequence_number` increases within one `boot_id`. A simulator restart creates a new `boot_id` and may restart the sequence at zero.
- Spontaneous telemetry uses `causation_id: null`. Telemetry emitted because a command changed the rate uses that `command_id` as `causation_id`.
- An event more than five seconds ahead of ingestion time is invalid. Clock-skew and delay scenarios must stay within an explicitly configured test override.

## Validation and quality

- `GOOD` readings participate normally.
- `ESTIMATED` readings participate but make the load window `PARTIAL` and increment `estimated_device_count`.
- `INVALID`, malformed, impossible, or excessively future-dated readings go to `grid.telemetry.dlq.v1` with a validation reason.
- A repeated `event_id` is ignored for aggregation and increments a deduplication metric. Identity is remembered for the retained window horizon (120 s behind the transformer watermark, [ADR-037](../decisions/ADR-037-continuous-fleet-and-rolling-retention.md)); a repeat of an older reading is audited on the late topic and never recounted.
- Missing readings are not converted to zero and are not silently carried forward in v1.

## Window aggregation

- Flink uses five-second tumbling event-time windows with half-open bounds `[window_start, window_end)`.
- A reading exactly at `window_end` belongs to the next window.
- When one device reports more than once in a window, its latest accepted reading by `(event_time, sequence_number)` represents that device for the window. Power samples are not summed.
- The transformer total is the sum of the selected per-device readings: consumption positive, solar generation negative.
- The watermark is ten seconds behind the greatest accepted event time.

## Transformer topology and capacity

Devices owns a versioned scenario-topology fixture containing transformer ID, capacity, expected device IDs/types, and assignment. Aggregation loads the same version as a ConfigMap or fixture; it does not call Devices or Action at runtime for capacity.

Capacity is fixed for one scenario run. Policy thresholds may change through Action's control boundary, but physical transformer capacity changes require a new topology version/run. A missing or non-positive capacity produces a `STALE` load with null utilization and `MISSING_CAPACITY`; the Action Service must not act on it.

## Completeness

The topology fixture provides `expected_device_count` for each transformer.

- `COMPLETE`: at least 95% of expected devices have `GOOD` readings in the window and capacity is valid.
- `PARTIAL`: at least 70% are represented but COMPLETE criteria are unmet, including any selected `ESTIMATED` reading. STALE takes precedence below 70%.
- `STALE`: fewer than 70% are represented, capacity is invalid, or the load result is already more than 15 seconds old when evaluated.

Load events include `expected_device_count`, `good_device_count`, `estimated_device_count`, `missing_device_count`, and `quality_flags`. Automatic control requires a fresh `COMPLETE` event; other states remain visible for operations and audit.

## Deterministic window and revision identity

- `window_id` is derived from `transformer_id`, UTC `window_start`, and window duration.
- The first emitted result has `revision: 0`.
- A corrected result keeps the same `window_id` and increments `revision` by one.
- The load `event_id` is deterministic from `(window_id, revision)`. Replaying the same committed result therefore reproduces the same ID.
- Consumers keep the highest processed revision for each window and ignore repeated or lower revisions.
- A correction updates historical projections but never erases an alert, action, or command already issued from an earlier revision.

## Late data

- Accepted events arriving at or before `window_end + 30 seconds` may create a corrected revision.
- The correction-limit boundary is inclusive; events arriving later are published to `grid.telemetry.late.v1` for audit and do not change control history.
- Very-late valid events are not poison records and therefore do not use the DLQ.

## Persistence and ownership

- Flink publishes `grid.transformer-load.v1` and never writes PostgreSQL.
- Action consumes loads for deterministic control.
- Projection consumes loads into the owned `projection` store for history, REST, SSE, UI, and agent evidence.
- Projection consumes `grid.action.v1`, alerts, and command results rather than reading Action's `control` tables.

## Health evidence

Aggregation exposes bounded metrics for input/output rate, per-partition lag, watermark delay, late/very-late counts, duplicate count, last successful checkpoint, checkpoint duration/failure, restart count, backpressure, and latest output time. Projection maps approved metric enums to these signals; the agent cannot submit arbitrary PromQL.

## Required golden cases

The shared contract fixtures include normal meter/EV/solar aggregation, repeated event ID, several readings from one device, estimated/missing/invalid readings, exact window boundary, exact lateness boundary, very-late audit, correction revision, missing capacity, checkpoint replay, and lower-revision replay. Each case records expected topics, payloads, state, and metrics.

