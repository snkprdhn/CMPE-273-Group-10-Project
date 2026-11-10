# Event Contracts

This document is the canonical human-readable Kafka contract. Machine-readable JSON Schemas will be added under `schemas/` and registered in Apicurio Registry during implementation. Protocol Buffers remain limited to the gRPC interface.

## Common envelope

Every event contains `event_id` (UUID), `event_type`, `schema_version`, `source`, `event_time` (UTC ISO 8601), `ingested_at`, `correlation_id`, `causation_id`, trace context in message headers, and `payload`. Control events also carry `leader_epoch` and `saga_version`. Unknown fields may be ignored within a compatible version; removing or changing a field requires a new major version.

## `grid.telemetry.v1`

Key: `transformer_id`.

Payload fields: `device_id`, `transformer_id`, `device_type` (`EV_CHARGER`, `SMART_METER`, `SOLAR`), `power_kw`, `quality` (`GOOD`, `ESTIMATED`, `INVALID`), `sequence_number`, and optional EV fields `session_id`, `current_rate_kw`, `minimum_rate_kw`, `departure_time`, and `priority`.

```json
{
  "event_id": "2a4b25cc-497d-4dc7-b53f-e88d46f1573c",
  "event_type": "telemetry.reading",
  "schema_version": 1,
  "source": "simulator.ev-01",
  "event_time": "2026-11-09T18:30:05Z",
  "ingested_at": "2026-11-09T18:30:06Z",
  "correlation_id": "scenario-evening-001",
  "payload": {
    "device_id": "ev-001",
    "transformer_id": "tx-001",
    "device_type": "EV_CHARGER",
    "power_kw": 7.2,
    "quality": "GOOD",
    "sequence_number": 412
  }
}
```

## `grid.transformer-load.v1`

Key: `transformer_id`. Payload: `transformer_id`, `window_id`, `window_start`, `window_end`, `revision`, `meter_demand_kw`, `ev_demand_kw`, `solar_generation_kw`, `net_load_kw`, `capacity_kw`, `utilization_pct`, `device_count`, `late_event_count`, and `completeness` (`COMPLETE`, `PARTIAL`, `STALE`).

## `grid.alert.v1`

Key: `transformer_id`. Payload: `alert_id`, `transformer_id`, `severity`, `status`, `load_window_id`, `load_revision`, `message`, `opened_at`, and optional `resolved_at`.

## `grid.charger-command.v1`

Key: `device_id`. Payload: `command_id`, `action_id`, `device_id`, `transformer_id`, `command_type` (`THROTTLE`, `RESTORE`), `target_rate_kw`, `issued_at`, `expires_at`, `reason`, `policy_version`, `saga_version`, and `leader_epoch`. `command_id` is the idempotency key; a device rejects an epoch older than its greatest accepted epoch.

## `grid.command-result.v1`

Key: `device_id`. Payload: `command_id`, `action_id`, `device_id`, `outcome` (`APPLIED`, `ALREADY_APPLIED`, `REJECTED`, `EXPIRED`, `FAILED`), `previous_rate_kw`, `applied_rate_kw`, `result_time`, and optional `reason`.

## Validation and compatibility

- Power is numeric kW; consumption is positive and generation is negative.
- Event time cannot be unreasonably far in the future.
- IDs and required fields cannot be blank.
- Invalid or poison telemetry goes to the `grid.telemetry.dlq.v1` dead-letter topic with the original event, failure stage, attempt count, and validation reasons.
- Producers add optional fields before requiring consumers to use them.
- Breaking changes require a new topic/schema major version and a migration window.
- CI validates examples against their JSON Schemas and checks the configured Apicurio compatibility rule before deployment.
