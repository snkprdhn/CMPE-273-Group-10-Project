# HTTP and Live-Update API Contracts

Base path: `/api/v1`. JSON uses `snake_case`; timestamps are UTC ISO 8601. List endpoints use `limit` and opaque `cursor`. Errors use `{ "error": { "code", "message", "request_id", "details" } }`.

The API runs as multiple Kubernetes replicas behind a Service. Configuration writes use optimistic versions and all database access uses the single required PostgreSQL primary. Responses still include data freshness where it affects operator interpretation. Trace context and authenticated role are preserved across internal calls.

## Planned endpoints

| Method and path | Role | Purpose |
|---|---|---|
| `GET /overview` | Viewer | Current KPIs and highest-risk transformers |
| `GET /transformers` | Viewer | Filtered transformer list |
| `GET /transformers/{id}` | Viewer | Current state and policy |
| `GET /transformers/{id}/loads` | Viewer | Time-range load history |
| `PATCH /transformers/{id}/policy` | Administrator | Create a validated policy version |
| `GET /devices` | Viewer | Filtered device inventory |
| `GET /devices/{id}` | Viewer | Device status, telemetry summary, and command history |
| `GET /alerts` | Viewer | Alert history |
| `POST /alerts/{id}/acknowledge` | Operator | Record acknowledgement |
| `GET /actions` | Viewer | Control plans and outcomes |
| `GET /actions/{id}` | Viewer | Decision explanation and commands |
| `GET /scenarios` | Viewer | Available scenarios and active run |
| `POST /scenario-runs` | Operator | Start an approved scenario |
| `POST /scenario-runs/{id}/stop` | Operator | Stop the active scenario |
| `POST /investigations` | Operator | Create a transformer/incident-scoped read-only agent investigation; returns `202` and an investigation ID |
| `GET /investigations/{id}` | Operator | Get status, structured result, usage metadata, and evidence references |
| `GET /investigations/{id}/events` | Operator | Stream bounded progress and final-result events for one investigation |
| `POST /investigations/{id}/cancel` | Operator | Idempotently cancel an investigation owned by the caller or allowed role |
| `GET /health` | Public/local | API liveness |
| `GET /ready` | Public/local | Dependency readiness |
| `GET /events` | Viewer | Server-Sent Events stream |

## SSE contract

Event types are `transformer.updated`, `alert.opened`, `alert.updated`, `action.updated`, `device.updated`, `scenario.updated`, and `system.health`. Each message includes an event cursor, entity ID, version, event time, and payload. Clients reconnect with `Last-Event-ID`; if the cursor cannot be replayed, the server emits `snapshot.required` and the client reloads REST state.

The dedicated investigation stream uses `investigation.started`, `investigation.tool_started`, `investigation.tool_completed`, `investigation.partial`, `investigation.completed`, `investigation.failed`, and `investigation.cancelled`. Progress events expose tool names and status but not hidden model reasoning, credentials, or raw prompts. Reconnecting clients first retrieve authoritative investigation status and then resume from the last retained event.

## Investigation contract

`POST /investigations` accepts a focused question plus an optional `transformer_id`, `action_id`, and bounded UTC time window. The server derives known entity context from the current UI route instead of asking the model to invent IDs. The result contains `summary`, `observations`, `hypotheses`, `unknowns`, `recommended_next_steps`, `evidence`, `freshness`, `provider_mode`, and status.

Every evidence item contains an investigation-local evidence ID, source type, entity/correlation reference, observed timestamp, freshness, truncation flag, and an authorized application deep link. The server rejects or labels factual observations whose evidence IDs were not returned by a tool in the same investigation.

## Status codes

`400` invalid request, `401` unauthenticated, `403` insufficient role, `404` absent entity, `409` conflicting state or active scenario, `422` valid JSON with invalid domain values, `429` rate limited, and `503` dependency unavailable.

## Contract rules

- APIs return server-calculated status; the UI does not recreate threshold logic.
- Policy writes require optimistic version matching and an audit record.
- Starting or stopping a scenario is idempotent by client request ID.
- Investigation creation is rate/concurrency limited, has a fixed deadline/tool-turn budget, and never grants the model a write-capable tool.
- Tool authorization is enforced using the authenticated caller and requested scope; model output cannot widen access.
- Cancelling an investigation does not undo already completed model/tool requests and never affects grid processing.
- Health endpoints do not expose credentials or sensitive configuration.
- Exact examples and OpenAPI output will be added when implementation begins.
