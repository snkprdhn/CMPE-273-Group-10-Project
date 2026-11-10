# Agent Tool and Evidence Contracts

## Boundary

These are internal read-only contracts between the Copilot Service and Tool Gateway. The model proposes a tool name and typed arguments, but server code authenticates, authorizes, validates, limits, and executes every call. No provider-generated URL, query language, credential, or executable text is accepted.

## Common request context

Every executed call receives server-owned context that the model cannot override:

| Field | Meaning |
|---|---|
| `investigation_id` | Current bounded workflow identifier |
| `actor_id` and `roles` | Authenticated caller and allowed scope |
| `deadline` | Remaining server deadline |
| `entity_scope` | Approved transformer/action/correlation scope |
| `time_scope` | Approved UTC start/end range |
| `max_items` | Server cap, even if the model asks for more |
| `trace_context` | Correlation for internal observability |

## Common evidence envelope

Every successful tool returns one or more evidence items with:

`evidence_id`, `source_type`, `source_version`, `entity_type`, `entity_id`, `observed_at`, `freshness` (`FRESH`, `STALE`, `PARTIAL`, `UNKNOWN`), `truncated`, `deep_link`, and a typed `data` object.

Evidence IDs are unique within an investigation. Deep links target an authorized application view, not a backend endpoint. Tool output is treated as untrusted data when returned to the model.

## Allowlisted tools

### `get_transformer_snapshot`

Input: `transformer_id`. Output: state, net load, utilization, capacity, load revision, completeness, policy version, and data age. The ID must match the approved entity scope.

### `get_load_history`

Input: `transformer_id`, `start`, `end`, and requested limit. Output: downsampled load windows with revisions, category totals, completeness, and late-event count. The server clamps the range and limit.

### `get_action_timeline`

Input: `transformer_id`, `start`, and `end`. Output: alert/action transitions, trigger window, policy version, expected reduction, status, and related action IDs. It does not expose mutation methods.

### `get_command_results`

Input: `action_id`. Output: command IDs, target device aliases, type, expected/applied rate, acknowledgement outcome, deadline, and aggregate confirmed reduction. Sensitive or unnecessary device fields are omitted.

### `get_stream_health`

Input: enum list selected from `KAFKA`, `FLINK`, `ACTION`, `API`, `REGISTRY`, and `DATABASE`. Output: predefined lag, watermark, checkpoint, readiness, restart, and freshness summaries. It does not accept PromQL or Kubernetes object selectors.

### `get_metric_window`

Input: approved metric enum, optional approved entity ID, `start`, `end`, and resolution enum. Output: downsampled points, unit, threshold metadata, missing-data intervals, and query timestamp. Metric enums map to server-owned Prometheus queries.

### `get_trace_summary`

Input: trace ID or correlation ID already present in investigation evidence. Output: redacted service/span path, durations, error categories, retry count, and time bounds. Events/attributes are allowlisted and size bounded.

## Error contract

Tool errors use `INVALID_ARGUMENT`, `OUT_OF_SCOPE`, `FORBIDDEN`, `NOT_FOUND`, `STALE`, `RATE_LIMITED`, `DEADLINE_EXCEEDED`, `DEPENDENCY_UNAVAILABLE`, or `RESULT_TOO_LARGE`. The response includes a safe message, retryability, and observed timestamp. It never returns credentials, backend query text, stack traces, or existence details the actor is not allowed to see.

## Final answer schema

The provider returns a structured draft containing `summary`, `observations`, `hypotheses`, `unknowns`, `recommended_next_steps`, and `evidence_ids`. Server validation confirms the schema, verifies every evidence ID, and prevents a hypothesis from being displayed as a direct observation. Invalid drafts may receive one bounded repair attempt before the investigation becomes partial or failed.

## Versioning

Tool and answer schemas carry explicit versions. Additive optional fields are compatible; removing fields, changing semantics, widening access, or adding a tool requires contract review and an ADR/security update. The deterministic mock provider and evaluation fixtures are versioned with the contracts.

