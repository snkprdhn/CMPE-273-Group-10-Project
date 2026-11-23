# Observability, SLIs, and SLOs

## Objectives

The platform must explain a user-visible grid outcome across HTTP/gRPC, Kafka, Flink, persistence, control processing, and SSE. Metrics identify scope, traces show the path, and structured logs explain local decisions. No single dashboard is treated as proof of cause.

## Implemented local pipeline

Explicit OpenTelemetry SDK instrumentation emits traces and metrics to a Helm-managed OpenTelemetry Collector. Correlated JSON logs go to the Kubernetes container log stream. The Prometheus Operator manages metric discovery and storage, Tempo stores traces, and services write structured JSON to Kubernetes logs. Grafana provides required metric and trace views. A separate log backend is excluded; `kubectl logs` plus correlation fields provide bounded class-demo log evidence. The team configures and instruments these projects instead of writing a collector, monitoring store, or trace backend.

W3C trace context propagates over HTTP/gRPC. Kafka records carry `traceparent`, `correlation_id`, and `causation_id` headers. Consumers create processing spans and use span links when asynchronous fan-out makes a strict parent relationship misleading.

## Required structured fields

`timestamp`, `severity`, `service`, `version`, `instance`, `trace_id`, `span_id`, `correlation_id`, `causation_id`, `event_id`, `transformer_id`, `action_id`, `command_id`, `investigation_id`, `tool_name`, `attempt`, `outcome`, `remaining_deadline_ms`, and a redacted error category.

Individual event/device IDs stay out of metric labels to prevent unbounded cardinality.

## Technical and business metrics

| Area | Planned signals |
|---|---|
| Producers | Events/sec, retries, bounded-buffer use, failed publication |
| Kafka | Partition throughput, consumer lag, under-replicated partitions, ISR changes |
| Flink | Watermark delay, late events, checkpoint age/duration/failure, backpressure |
| Control | Plans, commands per logical action, retries, timeouts, duplicate effects prevented |
| Database | Query latency, connection saturation, replication lag, outbox backlog |
| API/SSE | Request rate/error/latency, active clients, reconnects, cursor gaps, dropped updates |
| Kubernetes | Ready replicas, restarts, scheduling delay, resource saturation |
| Grid outcome | Critical transformers, time critical, expected/confirmed reduction, partial plans |
| Agent | Investigation outcome/duration, model/tool latency, tool errors, provider throttling, breaker state, token use, evidence count, rejected unsupported claims, mock-mode use |

## Proposed SLIs and SLOs

These are targets until measured on a named environment.

| SLI | Proposed SLO |
|---|---|
| Fresh load update visible | Measure from `window_end`, allowing the contractual 10-second watermark delay; set percentile target after baseline |
| Critical decision latency | 99% within 2 seconds of a complete critical load event |
| Command acknowledgement | 99% within the configured deadline |
| Query API good requests | 99.5% successful within 1 second during the measured window |
| Projection freshness | Measure publication-to-projection delay separately from event-time/window age; set target after baseline |
| Duplicate logical effects | Zero in acceptance and replay tests |
| Agent evidence integrity | 100% of displayed evidence IDs resolve to results returned in that investigation |
| Agent authorization | 100% of attempted unavailable/write tools are rejected in the evaluation suite |

The former 8-second event-time load target conflicts with the canonical 10-second watermark delay and is withdrawn. This preparation milestone preserves stream semantics; it does not claim a measured replacement latency SLO.

Each SLI defines eligible events, a good event, exclusions, aggregation window, and missing-data behavior. Error-budget calculations count one bad logical operation once even if it is both slow and unsuccessful.

## Required diagnostic exercise

Measure a stable baseline, inject bounded dependency latency or pod failure, restore service, and compare equivalent windows. Preserve trace IDs and record observations separately from hypotheses. Evidence must include one metric, one trace, one correlated log sequence, a business impact, recovery time, and an instrumentation gap or alternative explanation.

The Grid Incident Investigator may assemble this evidence for the operator, but its answer is another observed artifact rather than proof of root cause. The human diagnostic exercise must verify the linked evidence and record whether the agent omitted, contradicted, or overstated any signal.

## Component ownership

Each member defines bounded-cardinality metrics, spans, structured fields, a dashboard panel, alert evidence, and one fault experiment for their own workloads. Member_1 operates the shared collection/visualization stack. Member_4 combines submitted evidence into product SLO/error-budget views but does not add instrumentation inside another member's component.

## Implemented milestone boundary

[ADR-035](../decisions/ADR-035-local-observability.md) and the
[local runbook](../../infra/observability/README.md) define actual instrumented
boundaries, dashboards, finite cardinality/retention, and the automated SQL-lock
incident packet. Service logs are JSON on stderr; the Collector handles traces
and metrics, while `kubectl logs` preserves bounded log evidence. No separate
log store is installed. Native Flink Prometheus metrics and the operator-managed
Kafka exporter provide runtime health. Copilot/agent metrics remain deferred.

The query dashboard implements the proposed joint success/response-start-latency
SLI: each non-health, non-SSE GET is eligible once, including invalid or
unauthorized requests; a good response is <400 and <=1 second. The denominator
counts every eligible response; the numerator counts only jointly good responses.
Five-minute empty/missing windows are unknown. The 99.5% target is proposed,
not a claim inferred from a short seeded exercise. Other proposed targets still
require their own measured acceptance windows. Do not infer database replication
health or exact persistent action counts from the transaction/outcome metrics.
