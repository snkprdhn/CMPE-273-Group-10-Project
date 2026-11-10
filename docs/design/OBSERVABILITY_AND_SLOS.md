# Observability, SLIs, and SLOs

## Objectives

The platform must explain a user-visible grid outcome across HTTP/gRPC, Kafka, Flink, persistence, control processing, and SSE. Metrics identify scope, traces show the path, and structured logs explain local decisions. No single dashboard is treated as proof of cause.

## Planned pipeline

OpenTelemetry SDKs emit traces, metrics, and correlated log context to an OpenTelemetry Collector managed by the OpenTelemetry Operator. The Prometheus Operator manages metric discovery and storage, Tempo stores traces, and services write structured JSON to Kubernetes logs. Loki aggregation is a stretch goal; Grafana provides required metric and trace views. The team configures and instruments these projects instead of writing a collector, monitoring store, or trace backend.

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
| Fresh load update visible | 99% within 8 seconds of event time |
| Critical decision latency | 99% within 2 seconds of a complete critical load event |
| Command acknowledgement | 99% within the configured deadline |
| Query API good requests | 99.5% successful within 1 second during the measured window |
| Projection freshness | 99% less than 10 seconds old |
| Duplicate logical effects | Zero in acceptance and replay tests |
| Agent evidence integrity | 100% of displayed evidence IDs resolve to results returned in that investigation |
| Agent authorization | 100% of attempted unavailable/write tools are rejected in the evaluation suite |

Each SLI defines eligible events, a good event, exclusions, aggregation window, and missing-data behavior. Error-budget calculations count one bad logical operation once even if it is both slow and unsuccessful.

## Required diagnostic exercise

Measure a stable baseline, inject bounded dependency latency or pod failure, restore service, and compare equivalent windows. Preserve trace IDs and record observations separately from hypotheses. Evidence must include one metric, one trace, one correlated log sequence, a business impact, recovery time, and an instrumentation gap or alternative explanation.

The Grid Incident Investigator may assemble this evidence for the operator, but its answer is another observed artifact rather than proof of root cause. The human diagnostic exercise must verify the linked evidence and record whether the agent omitted, contradicted, or overstated any signal.

## Stretch: anomaly detection

After required observability is stable, the UI may flag departures from a rolling baseline for load, lag, error ratio, or acknowledgement latency. A simple transparent statistical rule is preferred before AI-assisted detection. Every anomaly retains the window, baseline, threshold, and contributing signals; correlation is not labeled as root cause.
