# ADR-035 — Local observability and collected incident evidence

- Status: Accepted within the requested observability milestone; execution evidence
  is maintained separately in IMPLEMENTATION_STATUS.md.

Use the already selected Collector 0.120.0/chart 0.117.3,
kube-prometheus-stack 69.8.2 (Operator 0.80.1/Prometheus 3.2.1),
Grafana 11.6.0 and single-binary Tempo 2.7.1/chart 1.18.2. Chart archives,
including packaged dependencies, are SHA-256 checked. Prometheus, Tempo and
Grafana use local PVCs; retention is 24 hours for metrics/traces. ClusterIP
services and explicit loopback port-forwards provide inspection. No cloud,
external reporting, log backend, OTel Operator or model call is introduced.

The explicit Python boundary instrumentation uses OTel SDK/API/OTLP HTTP
exporter 1.45.2, compatible with FastAPI's existing API >=1.44 requirement and
PyFlink's Protobuf 5.29.6. The earlier 1.31.1 candidate failed that dependency
check and is not an accepted runtime. A separate additive hash lock covers
macOS ARM64 and Linux ARM64. Install it in each component's own venv/image;
no component imports another service's implementation for instrumentation.
With no OTLP endpoint, standalone domain/tests use JSON logs without exporters.
When enabled, SDK import/configuration failure fails startup. Export queues,
timeouts, batch sizes, Collector memory, and retention are finite. Collection
loss does not hold a database transaction or change control decisions.

## Actual boundaries and semantics

- Devices spans Kafka enqueue and asynchronous delivery acknowledgements
  separately. A successful enqueue span is not a committed-delivery claim.
  Processing spans cover durable gateway effects, result delivery and offset
  commits. Current processing context is persisted in the result outbox.
- Devices Registry spans include authorization failures, RPC deadlines and the
  actual PostgreSQL transaction. Devices/3/4 gRPC clients inject current W3C
  context within their existing shared deadlines and retry policies.
- Aggregation Table source reads native Kafka header metadata; Table sinks write
  header metadata without changing raw event bodies or transactional sinks.
  Selected reading context is retained per device/window in the existing
  checkpointed run object. A publication span links to a contributing reading;
  the processing span remains its parent. The 100,000-event run ceiling and
  stream semantics remain unchanged. Existing old checkpoints lack this optional
  context and safely begin producing it with new selected readings.
- Action spans real load/result/timer/reconciliation, SQL transaction commits,
  and Lease calls. Additive outbox transport columns retain traceparent and
  envelope correlation/causation IDs through Debezium's header placement.
  Retrying a command retains its logical envelope and deduplication identity.
- Projection spans Kafka projection, SQL commits, HTTP response-start latency and
  the entire SSE lifecycle. An internal sequence-keyed trace table follows the
  existing stream retention/deletion rules. SSE links to the originating
  projection span across API replicas/restarts; JSON payloads remain v1.
  Snapshot resets and reconnects have bounded counters. Browser delivery/render
  acknowledgement is not implied by a successful ASGI send.

Structured JSON is emitted on stderr (also captured by `kubectl logs`) so CLI
JSON output remains parseable. IDs belong in logs/spans, never metric labels.
Metrics use only fixed boundary/operation/outcome names, fixed business signal
names, service/instance identities and the configured transformer inventory
(default `tx-001`). Kafka exporter discovery is restricted to managed topic/
group prefixes. Flink's bundled native Prometheus plugin exports checkpoint,
backpressure and semantic watermark signals; no custom exporter is written.

## Resolved contradictions and limitations

Earlier docs described full observability as deferred after kind packaging.
This milestone adds it to kind installation before application deployment.
The v1 event/proto/OpenAPI shapes are unchanged. SQL outbox/stream additions
are owned internal transport state, not public contract additions.
The old 8-second load SLO remains withdrawn: contractual watermark delay is
10 seconds. No replacement event-time SLO or sustained capacity claim is made.

The seeded diagnostic locks only a fresh projection schema's metadata table.
The real API must time out with 503, then pass three sub-second reads after
rollback. An automated packet queries Prometheus and Tempo and matches
structured logs by trace ID; absence/mismatch is failure. It includes baseline,
actual error, business snapshot impact, measured injection-to-recovery and
mitigation-to-recovery durations, hypothesis, alternative and missing signal.
This proves a bounded database-lock incident, not database crash, broker-loss,
leader handover or all proposed SLOs. Copilot remains a separate milestone.
