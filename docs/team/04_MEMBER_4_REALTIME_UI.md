# Member_4 — Real-Time Projection, Delivery, UI, and Integration

## Distributed-systems ownership

Member_4 owns the distributed read/operations product slice, not every component's platform work. This includes consuming domain events into owned query projections, REST/SSE delivery, the operator UI, Gateway/Keycloak, product SLO/health views, the read-only Tool Gateway and Copilot Service, and coordination of the final overlay. Member_1 installs the shared observability stack; every member owns their manifests, instrumentation, dashboard panel, and fault experiment.

## Implemented boundary — projections/API

The [standalone backend](../../member4/README.md) implements owned PostgreSQL
projections, Kafka replay/checkpoints, REST snapshots, filters, durable pagination,
SSE replay/reset, operator annotations and policy/scenario contract mocks.
[ADR-030](../decisions/ADR-030-member4-projections-api.md) records missing-context
and state/version decisions without moving domain logic out of its owner.
NiceGUI, Keycloak, Tool Gateway, Copilot and full kind packaging remain subsequent
milestones; the responsibilities below describe the full Member_4 workstream.

## Workstream responsibilities

1. Consume load, alert, action, result, and health events into idempotent read models.
2. Track entity versions so reordered or duplicate events cannot regress a projection.
3. Provide REST snapshots and SSE fan-out with cursors, heartbeats, bounded buffers, and slow-client handling.
4. Recover projections from durable data or replay and require a snapshot after an unrecoverable cursor gap.
5. Build the NiceGUI dashboard against the same snapshot and event contracts.
6. Implement loading, partial, stale, reconnecting, and unavailable states in the UI.
7. Define Prometheus metrics, Grafana dashboards, service health checks, and cross-service correlation IDs.
8. Consume recorded domain-event fixtures for independent projection, API, UI, and agent testing.
9. Own only shared Gateway/identity/observability resources and Member_4 workload manifests; combine, but do not author, other members' component overlays.
10. Correlate component-owned metrics, traces, logs, and dashboard panels from the Member_1-operated stack into the product SLO/error-budget evidence package.
11. Integrate Keycloak, the Helm-managed OpenTelemetry Collector, and Prometheus Operator; keep authentication and telemetry collection in mature platform components while owning application policy and instrumentation.
12. Define the seven typed tool schemas, evidence/output schema, authorization, bounds, deadlines, and agent audit lifecycle with the contributing component owners.
13. Build the provider-neutral adapter and deterministic mock-provider path without placing model logic in the control plane.
14. Implement the investigation drawer states, evidence deep links, AI-generated label, and provider/tool failure behavior.
15. Run the fixed agent evaluation suite for grounding, write-tool denial, prompt injection, stale evidence, latency, and provider outage.
16. Own the `projection` and `copilot_audit` logical stores and credentials without reading Member_3's `control` tables.

## Boundaries

Member_4 does not calculate grid load, make control decisions, implement other members' Kubernetes resources, or read their owned database schemas. Projections preserve server-published events, and the browser does not directly access Kafka or the database. The Copilot Service may explain projected evidence but cannot call Members 1–3 services, publish commands, update policies, start scenarios, or issue arbitrary infrastructure/data queries.

## Independent development profile

Replay recorded load, alert, action, result, and health fixtures into local projection storage. Use mock identities and deterministic model/tool responses. Member_4 can validate projections, REST/SSE, UI, authorization, agent grounding, and provider failure without live simulators, Flink, Action, or shared Kubernetes.

## Independent distributed demonstration

Feed duplicate and reordered domain events into the projection service, connect multiple live clients, disconnect and reconnect one client using its cursor, simulate a cursor gap, rebuild the snapshot, and show that the UI clearly represents fresh and stale states. Run a mock-provider investigation that selects several tools, cites valid evidence, rejects a write-tool request, and remains isolated during provider failure.

## Done when

Read models converge under replay, slow or disconnected clients do not block others, cursor recovery is verified, distributed failures are observable, and the professional UI explains live system behavior without inventing domain state. Agent answers pass the fixed evidence/security evaluations and the deterministic system remains healthy with the agent disabled.
