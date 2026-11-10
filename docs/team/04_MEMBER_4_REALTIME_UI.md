# Member_4 — Real-Time Projection, Delivery, UI, and Integration

## Distributed-systems ownership

Member_4 owns the Kubernetes platform and distributed read/operations side, not only the UI. This includes cluster discovery and health behavior, consuming domain events into query projections, distributing live updates, OpenTelemetry correlation, the read-only Tool Gateway and Copilot Service, SLOs, security integration, and coordinated fault injection.

## Planned responsibilities

1. Consume load, alert, action, result, and health events into idempotent read models.
2. Track entity versions so reordered or duplicate events cannot regress a projection.
3. Provide REST snapshots and SSE fan-out with cursors, heartbeats, bounded buffers, and slow-client handling.
4. Recover projections from durable data or replay and require a snapshot after an unrecoverable cursor gap.
5. Build the React dashboard against the same snapshot and event contracts.
6. Implement loading, partial, stale, reconnecting, and unavailable states in the UI.
7. Define Prometheus metrics, Grafana dashboards, service health checks, and cross-service correlation IDs.
8. Own local orchestration, network-fault scenarios, integration fixtures, CI planning, and the final end-to-end demonstration.
9. Define Kubernetes workloads, Services, probes, resources, scaling, leases, RBAC, and NetworkPolicies with the relevant component owner.
10. Correlate metrics, traces, and logs and maintain the SLO/error-budget evidence package.
11. Integrate Keycloak, the OpenTelemetry Operator, and Prometheus Operator; keep authentication and telemetry collection in mature platform components while owning application policy and instrumentation.
12. Define the seven typed tool schemas, evidence/output schema, authorization, bounds, deadlines, and agent audit lifecycle with the contributing component owners.
13. Build the provider-neutral adapter and deterministic mock-provider path without placing model logic in the control plane.
14. Implement the investigation drawer states, evidence deep links, AI-generated label, and provider/tool failure behavior.
15. Run the fixed agent evaluation suite for grounding, write-tool denial, prompt injection, stale evidence, latency, and provider outage.

## Boundaries

Member_4 does not calculate grid load or make control decisions. Projections preserve server-published domain state, and the browser does not directly access Kafka or the database. The Copilot Service may explain evidence but cannot publish commands, update policies, start scenarios, or issue arbitrary infrastructure/data queries.

## Independent distributed demonstration

Feed duplicate and reordered domain events into the projection service, connect multiple live clients, disconnect and reconnect one client using its cursor, simulate a cursor gap, rebuild the snapshot, and show that the UI clearly represents fresh and stale states. Run a mock-provider investigation that selects several tools, cites valid evidence, rejects a write-tool request, and remains isolated during provider failure.

## Done when

Read models converge under replay, slow or disconnected clients do not block others, cursor recovery is verified, distributed failures are observable, and the professional UI explains live system behavior without inventing domain state. Agent answers pass the fixed evidence/security evaluations and the deterministic system remains healthy with the agent disabled.
