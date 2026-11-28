# ADR-031 — Operator dashboard

- Status: Accepted, 2026-11-22
- Authority: Requested dashboard milestone and routine implementation decisions

## Decision

Implement the console in the selected NiceGUI 3.19.0 with built-in ECharts,
HTTPX 0.28.1 and the existing jsonschema 4.23.0 validator. Runtime and browser-test
locks target CPython 3.12/macOS ARM64, with separate environments. No project
JavaScript, new frontend framework, service-table reads or control calculations.
Bind the UI and explicit fixture API to loopback; accept only loopback HTTP API
URLs. The configured backend bearer token stays on the Python server. This is
one configured local identity per UI process, not user authentication or OIDC.

Each browser client has independent navigation, filters and REST/SSE state.
REST responses and SSE envelopes are checked against the frozen bundle. Anchor
before reading the collections; page through the API's opaque cursors. SSE
invalidates authoritative REST views rather than independently merging domain
payloads. Ignore duplicate/older entity versions. Reconnect takes fresh snapshots
and subscribes with their `X-Event-ID`; a cursor reset does the same. Bounded
backoff and the HTTP read timeout expose reconnecting on stream closure or missed
15-second heartbeats. Refresh every ten seconds to age silent data. Stage each multi-endpoint refresh before publishing it to the UI, retaining
endpoint errors and prior values together on failure. A successful REST refresh
cannot claim a disconnected stream is Live. Keep last values with explicit
errors during an outage. Mutation retries retain their
per-operation idempotency key after uncertain errors, even across intervening operations; no automatic mutation retries.

Use the existing theme/components and CSS media queries. Disable chart animation
entirely, including hidden tabs and reduced-motion preferences. Keep navigation,
filters, scenario forms and drawers outside data-panel rendering; preserve
keyboard focus, sorting, pagination and expansion state during inspection. While
focus is inside a data panel, announce pending updates and explicitly label the
displayed values as potentially stale; publish the newest data when focus leaves.
Header status and critical counts continue updating. Action explanations update
while open, with the same reading hold if a table inside them has focus. Status
announcements exclude changing snapshot timestamps to avoid repetitive speech.
Use Python-authored native table header slots with scoped columnheader roles,
keyboard sort buttons and aria-sort. The unavailable investigator is a responsive
right-positioned modal with Escape, focus return and a Tab guard for its sole
enabled control. No authored JavaScript is needed.

## Explicit design/contract resolutions

1. v1 scenarios are `normal`, `evening_overload`, `late_correction`, and
   `leader_failover`, with scale, duration and seed. Solar drop, duplicate,
   outage and transformer-count controls in the conceptual design are not v1
   inputs; do not send invented fields. Start/stop require confirmation and
   API-enforced Operator permission. Backend operations remain contract mocks.
2. v1 Transformer has no RECOVERING enum. Display the published Transformer state
   and the separate Action lifecycle. Never derive state from thresholds.
3. v1 ActionPayload provides target/expected/confirmed reduction, counts, trigger
   and policy references, but no candidate list or complete command envelopes.
   DeviceView provides bounded command results and latest telemetry, but no
   telemetry arrival timestamp. Label capability update time accurately. Display
   those published results and explain the missing data. The timeline contains
   retained load windows plus latest action/alert transitions and command results;
   it is not a complete historical saga ledger.
4. Overview demand is the API KPI; solar sums published inventory category totals.
   The trend is explicitly scoped to the first highest-risk transformer's history,
   with a selected time range. Count noncompleted plans as Open actions, not proven physical throttles. No aligned whole-grid history endpoint exists;
   do not zero-fill absent windows or imply fleet-wide completeness.
5. v1 has no metrics/component-health snapshot endpoint. Show reported SSE health
   and stream state; missing Kafka/Flink/database/control metrics remain Unknown.
   Grafana is a separate local engineering link, not a readiness assertion.
6. Only investigation entry points belong to this milestone. The scoped drawer
   reports service unavailable and planned MOCK mode, with no generated findings.
   Tool execution, evidence deep links, provider progress/failure states and
   Keycloak remain their own milestones. No external model calls are made.

## Fixture and acceptance boundary

An explicit standalone FastAPI fixture process uses copied versioned response
shapes, validated by tests. Controlled steps publish 72 → 96 → 75 → 72 kW and
throttle/restore results. Building demand falls before restore; the readings
balance arithmetically. Fault controls exercise UI loading/empty/stale/partial,
permission, unavailable/error, SSE closure and cursor reset without dependencies.
Fixture freshness is a controlled test input; it is not live sensor evidence.
Scenario start/stop records mock runs; fixture steps separately advance the story.

Acceptance uses real TCP HTTP/SSE and real Chromium through the selected Python
Playwright tooling. It proves UI rendering and recovery against a schema-checked
fixture boundary, not a live Kafka/control closed loop, kind deployment,
Keycloak, multi-UI-replica failover or a production security claim. Existing
backend acceptance remains separate. Evidence is recorded in implementation
status; screenshots stay ignored under `.tools/dashboard-evidence`.

## Control room amendment — 2026-11-27

`/live` is a four-tab control room (Grid operations, Transformers, Devices, Pipeline health) with one poller in `dashboard/fleet_data.py`. The chaos dock reuses the server-side rate limit for fleet control calls. The read paths use Prometheus, `/actions`, `/transformers/{id}/loads` for the five managed transformers and a bounded `/devices` request while the Devices tab is open. Business numbers use confirmed reductions and freeze at the last successful actions poll during an API outage. Prometheus selectors exclude the projection's re-exported gauges (`exported_job!~"projection.*"`), so they read the live Flink values. The detail chart falls back to Prometheus history when the loads API has nothing recent. In the hosted live configuration, `/operator` keeps Alerts & actions available and hides the synthetic Flow Map and Scenarios entries.

## Prepublication review amendment — 2026-11-22

Use strict finite JSON decoding and bounded parser/validator failure handling.
Bind successful history reads to both their entity and requested range. Compare
UTC timestamp instants (including fractions) before limiting recent records or
sorting a timeline. Plot negative net export rather than clipping the y-axis at
zero. Clear unrefreshable recorded health on stream reset. Confirmation dialogs
have a persistent client parent so a live panel update cannot dismiss them;
transient dialogs delete after hiding. Selected versions and v1 contracts are
unchanged. The real PostgreSQL/API boundary is additionally exercised from the
dashboard client using an isolated schema. This remains separate from full live
Kafka/control, kind and identity acceptance.
