# GridBalance demo UI plan (PRD adaptation)

Freeze: Sun 2026-11-29 18:00 PST · Demo: Mon 2026-11-30

Scope rule: Phase 1 covers the `ui` image, its Deployment settings and the separately verified projection topic split. The projection rolls out before the UI. Phase 2 adds fleet and UI behavior. Both phases preserve the v1 contracts and use the existing AKS capacity.

## Decisions (locked)

- [x] Reskin `/live` in place and keep the literal "GridBalance" text. The `/operator` console is the fallback and is not demoed.
- [x] Frame ROI as kW and kWh first. Label the $ figure "illustrative @ $25/kW-month".
- [x] Shadow load is derived in the dashboard in Phase 1. The fleet gauge comes in Phase 2.
- [x] Demo path: `/live` (four tabs after Phase 1b) plus Grafana deep links.
- [x] Deferred: driver override, edge disconnect fail-safe, V2G/BOOST, three.js.
- [x] Task 9 (hide Investigate) folded into Phase 1 as an env flag (Task 6 below).

## Phase 1: must do before freeze

### Task 0: Baseline

- [x] Record that `/live` has no browser test. Its only coverage is the `feed_rows`/`reduction` unit tests and the `demo_check` "GridBalance" check. (Verified 2026-11-27: `grep /live tests/` finds nothing; baseline `tests/dashboard` = 46 passed.)
- [x] Save the current `ui` digest from `.tools/aks-demo/images.json` as the rollback target. (Saved in ignored `.tools/aks-demo/ui-rollback-demo-ui.txt`: `ui@sha256:2d49fbd80363e405535fccaaa7f0b7fe816f6e50242754443fb103a6dab08d3a`.)

### Task 1: Business numbers (`dashboard/fleet_data.py`)

- [x] Add `epoch(value)`. It uses `dashboard.client.instant()`, returns `None` for naive datetimes, and otherwise returns `.timestamp()`. Every time comparison uses epoch floats.
- [x] Build a per-transformer throttle timeline of `(start, end, kW)` segments from action `transition_time`:
  - [x] Count only these states: `ACKS_PENDING` (partial confirmed kW), `REDUCTION_CONFIRMED`, `RECOVERY_MONITORING`.
  - [x] A segment ends at the `RESTORE_DISPATCHED` time. If a poll skipped straight to `COMPLETED`, it ends at the last time the action was seen active.
  - [x] Exclude the held states `PARTIALLY_APPLIED`, `TIMED_OUT` and `NEEDS_OPERATOR_REVIEW`.
- [x] Compute the KPIs from the timeline only (no `net`, no lag shift):
  - Load shed now
  - Peak avoided: 60 s rolling mean per transformer, maximum over 1 h, summed across transformers
  - kWh shifted
  - Illustrative $
- [x] Shadow chart data: read `GET /transformers/{id}/loads`. Add confirmed kW only to windows with `window_start ≥ segment start` and `window_end ≤ segment end`. No `EFFECT_LAG` constant.
- [x] Keep a per-transformer loads cache in `FleetData`, shared across tabs:
  - `fetch_loads(tx)` allows one in-flight request per transformer and waits at least 1 s between fetches.
  - The selected-load poll refreshes transformers selected in the last 60 s. A bounded trigger-window lookup also fetches managed transformer history for recent actions.
- [x] `backfill()`: seed the timeline for actions that are active at startup. Load history comes from the loads endpoint.
- [x] Use confirmed kW for the displayed reduction. The Decision Log uses `confirmed_reduction_kw` and the legacy `reduction()` helper was removed.
- [x] Tests (`tests/dashboard/test_control_room.py`):
  - [x] On a step overload, Peak avoided equals the sustained confirmed kW, not 0.
  - [x] No double count in windows before the throttle took effect.
  - [x] Nothing is counted after `RESTORE_DISPATCHED`/`COMPLETED`, and a skipped restore poll still closes the segment.
  - [x] `epoch()` handles `Z`, `+00:00`, naive and invalid input, and bad rows are skipped.
  - [x] Concurrent `fetch_loads` calls make one request, a stale reply doesn't change the selected chart, and a loads failure doesn't break the KPIs.
  - [x] Empty fleet and `None` capacity.
  - [x] Update the old `34.2 kW shed` assertion.

Implementation note: `FleetData.select(tx)` is the recently-selected map. It was missing and was added. Every active tab's `refresh()` tick now calls `data.select(state['selected'])`, so an idle projector keeps `poll_loads()` (every ~5 s) fetching `/loads` for its transformer beyond the 60 s TTL. `kpis()` uses a 1 h window for kWh shifted. Limit: actions already in `COMPLETED` at startup have no known duration and add no segment.

### Task 2: Theme and KPI header (`dashboard/control_room.py`)

- [x] Stepped zinc/slate surfaces with 1px borders. Emerald, amber and red are used only for status.
- [x] 5 KPIs: Active load (kW), Throttled EVs x/y, Peak avoided (kW, with $ as a sub-label), Energy shifted (kWh), Output freshness (s).
- [x] Keep the "GridBalance" header text.
- [x] Test: kW and kWh labels are never swapped.

### Task 3: Two tabs

- [x] Add `ui.tabs` with "Grid operations" and "Pipeline health".
- [x] Move the flow strip, throughput, lag, Flink busy and backpressure gauges, the correctness table and the retention panel to Tab 2.
- [x] Add Grafana deep links: `/grafana/d/grid-pipeline` and `/grafana/d/grid-kafka`.
- [x] Keep one shared `refresh()` timer. Existing tests still pass.

### Task 4: Main stage, 65 % width (`dashboard/control_charts.py`, `dashboard/control_room.py`)

- [x] Draw the topology on one hidden `cartesian2d` grid (axes `show:false`, fixed min/max):
  - [x] Nodes: a `scatter` series, plus `effectScatter` for critical transformers. Show the substation, the 5 heroes plus the 3 most-loaded other transformers, and the charger tiers. The transformer table opens from the link below the topology.
  - [x] Feeders: a `lines` series with `coordinateSystem:'cartesian2d'`, `effect` particles and `silent:true`.
  - [x] Colour nodes by band. Turn animation off when the user has reduced motion enabled.
  - [x] Update the topology chart only when a node's band or label changes. First check whether NiceGUI 3.19 merges `setOption` updates.
  - [x] Fallback if clicks don't fire: clickable NiceGUI cards over a non-interactive chart.
- [x] `pick()`, the transformer dropdown and "Follow incidents" select the transformer and immediately `await data.fetch_loads(tx)`. Show "Loading history…" until the data arrives.
- [x] Detail chart: managed line, dashed shadow line, critical threshold and action markers.
- [x] Tests:
  - [x] Node and line coordinates match.
  - [x] The lines series is silent.
  - [x] A click changes the selection and fetches immediately.
  - [x] An unchanged topology causes no chart update.
  - [x] The shadow series and the threshold are present.

Implementation notes: NiceGUI 3.19 `echart.js` calls `setOption` with merge (`notMerge` only when the series count changes). The topology keeps six series so `series.data` is replaced on update. Clicks on `scatter`/`effectScatter` nodes fire `on_point_click` in Chromium. Reduced motion is read once per page with `matchMedia` through `ui.run_javascript`.

### Task 5: Right rail (35 %) and chaos dock

- [x] Decision log sentences, e.g. "18:02:11 · tx-003 (triggered at 97 %) → throttled 18 lowest-priority chargers · 44/46 kW confirmed · acked 18/18". Restore and safe hold get their own wording.
- [x] Bottom chaos dock with "6 PM EV rush (tx-00N)" (overload), "Delayed packets" (very_late), "Out of order" and "Duplicates". It reuses `data.control()` with the existing 8 s rate limit.
- [x] Tests: every `ActionStatus` has a sentence, input is escaped, and the dock sends the same request bodies as today.

Implementation note: `control_room.decision()` builds the sentences. The dock is a Quasar footer and stays visible on all tabs. The "Injected in the last 5 minutes" summary is on Pipeline health below the correctness table.

### Task 6: Hide Investigate on AKS (`dashboard/app.py`)

- [x] Hide the Investigate buttons when `COPILOT_URL` is set to an empty string.
- [x] Set `COPILOT_URL=""` on the AKS `ui` Deployment. (Done in `infra/aks/kustomization.yaml`; `tools.aks_demo render Deployment/ui` shows it. It reaches the cluster only with the Task 7 rollout.)
- [x] `tests/dashboard/test_browser.py` still passes, because `tests/dashboard/conftest.py` sets `COPILOT_URL`.

Test: `tests/dashboard/test_investigate_flag.py` starts a second UI with `COPILOT_URL=''` and asserts the three Investigate buttons are absent.

### Task 7: Verify, roll out, document

- [x] Add a Playwright smoke test for `/live`: "GridBalance" renders, tabs switch, and a node click changes the chart title. (`tests/dashboard/test_live.py` against `tests/dashboard/fake_backend.py`.)
- [x] Run `make dashboard-test` and `make delivery-render` before the rollout pause. See the [verification results](IMPLEMENTATION_STATUS.md#executed-verification).
- [x] AKS rollout (deployed 2026-11-27 20:11 PST; projection lock/freshness fix deployed 20:50 PST and caught up at 21:11 PST):
  1. Confirmed spending limit (`On`) and approved `$85` ceiling.
  2. Preserved rollback templates in `.tools/aks-review/phase1-2/{fleet,projection,api,ui}-before.json`. Rolled out `Deployment/projection` and `Deployment/api` (`api@sha256:3bcb2e51e6b9eecd8ab9ce541a6e4e8a0d6cf1981fa58f30f96780e18da4a825`, `2/2` and `1/1` ready). Control/load lag drained to `0–3` records (`integration-kind.projection`) and telemetry drained to `36–41` records (`integration-kind.projection-telemetry`); `/api/v1/actions` and `/api/v1/overview` report `FRESH`.
  3. Deployed `Deployment/ui` (`ui@sha256:1b4b6361bf0d3b211af62cbc937565e977fe5b67c2a9a049f12582984913cdf3`, `1/1` ready) and verified `/live` in browser (Decision Log live without `feed stale: projection behind`, active saga stepper, live Decisions/Devices) plus `.venv/bin/python -m tools.demo_check` (`PASS`, `1.9 s` freshness).
- [x] `docs/MASTER_GUIDE.md`: map the PRD's four demo acts to our buttons, with measured numbers (about 12 s window latency, 33 s TaskManager recovery, ~200 events/s).
- [x] `docs/implementation/IMPLEMENTATION_STATUS.md`: record local verification, the rollback digest and known limitations. Add a one-paragraph note to ADR-031.
- [x] Keep the rollback ready: the currently deployed `ui` digest saved in Task 0.

## Phase 1b: closed-loop DERMS upgrade and audit fixes (UI only)

Guardrails: UI code, dashboard tests and docs, plus the Phase 1 hosted `COPILOT_URL` setting. Schema changes, projection work and additional infrastructure are outside this phase.

### Part 1: P0 and P1 fixes

- [x] P0 `lines.effect.trailLength = 0`. Unit test, and a Playwright test that cycles all four tabs with 0 console errors and a painted topology. (With `.25` the same test fails.)
- [x] P0 `exported_job!~"projection.*"` on every `grid_business_kw` and `grid_window_devices` selector. Live check through the AKS Grafana Prometheus proxy: filtered and unfiltered `max by (transformer)` both return 25 series; `count by (exported_job)` shows `aggregation.job` 25 and `projection.projection` 5.
- [x] P0 Decision Log trigger utilization: `_load_row` keeps `window_id` and `utilization_pct`; `FleetData.triggers[(window_id, revision)]` is looked up with the action's `trigger_window_id` and `trigger_revision`. A missing window omits "at X %". The poller also fetches loads for the 12 newest actions' transformers (3 tries per action) so the percentage does not depend on which transformer a tab selected.
- [x] P1 `useUTC=True` in `base()` and `spark()`.
- [x] P1 `fetch_loads()` and `select()` skip transformers that are not managed; the chart is labelled "Prometheus history (monitor-only)".
- [x] P1 The dock's EV rush defaults to the least loaded managed transformer without an incident (a manual choice stops the automatic target).
- [x] P1 `throughput()` grid top 48, legend centred; Correctness card above the four charts on Pipeline health.

### Part 2: closed-loop visuals and projector fit

- [x] Topology with four columns (Grid + Solar, transformers, charger groups, control saga), cyan telemetry lines, amber command and green acknowledgment return lines with arrow particles while an action is in flight, six series always, every line silent, `trailLength` 0, `changed()` guard kept.
- [x] `saga_steps(action)` stepper (seven steps, skipped polls mark earlier steps done, red "safe hold" for `PARTIALLY_APPLIED`, `TIMED_OUT`, `NEEDS_OPERATOR_REVIEW`, idle state).
- [x] Pipeline strip with Outbox / CDC, Fenced gateway ("N commands applied in 10 min") and the dashed return rail.
- [x] Fleet load moved under the Decision Log (log capped at 300 px). `@media (max-height: 760px)` tightens charts. Measured with Playwright: at 1440x900 both columns end above the dock; at 1280x720 the KPIs, topology, stepper and detail chart end above the dock (test asserts it).

### Part 3: asset explorer

- [x] Transformers (25): sortable, filterable, paginated table, row detail (demand breakdown from `tx_history`, managed vs shadow, own decisions). `tx_history` is filled by `poll()` and backfilled by four range queries (net, EV, meter, solar; capacity reuses the existing one). Table updates are pushed only while the tab is visible and only when rows changed.
- [x] Devices (1,000): inventory table and the managed chargers from one page of `/devices?device_type=EV_CHARGER&limit=200`, renewed like `select()` (30 s TTL, 10 s period), with Throttled / Spared / Normal and filters.
- [x] The "View all 25 transformers →" button opens the transformer tab.

### Part 4: hygiene and verification

- [x] No `gb-audit/` exists. `/tmp/preview-*.pid` are removed by the stop command. `.tools/aks-demo/ui-previous-image.txt` is ignored by Git (no `git checkout` possible); its value `ui@sha256:79ca20a7…` matches the ReplicaSet before the live one. `tests/dashboard/` is not in the `ui` image build context (`.dockerignore`, and a unit test).
- [x] Dashboard browser and manifest checks passed. See the [verification results](IMPLEMENTATION_STATUS.md#executed-verification).
- [x] Docs: `MASTER_GUIDE.md`, `IMPLEMENTATION_STATUS.md`, ADR-031.
- [ ] Pause for visual inspection and `/ponytail-review uncommitted`, then the AKS rollout above. (Browser inspection on live `/live` completed 2026-11-27 21:12 PST; `/ponytail-review` not invoked in-session.)

### Follow-up audit before rollout

- [x] Keep the rush target visible after an accepted injection, scroll the transformer detail panel into view, and hide the shadow comparison for monitor-only assets.
- [x] Show dispatch and full reduction only when the action provides evidence. Classify a charger as Spared only after the reduction settles.
- [x] Surface actions API outages and freeze the confirmed-reduction KPIs at the last successful poll.
- [x] Hide synthetic operator navigation in hosted live mode. Preserve Alerts & actions for inspection.

The projection topic split passed the kind load check and full closed-loop smoke. The two-node AKS estimate exceeds the $75 project ceiling, so the hosted rollout and soak remain pending. See [implementation status](IMPLEMENTATION_STATUS.md).

## Phase 2: only if Phase 1 is green and soaked by Sat 18:00

### Task 8: Fleet unmanaged-demand gauge

- [x] `devices/fleet.py` exports `devices_fleet_unmanaged_kw{transformer}`, the sum of offered power for a complete simulator window. The dashboard uses the gauge when available and keeps its action-based fallback for older fleets.
- [x] Test in `tests/devices`: the gauge equals the offered draw regardless of commands.
- [x] Rollout: new `fleet` image (`fleet@sha256:cc81a1355dcbfc2d87c9314635bc835fa1c79148c59142c90bc994d9dfd8ceb2`, rolled out 2026-11-27 21:11 PST with `Deployment/fleet` ready while `Deployment/action` remained running).

### Task 9: Hero-only cloud cover

- [x] `/inject kind=cloud_cover` (5–300 s) reduces solar generation to 20% for managed transformers tx-001 through tx-005. The 30-second dock button appears after the fleet gauge becomes available.
- [x] Tests: HTTP authorization and input validation, effect limited to heroes, and expiry.
- [x] Verified on AKS (2026-11-27 21:13 PST): `25` `devices_fleet_unmanaged_kw` series in Prometheus, `Cloud cover` dock button and `"Prometheus history · Without throttling uses fleet offered demand"` chart mode active on `/live`, and 3 × `10 s` `POST /inject` `cloud_cover` runs followed by `.venv/bin/python -m tools.demo_check` (`0` held plans; `PASS` across all checks). (Local kind `cloud_cover` test not run.)

## Phase 3: explicitly cut or deferred

- [ ] React/TypeScript/shadcn, Redis, ClickHouse/Timescale, Apache Iggy
- [ ] KEDA, 5k–30k msg/s, 8 partitions, two-stage sharding, sliding windows
- [ ] p99 < 2 s and < 3 s recovery as targets
- [ ] Edge disconnect fail-safe, driver override, V2G/BOOST (these need saga work and a v2 contract)
- [ ] Replay simulator, predictive pre-throttling, restore jitter
- [ ] Kill-worker button in the UI (needs RBAC from the UI pod), depot SoC scheduler
- [ ] three.js / `ui.scene` (PRD §10 excludes 3D)
- [ ] Deleting the console tabs and scenario routes (after the demo, via a reviewed contract change)
