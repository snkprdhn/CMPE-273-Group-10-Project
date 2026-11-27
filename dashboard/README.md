# GridBalance operator dashboard

Python/NiceGUI 3.19.0 console using the separate operations REST/SSE API. Includes
overview, transformer detail/history/thresholds, alerts and acknowledgement,
action explanations and command results, devices, scenario confirmations,
reported health, settings, and scoped investigation entry points. See
[UI design](../docs/design/UI_DESIGN.md) and [ADR-031](../docs/decisions/ADR-031-operator-dashboard.md).

On the hosted AKS review the site opens without a login (mock-auth mode). `/` and `/live` show the live control
room, and `/operator` shows the full console (Overview, Flow Map, Transformers, Alerts & actions, Devices, Scenarios,
System health, Settings). Local kind keeps normal OIDC. See the [master guide](../docs/MASTER_GUIDE.md) and the
[AKS runbook](../infra/aks/AGENT_RUNBOOK.md).

From the repository root, preparation (CPython 3.12.9, macOS ARM64):

```sh
.venv/bin/python -m venv .tools/dashboard-venv
.tools/dashboard-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r dashboard/requirements.txt
.venv/bin/python -m venv .tools/dashboard-test-venv
.tools/dashboard-test-venv/bin/python -m pip --cache-dir .tools/pip-cache install --require-hashes -r dashboard/requirements-test.txt
PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m playwright install chromium
```

For independent UI work, use two terminals:

```sh
make dashboard-fixtures
```

```sh
make dashboard-demo
```

Open [GridBalance](http://127.0.0.1:18086). Expand **Fixture controls**, then choose
Normal → Overload → Throttling → Recovery. Open `tx-001` for published load values,
thresholds, the response and command outcomes. The fixture toolbar also injects
loading, empty, stale, partial, denied, unavailable and error responses; healthy
restores service. Disconnect SSE and Reset cursor exercise recovery without
navigation changes. Data is synthetic and controlled; fixture scenario start/stop
records a run, while the toolbar independently advances the demonstration.

For real per-user Keycloak login, follow the [identity runbook](../identity/README.md).
OIDC is the normal runtime; logout and renewal run on the Python server.

For explicit mock mode with the existing backend, start/replay it using the [projection runbook](../projection/README.md).
Set `DASHBOARD_API_TOKEN` to one of its explicitly configured mock bearer tokens
in your shell (never commit it), then:

```sh
AUTH_MODE=mock DASHBOARD_API_URL=http://127.0.0.1:18084/api/v1 make dashboard
```

UI defaults to loopback `18086`; `--port` and `--api-url` override local ports.
The API default is loopback `18084/api/v1`; fixture mode uses `18085/api/v1`.
URLs must use HTTP and a loopback host. These are non-secret settings. `DASHBOARD_API_TOKEN` is required/secret in explicit mock mode and remains on the
UI server. The local fixture mode uses an explicit disposable Operator token.
A separate UI process can use a backend Viewer token to inspect denied operations.
Explicit mock mode has one configured identity per process. OIDC mode instead
uses isolated browser sessions and ignores `DASHBOARD_API_TOKEN`.
Stop the UI and fixture terminals with Ctrl-C; there is no persisted fixture state.

Verification:

```sh
make dashboard-test
make contracts BASE_REF=HEAD
.tools/dashboard-venv/bin/python -m pip check
.tools/dashboard-test-venv/bin/python -m pip check
```

Tests own disposable ports 18185/18186, clean up their processes, and write PNGs
and server logs to ignored `.tools/dashboard-evidence/`. They use real HTTP/SSE
and headless Chromium. Do not start other services on those test ports. Browser
startup/TCP listening may require sandbox approval in Codex. The locks include
Mac ARM64 native artifacts; other platforms need a separately verified lock.

The UI validates responses against `schemas/v1/bundle.json`, refreshes snapshots
on events/reconnect/reset and every ten seconds, and labels retained values on
failure. Charts disable animation; load values also have accessible tables.
Open action explanations update live. While keyboard focus is within a data
panel/table, a visible announcement labels retained values as potentially stale
and holds panel redraws; moving focus outside applies the latest data. Header
critical counts and connection status keep updating throughout. Tables expose
keyboard sorting and sort state, and the investigator supports Tab containment,
Escape and focus return. Navigation, labels, focus outlines and polite status announcements support
keyboard/screen-reader use. Keyboard Tab reaches navigation/controls, Enter
activates, and dialogs/drawers have explicit close controls.

Optional real backend boundary verification (existing local PostgreSQL, isolated
temporary schema; use the projection environment):

```sh
PROJECTION_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/projection-venv/bin/python -m pytest -q tests/projection/test_dashboard_boundary.py
```

Known API limits are displayed rather than invented: the trend is one explicitly
named transformer; candidate lists/full command envelopes, complete historical
saga transitions, telemetry arrival timestamps and component metrics are absent
from v1. Scenario effects are backend contract mocks. The investigator reports
unavailable; Copilot execution/evidence, kind packaging, Linux locks,
full live context feeds and end-to-end platform acceptance remain later gates.

### Grid View

Open **Transformers** or use **Open tx-…** on the overview. **Grid View**
shows net load, capacity, classification and freshness for that transformer.
Arrow direction distinguishes demand from generation and line width follows
the published group kW. Expand **EV charger readings** for individual
measurements and the highlighted throttle participant.

The historical timeline follows the latest retained action for the transformer.
It uses the existing Operator-authorized projection evidence route for command
send time and requested rate when available. The command result shows the
applied rate. Flow values come from load windows and individual measured draw
comes from the latest device telemetry. The UI labels missing values as
unknown. DeviceView has no per-device measurement timestamp or past reading
series. A window that ends after acknowledgement may straddle it, and changes
in aggregate demand cannot isolate a charger's contribution. The existing
action dialog links to the Investigator.

For a local UI walkthrough, run `make dashboard-fixtures` and
`make dashboard-demo`, open [GridBalance](http://127.0.0.1:18086), select
`tx-001`, and advance the fixture through Overload, Throttling and Recovery.
For a local platform walkthrough using the real seeded scenario, follow the
[master guide](../docs/MASTER_GUIDE.md) and select the same transformer.

**Flow Map** shows every transformer in the
published inventory. Each transformer has its own schematic with household,
solar, and EV branches. Arrow direction shows consumption or generation and
line width is relative to that transformer's largest branch. Moving lines
indicate fresh nonzero readings while the REST/SSE connection is live.
Stale, incomplete, or retained readings remain visible without motion.
The map is schematic because the API supplies device-to-transformer
assignments and no feeder links or geographic coordinates. Click household, solar, or EV nodes to expand individual readings on the map.
Open the transformer node for its incident history.

The Flow Map uses circular transformer nodes and moving dots on directional
branches. Green shows ordinary flow, teal shows solar input, and a labeled red
EV branch identifies an applied throttle. Expanded chargers show the applied
rate separately from measured draw. An acknowledged restore clears the throttle
highlight. Retained or incomplete readings do not animate.

On **Flow Map**, choose **Evening EV overload**, **Sudden solar drop**, or
**Steady demand** under **Demo scenario**, then select **Run simulation**.
The synthetic readings advance every four seconds. **Next phase** advances
immediately, and **Return to live grid** restores the REST/SSE dashboard.
The overload scenarios show separate acknowledgement and measurement frames.
Playback is confined to that browser tab and does not publish telemetry,
issue charger commands, or alter retained incident evidence. The graph holds
automatic changes while keyboard focus is inside the map for inspection.

In the hosted review, **Scenarios → Start scenario** opens the selected demo
in Flow Map. The hosted API has no live simulator adapter, so this entry point
uses the same playback as **Flow Map → Run simulation**. Each phase generates
2,500 synthetic device readings across 25 modeled transformers. The diagram
focuses on the transformer affected by the scenario. Expand **Telemetry scale**
for the measured local Kafka/Flink benchmark. In live mode the same row shows
the current inventory and fresh device readings.
