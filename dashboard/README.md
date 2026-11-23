# GridBalance operator dashboard — Member_4

Python/NiceGUI 3.19.0 console using the separate operations REST/SSE API. Includes
overview, transformer detail/history/thresholds, alerts and acknowledgement,
action explanations and command results, devices, scenario confirmations,
reported health, settings, and scoped investigation entry points. See
[UI design](../docs/design/UI_DESIGN.md) and [ADR-031](../docs/decisions/ADR-031-member4-operator-dashboard.md).
Member_1 retains simulation/Registry/observability, Member_2 Kafka/PyFlink,
Member_3 control/PostgreSQL/Debezium, Member_4 projection/API/UI/identity/Copilot.

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

For explicit mock mode with the existing backend, start/replay it using the [Member_4 runbook](../member4/README.md).
Set `DASHBOARD_API_TOKEN` to one of its explicitly configured mock bearer tokens
in your shell (never commit it), then:

```sh
MEMBER4_AUTH_MODE=mock DASHBOARD_API_URL=http://127.0.0.1:18084/api/v1 make dashboard
```

UI defaults to loopback `18086`; `--port` and `--api-url` override local ports.
The API default is loopback `18084/api/v1`; fixture mode uses `18085/api/v1`.
URLs must use HTTP and a loopback host. These non-secret settings are owned by
Member_4. `DASHBOARD_API_TOKEN` is required/secret in explicit mock mode and remains on the
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
temporary schema; use the existing Member_4 environment):

```sh
MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_dashboard_boundary.py
```

Known API limits are displayed rather than invented: the trend is one explicitly
named transformer; candidate lists/full command envelopes, complete historical
saga transitions, telemetry arrival timestamps and component metrics are absent
from v1. Scenario effects are backend contract mocks. The investigator reports
unavailable; Copilot execution/evidence, kind packaging, Linux locks,
full live context feeds and end-to-end platform acceptance remain later gates.
