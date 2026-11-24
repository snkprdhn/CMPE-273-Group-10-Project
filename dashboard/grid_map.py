"""Member_4 flow graph of the published transformer and device readings."""

from collections import defaultdict

from nicegui import ui

from dashboard.client import instant

GROUPS = (
    ('Household demand', 'SMART_METER', 'meter_demand_kw', 'home', '#197653', 'out'),
    ('Solar generation', 'SOLAR', 'solar_generation_kw', 'solar_power', '#13705d', 'in'),
    ('EV chargers', 'EV_CHARGER', 'ev_demand_kw', 'ev_station', '#197653', 'out'),
)

MAP_CSS = '''
.grid-map { background:#f7fafd; }
.grid-map-tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%, 365px),1fr)); gap:14px; width:100%; align-items:start; }
.grid-map-tile { background:#fff; padding:16px; }
.grid-schematic { display:grid; grid-template-columns:minmax(70px,1fr) minmax(12px,.32fr) minmax(48px,.65fr) minmax(12px,.32fr) minmax(80px,1.15fr);
  grid-template-rows:repeat(2,minmax(54px,auto)); gap:6px 2px; align-items:stretch; width:100%; }
.grid-schematic .grid-hub { grid-area:1 / 3 / 3 / 4; justify-content:center; align-items:center;
  text-align:center; min-width:0; position:relative; }
.grid-hub::after { content:""; position:absolute; top:50%; right:-4px; width:50%; border-top:2px solid #b8c7d6; }
.grid-schematic .grid-input { grid-area:1 / 1 / 3 / 2; align-self:center; }
.grid-schematic .grid-input-line { grid-area:1 / 2 / 3 / 3; align-self:center; margin:0; }
.grid-schematic .grid-output-house { grid-area:1 / 5; }
.grid-schematic .grid-output-ev { grid-area:2 / 5; }
.grid-schematic .grid-output-line-house { grid-area:1 / 4; }
.grid-schematic .grid-output-line-ev { grid-area:2 / 4; }
@media(max-width:600px) {
  .grid-schematic .grid-group .q-item { flex-wrap:wrap; padding:4px; }
  .grid-schematic .grid-group .q-item__section--avatar { display:none; }
  .grid-schematic .grid-branch.moving .grid-pulse { width:5px; height:5px; top:6px; }
}
.grid-hub > * { z-index:1; }
.grid-hub-circle { width:min(100%,92px); aspect-ratio:1; justify-content:center; align-items:center;
  border:2px solid #26629f; border-radius:50%; background:#edf5ff; color:#173963; }
.grid-hub-circle.critical { border-color:#b42332; background:#fff0ef; }
.grid-hub-circle.warning { border-color:#9d6216; background:#fff6df; }
.grid-hub-circle.stale { border-color:#8793a1; background:#f1f4f7; }
.grid-spine { grid-area:1 / 4 / 3 / 5; align-self:stretch; border-left:2px solid #b8c7d6; margin:24px 0; }
.grid-branch { display:flex; align-items:center; position:relative; height:16px; color:var(--flow-color); }
.grid-branch.in { flex-direction:row-reverse; }
.grid-branch::before { content:""; flex:1; height:var(--flow-width); border-radius:4px; background:currentColor; opacity:.65; }
.grid-branch.out::after { content:"▸"; } .grid-branch.in::after { content:"◂"; }
.grid-branch.idle { opacity:.45; } .grid-branch.idle::after { content:""; }
.grid-pulse { display:none; }
.grid-branch.moving .grid-pulse { display:block; position:absolute; top:4px; width:8px; height:8px;
  border-radius:50%; background:currentColor; outline:2px solid white; animation:grid-flow 1.7s linear infinite; }
.grid-branch.in .grid-pulse { animation-direction:reverse; }
@keyframes grid-flow { from { left:4%; } to { left:calc(100% - 12px); } }
@media (prefers-reduced-motion:reduce) { .grid-branch.moving .grid-pulse { display:none; } }
.grid-endpoint { display:flex; flex-wrap:nowrap; align-items:center; min-width:0; gap:6px; padding:5px;
  border-radius:8px; background:#f3f6fa; }
.grid-group { min-width:0; background:#f3f6fa; border-radius:8px; overflow:hidden; }
.grid-group.throttled, .grid-device.throttled { background:#fff1f0; color:#8f1a28; }
.grid-group .q-item__section--avatar .q-icon { border-radius:50%; padding:5px; background:#e3f2e9; color:#197653; }
.grid-group.throttled .q-item__section--avatar .q-icon { background:#fbdada; color:#b42332; }
.grid-group .q-item { min-height:48px; padding:5px 6px; font-size:13px; }
.grid-group .q-item__section--avatar { min-width:24px; padding-right:5px; }
.grid-group .q-item__section--side { padding-right:5px; }
.grid-group .q-item__label { overflow-wrap:anywhere; }
.grid-tree { border-left:2px solid #bccbdf; margin:0 10px 8px; padding:0 6px 0 9px; gap:5px; max-height:240px; overflow:auto; flex-wrap:nowrap; }
.grid-device { position:relative; overflow-wrap:anywhere; border-radius:6px; background:white; padding:6px; gap:2px; }
.grid-device::before { position:absolute; content:""; width:9px; height:1px; background:#8fa5c0; left:-9px; top:14px; }
.grid-group-branch { align-self:start; margin-top:16px; }
.grid-reading { font-variant-numeric:tabular-nums; font-weight:700; }
'''


def active_throttles(actions, devices):
    """Show applied rate decreases on active actions until an applied restore."""
    active = {a['action_id'] for a in actions if a['status'] != 'COMPLETED'}
    throttles = {}
    for device in devices:
        if device['device']['device_type'] != 'EV_CHARGER':
            continue
        results = [r for r in device['command_history'] if r['action_id'] in active
                   and r['outcome'] in ('APPLIED', 'ALREADY_APPLIED')
                   and r['previous_rate_kw'] != r['applied_rate_kw']]
        latest = max(results, key=lambda r: instant(r['result_time']), default=None)
        if latest and latest['applied_rate_kw'] < latest['previous_rate_kw']:
            throttles[device['device']['device_id']] = latest['applied_rate_kw']
    return throttles


def flow_diagram(transformer, kw, live, counts=None, *, groups=None, expanded=None, open_transformer=None, throttles=None):
    load = transformer['load']
    throttles = throttles or {}
    fresh = live and transformer['freshness'] == 'FRESH'
    largest = max(abs(load[key] or 0) for _, _, key, _, _, _ in GROUPS)
    with ui.element('div').classes('grid-schematic'):
        with ui.column().classes('grid-hub gap-1'):
            with ui.column().classes(f'grid-hub-circle gap-0 {transformer["state"].lower()}'):
                ui.icon('electrical_services', size='22px')
                ui.label(kw(load['net_load_kw'])).classes('font-bold text-xs')
            tx = transformer['transformer_id']
            if open_transformer:
                ui.button(f'Open {tx}', on_click=lambda: open_transformer(tx)).props('flat dense').classes('text-xs')
            else:
                ui.label(tx).classes('font-bold text-sm')
        ui.element('div').classes('grid-spine').props('aria-hidden=true')
        for name, kind, key, icon, color, direction in GROUPS:
            placement = {'SOLAR': 'grid-input', 'SMART_METER': 'grid-output-house',
                         'EV_CHARGER': 'grid-output-ev'}[kind]
            connector = {'SOLAR': 'grid-input-line', 'SMART_METER': 'grid-output-line-house',
                         'EV_CHARGER': 'grid-output-line-ev'}[kind]
            limits = throttles if kind == 'EV_CHARGER' else {}
            color = '#b42332' if limits else color if fresh else '#69788b'
            value = load[key]
            width = 2 + round(5 * abs(value or 0) / largest) if largest else 2
            state = 'idle' if not value else 'moving' if fresh else ''
            title = 'Direction unavailable' if value is None else 'No flow' if value == 0 else (
                'Into transformer' if direction == 'in' else 'From transformer')
            branch = 'grid-group-branch' if groups is not None else ''
            with ui.element('div').classes(f'grid-branch out {state} {branch} {connector}').style(
                    f'--flow-width:{width}px;--flow-color:{color}').props(f'role=img aria-label="{title}"'):
                ui.element('span').classes('grid-pulse').props('aria-hidden=true')
            if groups is not None:
                device_group(tx, name, kind, icon, value, groups.get((tx, kind), []), expanded, kw, placement, limits)
                continue
            with ui.row().classes(f'grid-endpoint {placement}').props(f'data-testid="grid-{kind}"'):
                ui.icon(icon, size='18px').style(f'color:{color}')
                with ui.column().classes('gap-0'):
                    label = f'{name} · {counts[kind]} listed' if counts is not None else name
                    ui.label(label).classes('text-xs')
                    ui.label(kw(value)).classes('grid-reading text-sm')
                    if limits:
                        ui.label('Throttle applied').classes('text-xs text-negative font-bold')


def device_group(tx, name, kind, icon, total, devices, expanded, kw, placement, limits):
    key = (tx, kind)
    label = f'{name} · {kw(total)}'
    caption = f'{len(devices)} listed'
    if limits:
        caption += f' · {len(limits)} throttle applied'
    def toggle(event):
        expanded[key] = event.value
        fill(event.value)

    with ui.expansion(label, caption=caption, icon=icon, value=expanded.get(key, False),
                      on_value_change=toggle).classes('grid-group' + (' throttled' if limits else '')).classes(placement).props(f'data-testid="grid-{kind}"'):
        tree = ui.column().classes('grid-tree').props('tabindex=0 role=group aria-label="Individual device readings"')

    def fill(is_open):
        tree.clear()
        if not is_open:
            return
        with tree:
            if not devices:
                ui.label('No individual device records').classes('text-xs muted')
            for device in devices:
                reading = device['latest_telemetry']
                power = reading['power_kw'] if reading and reading['quality'] != 'INVALID' else None
                arrow = '← ' if power is not None and power < 0 else '→ ' if power else ''
                device_id = device['device']['device_id']
                target = limits.get(device_id)
                with ui.column().classes('grid-device w-full' + (' throttled' if target is not None else '')):
                    ui.label(device_id).classes('text-xs font-bold')
                    if target is not None:
                        ui.label(f'Throttle applied · {kw(target)} rate').classes('text-xs font-bold')
                    ui.label(f'{arrow}{kw(power)} · {device["freshness"]}').classes('text-xs')

    fill(expanded.get(key, False))


def render_flow_map(transformers, devices, actions, live, kw, badge, open_transformer, expanded, *, simulation=False):
    groups = defaultdict(list)
    for device in devices:
        identity = device['device']
        groups[identity['transformer_id'], identity['device_type']].append(device)
    with ui.card().classes('grid-map w-full gap-4').props('aria-label="Grid electricity map"'):
        with ui.row().classes('w-full items-center justify-between'):
            ui.label('Grid electricity map').classes('text-xl font-bold').props('role=heading aria-level=2')
            ui.label('Simulation playback' if simulation else 'Live connection' if live else 'Updates paused or unavailable').classes('text-sm muted').props('role=status')
        fresh = sum(t['freshness'] == 'FRESH' for t in transformers)
        ui.label(f'{len(transformers)} transformers · {fresh} fresh readings · {len(transformers) - fresh} stale or incomplete').classes('text-sm muted')
        with ui.row().classes('items-center gap-4'):
            for text, color in [('Normal flow', '#197653'), ('Solar input', '#13705d'),
                                ('Throttle applied', '#b42332'), ('Retained or incomplete', '#69788b')]:
                with ui.row().classes('items-center gap-1'):
                    ui.icon('circle', size='10px').style(f'color:{color}')
                    ui.label(text).classes('text-xs')
        ui.label('Supply enters on the left. Demand leaves on the right. Dots follow fresh flow and line width reflects measured kW.').classes('text-sm muted')
        ui.label('Select a device group to expand its readings.' if simulation else 'Select a device group to expand its readings, or open a transformer for incident history.').classes('text-sm muted')
        if not transformers:
            ui.label('No transformers available.').classes('muted')
        with ui.element('div').classes('grid-map-tiles'):
            for t in transformers:
                load = t['load']
                with ui.card().classes('grid-map-tile gap-3'):
                    with ui.row().classes('w-full justify-between items-center'):
                        ui.label(f'{kw(load["net_load_kw"])} net · {kw(load["capacity_kw"])} capacity').classes('grid-reading')
                        badge(t['state'])
                    evs = groups[t['transformer_id'], 'EV_CHARGER']
                    flow_diagram(t, kw, live, groups=groups, expanded=expanded, open_transformer=open_transformer,
                                 throttles=active_throttles(actions, evs))
                    reading = t['freshness'] if live else f'Retained · {t["freshness"]}'
                    ui.label(f'{reading} · Window end {load["window_end"]} UTC').classes('text-xs muted')
