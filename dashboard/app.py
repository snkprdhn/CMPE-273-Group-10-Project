"""Python-only operator console, Member_4. Run with python -m dashboard."""
import asyncio
import json
import math
import os
from datetime import datetime, timezone
from urllib.parse import urlsplit

from jsonschema import ValidationError

import httpx
from nicegui import ui, app
from fastapi import Request
from fastapi.responses import RedirectResponse

from dashboard.client import DEFINITIONS, Operations, error_text, instant

CSS = '''
:root { --navy:#142a43; --ink:#172f49; --muted:#52657b; --line:#dbe3ec; }
body { background:#f3f6fa; color:var(--ink); font-family:Arial,sans-serif; font-size:16px; }
.q-header { background:white; color:var(--ink); border-bottom:1px solid var(--line); }
.q-drawer--left { background:#11283f; color:#e4edf7; }
.q-drawer--left .q-btn { color:#e4edf7!important; justify-content:flex-start; text-transform:none; font-size:16px; width:100%; }
.q-drawer--right { background:white; color:var(--ink); }
.q-drawer--left .selected { background:#27445f; border-left:3px solid #78b8ff; }
h1 { font-size:30px; font-weight:700; margin:0; line-height:1.3; }
h2 { font-size:20px; font-weight:700; margin:0; line-height:1.4; }
h3 { font-size:17px; font-weight:700; margin:0; }
.q-card { box-shadow:none; border:1px solid var(--line); border-radius:12px; padding:22px; }
.q-btn { text-transform:none; letter-spacing:0; }
.q-btn:focus-visible, a:focus-visible, input:focus-visible, .q-field--focused { outline:3px solid #1260c4!important; outline-offset:3px; }
.muted { color:var(--muted); }
.eyebrow { color:var(--muted); font-size:13px; letter-spacing:.09em; text-transform:uppercase; font-weight:bold; }
.metric { font-size:32px; font-weight:700; line-height:1.3; }
.kpis { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:16px; width:100%; }
.two-col { display:grid; grid-template-columns:minmax(0,1.7fr) minmax(290px,1fr); gap:20px; width:100%; }
.tx-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:16px; width:100%; }
.console { max-width:1560px; width:100%; margin:auto; padding:30px; gap:24px; }
.q-table__container { max-width:100%; width:100%; }
.q-table th { font-size:14px; color:#52657b; background:#f6f8fb; }
.q-table td { font-size:14px; white-space:normal; overflow-wrap:anywhere; }
.notice { border:1px solid #c5d5e6; border-radius:8px; padding:12px 16px; background:#eaf2fb; width:100%; }
@media(max-width:1100px) { .kpis { grid-template-columns:repeat(2,minmax(0,1fr)); } .two-col { grid-template-columns:1fr; } }
@media(max-width:600px) { .console { padding:16px; } .kpis { grid-template-columns:1fr; } .brand-sub { display:none; } h1 { font-size:26px; } }
@media(prefers-reduced-motion:reduce) { *, *:before, *:after { transition:none!important; animation:none!important; } }
'''
COLORS = {'NORMAL': ('check_circle', '#166344'), 'WARNING': ('warning', '#805000'),
          'CRITICAL': ('error', '#b42332'), 'STALE': ('schedule', '#52657b'),
          'FRESH': ('check_circle', '#166344'), 'PARTIAL': ('warning', '#805000')}
NAV = [('Overview', 'space_dashboard'), ('Transformers', 'electrical_services'),
       ('Alerts & actions', 'notifications_active'), ('Devices', 'ev_station'),
       ('Scenarios', 'science'), ('System health', 'monitor_heart'), ('Settings', 'settings')]


def heading(text, level=2):
    return ui.label(text).props(f'role=heading aria-level={level}').style(
        f'font-size:{30 if level == 1 else 20 if level == 2 else 17}px;font-weight:700')


def badge(text):
    icon, color = COLORS.get(text, ('info', '#265b93'))
    with ui.row().classes('items-center gap-1').style(f'color:{color};font-weight:600'):
        ui.icon(icon, size='18px')
        ui.label(text.replace('_', ' '))


def kw(value):
    return 'Unknown' if value is None else f'{value:,.1f} kW'


def table_sort_key(row, field):
    value = row.get(field)
    if value is not None and field in ('time', 'result_time', 'window_end', 'window_start'):
        value = instant(value)
    elif field == 'power':
        value = None if value == 'Unknown' else float(value.removesuffix(' kW').replace(',', ''))
    return value is None, value


def table(rows, fields, row_key):
    if not rows:
        ui.label('No records in this view.').classes('muted')
        return
    columns = [dict(name=key, label=label, field=key, align='left', sortable=True) for key, label in fields]
    grid = ui.table(columns=columns, rows=rows, row_key=row_key, pagination=10).classes('w-full').props('flat wrap-cells')
    headers = {}
    sort_state = {'key': None, 'descending': False}
    def sort_column(key):
        descending = not sort_state['descending'] if sort_state['key'] == key else False
        sort_state.update(key=key, descending=descending)
        grid.rows = sorted(grid.rows, key=lambda row: table_sort_key(row, key), reverse=descending)
        grid.pagination = dict(grid.pagination, sortBy=None, page=1)
        for name, header in headers.items():
            order = ('descending' if descending else 'ascending') if name == key else 'none'
            header.props(f'aria-sort={order}')
        grid.update()
    # QTable's default click-only headings are replaced using native Python slots.
    with grid.add_slot('header'):
        with ui.element('tr').props('role=row'):
            for key, label in fields:
                with ui.element('th').props('scope=col role=columnheader aria-sort=none').classes('text-left') as header:
                    headers[key] = header
                    ui.button(label, on_click=lambda k=key: sort_column(k)).props(f'flat dense aria-label="Sort by {label}"')
    return grid


def chart(loads, policy=None, capacity=None):
    if not loads:
        ui.label('No load history in the selected time range.').classes('muted')
        return
    series = dict(name='Net load', type='line', smooth=False, showSymbol=len(loads) == 1,
                  data=[p['net_load_kw'] for p in loads], lineStyle={'color': '#2563a6', 'width': 3},
                  areaStyle={'color': '#e4eef9'})
    if policy and capacity:
        series['markLine'] = {'symbol': 'none', 'data': [dict(yAxis=capacity*policy[f'{key}_pct']/100,
            name=key.title(), label={'formatter': f'{key.title()} {policy[f"{key}_pct"]}%', 'offset': [0, -7 if key == 'warning' else 8 if key == 'recovery' else 0]},
            lineStyle={'color': color}) for key, color in [('warning', '#946000'), ('critical', '#b42332'), ('recovery', '#166344')]]}
    ui.echart({'animation': False, 'aria': {'enabled': True, 'label': {'description': 'Net load history in kilowatts; values available in load history table'}}, 'tooltip': {'trigger': 'axis'},
        'grid': {'left': 55, 'right': 120 if policy else 24, 'top': 30, 'bottom': 45},
        'xAxis': {'type': 'category', 'data': [p['window_end'][11:19] for p in loads], 'name': 'UTC'},
        'yAxis': {'type': 'value', 'name': 'kW', 'min': min(0, min(p['net_load_kw'] for p in loads))}, 'series': [series]}).classes('w-full h-64').props('role=img aria-label="Net load history in kilowatts; values available in load history table"')


def ordered_records(name, rows):
    if name == 'actions':
        return sorted(rows, key=lambda a: instant(a['transition_time']), reverse=True)
    if name == 'alerts':
        return sorted(rows, key=lambda a: instant(a.get('resolved_at', a['opened_at'])), reverse=True)
    return rows


def configure(fixture=False, api_url=None):
    kind = os.getenv('GRID_LOCAL_KIND') == '1'
    base = ('http://api:18084/api/v1' if kind else
            (api_url or os.environ.get('DASHBOARD_API_URL', 'http://127.0.0.1:18084/api/v1')).rstrip('/'))
    parsed = urlsplit(base)
    if not kind and (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost', '::1')
                     or parsed.path.rstrip('/') != '/api/v1' or parsed.username or parsed.password
                     or parsed.query or parsed.fragment):
        raise ValueError('Dashboard API must be a loopback HTTP /api/v1 URL')
    token = 'fixture-operator-token' if fixture else os.environ.get('DASHBOARD_API_TOKEN', '')
    login = None
    if not fixture and os.environ.get('MEMBER4_AUTH_MODE') != 'mock':
        from identity.browser import BrowserLogin
        login = BrowserLogin(app)
        app.on_shutdown(login.http.aclose)
        if kind:
            from identity.local_routes import install
            install(app)
    elif not token:
        raise ValueError('Set DASHBOARD_API_TOKEN for explicit mock mode')
    ui.add_css(CSS, shared=True)

    @ui.page('/')
    async def index(client, request: Request):
        sid = request.cookies.get('grid_session')
        if login and not login.session(sid):
            return RedirectResponse('/auth/login')
        ui.colors(primary='#235b91', secondary='#52657b', positive='#166344', negative='#b42332', warning='#805000')
        source = Operations(base, token, token_provider=(lambda: login.token(sid)) if login else None)
        selected = {'page': 'Overview', 'transformer': None, 'search': '', 'severity': 'All', 'status': 'All'}
        last = {'fingerprint': None}
        tasks = []
        reading = {'content': False, 'detail': False}
        active_action = {'id': None, 'fingerprint': None}
        last_notice = {'fingerprint': None}
        expanded = {}
        ui.link('Skip to main content', '#main').classes('nicegui-skip-link')
        with ui.header().classes('items-center justify-between px-5 py-3'):
            with ui.row().classes('items-center gap-3'):
                ui.button(icon='menu', on_click=lambda: drawer.toggle()).props('flat round aria-label="Toggle navigation"')
                ui.icon('electric_bolt', size='30px', color='primary')
                ui.label('GridBalance').classes('text-xl font-bold')
                ui.label('OPERATIONS CONSOLE').classes('eyebrow brand-sub')
            with ui.row().classes('items-center gap-4'):
                status = ui.label('Loading').props('role=status aria-live=polite aria-atomic=true')
                snapshot_label = ui.label('Snapshot pending').classes('muted text-sm')
                role_label = ui.label(login.session(sid)['actor']['role'] if login else ('Fixture · Operator' if fixture else 'Local API · configured identity')).classes('muted')
                if login:
                    ui.link('Log out', '/auth/logout?csrf='+login.session(sid)['csrf'])
                    ui.link('Log in again', '/auth/login')
        with ui.left_drawer(value=None, top_corner=False, bottom_corner=True).props('breakpoint=1000 width=230') as drawer:
            ui.button('Close navigation', icon='close', on_click=lambda: drawer.hide()).props('flat').classes('lt-lg')
            ui.label('WORKSPACE').classes('eyebrow p-4').style('color:#afc5d9')
            nav_buttons = {}
            for title, icon in NAV:
                nav_buttons[title] = ui.button(title, color=None, icon=icon, on_click=lambda t=title: navigate(t)).props('flat align=left').classes('py-3')
            ui.separator().classes('my-6')
            ui.label('LOCAL DEVELOPMENT' if fixture else 'LOCAL OPERATIONS').classes('px-4 text-sm')
            ui.label('Member_4 · Read models').classes('px-4 text-sm').style('color:#afc5d9')
        with ui.column().classes('console').props('id=main tabindex=-1'):
            with ui.row().classes('w-full justify-between items-start'):
                with ui.column().classes('gap-1'):
                    ui.label('SMART GRID / EV FLEET').classes('eyebrow')
                    title_label = heading('Grid overview', 1)
                    ui.label('Observe demand. Follow the response. Verify recovery.').classes('muted')
                ui.button('Investigate', icon='manage_search', on_click=lambda: investigate(selected['transformer'])).props('outline')
            if fixture:
                with ui.expansion('Fixture controls · synthetic data only', icon='science').classes('w-full bg-white rounded-lg'):
                    ui.label('These controls change only the disposable UI fixture API. Start/stop below exercise the scenario contract mock.').classes('muted')
                    with ui.row().classes('gap-2'):
                        for i, label in enumerate(['Normal', 'Overload', 'Throttling', 'Recovery']):
                            ui.button(label, on_click=lambda step=i: fixture_change({'step': step})).props('outline dense')
                        ui.button('Reset cursor', on_click=lambda: fixture_change({'gap': True})).props('outline dense')
                        ui.button('Disconnect SSE', on_click=lambda: fixture_change({'disconnect': True})).props('outline dense')
                        ui.select(['healthy', 'loading', 'empty', 'stale', 'partial', 'denied', 'unavailable', 'error'],
                                  value='healthy', label='Fixture response', on_change=lambda e: fixture_change({'fault': '' if e.value == 'healthy' else e.value})).classes('w-48')
            notices = ui.column().classes('w-full gap-2')
            controls = ui.row().classes('w-full items-center gap-4')
            pending = ui.label('Live updates pending while focus is in the data panel. Values below may be stale. Move focus outside the panel to apply them.').classes('notice').props('role=status aria-live=polite')
            pending.set_visibility(False)
            content = ui.column().classes('w-full gap-5')
            ui.label('All times UTC · Values and states are published by the API. Missing data remains unknown.').classes('muted text-sm')
        with ui.dialog().props('transition-duration=0 aria-label="Action explanation"') as details, ui.card().classes('w-full').style('max-width:850px'):
            detail_body = ui.column().classes('w-full gap-4')
            detail_pending = ui.label('Live updates pending while focus is in the explanation. Displayed values may be stale. Move focus to Close details to apply them.').classes('notice').props('role=status aria-live=polite')
            detail_pending.set_visibility(False)
            ui.button('Investigate action', on_click=lambda: investigate_action()).props('outline')
            ui.button('Close details', on_click=details.close).props('outline autofocus')
        # Native modal semantics provide focus containment, Escape and focus return.
        with ui.dialog().props('transition-duration=0 position=right full-height aria-label="Incident investigator"') as investigator:
            with ui.card().classes('gap-4').style('width:420px;max-width:100vw;height:100%;border-radius:0'):
                heading('Incident investigator')
                scope_label = ui.label()
                ui.input('Focused question', placeholder='Investigation service unavailable').classes('w-full').disable()
                ui.label('Unavailable · Investigation service is not implemented in this milestone.').props('role=status')
                ui.label('Provider mode: MOCK (planned). No generated findings or evidence are available.').classes('muted')
                ui.label('Use transformer history, action explanations and command results to inspect the published evidence.').classes('muted')
                close_investigator = ui.button('Close investigator', on_click=investigator.close).props('outline autofocus')
                # This is the sole enabled control; keep Tab/Shift-Tab within the modal.
                close_investigator.on('keydown.tab.prevent', lambda: None)

        def track_reading(element, key):
            element.on('focusin', lambda: reading.update({key: True}))
            element.on('focusout', lambda: reading.update({key: False}))

        track_reading(content, 'content')
        track_reading(detail_body, 'detail')

        def investigate(entity=None, action=None, alert=None):
            scope_label.set_text(f'Scope: {entity or "grid"} · action {action or "all"} · alert {alert or "all"} · last {source.range_minutes} minutes · UTC')
            investigator.open()

        def investigate_action():
            action = next((a for a in items('actions') if a['action_id'] == active_action['id']), None)
            if action:
                investigate(action['transformer_id'], action['action_id'])

        async def fixture_change(body):
            async with httpx.AsyncClient(trust_env=False) as fixture_client:
                await fixture_client.post(base.removesuffix('/api/v1')+'/fixture', json=body)
            await source.snapshot()
            tick()

        def navigate(page, transformer=None):
            reading['content'] = False
            pending.set_visibility(False)
            selected.update(page=page, transformer=transformer)
            source.transformer = transformer
            title_label.set_text(f'Transformer {transformer}' if transformer else {'Overview': 'Grid overview', 'Alerts & actions': 'Alerts & actions'}.get(page, page))
            for name, button in nav_buttons.items():
                if name == page:
                    button.classes(add='selected')
                else:
                    button.classes(remove='selected')
            controls.clear()
            with controls:
                if page == 'Overview':
                    ui.select({15: 'Last 15 minutes', 60: 'Last hour'}, value=source.range_minutes,
                              label='History range', on_change=range_change).classes('w-48')
                if page == 'Transformers' and transformer:
                    ui.button('All transformers', on_click=lambda: navigate('Transformers')).props('flat')
                    ui.select({15: 'Last 15 minutes', 60: 'Last hour'}, value=source.range_minutes,
                              label='History range', on_change=range_change).classes('w-48')
                if page in ('Devices', 'Alerts & actions'):
                    ui.input('Search IDs or text', value=selected['search'], on_change=lambda e: set_filter('search', e.value)).classes('w-64')
                if page == 'Alerts & actions':
                    ui.select(['All', 'CRITICAL', 'WARNING'], value=selected['severity'], label='Severity',
                              on_change=lambda e: set_filter('severity', e.value)).classes('w-40')
                    ui.select(['All', 'OPEN', 'ACKNOWLEDGED', 'RESOLVED'] + DEFINITIONS['ActionStatus']['enum'], value=selected['status'], label='Status',
                              on_change=lambda e: set_filter('status', e.value)).classes('w-48')
                    ui.select({15: 'Last 15 minutes', 60: 'Last hour'}, value=source.range_minutes,
                              label='History range', on_change=range_change).classes('w-48')
                if page == 'Scenarios':
                    scenario_form()
            last['fingerprint'] = None
            render()
            if transformer:
                tasks.append(asyncio.create_task(source.snapshot()))

        async def range_change(e):
            source.range_minutes = e.value
            await source.snapshot()
            last['fingerprint'] = None
            tick()

        def set_filter(key, value):
            selected[key] = value
            render()

        async def perform(path, body=None):
            try:
                await source.mutate(path, body)
                with client:
                    ui.notify('Operation recorded by API', type='positive')
                tick()
            except (httpx.HTTPError, ValueError, ValidationError) as exc:
                with client, ui.dialog().props('transition-duration=0 aria-label="Operation failed"') as failure, ui.card():
                    heading('Operation failed')
                    ui.label(error_text(exc)).props('role=alert')
                    ui.label('Inspect current run or history before retrying. A retry of the same request retains its idempotency key.')
                    ui.button('Close error', on_click=failure.close)
                failure.on('hide', failure.delete)
                failure.open()

        def confirm(label, path, body=None):
            with client, ui.dialog().props(f'transition-duration=0 aria-label="{label}"') as dialog, ui.card():
                heading(label)
                ui.label('This records an operation through the configured API. Review the configuration before confirming.')
                if body:
                    ui.label(json.dumps(body))
                with ui.row():
                    ui.button('Cancel', on_click=dialog.close).props('outline')
                    async def submit():
                        dialog.close()
                        await perform(path, body)
                    ui.button('Confirm operation', on_click=submit)
            dialog.on('hide', dialog.delete)
            dialog.open()

        def scenario_form():
            with ui.card().classes('w-full'):
                heading('Run an approved scenario')
                ui.label('Scenario operations use the backend contract mock until the Member_1 live adapter is connected.').classes('muted')
                with ui.row().classes('w-full gap-4'):
                    scenario = ui.select(DEFINITIONS['ScenarioStart']['properties']['scenario']['enum'], value='evening_overload', label='Scenario').classes('w-56')
                    scale = ui.number('Scale', value=1, min=1, max=1000, step=1, precision=0).classes('w-32')
                    duration = ui.number('Duration (seconds)', value=60, min=1, max=3600, step=1, precision=0).classes('w-44')
                    seed = ui.number('Deterministic seed', value=42, min=0, step=1, precision=0).classes('w-44')
                def start():
                    values = [scale.value, duration.value, seed.value]
                    if any(v is None or not math.isfinite(v) or v != int(v) for v in values) or not 1 <= values[0] <= 1000 or not 1 <= values[1] <= 3600 or values[2] < 0:
                        ui.notify('Enter whole numbers within the labeled limits', type='negative')
                        return
                    confirm('Start scenario?', '/scenario-runs', dict(scenario=scenario.value, scale=int(scale.value), duration_seconds=int(duration.value), seed=int(seed.value)))
                ui.button('Start scenario', icon='play_arrow', on_click=start)

        def render_action(action):
            detail_body.clear()
            with detail_body:
                if action is None:
                    ui.label('Unavailable: action is absent from the current snapshot.').props('role=status')
                    return
                heading(f'Action {action["action_id"]}')
                for name in ('actions', 'devices'):
                    if name in source.errors:
                        ui.label(f'{name.title()}: {source.errors[name]}. Retained values may be stale.').props('role=status')
                badge(action['status'])
                ui.label(action['reason'])
                ui.label(f'Trigger window {action["trigger_window_id"]} · revision {action["trigger_revision"]} · policy v{action["policy_version"]}')
                ui.label(f'Target {kw(action["target_reduction_kw"])} · expected {kw(action["expected_reduction_kw"])} · confirmed {kw(action["confirmed_reduction_kw"])}')
                ui.label(f'Acknowledged {action["acknowledged_device_count"]} / {action["selected_device_count"]} selected devices · epoch {action["leader_epoch"]}')
                heading('Command results', 3)
                results = [r for d in items('devices') for r in d['command_history'] if r['action_id'] == action['action_id']]
                table(results, [('command_id', 'Command'), ('device_id', 'Device'), ('outcome', 'Outcome'), ('applied_rate_kw', 'Applied kW'), ('result_time', 'Result UTC')], 'command_id')
                ui.label('Candidate lists and full command envelopes are not exposed by v1. No acknowledgement implies success without a published result.').classes('muted')

        async def open_action(action):
            active_action.update(id=action['action_id'], fingerprint=None)
            render_action(action)
            details.open()

        def items(name):
            return ordered_records(name, source.data.get(name, {}).get('items', []))

        def show_errors():
            notices.clear()
            with notices:
                if source.status == 'Loading':
                    ui.spinner(size='24px')
                    ui.label('Loading authoritative snapshots…').props('role=status')
                if source.status in ('Reconnecting', 'Unavailable'):
                    ui.label(f'{source.status} · Last snapshot retained; values may be stale. Retrying automatically.').classes('notice').props('role=status aria-live=polite')
                for name, error in source.errors.items():
                    ui.label(f'{name.title()}: {error}. Last values may be stale.').classes('notice').props('role=status')
                overview = source.data.get('overview', {})
                freshness = overview.get('freshness')
                if freshness in ('STALE', 'PARTIAL', 'UNKNOWN'):
                    ui.label(f'{freshness} data · Missing or old readings cannot establish recovery.').classes('notice').props('role=status')

        def expansion(label):
            element = ui.expansion(label, value=expanded.get(label, False), on_value_change=lambda e: expanded.update({label: e.value}))
            return element

        def transformer_card(t):
            with ui.card().classes('w-full gap-3'):
                with ui.row().classes('w-full items-center justify-between'):
                    heading(t['transformer_id'], 3)
                    badge(t['state'])
                ui.label(kw(t['load']['net_load_kw'])).classes('metric')
                u = t['load']['utilization_pct']
                ui.label(f'{u if u is not None else "Unknown"}% utilization · capacity {kw(t["load"]["capacity_kw"])}').classes('muted')
                if u is not None:
                    ui.linear_progress(value=min(1, max(0, u/100)), show_value=False, color={'CRITICAL':'negative','WARNING':'warning'}.get(t['state'], 'primary')).props(f'aria-label="Utilization {u} percent"')
                ui.label(f'{t["freshness"]} · data age {t["data_age_seconds"]:.0f}s · {t["load"]["window_end"]}').classes('text-sm muted')
                ui.button(f'Open {t["transformer_id"]}', on_click=lambda: navigate('Transformers', t['transformer_id'])).props('outline')

        def action_list(actions):
            if not actions:
                ui.label('No actions in this view.').classes('muted')
            for action in actions:
                with ui.card().classes('w-full gap-2'):
                    with ui.row().classes('w-full justify-between items-center'):
                        ui.button(action['action_id'], on_click=lambda a=action: open_action(a)).props('flat dense')
                        badge(action['status'])
                    ui.label(action['reason'])
                    ui.label(f'{action["transformer_id"]} · {action["transition_time"]}').classes('muted text-sm')
                    ui.label(f'Target {kw(action["target_reduction_kw"])} · confirmed {kw(action["confirmed_reduction_kw"])}')

        def render():
            content.clear()
            with content:
                page = selected['page']
                ts = items('transformers')
                if not source.data:
                    ui.label('Waiting for API data. Check connection and identity.').classes('muted')
                    return
                if page == 'Overview':
                    overview = source.data.get('overview', {})
                    with ui.element('div').classes('kpis'):
                        solar = sum(t['load']['solar_generation_kw'] for t in ts) if ts else None
                        active = sum(a['status'] != 'COMPLETED' for a in items('actions'))
                        for label, value, sub in [('Total net demand', kw(overview.get('total_net_load_kw')), 'Published transformer load'),
                            ('Solar contribution', kw(solar), 'Generation magnitude'), ('Critical transformers', str(overview.get('critical_count', 'Unknown')), 'Published severity'),
                            ('Open actions', str(active) if 'actions' in source.data else 'Unknown', 'Noncompleted throttle / recovery plans')]:
                            with ui.card():
                                ui.label(label).classes('muted')
                                ui.label(value).classes('metric')
                                ui.label(sub).classes('text-sm muted')
                    with ui.element('div').classes('two-col'):
                        with ui.card().classes('w-full'):
                            heading('Net load · ' + source.data.get('history_transformer', 'no transformer'))
                            ui.label(f'Last {source.data.get("history_minutes", source.range_minutes)} minutes · selected transformer history').classes('muted')
                            chart(items('loads'))
                            with expansion('Load history values').classes('w-full'):
                                table(items('loads'), [('window_end','Window end UTC'),('net_load_kw','Net kW'),('completeness','Completeness')], 'window_id')
                        with ui.column().classes('w-full'):
                            heading('Platform response')
                            action_list(items('actions')[:3])
                    heading('Transformers requiring attention')
                    with ui.element('div').classes('tx-grid'):
                        for t in sorted(ts, key=lambda t: ({'CRITICAL':0,'WARNING':1,'STALE':2,'NORMAL':3}[t['state']], -(t['load']['utilization_pct'] or 0))):
                            transformer_card(t)
                    if not ts:
                        ui.label('No transformers available.').classes('muted')
                    alerts_view(items('alerts')[:3])
                elif page == 'Transformers':
                    if selected['transformer']:
                        transformer = next((t for t in ts if t['transformer_id'] == selected['transformer']), None)
                        if transformer:
                            detail_view(transformer)
                        else:
                            ui.label('Unavailable: transformer is absent from the current inventory.').props('role=status')
                    else:
                        with ui.element('div').classes('tx-grid'):
                            for t in ts:
                                transformer_card(t)
                        if not ts:
                            ui.label('No transformers available.')
                elif page == 'Alerts & actions':
                    def matches(row):
                        time = row.get('transition_time', row.get('resolved_at', row.get('opened_at')))
                        age = (datetime.now(timezone.utc)-datetime.fromisoformat(time.replace('Z', '+00:00'))).total_seconds() if time else 0
                        return selected['search'].lower() in json.dumps(row).lower() and (selected['status'] == 'All' or row['status'] == selected['status']) and age <= source.range_minutes*60
                    alerts_view([a for a in items('alerts') if matches(a) and (selected['severity'] == 'All' or a['severity'] == selected['severity'])])
                    heading('Action and command history')
                    action_list([a for a in items('actions') if matches(a)])
                elif page == 'Devices':
                    devices_view([d for d in items('devices') if selected['search'].lower() in json.dumps(d).lower()])
                elif page == 'Scenarios':
                    heading('Current run')
                    run = source.data.get('scenarios', {}).get('active_run')
                    if run:
                        with ui.card().classes('w-full'):
                            badge(run['status'])
                            ui.label(f'{run["run_id"]} · {run["configuration"]["scenario"]} · updated {run["updated_at"]}')
                            ui.label(f'Seed {run["configuration"]["seed"]} · scale {run["configuration"]["scale"]} · duration {run["configuration"]["duration_seconds"]}s')
                            ui.label('Run progress: status is reported by the API; completion percentage is not supplied.').classes('muted')
                            ui.button('Stop scenario', on_click=lambda: confirm('Stop scenario?', f'/scenario-runs/{run["run_id"]}/stop')).props('outline')
                    else:
                        ui.label('No active scenario.').classes('muted')
                    heading('Approved scenarios')
                    with ui.element('div').classes('tx-grid'):
                        for scenario in source.data.get('scenarios', {}).get('scenarios', []):
                            with ui.card():
                                heading(scenario.replace('_', ' ').title(), 3)
                                ui.label({'normal':'Steady demand within capacity.', 'evening_overload':'EV demand spike and response.', 'late_correction':'Delayed telemetry and revised windows.', 'leader_failover':'Control ownership interruption.'}.get(scenario, scenario)).classes('muted')
                elif page == 'System health':
                    heading('Reported health')
                    table([dict(component=k, **v) for k,v in source.health.items()], [('component','Component'),('status','Status'),('observed_at','Observed UTC')], 'component')
                    with ui.card().classes('w-full'):
                        for component in ['Kafka', 'Flink', 'Database', 'Action service', 'API', 'Simulator', 'Live stream']:
                            ui.label(f'{component}: {source.status if component == "Live stream" else "Unknown · no component metrics endpoint in v1"}')
                        ui.label('Consumer lag, checkpoint age, DLQ rate and API error rate: unavailable from this API.').classes('muted')
                        ui.label(f'Stale transformers in loaded inventory: {sum(t["freshness"] == "STALE" for t in ts)}')
                        ui.link('Open local Grafana', 'http://127.0.0.1:3000', new_tab=True)
                        ui.button('Investigate health', on_click=lambda: investigate()).props('outline')
                elif page == 'Settings':
                    with ui.card().classes('w-full'):
                        heading('Connection and display')
                        ui.label(f'Mode: {"explicit fixtures" if fixture else "live REST/SSE"} · one UI process')
                        ui.label('Timestamps: UTC · Charts: animation disabled · Keyboard: Tab and Enter')
                        ui.label('Identity is configured on the local server. Keycloak integration is a later milestone.').classes('muted')


        def alerts_view(alerts):
            heading('Alerts')
            if not alerts:
                ui.label('No alerts in this view.').classes('muted')
            for alert in alerts:
                with ui.card().classes('w-full gap-2'):
                    badge(alert['severity'])
                    ui.label(f'{alert["alert_id"]} · {alert["status"]} · {alert["transformer_id"]}').classes('font-bold')
                    ui.label(alert['message'])
                    ui.label(f'Window {alert["load_window_id"]} · revision {alert["load_revision"]} · opened {alert["opened_at"]}').classes('muted text-sm')
                    with ui.row():
                        ui.button('Open transformer', on_click=lambda a=alert: navigate('Transformers', a['transformer_id'])).props('outline dense')
                        ui.button('Investigate alert', on_click=lambda a=alert: investigate(a['transformer_id'], alert=a['alert_id'])).props('outline dense')
                        if alert['status'] == 'OPEN':
                            ui.button('Acknowledge alert', on_click=lambda a=alert: perform(f'/alerts/{a["alert_id"]}/acknowledge')).props('dense')

        def devices_view(devices):
            heading('Device inventory')
            table([dict(d['device'], freshness=d['freshness'], power=kw((d.get('latest_telemetry') or {}).get('power_kw'))) for d in devices],
                  [('device_id','Device'),('transformer_id','Transformer'),('power','Current power'),('freshness','Freshness'),('controllable','Controllable')], 'device_id')
            ui.label('Last telemetry arrival time is not exposed by DeviceView v1; capability update time is labeled separately.').classes('muted text-sm')
            for d in devices:
                with expansion(f'Device {d["device"]["device_id"]} · telemetry and commands').classes('w-full bg-white'):
                    ui.label(f'Type: {d["device"]["device_type"]} · priority: {d["device"]["priority"]} · capability updated: {d["device"]["updated_at"]}')
                    ui.label('Latest telemetry: ' + json.dumps(d.get('latest_telemetry')))
                    table(d['command_history'], [('command_id','Command'),('action_id','Action'),('outcome','Outcome'),('previous_rate_kw','Previous kW'),('applied_rate_kw','Applied kW'),('result_time','Result UTC')], 'command_id')

        def detail_view(t):
            transformer_card(t)
            load = t['load']
            history_matches = source.data.get('history_transformer') == t['transformer_id'] and source.data.get('history_minutes') == source.range_minutes
            history = items('loads') if history_matches else []
            with ui.card().classes('w-full'):
                heading('Load and policy thresholds')
                if not history_matches:
                    ui.label('Load history unavailable.' if 'loads' in source.errors else 'Loading load history…').props('role=status')
                else:
                    chart(history, t['policy'], load['capacity_kw'])
                with ui.row().classes('gap-6'):
                    for name, key in [('Building','meter_demand_kw'),('EV demand','ev_demand_kw'),('Solar','solar_generation_kw')]:
                        ui.label(f'{name}: {kw(load[key])}')
                with expansion('Load history values').classes('w-full'):
                    table(history, [('window_end','Window end UTC'),('net_load_kw','Net kW'),('utilization_pct','Utilization %'),('revision','Revision'),('completeness','Completeness')], 'window_id')
            heading('Action plan and outcomes')
            action_list([a for a in items('actions') if a['transformer_id'] == t['transformer_id']])
            devices_view([d for d in items('devices') if d['device']['transformer_id'] == t['transformer_id']])
            heading('Published event timeline')
            timeline = [dict(time=p['window_end'], event=f'Load {kw(p["net_load_kw"])} · {p["completeness"]}', reference=p['window_id']) for p in history]
            timeline += [dict(time=a['transition_time'], event=a['status'].replace('_',' '), reference=a['action_id']) for a in items('actions') if a['transformer_id'] == t['transformer_id']]
            timeline += [dict(time=a.get('resolved_at', a['opened_at']), event=f'Alert {a["status"]}', reference=a['alert_id']) for a in items('alerts') if a['transformer_id'] == t['transformer_id']]
            timeline += [dict(time=r['result_time'], event=f'Command {r["outcome"]} · {kw(r.get("applied_rate_kw"))}', reference=r['command_id']) for d in items('devices') if d['device']['transformer_id'] == t['transformer_id'] for r in d['command_history']]
            table(sorted(timeline, key=lambda r:instant(r['time'])), [('time','Time UTC'),('event','Published event'),('reference','Reference')], 'reference')
            ui.label('Timeline contains retained load windows and latest alert/action transitions. v1 does not expose every historical saga transition.').classes('muted text-sm')

        def tick():
            if login and not login.session(sid):
                # Invalidate every open tab, including panels held for keyboard reading.
                source.closed = True
                for task in tasks:
                    task.cancel()
                source.data.clear()
                source.health.clear()
                content.clear()
                detail_body.clear()
                details.close()
                timer.cancel()
                ui.navigate.to('/auth/signed-out')
                return
            status.set_text(f'{source.status.upper()} · {source.data.get("overview", {}).get("critical_count", "Unknown")} critical')
            snapshot_label.set_text(f'Snapshot {source.observed or "pending"}')
            if login:
                session = login.session(sid)
                role_label.set_text(session['actor']['role'] if session else 'Login required')
            notice_fingerprint = json.dumps([source.status, source.errors, source.data.get('overview', {}).get('freshness')], sort_keys=True)
            if notice_fingerprint != last_notice['fingerprint']:
                show_errors()
                last_notice['fingerprint'] = notice_fingerprint
            fingerprint = json.dumps([source.data, source.errors, source.status, source.health], sort_keys=True)
            changed = fingerprint != last['fingerprint']
            pending.set_visibility(changed and reading['content'])
            if changed and not reading['content']:
                render()
                last['fingerprint'] = fingerprint
            if details.value:
                action = next((a for a in items('actions') if a['action_id'] == active_action['id']), None)
                detail_fingerprint = json.dumps([action, source.data.get('devices'), source.errors], sort_keys=True)
                detail_changed = detail_fingerprint != active_action['fingerprint']
                detail_pending.set_visibility(detail_changed and reading['detail'])
                if detail_changed and not reading['detail']:
                    render_action(action)
                    active_action['fingerprint'] = detail_fingerprint

        navigate('Overview')
        show_errors()
        async def start():
            await client.connected()
            tasks.extend([asyncio.create_task(source.listen()), asyncio.create_task(source.refresh())])
        ui.timer(.1, start, once=True)
        timer = ui.timer(.3, tick)
        async def cleanup():
            timer.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await source.close()
        client.on_delete(cleanup)
