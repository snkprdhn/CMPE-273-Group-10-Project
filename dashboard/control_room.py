"""Live grid control room: the whole fleet at a glance, fed by one shared poller."""
import asyncio
from datetime import datetime, timezone
from html import escape
import os

from nicegui import app, ui

from dashboard import control_charts as charts
from dashboard.fleet_data import FleetData, from_environment

CSS = '''
body.cr { background:#0a1220; color:#dce8f8; }
.cr .q-header { background:#0d1830; color:#dce8f8; border-bottom:1px solid #233a5c; }
.cr-wrap { max-width:1760px; width:100%; margin:auto; padding:18px 22px 40px; gap:16px; }
.cr-card { background:#101c30; border:1px solid #233a5c; border-radius:12px; padding:14px 16px; box-shadow:none; }
.cr-card h3 { margin:0 0 6px; font-size:13px; letter-spacing:.09em; text-transform:uppercase; color:#8ea6c4; font-weight:700; }
.cr-kpis { display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:12px; width:100%; }
.cr-kpi { background:#101c30; border:1px solid #233a5c; border-radius:12px; padding:10px 14px 6px; min-width:0; }
.cr-kpi .k { color:#8ea6c4; font-size:11px; letter-spacing:.09em; text-transform:uppercase; font-weight:700; }
.cr-kpi .v { font-size:26px; font-weight:700; line-height:1.15; }
.cr-kpi .u { font-size:13px; color:#8ea6c4; margin-left:4px; font-weight:400; }
.cr-kpi .s { color:#8ea6c4; font-size:12px; }
.cr-row { display:grid; gap:14px; width:100%; }
.cr-r1 { grid-template-columns:minmax(0,2fr) minmax(280px,1fr); }
.cr-r2 { grid-template-columns:minmax(0,1fr) minmax(0,1.5fr) minmax(0,1fr); }
.cr-r3 { grid-template-columns:minmax(0,1.3fr) minmax(0,1fr) minmax(0,1fr) minmax(0,1fr); }
.cr-r4 { grid-template-columns:minmax(0,1.2fr) minmax(0,1fr) minmax(0,1.3fr); }
.cr .q-btn { text-transform:none; }
.cr-flow { display:grid; grid-template-columns:repeat(9,auto); align-items:center; gap:10px; width:100%; background:#0d1830; border:1px solid #233a5c; border-radius:12px; padding:10px 16px; }
.cr-stage { min-width:0; } .cr-stage .t { font-size:11px; letter-spacing:.09em; text-transform:uppercase; color:#8ea6c4; font-weight:700; }
.cr-stage .n { font-size:20px; font-weight:700; line-height:1.2; } .cr-stage .d { font-size:12px; color:#8ea6c4; }
.cr-arrow { height:2px; min-width:30px; background:linear-gradient(90deg,#233a5c,#2bd4ff,#233a5c); background-size:200% 100%; animation:flow 1.6s linear infinite; position:relative; }
.cr-arrow:after { content:''; position:absolute; right:-1px; top:-4px; border-left:7px solid #2bd4ff; border-top:5px solid transparent; border-bottom:5px solid transparent; }
@keyframes flow { from { background-position:200% 0; } to { background-position:0 0; } }
.cr-pill { display:inline-block; padding:1px 9px; border-radius:999px; font-size:12px; font-weight:700; }
.cr-feed td, .cr-feed th { padding:5px 8px; font-size:13px; text-align:left; border-bottom:1px solid #1b2c47; }
.cr-feed td { white-space:nowrap; }
.cr-feed th { color:#8ea6c4; white-space:nowrap; font-weight:700; font-size:11px; letter-spacing:.08em; text-transform:uppercase; }
.cr-feed table { width:100%; border-collapse:collapse; }
.cr a { color:#2bd4ff; }
.cr .muted { color:#8ea6c4; }
.dot { width:9px; height:9px; border-radius:50%; display:inline-block; margin-right:6px; }
@media(max-width:1300px) { .cr-kpis { grid-template-columns:repeat(4,minmax(0,1fr)); } .cr-r1,.cr-r2,.cr-r3,.cr-r4 { grid-template-columns:minmax(0,1fr); } }
'''

STATUS_COLORS = dict(COMMANDS_DISPATCHED='#ffb020', ACKS_PENDING='#ffb020', PLAN_CREATED='#ffb020', REDUCTION_CONFIRMED='#3ddc97',
                     RECOVERY_MONITORING='#2bd4ff', RESTORE_DISPATCHED='#2bd4ff', COMPLETED='#a78bfa',
                     NEEDS_OPERATOR_REVIEW='#ff5d5d', PARTIALLY_APPLIED='#ff5d5d', TIMED_OUT='#ff5d5d')


def H(content=''):
    # Every dynamic value that reaches these strings is a number or is passed through escape().
    return ui.html(content, sanitize=False)


def fmt_kw(value):
    if value is None:
        return '–'
    return f'{value / 1000:,.2f}' if abs(value) >= 10000 else f'{value:,.0f}'


def kw_unit(value):
    return 'MW' if value is not None and abs(value) >= 10000 else 'kW'


def fmt(value, digits=0, default='–'):
    return default if value is None else f'{value:,.{digits}f}'


SHORT = dict(RECOVERY_MONITORING='recovering', COMMANDS_DISPATCHED='throttling', REDUCTION_CONFIRMED='reduced', RESTORE_DISPATCHED='restoring',
             ACKS_PENDING='awaiting acks', NEEDS_OPERATOR_REVIEW='needs review', PARTIALLY_APPLIED='partial', PLAN_CREATED='planned')


def status_pill(status):
    color = STATUS_COLORS.get(status, '#8ea6c4')
    label = escape(SHORT.get(status) or status.replace('_', ' ').lower())
    return f'<span class="cr-pill" style="background:{color}22;color:{color};border:1px solid {color}66">{label}</span>'


def reduction(a):
    target, confirmed = a.get('target_reduction_kw'), a.get('confirmed_reduction_kw')
    if a['status'] == 'COMPLETED':
        return f'{fmt(target, 1)} kW shed'
    return f'{fmt(target, 1)} → {fmt(confirmed, 1)} kW'


def feed_rows(actions, limit=9):
    rows = []
    for a in actions[:limit]:
        when = a['transition_time'][11:19]
        rows.append(f'<tr><td>{when}</td><td><b>{escape(a["transformer_id"])}</b></td><td>{status_pill(a["status"])}</td>'
                    f'<td>{reduction(a)}</td>'
                    f'<td>{a.get("acknowledged_device_count", 0)}/{a.get("selected_device_count", 0)}</td></tr>')
    return ''.join(rows) or '<tr><td colspan="5" class="muted">No actions yet: the fleet raises an incident every few minutes.</td></tr>'


def register(configure_homepage=False):
    """Create the shared poller and the /live page. Call once from dashboard.app.configure()."""
    data = from_environment()
    holder = {'task': None}

    async def start():
        holder['task'] = asyncio.create_task(data.run())

    async def stop():
        if holder['task']:
            holder['task'].cancel()
        await data.close()

    app.on_startup(start)
    app.on_shutdown(stop)
    grafana = os.getenv('GRAFANA_PUBLIC_PATH', '/grafana/')

    @ui.page('/live')
    async def live():
        ui.add_css(CSS)
        ui.query('body').classes('cr')
        ui.colors(primary='#2bd4ff')
        state = {'selected': None, 'pinned': False, 'message': ''}
        charts_ = {}

        with ui.header().classes('items-center justify-between px-5 py-2'):
            with ui.row().classes('items-center gap-3'):
                ui.icon('electric_bolt', size='28px', color='primary')
                ui.label('GridBalance').classes('text-xl font-bold')
                ui.label('LIVE GRID CONTROL ROOM').classes('muted').style('font-size:12px;letter-spacing:.1em')
            with ui.row().classes('items-center gap-5'):
                connection = H('').classes('text-sm')
                clock = ui.label('').classes('muted text-sm')
                ui.link('Grafana', grafana, new_tab=True)
                ui.link('Operations console', '/operator')

        with ui.column().classes('cr-wrap'):
            with ui.element('div').classes('cr-kpis'):
                kpis = {}
                for key, title, color in (('demand', 'Fleet demand', '#4c8dff'), ('utilization', 'Fleet utilization', '#2bd4ff'),
                                          ('ev', 'EV charging', '#ffb020'), ('solar', 'Solar generation', '#3ddc97'),
                                          ('risk', 'Transformers at risk', '#ffb020'), ('actions', 'Automatic actions', '#a78bfa'),
                                          ('rate', 'Events per second', '#2bd4ff'), ('latency', 'Output freshness', '#3ddc97')):
                    with ui.element('div').classes('cr-kpi'):
                        H(f'<div class="k">{title}</div>')
                        kpis[key] = dict(value=H('').classes(''), sub=H('').classes('s'),
                                         spark=ui.echart(charts.spark([], 1, color)).classes('w-full').style('height:34px'), color=color)

            flow = H('').classes('w-full')

            with ui.element('div').classes('cr-row cr-r1'):
                with ui.element('div').classes('cr-card'):
                    H('<h3>Fleet load and capacity</h3>')
                    charts_['load'] = ui.echart(charts.fleet_load([])).classes('w-full').style('height:300px')
                with ui.element('div').classes('cr-card'):
                    with ui.row().classes('items-center justify-between w-full'):
                        H('<h3>Transformer heat map</h3>')
                        H('<span class="muted" style="font-size:12px">tx number · utilization · outlined = managed</span>')

                    def pick(event):
                        name = event.name if hasattr(event, 'name') else None
                        if name:
                            state.update(selected=name, pinned=True)
                            refresh()
                    charts_['heat'] = ui.echart(charts.heat_map({}), on_point_click=pick).classes('w-full').style('height:280px')

            with ui.element('div').classes('cr-row cr-r2'):
                with ui.element('div').classes('cr-card'):
                    H('<h3>Most loaded transformers</h3>')
                    charts_['top'] = ui.echart(charts.top_loaded({})).classes('w-full').style('height:300px')
                with ui.element('div').classes('cr-card'):
                    with ui.row().classes('items-center justify-between w-full'):
                        selector = ui.select([], label='Transformer', on_change=lambda e: (state.update(selected=e.value, pinned=True), refresh())
                                             ).props('dense dark outlined').classes('w-40')
                        ui.button('Follow incidents', icon='bolt', on_click=lambda: (state.update(pinned=False), refresh())).props('flat dense')
                    charts_['detail'] = ui.echart(charts.transformer_detail([], [], '')).classes('w-full').style('height:264px')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Automatic actions, live</h3>')
                    feed = H('').classes('cr-feed')

            with ui.element('div').classes('cr-row cr-r3'):
                with ui.element('div').classes('cr-card'):
                    H('<h3>Pipeline throughput</h3>')
                    charts_['flow'] = ui.echart(charts.throughput([])).classes('w-full').style('height:210px')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Consumer lag, records</h3>')
                    charts_['lag'] = ui.echart(charts.lag_bars({})).classes('w-full').style('height:210px')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Flink busy time</h3>')
                    charts_['busy'] = ui.echart(charts.gauge(0, 'busy')).classes('w-full').style('height:210px')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Flink backpressure</h3>')
                    charts_['pressure'] = ui.echart(charts.gauge(0, 'blocked', warn=10, bad=30)).classes('w-full').style('height:210px')

            with ui.element('div').classes('cr-row cr-r4'):
                with ui.element('div').classes('cr-card'):
                    H('<h3>Correctness, last 5 minutes</h3>')
                    correctness = H('')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Rolling retention and storage</h3>')
                    charts_['volumes'] = ui.echart(charts.volumes({})).classes('w-full').style('height:130px')
                    retention = H('').classes('muted').style('font-size:13px')
                with ui.element('div').classes('cr-card'):
                    H('<h3>Try it: faults and incidents</h3>')
                    H('<div class="muted" style="font-size:13px;margin-bottom:8px">Real events on the live pipeline. Counters on the left move within seconds.</div>')
                    with ui.row().classes('gap-2'):
                        for label, kind in (('Out of order', 'out_of_order'), ('Duplicates', 'duplicates'), ('Very late', 'very_late')):
                            ui.button(label, on_click=lambda k=kind, l=label: send('/inject', {'kind': k, 'seconds': 30}, l + ' for 30 s')).props('outline dense')
                    with ui.row().classes('items-center gap-2 mt-2'):
                        target = ui.select(['tx-001', 'tx-002', 'tx-003', 'tx-004', 'tx-005'], value='tx-003', label='Incident on').props('dense dark outlined').classes('w-32')
                        ui.button('Trigger overload', icon='warning', on_click=lambda: send(
                            '/overload', {'transformer': target.value, 'seconds': 90}, f'Overload on {target.value} for 90 s')).props('color=warning dense')
                    message = ui.label('').classes('muted text-sm mt-2').props('aria-live=polite')
                    injected = H('').classes('muted').style('font-size:13px;margin-top:10px')

        async def send(path, body, label):
            try:
                await data.control(path, body)
                message.set_text(f'Sent: {label}.')
            except Exception as exc:
                message.set_text(f'Not sent: {exc}')

        def refresh():
            s, sc = data.summary(), data.scalars
            totals, now = s['totals'], datetime.now(timezone.utc)
            age = data.age()
            fresh = age is not None and age < 15 and 'prometheus' not in data.errors
            connection.set_content(f'<span class="dot" style="background:{"#3ddc97" if fresh else "#ff5d5d"}"></span>'
                                   f'{"LIVE" if fresh else "NO DATA"} · {s["count"]} transformers · {fmt(sc.get("devices"))} devices')
            clock.set_text(now.strftime('%Y-%m-%d %H:%M:%S UTC'))
            hist, pipe = list(data.fleet_history), list(data.pipeline_history)

            def tile(key, value, sub, index=None, rows=None):
                kpis[key]['value'].set_content(f'<div class="v">{value}</div>')
                kpis[key]['sub'].set_content(sub)
                if rows is not None and index is not None:
                    kpis[key]['spark'].options.clear()
                    kpis[key]['spark'].options.update(charts.spark(rows, index, kpis[key]['color']))
                    kpis[key]['spark'].update()

            tile('demand', f'{fmt_kw(totals["net"])}<span class="u">{kw_unit(totals["net"])}</span>', f'net, {s["count"]} transformers', 1, hist)
            share = [(r[0], r[1] / r[5] * 100) for r in hist if r[1] is not None and r[5]]
            tile('utilization', f'{fmt(s["utilization"], 0)}<span class="u">%</span>', f'of {fmt_kw(totals["capacity"])} {kw_unit(totals["capacity"])} capacity', 1, share)
            tile('ev', f'{fmt_kw(totals["ev"])}<span class="u">{kw_unit(totals["ev"])}</span>', 'charger demand', 2, hist)
            tile('solar', f'{fmt_kw(totals["solar"])}<span class="u">{kw_unit(totals["solar"])}</span>', 'rooftop generation', 4, hist)
            risk = s['counts']['warning'] + s['counts']['critical']
            tile('risk', f'{risk}<span class="u">of {s["count"]}</span>', f'{s["counts"]["critical"]} critical · {s["counts"]["warning"]} warning')
            active = [a for a in data.actions if a['status'] not in ('COMPLETED',)]
            tile('actions', f'{len(active)}<span class="u">active</span>', f'{fmt(sc.get("commands"))} commands in 10 min')
            tile('rate', f'{fmt(sc.get("kafka_in"))}<span class="u">/s</span>', f'{fmt(sc.get("producer_rate"))} published', 2, pipe)
            tile('latency', f'{fmt((sc.get("output_age_ms") or 0) / 1000, 1) if sc.get("output_age_ms") is not None else "–"}<span class="u">s</span>',
                 'since the last result window')

            def stage(title, number, detail):
                return f'<div class="cr-stage"><div class="t">{title}</div><div class="n">{number}</div><div class="d">{detail}</div></div>'
            arrow = '<div class="cr-arrow"></div>'
            flow.set_content('<div class="cr-flow">' + arrow.join([
                stage('Devices', fmt(sc.get('devices')), 'meters, chargers, solar'),
                stage('Kafka', f'{fmt(sc.get("kafka_in"))}/s', '3 partitions, keyed by transformer'),
                stage('Flink windows', f'{fmt(sc.get("kafka_out"), 1)}/s', f'5 s windows, {fmt(sc.get("flink_busy"))}% busy'),
                stage('Action service', f'{len(active)} active', f'{s["counts"]["critical"]} critical now'),
                stage('Chargers', f'{fmt(sc.get("controllable"))} managed', f'{fmt(sc.get("commands"))} commands in 10 min'),
            ]) + '</div>')
            charts_['load'].options.clear(); charts_['load'].options.update(charts.fleet_load(hist)); charts_['load'].update()
            names = sorted(data.tx)
            if not state['pinned']:
                incident = next((n for n in names if data.tx[n]['incident'] or data.tx[n]['band'] == 'critical'), None)
                state['selected'] = incident or state['selected'] or (names[0] if names else None)
            selector.set_options(names, value=state['selected'])
            charts_['heat'].options.clear(); charts_['heat'].options.update(charts.heat_map(data.tx, state['selected'])); charts_['heat'].update()
            charts_['top'].options.clear(); charts_['top'].options.update(charts.top_loaded(data.tx)); charts_['top'].update()
            history = list(data.util_history.get(state['selected'], []))
            charts_['detail'].options.clear()
            charts_['detail'].options.update(charts.transformer_detail(history, data.actions, state['selected'] or ''))
            charts_['detail'].update()
            feed.set_content('<table><tr><th>UTC</th><th>Transformer</th><th>Stage</th><th>Reduction</th><th>Devices</th></tr>' + feed_rows(data.actions) + '</table>')
            charts_['flow'].options.clear(); charts_['flow'].options.update(charts.throughput(pipe)); charts_['flow'].update()
            charts_['lag'].options.clear(); charts_['lag'].options.update(charts.lag_bars(data.lag)); charts_['lag'].update()
            charts_['busy'].options.clear(); charts_['busy'].options.update(charts.gauge(sc.get('flink_busy'), 'busy')); charts_['busy'].update()
            charts_['pressure'].options.clear(); charts_['pressure'].options.update(charts.gauge(sc.get('flink_backpressure'), 'blocked', warn=10, bad=30)); charts_['pressure'].update()
            correctness.set_content(
                '<table class="cr-feed" style="width:100%"><tr><th>Check</th><th>Count</th><th>What happened</th></tr>'
                f'<tr><td>Duplicates dropped</td><td>{fmt(sc.get("duplicates"))}</td><td class="muted">same event ID seen again, counted once</td></tr>'
                f'<tr><td>Late, accepted</td><td>{fmt(sc.get("accepted_late"))}</td><td class="muted">arrived after the window closed, window corrected</td></tr>'
                f'<tr><td>Window corrections</td><td>{fmt(sc.get("corrections"))}</td><td class="muted">new revision published, old one kept</td></tr>'
                f'<tr><td>Too late, audited</td><td>{fmt(sc.get("late"))}</td><td class="muted">beyond 30 s, written to the late topic</td></tr>'
                f'<tr><td>Rejected (DLQ)</td><td>{fmt(sc.get("dlq"))}</td><td class="muted">failed validation, kept for inspection</td></tr>'
                f'<tr><td>Device coverage</td><td>{fmt(s["coverage"], 1)}%</td><td class="muted">devices reporting in the latest windows</td></tr>'
                f'<tr><td>Checkpoints</td><td>{fmt(sc.get("checkpoint_done"))}</td><td class="muted">last {fmt(sc.get("checkpoint_ms"))} ms · {fmt(sc.get("checkpoint_failed"))} failed</td></tr></table>')
            injected.set_content('<b>Injected in the last 5 minutes</b><br>'
                                 f'{fmt(sc.get("injected_late"))} readings delayed · {fmt(sc.get("injected_duplicates"))} duplicates · '
                                 f'{fmt(sc.get("injected_dropped"))} dropped. The correctness table shows how each was handled.')
            charts_['volumes'].options.clear(); charts_['volumes'].options.update(charts.volumes(data.volumes)); charts_['volumes'].update()
            rate = sc.get('kafka_in') or 0
            minutes = (sc.get('telemetry_retained') or 0) / rate / 60 if rate else None
            retention.set_content(f'Telemetry on Kafka covers about <b>{fmt(minutes)} minutes</b> ({fmt(sc.get("telemetry_retained"))} records). '
                                  'Older records are deleted automatically, so the demo runs for days on small disks.')

        ui.timer(3.0, refresh)
        ui.timer(.2, refresh, once=True)
