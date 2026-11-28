"""Live grid control room: the whole fleet at a glance, fed by one shared poller."""
import asyncio
from datetime import datetime, timezone
from html import escape
import json
import os

from nicegui import app, background_tasks, ui

from dashboard import control_charts as charts
from dashboard.fleet_data import COUNTED, DEMAND_CHARGE, FleetData, HELD_STATES, SETTLED, charger_state, epoch, from_environment, throttling_actions

CSS = '''
body.cr { background:#09090b; color:#e4e4e7; }
.cr .q-header { background:#18181b; color:#e4e4e7; border-bottom:1px solid #27272a; }
.cr-wrap { max-width:1760px; width:100%; margin:auto; padding:0 22px 72px; gap:8px; }
.cr-card { background:#18181b; border:1px solid #27272a; border-radius:10px; padding:10px 14px; box-shadow:none; }
.cr .nicegui-content { padding:6px 16px 0; }
.cr .cr-link { min-height:22px; padding:0 4px; }
.cr-behind { font-size:12px; color:#fbbf24; white-space:nowrap; }
.cr-hint { font-size:12px; white-space:nowrap; }
.cr-card h3 { white-space:nowrap; margin:0 0 6px; font-size:12px; letter-spacing:.09em; text-transform:uppercase; color:#a1a1aa; font-weight:700; }
.cr-kpis { display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px; width:100%; }
.cr-kpi { background:#18181b; border:1px solid #27272a; border-radius:10px; padding:6px 14px 4px; min-width:0; }
.cr-kpi .k { color:#a1a1aa; font-size:11px; letter-spacing:.09em; text-transform:uppercase; font-weight:700; }
.cr-kpi .v { font-size:24px; font-weight:700; line-height:1.1; color:#fafafa; }
.cr-kpi .u { font-size:13px; color:#a1a1aa; margin-left:4px; font-weight:400; }
.cr-kpi .s { color:#a1a1aa; font-size:12px; }
.cr-row { display:grid; gap:14px; width:100%; }
.cr-stage-grid { display:grid; grid-template-columns:minmax(0,65fr) minmax(0,35fr); gap:10px; width:100%; align-items:start; }
.cr-col { display:flex; flex-direction:column; gap:10px; min-width:0; }
.cr-r3 { grid-template-columns:minmax(0,1.3fr) minmax(0,1fr) minmax(0,1fr) minmax(0,1fr); }
.cr-r4 { grid-template-columns:minmax(0,1.3fr) minmax(0,1fr); }
.cr .q-btn { text-transform:none; }
.cr .q-tab-panels, .cr .q-tab-panel { background:transparent; padding:0; }
.cr .q-tab-panel { display:flex; flex-direction:column; gap:8px; padding-top:8px; overflow:visible; }
.cr .q-tabs { color:#a1a1aa; border-bottom:1px solid #27272a; }
.cr .q-tab--active { color:#fafafa; }
.cr .q-table--dark { background:transparent; color:#e4e4e7; } .cr .q-table--dark td, .cr .q-table--dark th, .cr .q-table--dark .q-table__bottom { border-color:#27272a; }
.cr .q-tab-panel > .cr-card { width:100%; box-sizing:border-box; }
.cr .q-table thead tr, .cr .q-table thead th { background:#18181b; }
.cr .q-table th { color:#a1a1aa; font-weight:700; font-size:11px; letter-spacing:.06em; text-transform:uppercase; }
.cr .q-table tbody tr { cursor:pointer; } .cr .q-table tbody tr:hover td { background:#27272a; }
.band-critical, .st-Throttled { color:#fbbf24; font-weight:700; } .band-critical { color:#f87171; } .band-warning { color:#fbbf24; } .band-normal, .st-Spared { color:#34d399; }
.st-Spared { font-weight:700; }
.cr-tx-grid { display:grid; grid-auto-flow:column; grid-auto-columns:minmax(0,1fr); gap:12px; align-items:start; width:100%; }
.cr .cr-tx-chart.nicegui-echart { height:230px !important; }
@media(max-width:1100px) { .cr-tx-grid { grid-auto-flow:row; grid-auto-columns:minmax(0,1fr); } }
#tx-panel { scroll-margin-bottom:70px; }
.cr-flow { display:grid; grid-template-columns:repeat(13,auto); justify-content:space-between; align-items:center; gap:8px; width:100%; background:#18181b; border:1px solid #27272a; border-radius:10px; padding:10px 16px 6px; }
.cr-rail { grid-column:1 / 12; grid-row:2; position:relative; height:24px; margin:0 56px 14px; border:2px dashed #38bdf8; border-top:none; border-radius:0 0 16px 16px; }
.cr-rail:before { content:''; position:absolute; left:-7px; top:-4px; border-bottom:8px solid #38bdf8; border-left:5px solid transparent; border-right:5px solid transparent; }
.cr-rail span { position:absolute; left:50%; bottom:-10px; transform:translateX(-50%); background:#18181b; padding:0 10px; color:#7dd3fc; font-size:12px; white-space:nowrap; }
.cr-saga { display:flex; align-items:stretch; gap:14px; width:100%; }
.cr-step { flex:1 1 auto; min-width:0; position:relative; background:#09090b; border:1px solid #27272a; border-radius:6px; padding:2px 7px; font-size:11px; line-height:1.25; color:#71717a; }
.cr-step + .cr-step:before { content:'›'; position:absolute; left:-11px; top:50%; transform:translateY(-52%); color:#71717a; font-size:16px; }
.cr-step.done { color:#34d399; border-color:#14532d; }
.cr-step.active { color:#fbbf24; border-color:#fbbf24; background:#fbbf2414; font-weight:700; }
.cr-step.hold { color:#f87171; border-color:#f87171; background:#f8717114; font-weight:700; }
.cr-step .sub { display:block; font-size:10px; font-weight:400; }
.cr-note { font-size:11px; color:#a1a1aa; margin-top:2px; }
.cr .cr-topo.nicegui-echart { height:222px !important; }
.cr .cr-detail.nicegui-echart { height:188px !important; }
.cr .cr-fleet.nicegui-echart { height:166px !important; }
.cr-log { max-height:224px; }
@media(max-height:760px) {
  .cr-wrap { gap:6px; padding-bottom:60px; } .cr .nicegui-content { padding-top:4px; } .cr .q-tab-panel { padding-top:6px; gap:6px; }
  .cr .q-tab { min-height:30px; } .cr-card { padding:6px 12px; } .cr-card h3 { margin-bottom:2px; }
  .cr-kpi { padding:3px 12px 2px; } .cr-kpi .v { font-size:19px; } .cr-kpi .s { font-size:11px; line-height:1.2; } .cr-kpi .k { font-size:10px; }
  .cr .cr-topo.nicegui-echart { height:156px !important; } .cr .cr-detail.nicegui-echart { height:150px !important; } .cr-kpi .nicegui-echart, .cr-hint { display:none; }
  .cr .cr-fleet.nicegui-echart { height:150px !important; } .cr-log { max-height:190px; }
  .cr-saga { gap:10px; } .cr-step { padding:2px 4px; font-size:10px; }
  .cr-step + .cr-step:before { left:-8px; } .cr .q-footer { min-height:0; } .cr-note { display:none; }
}
.cr-stage { min-width:0; } .cr-stage .t { font-size:11px; letter-spacing:.09em; text-transform:uppercase; color:#a1a1aa; font-weight:700; }
.cr-stage .n { font-size:20px; font-weight:700; line-height:1.2; } .cr-stage .d { font-size:12px; color:#a1a1aa; }
.cr-arrow { height:2px; min-width:30px; background:linear-gradient(90deg,#27272a,#94a3b8,#27272a); background-size:200% 100%; animation:flow 1.6s linear infinite; position:relative; }
.cr-arrow:after { content:''; position:absolute; right:-1px; top:-4px; border-left:7px solid #94a3b8; border-top:5px solid transparent; border-bottom:5px solid transparent; }
@keyframes flow { from { background-position:200% 0; } to { background-position:0 0; } }
@media(prefers-reduced-motion:reduce) { .cr-arrow { animation:none; } }
.cr-pill { display:inline-block; padding:1px 9px; border-radius:999px; font-size:12px; font-weight:700; }
.cr-log { display:flex; flex-direction:column; gap:6px; overflow:auto; }
.cr-log-row { background:#09090b; border-radius:6px; padding:7px 10px; font-size:13px; line-height:1.35; }
.cr-dock { background:#18181b; border-top:1px solid #27272a; color:#e4e4e7; }
.cr-feed td, .cr-feed th { padding:5px 8px; font-size:13px; text-align:left; border-bottom:1px solid #27272a; }
.cr-feed td { white-space:nowrap; }
.cr-feed th { color:#a1a1aa; white-space:nowrap; font-weight:700; font-size:11px; letter-spacing:.08em; text-transform:uppercase; }
.cr-feed table { width:100%; border-collapse:collapse; }
.cr a { color:#7dd3fc; }
.cr .muted { color:#a1a1aa; }
.dot { width:9px; height:9px; border-radius:50%; display:inline-block; margin-right:6px; }
@media(max-width:1000px) { .cr-kpis { grid-template-columns:repeat(3,minmax(0,1fr)); } }
@media(max-width:1100px) { .cr-stage-grid,.cr-r3,.cr-r4 { grid-template-columns:minmax(0,1fr); } }
'''

STATUS_COLORS = dict(COMMANDS_DISPATCHED='#ffb020', ACKS_PENDING='#ffb020', PLAN_CREATED='#ffb020', REDUCTION_CONFIRMED='#3ddc97',
                     RECOVERY_MONITORING='#2bd4ff', RESTORE_DISPATCHED='#2bd4ff', COMPLETED='#a78bfa',
                     NEEDS_OPERATOR_REVIEW='#ff5d5d', PARTIALLY_APPLIED='#ff5d5d', TIMED_OUT='#ff5d5d')


def H(content=''):
    # Every dynamic value that reaches these strings is a number or is passed through escape().
    return ui.html(content, sanitize=False)


def count(value, noun):
    """'1 command', '2 commands', '– commands' when unknown."""
    return f'{fmt(value)} {noun}' + ('' if value is not None and round(value) == 1 else 's')


def fmt(value, digits=0, default='–'):
    return default if value is None else f'{value:,.{digits}f}'


SHORT = dict(RECOVERY_MONITORING='recovering', COMMANDS_DISPATCHED='throttling', REDUCTION_CONFIRMED='reduced', RESTORE_DISPATCHED='restoring',
             ACKS_PENDING='awaiting acks', NEEDS_OPERATOR_REVIEW='needs review', PARTIALLY_APPLIED='partial', PLAN_CREATED='planned')


KPI_ORDER = (('load', 'Active load'), ('throttled', 'Throttled EVs'), ('peak', 'Peak avoided'),
             ('energy', 'Energy shifted'), ('fresh', 'Output freshness'))


def kpi_tiles(summary, scalars, actions, business, api_down=False):
    """The five header tiles as plain data. kW is power, kWh is energy: the units are fixed per tile."""
    held = sum(int(a.get('acknowledged_device_count') or 0) for a in actions if a.get('status') in COUNTED)
    totals, age = summary['totals'], scalars.get('output_age_ms')
    return [
        dict(key='load', title='Active load', value=fmt(totals['net']) if summary['count'] else '–', unit='kW',
             sub=f'{fmt(summary["utilization"], 0)}% of {fmt(totals["capacity"])} kW capacity' if summary['count'] else 'waiting for data'),
        dict(key='throttled', title='Throttled EVs', value=f'{held}/{fmt(scalars.get("controllable"))}', unit='',
             sub='actions API unreachable, last good value' if api_down else 'chargers under a confirmed limit'),
        dict(key='peak', title='Peak avoided', value=fmt(business['peak_avoided_kw'], 1), unit='kW',
             sub='actions API unreachable, frozen' if api_down else f'${fmt(business["dollars"])} illustrative @ ${DEMAND_CHARGE:g}/kW-month'),
        dict(key='energy', title='Energy shifted', value=fmt(business['kwh_shifted'], 1), unit='kWh', sub='actions API unreachable, frozen' if api_down else 'last hour, confirmed throttles'),
        dict(key='fresh', title='Output freshness', value='–' if age is None else fmt(age / 1000, 1), unit='s', sub='since the last result window'),
    ]


def tile_value(tile):
    unit = f'<span class="u">{tile["unit"]}</span>' if tile['unit'] else ''
    return f'<div class="v">{tile["value"]}{unit}</div>'


def decision(a, utilization=None):
    """One sentence for one action row. Every dynamic value is a number or goes through escape()."""
    tx, status = escape(str(a.get('transformer_id', '?'))), a.get('status')
    when = escape(str(a.get('transition_time', ''))[11:19])
    selected, acked = int(a.get('selected_device_count') or 0), int(a.get('acknowledged_device_count') or 0)
    target, confirmed = a.get('target_reduction_kw') or 0, a.get('confirmed_reduction_kw') or 0
    kw = f'{fmt(confirmed)}/{fmt(target)} kW confirmed'
    at = '' if utilization is None else f' (triggered at {fmt(utilization)} %)'
    text = dict(
        PLAN_CREATED=f'plan created, choosing the lowest-priority chargers · target {fmt(target)} kW',
        COMMANDS_DISPATCHED=f'throttle sent to {selected} lowest-priority chargers · waiting for acks · target {fmt(target)} kW',
        ACKS_PENDING=f'throttling {selected} lowest-priority chargers · {kw} · acked {acked}/{selected}',
        REDUCTION_CONFIRMED=f'throttled {selected} lowest-priority chargers · {kw} · acked {acked}/{selected}',
        PARTIALLY_APPLIED=f'safe hold: only {acked}/{selected} chargers acked · {kw} · no further commands',
        TIMED_OUT=f'safe hold: acks timed out · acked {acked}/{selected} · {kw}',
        RECOVERY_MONITORING=f'load is back under the limit, watching before restore · holding {fmt(confirmed)} kW',
        RESTORE_DISPATCHED=f'restoring {selected} chargers to their normal rate',
        COMPLETED=f'restored · {fmt(confirmed)} kW shed, chargers back to their normal rate',
        NEEDS_OPERATOR_REVIEW=f'safe hold: needs operator review · {escape(str(a.get("reason", ""))[:90])}',
    ).get(status, f'status {escape(str(status))}')
    return f'{when} · <b>{tx}</b>{at} → {text}'


def decision_log(actions, trigger_pct=None, limit=12):
    """`trigger_pct` maps an action to the utilization of the window that fired it (None when that window is not cached)."""
    rows = []
    for a in actions[:limit]:
        pct = trigger_pct(a) if trigger_pct else None
        color = STATUS_COLORS.get(a.get('status'), '#a1a1aa')
        rows.append(f'<div class="cr-log-row" style="border-left:3px solid {color}">{decision(a, pct)}</div>')
    return ''.join(rows) or '<div class="muted">No actions yet: the fleet raises an incident every few minutes.</div>'


DOCK = (('Delayed packets', '/inject', {'kind': 'very_late', 'seconds': 30}),
        ('Out of order', '/inject', {'kind': 'out_of_order', 'seconds': 30}),
        ('Duplicates', '/inject', {'kind': 'duplicates', 'seconds': 30}),
        ('Cloud cover', '/inject', {'kind': 'cloud_cover', 'seconds': 30}))
RUSH_HOLD = 30     # seconds the dock keeps its target after a rush, until the incident shows up in the metrics
DOCK_TARGETS = ('tx-001', 'tx-002', 'tx-003', 'tx-004', 'tx-005')


def busy(transformer):
    return bool(transformer.get('incident')) or transformer.get('band') == 'critical'


def rush_target(transformers, default=DOCK_TARGETS[2], current=None):
    """The managed transformer a 6 PM EV rush should hit: the least loaded one that has no incident right now.

    A `current` target that is still quiet is kept, so the dock only retargets when its transformer becomes busy.
    """
    heroes = {n: t for n, t in transformers.items() if t.get('managed')}
    quiet = {n: t for n, t in heroes.items() if not busy(t)}
    if current in quiet:
        return current
    pool = quiet or heroes
    if not pool:
        return default
    return min(pool, key=lambda n: (pool[n].get('utilization') if pool[n].get('utilization') is not None else 1e9, n))


def explain(exc):
    """Short, safe reason for a failed control call (never the internal URL)."""
    response = getattr(exc, 'response', None)
    if response is not None:
        try:
            detail = response.json().get('error')
        except (ValueError, AttributeError):
            detail = None
        return f'the fleet refused it ({response.status_code}): {str(detail)[:120]}' if detail else f'the fleet answered {response.status_code}'
    if isinstance(exc, RuntimeError):
        return str(exc)
    return 'the fleet control port is not reachable'


TAB_TX = 'Transformers (25)'
SAGA = ('Overload', 'Plan + outbox', 'Dispatched (CDC)', 'Acks', 'Reduction confirmed', 'Recovery monitoring', 'Restored')
SAGA_STEP = dict(PLAN_CREATED=1, COMMANDS_DISPATCHED=2, ACKS_PENDING=3, REDUCTION_CONFIRMED=4, RECOVERY_MONITORING=5, RESTORE_DISPATCHED=6, COMPLETED=6,
                 PARTIALLY_APPLIED=3, TIMED_OUT=3)
SAGA_SHOW_DONE = 120      # seconds a finished action stays on the stepper


def saga_action(actions, tx, now=None):
    """The action the stepper shows for `tx`: its newest one while it is in flight or held, or just finished."""
    for a in actions:                                # newest first
        if a.get('transformer_id') != tx:
            continue
        if a.get('status') != 'COMPLETED':
            return a
        done = epoch(a.get('transition_time'))
        now = datetime.now(timezone.utc).timestamp() if now is None else now
        return a if done is not None and now - done <= SAGA_SHOW_DONE else None
    return None


def saga_steps(action, trigger_pct=None, name=''):
    """The closed control loop for one transformer as a strip of seven steps (HTML, every dynamic value escaped).

    Overload, plan + outbox, dispatched (CDC), acks x/y, reduction confirmed, recovery monitoring, restored. A poll that
    skipped states still shows every earlier step as done. A safe-hold status marks its step red with "safe hold".
    """
    status = action.get('status') if action else None
    tx = escape(str((action or {}).get('transformer_id') or name or ''))
    if action is None or (status not in SAGA_STEP and status not in HELD_STATES):
        idle = f'status {escape(str(status))}' if action else f'Monitoring {tx}: no active incident, the saga starts at 95 % of capacity.'
        steps = ''.join(f'<div class="cr-step todo" role="listitem">{escape(label)}</div>' for label in SAGA)
        return (f'<div class="cr-saga" role="list" aria-label="Control saga for {tx}">{steps}</div>'
                f'<div class="cr-note">{idle}</div>')
    selected, acked = int(action.get('selected_device_count') or 0), int(action.get('acknowledged_device_count') or 0)
    confirmed = action.get('confirmed_reduction_kw') or 0
    held = status in HELD_STATES
    reason = str(action.get('reason') or '')
    if status == 'NEEDS_OPERATOR_REVIEW':
        not_dispatched = selected == 0 or reason.startswith('insufficient flexible capacity')
        at = 1 if not_dispatched else 3 if acked < selected else 4
    else:
        at = SAGA_STEP[status]
    # A restore that follows a held plan never confirmed a reduction: steps without evidence are skipped, not ticked.
    skipped = set()
    if status in ('RESTORE_DISPATCHED', 'COMPLETED'):
        reduction_confirmed = selected > 0 and acked >= selected and confirmed > 0
        skipped = {i for i, seen in ((2, selected > 0), (3, acked > 0), (4, reduction_confirmed),
                                    (5, reduction_confirmed and not reason.startswith('approved compensation'))) if not seen}
    cells = []
    for i, label in enumerate(SAGA):
        state = ('skip' if i in skipped else 'done' if i < at or (i == at and status == 'COMPLETED') else
                 'todo' if i > at else 'hold' if held else 'active')
        text = escape(label)
        if i == 0 and trigger_pct is not None:
            text = f'{text} at {fmt(trigger_pct)} %'
        elif i == 3 and state not in ('todo', 'skip'):
            text = f'Acks {acked}/{selected}'
        elif i == 4 and state not in ('todo', 'skip') and confirmed:
            text = f'{text} ({fmt(confirmed)} kW)'
        elif i == 6 and status == 'RESTORE_DISPATCHED':
            text = 'Restoring'
        mark = '✓ ' if state == 'done' else ''
        sub = '<span class="sub">safe hold</span>' if state == 'hold' else '<span class="sub">skipped</span>' if state == 'skip' else ''
        current = ' aria-current="step"' if state in ('active', 'hold') else ''
        cells.append(f'<div class="cr-step {state}" role="listitem"{current}>{mark}{text}{sub}</div>')
    epoch_no = action.get('leader_epoch')
    fence = f' · leader epoch {int(epoch_no)}' if isinstance(epoch_no, int) and not isinstance(epoch_no, bool) else ''
    if held:
        note = f'{tx} is in a safe hold: no further commands until the plan is resolved{fence}.'
    elif 4 in skipped:
        restore = 'restoring' if status == 'RESTORE_DISPATCHED' else 'restored'
        note = f'{tx}: {restore} without a confirmed reduction (compensation){fence}.'
    else:
        note = f'{tx}: {escape(SHORT.get(status, status.replace("_", " ").lower()))}, {fmt(confirmed)} of {fmt(action.get("target_reduction_kw"))} kW confirmed{fence}.'
    return f'<div class="cr-saga" role="list" aria-label="Control saga for {tx}">{"".join(cells)}</div><div class="cr-note">{note}</div>'


def flow_html(summary, scalars, active):
    """Pipeline strip: devices, Kafka, Flink, Action, Outbox/CDC, fenced gateway, chargers, with the return rail."""
    def stage(title, number, detail):
        return f'<div class="cr-stage"><div class="t">{title}</div><div class="n">{number}</div><div class="d">{detail}</div></div>'
    sc = scalars
    stages = [
        stage('Devices', fmt(sc.get('devices')), 'meters, chargers, solar'),
        stage('Kafka', f'{fmt(sc.get("kafka_in"))}/s', '3 partitions · ' + ('–' if sc.get('telemetry_total') is None else f'{sc["telemetry_total"] / 1e6:,.2f} M') + ' events ingested'),
        stage('Flink windows', f'{fmt(sc.get("kafka_out"), 1)}/s', f'5 s windows, {fmt(sc.get("flink_busy"))}% busy'),
        stage('Action service', f'{active} active', f'{summary["counts"]["critical"]} critical now'),
        stage('Outbox / CDC', f'{active} in flight', 'saga row, Debezium, command topic'),
        stage('Fenced gateway', 'epoch-fenced', f'{count(sc.get("commands"), "command")} applied in 10 min'),
        stage('Chargers', f'{fmt(sc.get("controllable"))} managed', 'throttle, ack, restore'),
    ]
    rail = '<div class="cr-rail"><span>acks + lower measured kW re-enter as telemetry · loop ≈ 20 s</span></div>'
    return '<div class="cr-flow">' + '<div class="cr-arrow"></div>'.join(stages) + rail + '</div>'


TAB_OPS, TAB_PIPE, TAB_DEV = 'Grid operations', 'Pipeline health', 'Devices (1,000)'


def column(name, label, sortable=True, align='left'):
    return dict(name=name, label=label, field=name, sortable=sortable, align=align)


TX_COLUMNS = [column('id', 'ID'), column('band', 'Band'), column('utilization', 'Utilization %', align='right'), column('net', 'Net kW', align='right'),
              column('ev', 'EV kW', align='right'), column('meter', 'Meter kW', align='right'), column('solar', 'Solar kW', align='right'),
              column('capacity', 'Capacity kW', align='right'), column('managed', 'Managed'), column('saga', 'Saga status'),
              column('throttled', 'Throttled', align='right'), column('coverage', 'Coverage %', align='right')]
INVENTORY_COLUMNS = [column('id', 'Transformer'), column('good', 'Good', align='right'), column('estimated', 'Estimated', align='right'),
                     column('missing', 'Missing', align='right'), column('late', 'Late', align='right'), column('ev', 'EV kW', align='right'),
                     column('meter', 'Meter kW', align='right'), column('solar', 'Solar kW', align='right')]
CHARGER_COLUMNS = [column('id', 'Device ID'), column('tx', 'Transformer'), column('priority', 'Priority', align='right'), column('departure', 'Departure (UTC)'),
                   column('rate', 'Rate (now / max kW)', sortable=False), column('minimum', 'Min kW', align='right'),
                   column('last', 'Last command (outcome, applied kW, time)', sortable=False), column('state', 'State'), column('freshness', 'Freshness')]
BAND_ALL, STATE_ALL = 'All bands', 'All states'


def rounded(value, digits=1):
    return None if value is None else round(value, digits)


def transformer_rows(transformers, actions, query='', band='All bands'):
    """One table row per transformer, in id order, after the search text and band filter."""
    in_flight, held = charts.loop_state(actions)
    throttled = throttled_by_transformer(actions)
    rows = []
    for name in sorted(transformers):
        t = transformers[name]
        if band not in (BAND_ALL, None) and t.get('band') != band:
            continue
        if query and query.strip().lower() not in name.lower():
            continue
        seen = (t.get('good') or 0) + (t.get('estimated') or 0)
        expected = seen + (t.get('missing') or 0)
        saga = ('safe hold' if name in held else SHORT.get(in_flight[name].get('status'), 'active') if name in in_flight
                else 'idle' if t.get('managed') else '–')
        rows.append(dict(id=name, band=t.get('band') or 'unknown', utilization=rounded(t.get('utilization')), net=rounded(t.get('net')),
                         ev=rounded(t.get('ev')), meter=rounded(t.get('meter')), solar=rounded(t.get('solar')), capacity=rounded(t.get('capacity'), 0),
                         managed='Managed' if t.get('managed') else 'Monitor-only', saga=saga, throttled=throttled.get(name, 0) if t.get('managed') else None,
                         coverage=rounded(seen / expected * 100) if expected else None))
    return rows


def inventory_rows(transformers):
    return [dict(id=n, good=t.get('good'), estimated=t.get('estimated'), missing=t.get('missing'), late=t.get('late'),
                 ev=rounded(t.get('ev')), meter=rounded(t.get('meter')), solar=rounded(t.get('solar'))) for n, t in sorted(transformers.items())]


def charger_rows(chargers, actions, transformer='All', priority='All', state=STATE_ALL, query=''):
    """Table rows of the managed chargers after the filters. State comes from the live throttles of `actions`."""
    throttling, settled = throttling_actions(actions), throttling_actions(actions, SETTLED)
    rows = []
    for c in chargers:
        kind = charger_state(c, throttling, settled)
        if transformer != 'All' and c['tx'] != transformer or priority != 'All' and str(c['priority']) != str(priority):
            continue
        if state != STATE_ALL and kind != state or query and query.strip().lower() not in c['id'].lower():
            continue
        last = c['last']
        rows.append(dict(
            id=c['id'], tx=c['tx'], priority=c['priority'], departure=str(c['departure'])[11:16] if isinstance(c['departure'], str) and len(c['departure']) >= 16 else '–',
            rate=f'{fmt(c["rate"], 1)} / {fmt(c["maximum"], 1)}', minimum=rounded(c['minimum']), state=kind, freshness=c['freshness'].title(),
            last='–' if not last else f'{last["outcome"] or "?"} · {fmt(last["applied"], 1)} kW · {str(last["at"])[11:19]}'))
    return rows


def charger_counts(chargers, actions):
    throttling, settled = throttling_actions(actions), throttling_actions(actions, SETTLED)
    counts = dict(Throttled=0, Spared=0, Normal=0)
    for c in chargers:
        counts[charger_state(c, throttling, settled)] += 1
    return counts


def history_note(source, fetched, errors):
    """The small caption above a detail chart: where its series came from."""
    if source == 'loads':
        return ''
    if source == 'offered':
        return 'Prometheus history · Without throttling uses fleet offered demand'
    if source == 'monitor-only':
        return 'Prometheus history (monitor-only)'
    if not fetched:
        return 'History unavailable' if 'loads' in errors else 'Loading history…'
    return 'Prometheus history (loads API unavailable)' if 'loads' in errors else 'Prometheus history (loads API behind)'


def rush_request(transformer):
    return '/overload', {'transformer': transformer, 'seconds': 90}


def changed(store, key, option):
    """True when `option` differs from the last one stored under `key`. Lets a chart skip identical updates."""
    signature = json.dumps(option, sort_keys=True, allow_nan=False)
    if store.get(key) == signature:
        return False
    store[key] = signature
    return True


def throttled_by_transformer(actions):
    held = {}
    for a in actions:
        if a.get('status') in COUNTED:
            held[a['transformer_id']] = held.get(a['transformer_id'], 0) + int(a.get('acknowledged_device_count') or 0)
    return held


def register():
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
        ui.colors(primary='#94a3b8')
        state = {'selected': None, 'pinned': False, 'message': '', 'requested': None, 'reduced': False, 'compact': False, 'tab': TAB_OPS, 'tx_detail': None, 'signatures': {}, 'rush_manual': False, 'rush_lock': None}
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
                for key, title in KPI_ORDER:
                    with ui.element('div').classes('cr-kpi').props(f'id=kpi-{key}'):
                        H(f'<div class="k">{title}</div>')
                        kpis[key] = dict(value=H(''), sub=H('').classes('s'))
                        if key == 'load':
                            kpis[key]['spark'] = ui.echart(charts.spark([], 1, '#94a3b8')).classes('w-full').style('height:16px')

            with ui.tabs().props('dense align=left no-caps').classes('w-full') as tabs:
                for name in (TAB_OPS, TAB_TX, TAB_DEV, TAB_PIPE):
                    ui.tab(name)
            with ui.tab_panels(tabs, value=TAB_OPS, keep_alive=True).classes('w-full'):
                with ui.tab_panel(TAB_OPS):
                    with ui.element('div').classes('cr-stage-grid'):
                        with ui.element('div').classes('cr-col'):
                            with ui.element('div').classes('cr-card'):
                                with ui.row().classes('items-center justify-between w-full no-wrap'):
                                    H('<h3>Closed-loop topology</h3>')
                                    H('<span class="muted cr-hint">click a transformer · <b style="color:#22d3ee">telemetry</b> · <b style="color:#fbbf24">commands</b> · <b style="color:#34d399">acks</b></span>')
                                    all_link = ui.button('View all 25 transformers →', on_click=lambda: tabs.set_value(TAB_TX)).props('flat dense no-caps color=info').classes('text-sm cr-link')
                                charts_['topology'] = ui.echart(charts.topology({}), on_point_click=lambda e: pick(e)).classes('w-full cr-topo')
                            with ui.element('div').classes('cr-card'):
                                saga = H('')
                            with ui.element('div').classes('cr-card'):
                                with ui.row().classes('items-center justify-between w-full'):
                                    selector = ui.select([], label='Transformer', on_change=lambda e: on_select(e)).props('dense dark outlined').classes('w-40')
                                    loading = H('').classes('muted').style('font-size:13px')
                                    ui.button('Follow incidents', icon='bolt', on_click=lambda: follow()).props('flat dense')
                                charts_['detail'] = ui.echart(charts.transformer_detail([], [], '')).classes('w-full cr-detail')
                        with ui.element('div').classes('cr-col'):
                            with ui.element('div').classes('cr-card'):
                                with ui.row().classes('items-center justify-between w-full no-wrap'):
                                    H('<h3>Decision log</h3>')
                                    behind = H('')
                                feed = H('').classes('cr-log')
                            with ui.element('div').classes('cr-card'):
                                H('<h3>Fleet load and capacity, kW</h3>')
                                charts_['load'] = ui.echart(charts.fleet_load([])).classes('w-full cr-fleet')
                with ui.tab_panel(TAB_TX):
                    with ui.element('div').classes('cr-card'):
                        with ui.row().classes('items-center gap-3 w-full'):
                            H('<h3 style="margin:0">All transformers</h3>')
                            tx_query = ui.input(placeholder='Search transformer', on_change=lambda: refresh()).props('dense dark outlined clearable aria-label="Search transformers"').classes('w-56')
                            tx_band = ui.select([BAND_ALL, 'critical', 'warning', 'normal'], value=BAND_ALL, on_change=lambda: refresh()).props('dense dark outlined aria-label="Band filter"').classes('w-40')
                            H('<span class="muted" style="font-size:12px">click a row for its history, demand breakdown and decisions</span>')
                        tx_table = ui.table(columns=TX_COLUMNS, rows=[], row_key='id', pagination=10).props('dark flat dense').classes('w-full')
                        tx_table.add_slot('body-cell-band', '<q-td :props="props" :class="\'band-\' + props.value">{{ props.value }}</q-td>')
                        tx_table.on('rowClick', lambda e: open_transformer(e))
                    with ui.element('div').classes('cr-card').props('id=tx-panel') as tx_panel:
                        with ui.row().classes('items-center justify-between w-full'):
                            tx_title = H('')
                            tx_loading = H('').classes('muted').style('font-size:13px')
                            ui.button('Close', icon='close', on_click=lambda: close_transformer()).props('flat dense')
                        with ui.element('div').classes('cr-tx-grid'):
                            with ui.element('div'):
                                H('<h3>Demand breakdown</h3>')
                                charts_['tx_break'] = ui.echart(charts.fleet_load([])).classes('w-full cr-tx-chart')
                            with ui.element('div') as tx_middle:
                                H('<h3>Managed vs. without throttling</h3>')
                                charts_['tx_detail'] = ui.echart(charts.transformer_detail([], [], '')).classes('w-full cr-tx-chart')
                            with ui.element('div'):
                                H('<h3>Decision log</h3>')
                                tx_feed = H('').classes('cr-log')
                    tx_panel.set_visibility(False)
                with ui.tab_panel(TAB_DEV):
                    with ui.element('div').classes('cr-card'):
                        H('<h3>Fleet inventory</h3>')
                        inventory_note = H('').classes('muted').style('font-size:13px')
                        inventory_table = ui.table(columns=INVENTORY_COLUMNS, rows=[], row_key='id', pagination=5).props('dark flat dense').classes('w-full')
                    with ui.element('div').classes('cr-card'):
                        with ui.row().classes('items-center justify-between w-full'):
                            H('<h3 style="margin:0">Managed EV chargers</h3>')
                            charger_counts_html = H('').style('font-size:13px')
                        with ui.row().classes('items-center gap-3 w-full'):
                            dev_tx = ui.select(['All'], value='All', label='Transformer', on_change=lambda: refresh()).props('dense dark outlined').classes('w-36')
                            dev_priority = ui.select(['All'], value='All', label='Priority', on_change=lambda: refresh()).props('dense dark outlined').classes('w-32')
                            dev_state = ui.select([STATE_ALL, 'Throttled', 'Spared', 'Normal'], value=STATE_ALL, label='State', on_change=lambda: refresh()).props('dense dark outlined').classes('w-40')
                            dev_query = ui.input(placeholder='Search device', on_change=lambda: refresh()).props('dense dark outlined clearable aria-label="Search devices"').classes('w-48')
                        charger_table = ui.table(columns=CHARGER_COLUMNS, rows=[], row_key='id', pagination=10).props('dark flat dense').classes('w-full')
                        charger_table.add_slot('body-cell-state', '<q-td :props="props" :class="\'st-\' + props.value">{{ props.value }}</q-td>')
                        charger_note = H('').classes('muted').style('font-size:12px;margin-top:6px')
                with ui.tab_panel(TAB_PIPE):
                    with ui.row().classes('items-center gap-4'):
                        H('<span class="muted" style="font-size:13px">Deep dives in Grafana:</span>')
                        ui.link('Pipeline dashboard', grafana.rstrip('/') + '/d/grid-pipeline', new_tab=True)
                        ui.link('Kafka dashboard', grafana.rstrip('/') + '/d/grid-kafka', new_tab=True)
                    flow = H('').classes('w-full')
                    with ui.element('div').classes('cr-row cr-r4'):
                        with ui.element('div').classes('cr-card'):
                            H('<h3>Correctness, last 5 minutes</h3>')
                            correctness = H('')
                            injected = H('').classes('muted').style('font-size:13px;margin-top:10px')
                        with ui.element('div').classes('cr-card'):
                            H('<h3>Rolling retention and storage</h3>')
                            charts_['volumes'] = ui.echart(charts.volumes({})).classes('w-full').style('height:130px')
                            retention = H('').classes('muted').style('font-size:13px')
                            partitions = H('').classes('muted').style('font-size:12px;margin-top:8px')

                    with ui.element('div').classes('cr-row cr-r3'):
                        with ui.element('div').classes('cr-card'):
                            H('<h3>Pipeline throughput, events/s</h3>')
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
        with ui.footer().classes('cr-dock items-center gap-3 px-5 py-2'):
            H('<span class="muted" style="font-size:12px;letter-spacing:.09em;text-transform:uppercase;font-weight:700">Chaos dock</span>')
            target = ui.select(list(DOCK_TARGETS), value=DOCK_TARGETS[2]).props('dense dark outlined options-dense aria-label="Rush transformer"').classes('w-28')
            target.on('update:model-value', lambda: state.update(rush_manual=True))      # only a person's choice stops the automatic target
            ui.button(on_click=lambda: rush(target.value), icon='bolt'
                      ).bind_text_from(target, 'value', backward=lambda v: f'6 PM EV rush ({v})').props('color=warning text-color=grey-10 dense no-caps').classes('font-semibold')
            for label, path, body in DOCK:
                button = ui.button(label, on_click=lambda p=path, b=body, l=label: send(p, b, l + ' for 30 s')).props('outline dense no-caps')
                if body['kind'] == 'cloud_cover':
                    cloud_cover = button
                    cloud_cover.set_visibility(False)
            message = ui.label('').classes('muted text-sm').props('aria-live=polite')

        async def ensure(tx):
            """Fetch the load history of `tx` now (shared, rate limited in FleetData) and redraw if it is still shown."""
            state['requested'] = tx
            if not data.loadable(tx):
                return                     # monitor-only transformer: no /loads, Prometheus history only
            await data.fetch_loads(tx)
            if state['selected'] == tx:
                refresh()

        async def choose(tx, pinned=True):
            state.update(selected=tx, pinned=pinned)
            data.select(tx)
            refresh()                      # shows "Loading history…" until the fetch returns
            await ensure(tx)

        async def pick(event):
            name = (getattr(event, 'name', None) or '').removesuffix(' chargers')
            if name in data.tx:
                await choose(name)

        async def on_select(event):
            if event.value and event.value != state['selected']:      # programmatic updates already match the state
                await choose(event.value)

        async def follow():
            state['pinned'] = False
            refresh()
            if state['selected']:
                await ensure(state['selected'])

        async def send(path, body, label, hint=''):
            """Call a fleet control. Always answers on screen: the status line, plus a toast that is hard to miss."""
            try:
                await data.control(path, body)
            except Exception as exc:
                reason = explain(exc)
                message.set_text(f'Not sent: {reason}')
                ui.notify(reason, type='negative', position='bottom-right')
                return False
            message.set_text(f'Sent: {label}.')
            ui.notify(f'{label} accepted. {hint}'.strip(), type='positive', position='bottom-right')
            return True

        async def rush(tx):
            """6 PM EV rush: force an overload, then show that transformer so its response is visible."""
            path, body = rush_request(tx)
            if await send(path, body, f'6 PM EV rush on {tx} for 90 s', 'The grid reacts in about 10 to 15 s.'):
                state['rush_lock'] = (tx, data.clock())
                await choose(tx)

        async def on_tab(event):
            state['tab'] = event.value
            if event.value == TAB_DEV:
                data.watch_devices()
                await data.poll_devices(force=True)
            refresh()

        async def open_transformer(event):
            row = event.args[1] if isinstance(event.args, list) and len(event.args) > 1 and isinstance(event.args[1], dict) else {}
            name = row.get('id')
            if name in data.tx:
                state['tx_detail'] = name
                data.select(name)
                refresh()
                ui.run_javascript("setTimeout(() => document.getElementById('tx-panel')?.scrollIntoView({behavior: 'smooth', block: 'nearest'}), 150)")
                if data.loadable(name):
                    await data.fetch_loads(name)
                    refresh()

        def close_transformer():
            state['tx_detail'] = None
            refresh()

        def refresh_transformers():
            rows = transformer_rows(data.tx, data.actions, tx_query.value or '', tx_band.value)
            if changed(state['signatures'], 'tx_rows', rows):
                tx_table.update_rows(rows, clear_selection=False)
            name = state['tx_detail']
            tx_panel.set_visibility(bool(name))
            if not name:
                return
            data.select(name)
            t = data.tx.get(name, {})
            tx_title.set_content(f'<h3 style="margin:0">{escape(name)} · {escape(str(t.get("band", "unknown")))} · {fmt(t.get("utilization"))} % of {fmt(t.get("capacity"))} kW</h3>')
            breakdown = charts.fleet_load(list(data.tx_history.get(name, [])))
            detail_rows, source = data.history(name)
            tx_loading.set_content(history_note(source, name in data.loads, data.errors))
            for key, option in (('tx_break', breakdown), ('tx_detail', charts.transformer_detail(detail_rows, data.actions, name, managed=data.loadable(name)))):
                if changed(state['signatures'], key, option):
                    charts_[key].options.clear(); charts_[key].options.update(option); charts_[key].update()
            tx_middle.set_visibility(data.loadable(name))      # a monitor-only transformer cannot be throttled: no managed-vs-shadow chart
            tx_feed.set_content(decision_log([a for a in data.actions if a.get('transformer_id') == name], data.trigger_pct) if data.loadable(name)
                                else '<div class="muted">Monitor-only: no actions are planned for this transformer.</div>')

        def refresh_devices():
            data.watch_devices()           # renewed on every tick: the charger list is only fetched while this view is open
            sc = data.scalars
            monitored = None if sc.get('devices') is None or sc.get('controllable') is None else sc['devices'] - sc['controllable']
            inventory_note.set_content(f'{fmt(monitored)} monitor-only devices are aggregated per transformer; {fmt(sc.get("controllable"))} controllable EV chargers are tracked individually below.')
            rows = inventory_rows(data.tx)
            if changed(state['signatures'], 'inventory', rows):
                inventory_table.update_rows(rows, clear_selection=False)
            for select, wanted in ((dev_tx, ['All'] + sorted({c['tx'] for c in data.chargers})),
                                   (dev_priority, ['All'] + [str(p) for p in sorted({c['priority'] for c in data.chargers if c['priority'] is not None})])):
                if list(select.options) != wanted:
                    select.set_options(wanted, value=select.value if select.value in wanted else 'All')
            counts = charger_counts(data.chargers, data.actions)
            charger_counts_html.set_content(' · '.join(f'{k} <b>{v}</b>' for k, v in counts.items()) + f' · {len(data.chargers)} chargers' +
                                            ('' if 'devices' not in data.errors else ' · <span style="color:#f87171">list unavailable</span>'))
            rows = charger_rows(data.chargers, data.actions, dev_tx.value, dev_priority.value, dev_state.value, dev_query.value or '')
            if changed(state['signatures'], 'chargers', rows):
                charger_table.update_rows(rows, clear_selection=False)
            charger_note.set_content('<b>Spared</b>: the transformer is being throttled and this charger is online, charging and controllable, '
                                    'but received no throttle command, so its priority and departure time protected it. '
                                    f'Showing {len(rows)} of {len(data.chargers)}.')

        def refresh():
            cloud_cover.set_visibility(bool(data.unmanaged_history))
            s, sc = data.summary(), data.scalars
            totals, now = s['totals'], datetime.now(timezone.utc)
            age = data.age()
            fresh = age is not None and age < 15 and 'prometheus' not in data.errors
            api_down = 'api' in data.errors
            if api_down:
                status, color = 'ACTIONS API UNREACHABLE', '#fbbf24'
            elif fresh:
                status, color = 'LIVE', '#3ddc97'
            else:
                status, color = 'NO DATA', '#ff5d5d'
            connection.set_content(f'<span class="dot" style="background:{color}"></span>'
                                   f'{status} · {s["count"]} transformers · {fmt(sc.get("devices"))} devices')
            clock.set_text(now.strftime('%Y-%m-%d %H:%M:%S UTC'))
            hist, pipe = list(data.fleet_history), list(data.pipeline_history)

            active = [a for a in data.actions if a['status'] not in ('COMPLETED',)]
            for t in kpi_tiles(s, sc, data.actions, data.kpis(), api_down):
                kpis[t['key']]['value'].set_content(tile_value(t))
                kpis[t['key']]['sub'].set_content(t['sub'])
            kpis['load']['spark'].options.clear()
            kpis['load']['spark'].options.update(charts.spark(hist, 1, '#94a3b8'))
            kpis['load']['spark'].update()

            flow.set_content(flow_html(s, sc, len(active)))
            charts_['load'].options.clear(); charts_['load'].options.update(charts.fleet_load(hist)); charts_['load'].update()
            names = sorted(data.tx)
            if not state['pinned']:
                incident = next((n for n in names if data.tx[n]['incident'] or data.tx[n]['band'] == 'critical'), None)
                state['selected'] = incident or state['selected'] or (names[0] if names else None)
            selected = state['selected']
            selector.set_options(names, value=selected)
            heroes = sorted(n for n in names if data.tx[n]['managed'])
            if heroes and list(target.options) != heroes:
                target.set_options(heroes, value=target.value if target.value in heroes else heroes[0])
            lock = state['rush_lock']       # (transformer, time): the button keeps naming the transformer it just hit
            if lock and not (lock[0] == target.value and (busy(data.tx.get(lock[0], {})) or data.clock() - lock[1] < RUSH_HOLD)):
                state['rush_lock'] = lock = None
            if not state['rush_manual'] and heroes and not lock:
                target.set_value(rush_target(data.tx, current=target.value))
            data.select(selected)      # renew on every tick: an idle tab keeps its transformer's /loads polling alive
            if selected and state['requested'] != selected:
                background_tasks.create(ensure(selected), name=f'loads-{selected}')
            topology = charts.topology(data.tx, throttled_by_transformer(data.actions), selected, motion=not state['reduced'], actions=data.actions, compact=state['compact'])
            if changed(state['signatures'], 'topology', topology):
                charts_['topology'].options.clear(); charts_['topology'].options.update(topology); charts_['topology'].update()
            rows, source = data.history(selected)
            loading.set_content(history_note(source, selected in data.loads, data.errors))
            detail = charts.transformer_detail(rows, data.actions, selected or '', compact=state['compact'], managed=data.loadable(selected))
            if changed(state['signatures'], 'detail', detail):
                charts_['detail'].options.clear(); charts_['detail'].options.update(detail); charts_['detail'].update()
            pending = saga_action(data.actions, selected)
            saga.set_content(saga_steps(pending, data.trigger_pct(pending) if pending else None, selected or ''))
            all_link.set_text(f'View all {len(data.tx) or 25} transformers →')
            feed.set_content(decision_log(data.actions, data.trigger_pct))
            behind.set_content('' if data.actions_freshness in (None, 'FRESH') else
                               f'<span class="cr-behind">feed {escape(str(data.actions_freshness).lower())}: projection behind</span>')
            if state['tab'] == TAB_TX:
                refresh_transformers()
            elif state['tab'] == TAB_DEV:
                refresh_devices()
            charts_['flow'].options.clear(); charts_['flow'].options.update(charts.throughput(pipe)); charts_['flow'].update()
            charts_['lag'].options.clear(); charts_['lag'].options.update(charts.lag_bars(data.lag)); charts_['lag'].update()
            charts_['busy'].options.clear(); charts_['busy'].options.update(charts.gauge(sc.get('flink_busy'), 'busy')); charts_['busy'].update()
            charts_['pressure'].options.clear(); charts_['pressure'].options.update(charts.gauge(sc.get('flink_backpressure'), 'blocked', warn=10, bad=30)); charts_['pressure'].update()
            flink_live = sc.get('flink_busy') is not None or sc.get('checkpoint_done') is not None
            dlq_count = 0 if sc.get('dlq') is None and flink_live else sc.get('dlq')
            wm_sec = None if sc.get('watermark_delay_ms') is None else sc['watermark_delay_ms'] / 1000
            correctness.set_content(
                '<table class="cr-feed" style="width:100%"><tr><th>Check</th><th>Count</th><th>What happened</th></tr>'
                f'<tr><td>Duplicates dropped</td><td>{fmt(sc.get("duplicates"))}</td><td class="muted">same event ID seen again, counted once</td></tr>'
                f'<tr><td>Late, accepted</td><td>{fmt(sc.get("accepted_late"))}</td><td class="muted">arrived after the window closed, window corrected</td></tr>'
                f'<tr><td>Window corrections</td><td>{fmt(sc.get("corrections"))}</td><td class="muted">new revision published, old one kept</td></tr>'
                f'<tr><td>Too late, audited</td><td>{fmt(sc.get("late"))}</td><td class="muted">beyond 30 s, written to the late topic</td></tr>'
                f'<tr><td>Rejected (DLQ)</td><td>{fmt(dlq_count)}</td><td class="muted">failed validation, kept for inspection</td></tr>'
                f'<tr><td>Watermark delay</td><td>{fmt(wm_sec, 1)} s</td><td class="muted">event-time lag behind greatest reading (10 s target)</td></tr>'
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
            p_ids = sorted(set(data.partition_rate) | set(data.partition_lag))
            p_text = ' · '.join(f'P{escape(str(p))}: {fmt(data.partition_rate.get(p), 1)}/s (lag {fmt(data.partition_lag.get(p))})' for p in p_ids)
            s_text = ' · '.join(f'S{escape(str(k))}: {fmt(data.subtask_rate[k], 1)}/s' for k in sorted(data.subtask_rate))
            partitions.set_content(('<b>Partitions (key = transformer_id):</b> ' + p_text if p_text else 'Partitions: –')
                                   + ('<br><b>Flink subtasks:</b> ' + s_text if s_text else ''))

        tabs.on_value_change(on_tab)
        try:
            await ui.context.client.connected()
            state['reduced'], state['compact'] = (bool(v) for v in await ui.run_javascript(
                '[matchMedia("(prefers-reduced-motion: reduce)").matches, matchMedia("(max-height: 760px)").matches]', timeout=3))
        except (TimeoutError, RuntimeError):
            pass
        ui.timer(3.0, refresh)
        ui.timer(.2, refresh, once=True)
