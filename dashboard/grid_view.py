"""Member_4 transformer flow and action evidence from existing v1 snapshots."""
from nicegui import ui

from dashboard.client import instant

from dashboard.grid_map import GROUPS, active_throttles, flow_diagram

SUCCESS = {'APPLIED', 'ALREADY_APPLIED'}


def observed_after(history, result, baseline, before=None):
    """Use later published measurements without treating command rates as telemetry."""
    if baseline is None:
        return None
    rate_down = result['applied_rate_kw'] < result['previous_rate_kw']
    after = instant(result['result_time'])
    candidates = sorted(history, key=lambda w: instant(w['window_start']))
    for window in candidates:
        if (instant(window['window_end']) <= after
                or (before is not None and instant(window['window_end']) >= before)
                or set(window['quality_flags']) - {'OLD_RESULT'}):
            continue
        change = window['ev_demand_kw'] - baseline['ev_demand_kw']
        if (rate_down and change < 0) or (not rate_down and change > 0):
            return window
    return None


def render_grid(t, devices, actions, history, live, kw, expansion, open_action, commands):
    load = t['load']
    action = next(iter(actions), None)
    # Retained results identify participating chargers. Candidate IDs are absent.
    results = sorted((r for d in devices if d['device']['device_type'] == 'EV_CHARGER'
                      for r in d['command_history'] if action and r['action_id'] == action['action_id']),
                     key=lambda r: (instant(r['result_time']), r['command_id']))
    commands = {c['command_id']: c for c in commands if action and c['action_id'] == action['action_id']}
    involved = {c['device_id'] for c in commands.values() if c['command_type'] == 'THROTTLE'}
    involved.update(r['device_id'] for r in results if r['outcome'] in SUCCESS and r['previous_rate_kw'] > r['applied_rate_kw'])
    with ui.card().classes('w-full gap-4').props('aria-label="Grid View"'):
        ui.label('Grid View').classes('text-xl font-bold').props('role=heading aria-level=2')
        label = 'Live reading' if live and t['freshness'] == 'FRESH' else 'Stale reading' if t['freshness'] == 'STALE' else 'Retained or incomplete reading'
        ui.label(f'{label} · {t["freshness"]} · window ends {load["window_end"]} UTC')
        ui.label(f'{t["transformer_id"]} · Net load {kw(load["net_load_kw"])} / capacity {kw(load["capacity_kw"])} · {t["state"]}').classes('font-bold')
        counts = {kind: sum(d['device']['device_type'] == kind for d in devices)
                  for _, kind, *_ in GROUPS}
        flow_diagram(t, kw, live, counts, throttles=active_throttles(actions, devices))
        ui.label('Arrows show flow direction. Thicker lines indicate larger published group totals.').classes('muted text-sm')
        for name, kind, _, _, _, _ in GROUPS:
            label = 'EV charger' if kind == 'EV_CHARGER' else name
            with expansion(f'{label} readings · {t["transformer_id"]}').classes('w-full'):
                for d in devices:
                    device = d['device']
                    if device['device_type'] != kind:
                        continue
                    device_id = device['device_id']
                    reading = d['latest_telemetry']
                    power = reading['power_kw'] if reading and reading['quality'] != 'INVALID' else None
                    marker = ' · Throttle participant' if device_id in involved else ''
                    direction = ' ← ' if power is not None and power < 0 else ' → ' if power else ' · '
                    with ui.row().classes('items-center gap-2'):
                        if power:
                            ui.label(direction.strip()).props('aria-label="Into transformer"' if power < 0 else 'aria-label="From transformer"')
                        row = ui.label(f'{device_id}{marker} · Measured draw {kw(power)} · {d["freshness"]}')
                        if marker:
                            row.classes('font-bold text-primary')
        ui.label('Historical action timeline · retained evidence').props('role=heading aria-level=3').classes('font-bold')
        if not action:
            ui.label('No retained action for this transformer.')
            return
        ui.button(f'Open action {action["action_id"]}', on_click=lambda: open_action(action)).props('outline')
        ui.label(f'Action state · {action["status"]} · {action["transition_time"]} UTC')
        timeline = []
        for c in commands.values():
            kind = c['command_type'].title()
            timeline.append((c['issued_at'], f'{kind} command sent · {c["device_id"]} · Requested charging rate {kw(c["target_rate_kw"])}'))
        if not commands and action['status'] == 'COMMANDS_DISPATCHED':
            ui.label(f'Command sent · dispatch reported {action["transition_time"]} UTC')
        elif not commands:
            ui.label('Command sent · exact dispatch time unavailable in this action snapshot')
        ui.label('Measured changes compare published EV group windows. A window can include readings from before and after an acknowledgement.').classes('muted text-sm')
        if not commands:
            ui.label('Requested charging rate · Unknown. Applied rate and measured draw are shown separately.')
        ui.label(f'Confirmed EV reduction reported by Action Service · {kw(action["confirmed_reduction_kw"])}')
        by_window = {}
        for window in sorted(history, key=lambda w: w['revision']):
            by_window[window['window_id']] = window
        baseline = next((w for w in history if w['window_id'] == action['trigger_window_id']
                         and w['revision'] == action['trigger_revision']), None)
        if baseline:
            timeline.append((baseline['window_end'], f'Overload trigger window · net load {kw(baseline["net_load_kw"])} · EV draw {kw(baseline["ev_demand_kw"])}'))
        restore_times = [instant(c['issued_at']) for c in commands.values() if c['command_type'] == 'RESTORE']
        if not restore_times:
            restore_times = [instant(r['result_time']) for r in results
                             if r['outcome'] in SUCCESS and r['applied_rate_kw'] > r['previous_rate_kw']]
        seen = set()
        for r in results:
            if r['outcome'] in SUCCESS and r['command_id'] in seen:
                continue
            if r['outcome'] in SUCCESS:
                seen.add(r['command_id'])
            previous, applied = r['previous_rate_kw'], r['applied_rate_kw']
            changing = r['outcome'] in SUCCESS and previous is not None and applied is not None and applied != previous
            command = commands.get(r['command_id'])
            label = 'Command result'
            if changing:
                kind = command['command_type'] if command else ('THROTTLE' if applied < previous else 'RESTORE')
                label = f'{kind.title()} acknowledgement'
            timeline.append((r['result_time'], f'{label} · {r["device_id"]} · {r["outcome"]} · Applied rate {kw(applied)}'))
            measurement = (observed_after(by_window.values(), r, baseline, min(restore_times, default=None))
                           if changing and applied < previous else None)
            if measurement:
                reduction = baseline['ev_demand_kw'] - measurement['ev_demand_kw']
                timeline.append((measurement['window_end'], f'Historical measured EV reduction · {kw(reduction)} · net load {kw(measurement["net_load_kw"])}'))
                baseline = measurement
            elif changing and applied > previous:
                restored = observed_after(by_window.values(), r, baseline)
                if restored:
                    timeline.append((restored['window_end'], f'Historical EV draw after restore · {kw(restored["ev_demand_kw"])} · net load {kw(restored["net_load_kw"])}'))
                else:
                    ui.label('Post-restore measured change unavailable in retained windows.')
            elif changing:
                ui.label('Post-throttle measured reduction unavailable in retained windows.')
        for at, label in sorted(timeline, key=lambda entry: instant(entry[0])):
            ui.label(f'{at} UTC · {label}')
