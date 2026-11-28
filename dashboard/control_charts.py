"""ECharts option builders for the control room. Pure functions: data in, JSON-serializable dict out."""
from datetime import datetime, timezone
from html import escape

from dashboard.fleet_data import ACTIVE_STATES, HELD_STATES, band as band_of

PALETTE = dict(bg='#18181b', line='#3f3f46', text='#e4e4e7', muted='#a1a1aa', cyan='#7dd3fc', green='#34d399',
               amber='#fbbf24', red='#f87171', blue='#64748b', violet='#a78bfa', sky='#38bdf8', grid='#27272a')
AXIS = dict(axisLine=dict(lineStyle=dict(color=PALETTE['line'])), axisLabel=dict(color=PALETTE['muted']),
            splitLine=dict(lineStyle=dict(color=PALETTE['grid'])))
WINDOW = 30 * 60   # seconds shown by the line charts


def recent(rows, seconds=WINDOW, now=None):
    if not rows:
        return []
    horizon = (now if now is not None else rows[-1][0]) - seconds
    return [r for r in rows if r[0] >= horizon]


def ms(t):
    return int(t * 1000)


def base(title=None):
    return dict(animation=False, useUTC=True, backgroundColor='transparent', textStyle=dict(color=PALETTE['text']),
                grid=dict(left=52, right=18, top=34 if title else 18, bottom=28),
                tooltip=dict(trigger='axis', backgroundColor='#09090b', borderColor=PALETTE['line'], textStyle=dict(color=PALETTE['text'])),
                legend=dict(textStyle=dict(color=PALETTE['muted']), top=0, right=8, itemWidth=12, itemHeight=8),
                aria=dict(enabled=True))


def time_axis():
    return dict(AXIS, type='time', axisLabel=dict(color=PALETTE['muted'], hideOverlap=True))


def fleet_load(history):
    """Stacked demand split with the net-load line and the capacity ceiling, in kW."""
    rows = recent(history)
    def column(i, sign=1):
        return [[ms(r[0]), None if r[i] is None else round(sign * r[i], 2)] for r in rows]
    option = base('Fleet demand, kW')
    option['grid']['top'] = 56         # the five-entry legend wraps onto two rows in the right rail
    option['legend'].update(left='center', right=None, top=0)
    option.update(xAxis=time_axis(), yAxis=dict(type='value', **AXIS),
        series=[
            dict(name='Households', type='line', stack='demand', areaStyle=dict(opacity=.55), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['blue']), data=column(3)),
            dict(name='EV charging', type='line', stack='demand', areaStyle=dict(opacity=.65), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['violet']), data=column(2)),
            dict(name='Solar generation', type='line', stack='solar', areaStyle=dict(opacity=.5), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['sky']), data=column(4, -1)),
            dict(name='Net load', type='line', showSymbol=False, lineStyle=dict(width=2, color=PALETTE['text']),
                 itemStyle=dict(color=PALETTE['text']), data=column(1)),
            dict(name='Capacity', type='line', showSymbol=False, lineStyle=dict(width=1.5, type='dashed', color=PALETTE['red']),
                 itemStyle=dict(color=PALETTE['red']), data=column(5)),
        ])
    return option


BAND_COLORS = dict(normal=PALETTE['green'], warning=PALETTE['amber'], critical=PALETTE['red'], unknown='#52525b')
STAGE_X = dict(source=11, transformer=37, charger=63, saga=89)
GRID = dict(left=8, right=8, top=4, bottom=4)      # pixels around the topology plot
Y_MAX = 108                                         # y runs 0..108: nodes use 6..94, column headings sit at 103
EXTRA_LOADED = 3   # most-loaded unmanaged transformers shown next to the managed ones
TELEMETRY = '#22d3ee'
COMMANDING = ('COMMANDS_DISPATCHED', 'ACKS_PENDING', 'RESTORE_DISPATCHED')   # commands are travelling to the chargers


def topology_nodes(transformers, extra=EXTRA_LOADED):
    """Transformers to draw (managed ones plus the most loaded others, in id order) and how many are left out."""
    managed = sorted(n for n, t in transformers.items() if t.get('managed'))
    loaded = sorted((n for n, t in transformers.items() if not t.get('managed') and t.get('utilization') is not None),
                    key=lambda n: -transformers[n]['utilization'])[:extra]
    shown = managed + sorted(loaded)
    return shown, len(transformers) - len(shown)


def loop_state(actions):
    """Where the control loop is active. `actions` is newest first. Per transformer only its newest action counts.

    Returns (in_flight, held): in_flight maps a transformer to its action (commands are on their way or applied),
    held is the set of transformers whose newest action stopped in a safe hold.
    """
    in_flight, held, seen = {}, set(), set()
    for a in actions:
        tx = a.get('transformer_id')
        if tx in seen or not isinstance(tx, str):
            continue
        seen.add(tx)
        if a.get('status') in ACTIVE_STATES:
            in_flight[tx] = a
        elif a.get('status') in HELD_STATES:
            held.add(tx)
    return in_flight, held


def topology(transformers, throttled=None, selected=None, motion=True, actions=None, compact=False):
    """The closed loop on one hidden cartesian grid (x and y fixed): source, transformers, chargers, control saga.

    Telemetry runs from every managed charger group to the saga (cyan). While an action is in flight, curved amber
    command lines run from the saga back to that transformer's chargers and green acknowledgment lines return once
    acks arrive. Feeders and every line series are silent so clicks reach the nodes, and the series count is fixed
    (empty `data` when idle) so a chart update merges instead of rebuilding. Critical transformers pulse unless
    `motion` is False. `throttled` maps a transformer to the number of chargers under a confirmed limit.
    """
    throttled = throttled or {}
    in_flight, held = loop_state(actions or [])
    height = 16 if compact else 20
    shown, _ = topology_nodes(transformers)
    rows = [t for t in transformers.values() if t.get('net') is not None]
    capacity = sum(t['capacity'] or 0 for t in rows)
    total = sum(t['net'] for t in rows) / capacity * 100 if capacity else None
    solar = sum(t.get('solar') or 0 for t in rows)
    source = [STAGE_X['source'], 50]
    saga = [STAGE_X['saga'], 50]
    nodes, pulses, feeders, telemetry, commands, acks = [], [], [], [], [], []
    headings = (('source', 'GRID + SOLAR'), ('transformer', 'TRANSFORMERS'), ('charger', 'EV CHARGERS'), ('saga', 'CONTROL SAGA'))
    for key, text in headings:
        nodes.append(dict(name=f'heading {text}', value=[STAGE_X[key], 103, None], symbolSize=0, silent=True,
                          label=dict(show=True, color=PALETTE['muted'], fontSize=9, fontWeight='bold', formatter=text), tooltip=dict(show=False)))
    nodes.append(dict(name='Grid + Solar', value=source + [None if total is None else round(total, 1)], symbolSize=[176, height + 8], symbol='roundRect',
                      itemStyle=dict(color=PALETTE['sky'], borderColor='#e4e4e7', borderWidth=2),
                      label=dict(show=True, color='#09090b', fontWeight='bold', fontSize=11,
                                 formatter=('Grid – ' if total is None else f'Grid {total:.0f}% ') + f'· Solar {solar:,.0f} kW' + (' (no output)' if solar < .5 else '')),
                      tooltip=dict(formatter='Substation load and fleet solar generation')))
    for i, name in enumerate(shown):
        t = transformers[name]
        y = 50 if len(shown) == 1 else 94 - i * 88 / (len(shown) - 1)
        pct = t.get('utilization')
        b = t.get('band') or band_of(pct)
        color = BAND_COLORS.get(b, BAND_COLORS['unknown'])
        point = [STAGE_X['transformer'], round(y, 2)]
        node = dict(name=name, value=point + [None if pct is None else round(pct, 1)], symbolSize=[84, height + 4] if name == selected else [76, height], symbol='roundRect',
                    itemStyle=dict(color=color, borderColor='#ffffff' if name == selected else '#27272a', borderWidth=3 if name == selected else 1),
                    label=dict(show=True, color='#09090b', fontWeight='bold', fontSize=11,
                               formatter=f'{name} ' + ('–' if pct is None else f'{pct:.0f}%')),
                    tooltip=dict(formatter=f'{escape(name)}: ' + ('no data' if pct is None else f'{pct:.0f}% of {t["capacity"]:.0f} kW')))
        (pulses if b == 'critical' and motion else nodes).append(node)
        feeders.append(dict(coords=[source, point], lineStyle=dict(color=color)))
        if t.get('managed'):
            count = throttled.get(name, 0)
            charger = [STAGE_X['charger'], round(y, 2)]
            nodes.append(dict(name=f'{name} chargers', value=charger + [count], symbolSize=[92, height], symbol='roundRect',
                              itemStyle=dict(color=PALETTE['amber'] if count else '#3f3f46', borderColor='#27272a', borderWidth=1),
                              label=dict(show=True, color='#09090b' if count else PALETTE['text'], fontSize=11, fontWeight='bold' if count else 'normal',
                                         formatter=f'{count} throttled' if count else 'chargers'),
                              tooltip=dict(formatter=f'{escape(name)} EV chargers: {count} throttled')))
            feeders.append(dict(coords=[point, charger], lineStyle=dict(color=PALETTE['amber'] if count else '#52525b')))
            telemetry.append(dict(coords=[charger, saga]))
            action = in_flight.get(name)
            if action:
                if action.get('status') in COMMANDING:
                    commands.append(dict(coords=[saga, charger]))
                if int(action.get('acknowledged_device_count') or 0) > 0:
                    acks.append(dict(coords=[charger, saga]))
    n = len(in_flight) + len(held)
    nodes.append(dict(name='Control saga', value=saga + [n], symbolSize=[92, 36], symbol='roundRect',
                      itemStyle=dict(color=PALETTE['red'] if held else PALETTE['amber'] if n else '#3f3f46', borderColor='#e4e4e7' if n else '#27272a', borderWidth=2 if n else 1),
                      label=dict(show=True, color='#09090b' if n else PALETTE['text'], fontWeight='bold', fontSize=11,
                                 formatter=f'Control saga\n{n} active' + (' · safe hold' if held else '')),
                      tooltip=dict(formatter='Control saga: plans, outbox, fenced commands and acks')))

    def lines(data, color, width, opacity, curve=0, effect=None):
        return dict(type='lines', coordinateSystem='cartesian2d', silent=True, z=1 if effect is None else 2,
                    effect=dict(show=bool(effect) and motion, period=2.5, trailLength=0, symbol='arrow', symbolSize=7, color=effect or color),
                    lineStyle=dict(color=color, width=width, opacity=opacity, curveness=curve), data=data)
    feeder_series = lines(feeders, '#52525b', 1.5, .55)
    feeder_series['effect'] = dict(show=motion, period=4, trailLength=0, symbolSize=4, color='#e4e4e7')
    return dict(animation=False, backgroundColor='transparent', aria=dict(enabled=True),
        tooltip=dict(trigger='item', backgroundColor='#09090b', borderColor=PALETTE['line'], textStyle=dict(color=PALETTE['text'])),
        grid=dict(GRID), xAxis=dict(type='value', min=0, max=100, show=False), yAxis=dict(type='value', min=0, max=Y_MAX, show=False),
        series=[feeder_series,
                lines(telemetry, TELEMETRY, 1, .5),
                lines(commands, PALETTE['amber'], 2, .9, .3, effect=PALETTE['amber']),
                lines(acks, PALETTE['green'], 2, .9, .3, effect=PALETTE['green']),
                dict(type='scatter', z=3, data=nodes),
                dict(type='effectScatter', z=4, rippleEffect=dict(scale=1.7, brushType='stroke', period=3), data=pulses)])


def transformer_detail(rows, actions, name, compact=False, managed=True):
    """Managed load against the shadow (what the load would be without throttling), in kW.

    rows are (time, managed kW, shadow kW or None, capacity kW or None). The critical line is 95 % of capacity.
    """
    rows = recent(rows)
    option = base(name)
    option['grid']['top'] = 46 if compact else 62      # room for the legend row and the marker labels
    option['title'] = dict(text=f'{name}  load and actions, kW' if managed else f'{name}  load, kW (monitor-only)', left=8, top=2, textStyle=dict(color=PALETTE['muted'], fontSize=12))
    option['legend'].update(top=2, right=8)
    marks = []
    colors = dict(COMMANDS_DISPATCHED=PALETTE['amber'], REDUCTION_CONFIRMED=PALETTE['green'], RESTORE_DISPATCHED=PALETTE['cyan'],
                  COMPLETED=PALETTE['violet'], NEEDS_OPERATOR_REVIEW=PALETTE['red'], PARTIALLY_APPLIED=PALETTE['red'])
    short = dict(COMMANDS_DISPATCHED='throttled', REDUCTION_CONFIRMED='confirmed', RESTORE_DISPATCHED='restoring',
                 COMPLETED='restored', NEEDS_OPERATOR_REVIEW='review', PARTIALLY_APPLIED='partial')
    horizon = rows[0][0] if rows else 0
    for action in sorted(actions, key=lambda a: str(a.get('transition_time'))):
        if not managed or action.get('transformer_id') != name or action.get('status') not in colors:
            continue
        at = _epoch(action['transition_time'])
        if at is not None and at >= horizon:
            marks.append(dict(xAxis=ms(at), name=short[action['status']], lineStyle=dict(color=colors[action['status']], type='dashed'),
                              label=dict(formatter=short[action['status']], color=colors[action['status']], fontSize=10,
                                         position='end', offset=[0, -14 * (len(marks) % 2)])))
    capacity = next((r[3] for r in reversed(rows) if r[3]), None)
    critical = [dict(yAxis=round(capacity * .95, 1), lineStyle=dict(color=PALETTE['red'], type='dotted'),
                     label=dict(formatter='critical 95%', color=PALETTE['red'], position='insideStartTop'))] if capacity else []
    option.update(xAxis=time_axis(), yAxis=dict(type='value', min=0, **AXIS),
        series=[dict(name='Managed load' if managed else 'Load', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['text']), lineStyle=dict(width=2.5),
                     areaStyle=dict(opacity=.08), data=[[ms(r[0]), round(r[1], 1)] for r in rows],
                     markLine=dict(symbol='none', silent=True, data=critical + marks)),
                dict(name='Without throttling', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['cyan']),
                     lineStyle=dict(width=1.5, type='dashed'), data=[[ms(r[0]), round(r[2], 1)] for r in rows if r[2] is not None])][:2 if managed else 1])
    return option


def _epoch(value):
    try:
        moment = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        return None
    return None if moment.tzinfo is None else moment.astimezone(timezone.utc).timestamp()


def throughput(history):
    rows = recent(history)
    def column(i):
        return [[ms(r[0]), None if r[i] is None else round(r[i], 1)] for r in rows]
    option = base('Events per second')
    option['grid']['top'] = 48        # the three-entry legend wraps onto a second row in a narrow card
    option['legend'].update(left='center', right=None)
    option.update(xAxis=time_axis(), yAxis=dict(type='value', min=0, **AXIS), series=[
        dict(name='Devices publish', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['blue']), data=column(1)),
        dict(name='Kafka ingest', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['cyan']), areaStyle=dict(opacity=.15), data=column(2)),
        dict(name='Load windows out', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['green']), data=column(3))])
    return option


def lag_bars(lag):
    names = {'integration-kind.action.control.v1': 'Action', 'integration-kind.projection': 'Projection',
             'integration-kind.projection-telemetry': 'Projection telemetry'}
    rows = [(names.get(g, 'Flink aggregation' if 'aggregation' in g else g), v) for g, v in sorted(lag.items())]
    option = base()
    option.update(grid=dict(left=136, right=40, top=8, bottom=22), tooltip=dict(trigger='item'),
        xAxis=dict(type='value', min=0, **dict(AXIS, axisLabel=dict(color=PALETTE['muted'], hideOverlap=True, formatter='{value}'))),
        yAxis=dict(type='category', data=[n for n, _ in rows], **AXIS),
        series=[dict(type='bar', barWidth='55%', data=[dict(value=round(v), itemStyle=dict(color=PALETTE['green'] if v < 500 else PALETTE['amber'] if v < 5000 else PALETTE['red'])) for _, v in rows],
                     label=dict(show=True, position='right', color=PALETTE['text']))])
    return option


def gauge(value, name, maximum=100, warn=70, bad=90):
    value = 0 if value is None else value
    return dict(animation=False, backgroundColor='transparent', series=[dict(type='gauge', min=0, max=maximum, radius='96%', startAngle=210, endAngle=-30,
        progress=dict(show=True, width=10), pointer=dict(show=False), axisLine=dict(lineStyle=dict(width=10, color=[[1, '#27272a']])),
        axisTick=dict(show=False), splitLine=dict(show=False), axisLabel=dict(show=False),
        itemStyle=dict(color=PALETTE['green'] if value < warn else PALETTE['amber'] if value < bad else PALETTE['red']),
        detail=dict(valueAnimation=False, offsetCenter=[0, '8%'], formatter='{value}%', color=PALETTE['text'], fontSize=22, fontWeight='bold'),
        title=dict(offsetCenter=[0, '62%'], color=PALETTE['muted'], fontSize=12), data=[dict(value=round(value), name=name)])])


def volumes(values):
    labels = {'data-0-grid-brokers-0': 'Kafka log', 'data-postgres-0': 'PostgreSQL', 'checkpoints': 'Flink checkpoints'}
    rows = sorted(((labels.get(k, k), v) for k, v in values.items() if k in labels), key=lambda r: r[1])
    option = base()
    option.update(grid=dict(left=110, right=40, top=8, bottom=22), tooltip=dict(trigger='item', formatter='{b}: {c}%'),
        xAxis=dict(type='value', min=0, max=100, **AXIS), yAxis=dict(type='category', data=[n for n, _ in rows], **AXIS),
        series=[dict(type='bar', barWidth='55%', label=dict(show=True, position='right', color=PALETTE['text'], formatter='{c}%'),
                     data=[dict(value=round(v, 1), itemStyle=dict(color=PALETTE['green'] if v < 60 else PALETTE['amber'] if v < 80 else PALETTE['red'])) for _, v in rows])])
    return option


def spark(rows, index, color):
    data = [[ms(r[0]), r[index]] for r in recent(rows, 600) if r[index] is not None]
    return dict(animation=False, useUTC=True, backgroundColor='transparent', grid=dict(left=0, right=0, top=2, bottom=2),
                xAxis=dict(type='time', show=False), yAxis=dict(type='value', show=False, scale=True),
                series=[dict(type='line', showSymbol=False, smooth=True, lineStyle=dict(width=1.5, color=color), itemStyle=dict(color=color),
                             areaStyle=dict(color=color, opacity=.18), data=data)])
