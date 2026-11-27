"""ECharts option builders for the control room. Pure functions: data in, JSON-serializable dict out."""
from datetime import datetime, timezone

PALETTE = dict(bg='#101c30', line='#233a5c', text='#dce8f8', muted='#8ea6c4', cyan='#2bd4ff', green='#3ddc97',
               amber='#ffb020', red='#ff5d5d', blue='#4c8dff', violet='#a78bfa', grid='#1b2c47')
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
    return dict(animation=False, backgroundColor='transparent', textStyle=dict(color=PALETTE['text']),
                grid=dict(left=52, right=18, top=34 if title else 18, bottom=28),
                tooltip=dict(trigger='axis', backgroundColor='#0b1424', borderColor=PALETTE['line'], textStyle=dict(color=PALETTE['text'])),
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
    option['title'] = dict(text='kW', left=8, top=2, textStyle=dict(color=PALETTE['muted'], fontSize=12))
    option.update(xAxis=time_axis(), yAxis=dict(type='value', **AXIS),
        series=[
            dict(name='Households', type='line', stack='demand', areaStyle=dict(opacity=.55), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['blue']), data=column(3)),
            dict(name='EV charging', type='line', stack='demand', areaStyle=dict(opacity=.65), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['amber']), data=column(2)),
            dict(name='Solar generation', type='line', stack='solar', areaStyle=dict(opacity=.5), showSymbol=False, lineStyle=dict(width=1),
                 itemStyle=dict(color=PALETTE['green']), data=column(4, -1)),
            dict(name='Net load', type='line', showSymbol=False, lineStyle=dict(width=2, color=PALETTE['text']),
                 itemStyle=dict(color=PALETTE['text']), data=column(1)),
            dict(name='Capacity', type='line', showSymbol=False, lineStyle=dict(width=1.5, type='dashed', color=PALETTE['red']),
                 itemStyle=dict(color=PALETTE['red']), data=column(5)),
        ])
    return option


def heat_map(transformers, selected=None, columns=5):
    """One tile per transformer, colored by utilization. Managed (incident-capable) tiles are outlined."""
    ids = sorted(transformers)
    rows = -(-max(len(ids), 1) // columns)
    data = []
    for i, name in enumerate(ids):
        t = transformers[name]
        value = None if t['utilization'] is None else round(t['utilization'], 1)
        item = dict(name=name, value=[i % columns, rows - 1 - i // columns, value],
                    label=dict(show=True, formatter=f'{name[3:]}\n{"–" if value is None else f"{value:.0f}%"}',
                               color='#ffffff', fontSize=12, fontWeight='bold'))
        edge = PALETTE['cyan'] if t.get('managed') else PALETTE['line']
        width = 4 if t.get('incident') else 3 if name == selected else 2 if t.get('managed') else 1
        if t.get('incident'):
            edge = '#ffffff'
        item['itemStyle'] = dict(borderColor=edge, borderWidth=width, borderRadius=4)
        data.append(item)
    return dict(animation=False, backgroundColor='transparent', aria=dict(enabled=True),
        tooltip=dict(formatter='{b}', backgroundColor='#0b1424', textStyle=dict(color=PALETTE['text'])),
        grid=dict(left=4, right=4, top=4, bottom=4),
        xAxis=dict(type='category', data=list(range(columns)), show=False, splitArea=dict(show=False)),
        yAxis=dict(type='category', data=list(range(rows)), show=False),
        visualMap=dict(type='piecewise', show=False, dimension=2, pieces=[
            dict(max=40, color='#1d6b53'), dict(min=40, max=60, color='#2a8f6a'), dict(min=60, max=80, color='#7c9a2e'),
            dict(min=80, max=95, color='#c98a14'), dict(min=95, color='#c8373f')], outOfRange=dict(color='#2a3a55')),
        series=[dict(type='heatmap', data=data, itemStyle=dict(borderColor='#0b1424', borderWidth=2), emphasis=dict(itemStyle=dict(borderColor='#ffffff')))])


def top_loaded(transformers, count=10):
    ranked = sorted(((n, t['utilization']) for n, t in transformers.items() if t['utilization'] is not None),
                    key=lambda r: r[1])[-count:]
    colors = [PALETTE['red'] if u >= 95 else PALETTE['amber'] if u >= 80 else PALETTE['green'] for _, u in ranked]
    option = base()
    option.update(grid=dict(left=58, right=36, top=10, bottom=22), tooltip=dict(trigger='item', formatter='{b}: {c}%'),
        xAxis=dict(type='value', min=0, max=120, **AXIS), yAxis=dict(type='category', data=[n for n, _ in ranked], **AXIS),
        series=[dict(type='bar', barWidth='62%', data=[dict(value=round(u, 1), itemStyle=dict(color=c)) for (_, u), c in zip(ranked, colors)],
                     label=dict(show=True, position='right', color=PALETTE['text'], formatter='{c}%'),
                     markLine=dict(symbol='none', silent=True, data=[
                         dict(xAxis=80, lineStyle=dict(color=PALETTE['amber'], type='dashed')),
                         dict(xAxis=95, lineStyle=dict(color=PALETTE['red'], type='dashed'))], label=dict(show=False)))])
    return option


def transformer_detail(history, actions, name):
    rows = recent(history)
    option = base(f'{name}: utilization and actions')
    option['title'] = dict(text=f'{name}  utilization and actions', left=8, top=2, textStyle=dict(color=PALETTE['muted'], fontSize=12))
    option['legend'].update(top=20, left=150, right='auto')
    marks = []
    colors = dict(COMMANDS_DISPATCHED=PALETTE['amber'], REDUCTION_CONFIRMED=PALETTE['green'], RESTORE_DISPATCHED=PALETTE['cyan'],
                  COMPLETED=PALETTE['violet'], NEEDS_OPERATOR_REVIEW=PALETTE['red'], PARTIALLY_APPLIED=PALETTE['red'])
    short = dict(COMMANDS_DISPATCHED='throttle sent', REDUCTION_CONFIRMED='reduction confirmed', RESTORE_DISPATCHED='restore sent',
                 COMPLETED='restored', NEEDS_OPERATOR_REVIEW='needs review', PARTIALLY_APPLIED='partial')
    horizon = rows[0][0] if rows else 0
    for action in actions:
        if action.get('transformer_id') != name or action.get('status') not in colors:
            continue
        stamp = action['transition_time'].replace('Z', '+00:00')
        at = datetime.fromisoformat(stamp).astimezone(timezone.utc).timestamp()
        if at >= horizon:
            marks.append(dict(xAxis=ms(at), name=short[action['status']], lineStyle=dict(color=colors[action['status']], type='dashed'),
                              label=dict(formatter=short[action['status']], color=colors[action['status']], fontSize=10, position='insideEndTop')))
    option.update(xAxis=time_axis(),
        yAxis=[dict(type='value', min=0, axisLabel=dict(color=PALETTE['muted'], formatter='{value}%'), **{k: v for k, v in AXIS.items() if k != 'axisLabel'}),
               dict(type='value', splitLine=dict(show=False), axisLabel=dict(color=PALETTE['muted'], formatter='{value} kW'))],
        series=[dict(name='Utilization', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['cyan']), lineStyle=dict(width=2),
                     areaStyle=dict(opacity=.18), data=[[ms(r[0]), round(r[1], 1)] for r in rows],
                     markLine=dict(symbol='none', data=[
                         dict(yAxis=80, lineStyle=dict(color=PALETTE['amber'], type='dotted'), label=dict(formatter='warning', color=PALETTE['amber'])),
                         dict(yAxis=95, lineStyle=dict(color=PALETTE['red'], type='dotted'), label=dict(formatter='critical', color=PALETTE['red']))] + marks)),
                dict(name='Net load kW', type='line', yAxisIndex=1, showSymbol=False, itemStyle=dict(color=PALETTE['text']),
                     lineStyle=dict(width=1, opacity=.7), data=[[ms(r[0]), round(r[2], 1)] for r in rows])])
    return option


def throughput(history):
    rows = recent(history)
    def column(i):
        return [[ms(r[0]), None if r[i] is None else round(r[i], 1)] for r in rows]
    option = base('Events per second')
    option['title'] = dict(text='events/s', left=8, top=2, textStyle=dict(color=PALETTE['muted'], fontSize=12))
    option.update(xAxis=time_axis(), yAxis=dict(type='value', min=0, **AXIS), series=[
        dict(name='Devices publish', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['blue']), data=column(1)),
        dict(name='Kafka ingest', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['cyan']), areaStyle=dict(opacity=.15), data=column(2)),
        dict(name='Load windows out', type='line', showSymbol=False, itemStyle=dict(color=PALETTE['green']), data=column(3))])
    return option


def lag_bars(lag):
    names = {'integration-kind.action.control.v1': 'Action', 'integration-kind.projection': 'Projection'}
    rows = [(names.get(g, 'Flink aggregation' if 'aggregation' in g else g), v) for g, v in sorted(lag.items())]
    option = base()
    option.update(grid=dict(left=120, right=40, top=8, bottom=22), tooltip=dict(trigger='item'),
        xAxis=dict(type='value', min=0, **dict(AXIS, axisLabel=dict(color=PALETTE['muted'], hideOverlap=True, formatter='{value}'))),
        yAxis=dict(type='category', data=[n for n, _ in rows], **AXIS),
        series=[dict(type='bar', barWidth='55%', data=[dict(value=round(v), itemStyle=dict(color=PALETTE['green'] if v < 500 else PALETTE['amber'] if v < 5000 else PALETTE['red'])) for _, v in rows],
                     label=dict(show=True, position='right', color=PALETTE['text']))])
    return option


def gauge(value, name, maximum=100, warn=70, bad=90):
    value = 0 if value is None else value
    return dict(animation=False, backgroundColor='transparent', series=[dict(type='gauge', min=0, max=maximum, radius='96%', startAngle=210, endAngle=-30,
        progress=dict(show=True, width=10), pointer=dict(show=False), axisLine=dict(lineStyle=dict(width=10, color=[[1, '#1b2c47']])),
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
    return dict(animation=False, backgroundColor='transparent', grid=dict(left=0, right=0, top=2, bottom=2),
                xAxis=dict(type='time', show=False), yAxis=dict(type='value', show=False, scale=True),
                series=[dict(type='line', showSymbol=False, smooth=True, lineStyle=dict(width=1.5, color=color), itemStyle=dict(color=color),
                             areaStyle=dict(color=color, opacity=.18), data=data)])
