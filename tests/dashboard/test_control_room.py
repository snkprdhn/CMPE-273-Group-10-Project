"""Control-room data layer and chart builders, without a browser or a cluster."""
import asyncio
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import json

import httpx

from dashboard import control_charts as charts
from dashboard.control_room import DOCK, SAGA, changed, count, explain, decision, decision_log, flow_html, rush_request, rush_target, kpi_tiles, saga_action, saga_steps, throttled_by_transformer, tile_value
from dashboard.fleet_data import FleetData, LIVE, TX, band, by_label, epoch, number, scalar


def run(coroutine):
    # The browser fixtures keep an event loop running in this thread; use a private one.
    with ThreadPoolExecutor(1) as pool:
        return pool.submit(asyncio.run, coroutine).result()


def vector(rows):
    return {'status': 'success', 'data': {'resultType': 'vector', 'result': [
        {'metric': labels, 'value': [1, str(value)]} for labels, value in rows]}}


def prometheus(request):
    query = request.url.params['query']
    by = lambda **values: vector([({'transformer': t}, v) for t, v in values.items()])
    if 'signal="net_load_kw"' in query:
        return httpx.Response(200, json=by(tx_001=60, tx_002=110, tx_003=30))
    if 'signal="capacity_kw"' in query:
        return httpx.Response(200, json=by(tx_001=100, tx_002=100, tx_003=100))
    if 'signal="ev_demand_kw"' in query:
        return httpx.Response(200, json=by(tx_001=30, tx_002=70, tx_003=10))
    if 'devices_fleet_overload_active' in query:
        return httpx.Response(200, json=by(tx_002=1))
    if 'consumergroup' in query:
        return httpx.Response(200, json=vector([({'consumergroup': 'integration-kind.projection'}, 12)]))
    if 'kubelet_volume' in query:
        return httpx.Response(200, json=vector([({'persistentvolumeclaim': 'data-postgres-0'}, 24.5)]))
    if 'sum(rate(devices_fleet_events_total' in query:
        return httpx.Response(200, json=vector([({}, 199)]))
    return httpx.Response(200, json=vector([]))


def data():
    fleet = FleetData('http://prometheus', clock=lambda: 1000.0)
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(prometheus))
    return fleet


def test_poll_builds_transformer_state_summary_and_bands():
    fleet = data()
    run(fleet.poll())
    assert set(fleet.tx) == {'tx_001', 'tx_002', 'tx_003'}
    assert round(fleet.tx['tx_002']['utilization'], 6) == 110 and fleet.tx['tx_002']['band'] == 'critical'
    assert fleet.tx['tx_002']['incident'] and fleet.tx['tx_002']['managed'] and not fleet.tx['tx_001']['managed']
    summary = fleet.summary()
    assert summary['counts'] == {'normal': 2, 'warning': 0, 'critical': 1}
    assert summary['totals']['net'] == 200 and summary['totals']['capacity'] == 300
    assert round(summary['utilization'], 1) == 66.7
    assert fleet.lag == {'integration-kind.projection': 12} and fleet.volumes == {'data-postgres-0': 24.5}
    assert fleet.scalars['producer_rate'] == 199
    assert len(fleet.fleet_history) == 1 and fleet.age() == 0


def test_unreachable_prometheus_is_reported_not_raised():
    fleet = FleetData('http://prometheus', clock=lambda: 5.0)
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503)))
    run(fleet.poll())
    assert fleet.errors == {'prometheus': 'unreachable'} and fleet.tx == {} and fleet.age() is None


def test_parsers_reject_non_finite_values_and_band_edges():
    assert number('nan') is None and number('inf') is None and number(None) is None and number('2.5') == 2.5
    assert by_label([{'metric': {'transformer': 'a'}, 'value': [0, 'NaN']}, {'metric': {'transformer': 'b'}, 'value': [0, '4']}]) == {'b': 4.0}
    assert scalar([]) is None and scalar([{'value': [0, '3']}]) == 3.0
    assert [band(v) for v in (None, 10, 79.9, 80, 94.9, 95)] == ['unknown', 'normal', 'normal', 'warning', 'warning', 'critical']


def test_controls_are_rate_limited_and_need_a_token():
    now = [100.0]
    fleet = FleetData('http://prometheus', fleet='http://fleet', fleet_token='t', clock=lambda: now[0])
    seen = []
    def handler(request):
        seen.append((request.url.path, request.headers['authorization'], json.loads(request.content)))
        return httpx.Response(200, json={'ok': True})
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    assert run(fleet.control('/inject', {'kind': 'duplicates', 'seconds': 30})) == {'ok': True}
    try:
        run(fleet.control('/inject', {'kind': 'duplicates', 'seconds': 30}))
        raise AssertionError('second call inside the window must fail')
    except RuntimeError as exc:
        assert 'wait' in str(exc)
    now[0] += 9
    run(fleet.control('/overload', {'transformer': 'tx-001', 'seconds': 90}))
    assert [s[0] for s in seen] == ['/inject', '/overload'] and all(s[1] == 'Bearer t' for s in seen)
    unconfigured = FleetData('http://prometheus')
    try:
        run(unconfigured.control('/inject', {}))
        raise AssertionError('missing token must fail')
    except RuntimeError as exc:
        assert 'not configured' in str(exc)


def test_chart_options_serialize_and_have_the_expected_shape():
    fleet = data()
    run(fleet.poll())
    history = [(t, 100 + t, 40, 50, 10, 300) for t in range(1000, 1600, 5)]
    rows = [(t, 60 + t % 7, 80 + t % 7, 100) for t in range(1000, 1600, 5)]
    actions = [dict(transformer_id='tx_002', status='REDUCTION_CONFIRMED', transition_time='1970-01-01T00:20:00Z')]
    options = [charts.fleet_load(history), charts.topology(fleet.tx, {'tx_002': 4}, 'tx_002'),
               charts.transformer_detail(rows, actions, 'tx_002'), charts.throughput([(t, 1, 2, 3, 4, 5) for t in range(10)]),
               charts.lag_bars({'integration-kind.projection': 20, 'agg-fleet-v1-aggregation-v1': 3}), charts.gauge(40, 'busy'),
               charts.volumes({'data-postgres-0': 30, 'other': 1}), charts.spark(history, 1, '#fff')]
    for option in options:
        json.dumps(option, allow_nan=False)
    assert [s['name'] for s in options[0]['series']] == ['Households', 'EV charging', 'Solar generation', 'Net load', 'Capacity']
    assert all(point[1] <= 0 for point in options[0]['series'][2]['data'])          # generation plots below zero
    managed, shadow = options[2]['series']
    assert managed['name'] == 'Managed load' and shadow['name'] == 'Without throttling' and shadow['lineStyle']['type'] == 'dashed'
    marks = managed['markLine']['data']
    assert any(m.get('xAxis') == 1200_000 for m in marks) and any(m.get('yAxis') == 95.0 for m in marks)    # action marker, 95 % of 100 kW
    assert options[6]['yAxis']['data'] == ['PostgreSQL']


def node_points(option):
    scatter, pulses = option['series'][4]['data'], option['series'][5]['data']
    return {n['name']: n['value'][:2] for n in scatter + pulses if not n['name'].startswith('heading')}


def fleet(critical=('tx-020', 'tx-011')):
    tx = {f'tx-{i:03d}': dict(utilization=50 + i, net=50 + i, capacity=100, band='normal', managed=i <= 5, incident=False, solar=10.0) for i in range(1, 26)}
    for name in critical:
        tx[name]['utilization'] = 99
        tx[name]['band'] = 'critical'
    return tx


def test_topology_has_four_columns_and_every_line_ends_on_a_node():
    option = charts.topology(fleet(), {'tx-002': 7}, 'tx-002')
    json.dumps(option, allow_nan=False)
    feeders, telemetry, commands, acks, scatter, pulses = option['series']
    points = node_points(option)
    assert [s['type'] for s in option['series']] == ['lines'] * 4 + ['scatter', 'effectScatter']
    assert [n['name'] for n in pulses['data']] == ['tx-011', 'tx-020']                    # critical nodes pulse
    # source, five heroes, three most-loaded others, five charger groups, saga
    assert len(points) == 1 + 5 + 3 + 5 + 1 and 'tx-025' in points and 'tx-006' not in points
    columns = {name: x for name, (x, _) in points.items()}
    assert columns['Grid + Solar'] == charts.STAGE_X['source'] and columns['tx-001'] == charts.STAGE_X['transformer']
    assert columns['tx-001 chargers'] == charts.STAGE_X['charger'] and columns['Control saga'] == charts.STAGE_X['saga']
    assert list(charts.STAGE_X.values()) == sorted(charts.STAGE_X.values())
    assert [n['label']['formatter'] for n in scatter['data'] if n['name'].startswith('heading')] == ['GRID + SOLAR', 'TRANSFORMERS', 'EV CHARGERS', 'CONTROL SAGA']
    known = list(points.values())
    for series in (feeders, telemetry, commands, acks):
        assert all(end in known for line in series['data'] for end in line['coords'])    # every line end sits on a node
    assert len(feeders['data']) == 8 + 5 and len(telemetry['data']) == 5 and commands['data'] == [] and acks['data'] == []
    assert all(line['coords'][1] == [charts.STAGE_X['saga'], 50] for line in telemetry['data'])
    assert telemetry['lineStyle']['color'] == charts.TELEMETRY
    assert option['xAxis'] == dict(type='value', min=0, max=100, show=False) and option['yAxis']['show'] is False and option['yAxis']['max'] == charts.Y_MAX
    source = next(n for n in scatter['data'] if n['name'] == 'Grid + Solar')
    assert source['label']['formatter'].endswith('Solar 250 kW') and source['itemStyle']['color'] == charts.PALETTE['sky']
    chargers = {n['name']: n for n in scatter['data'] if n['name'].endswith('chargers')}
    assert chargers['tx-002 chargers']['label']['formatter'] == '7 throttled' and chargers['tx-001 chargers']['label']['formatter'] == 'chargers'
    selected = next(n for n in scatter['data'] if n['name'] == 'tx-002')
    assert selected['itemStyle']['borderColor'] == '#ffffff' and node_points(charts.topology(fleet()))['tx-002'] == points['tx-002']
    assert next(n for n in scatter['data'] if n['name'] == 'Control saga')['label']['formatter'] == 'Control saga\n0 active'


def a_action(tx, status, acked=0, **extra):
    return dict(transformer_id=tx, status=status, acknowledged_device_count=acked, selected_device_count=10, **extra)


def test_closed_loop_draws_commands_and_acks_only_while_an_action_is_in_flight():
    idle = charts.topology(fleet())
    busy = charts.topology(fleet(), {'tx-002': 7}, actions=[a_action('tx-002', 'COMMANDS_DISPATCHED'), a_action('tx-003', 'ACKS_PENDING', 4),
                                                           a_action('tx-004', 'COMPLETED', 9), a_action('tx-001', 'RECOVERY_MONITORING', 3)])      # only dispatched and acks-pending draw commands
    held = charts.topology(fleet(), actions=[a_action('tx-005', 'PARTIALLY_APPLIED', 2), a_action('tx-005', 'COMMANDS_DISPATCHED', 0)])   # newest wins
    assert [len(o['series']) for o in (idle, busy, held)] == [6, 6, 6]                     # fixed series count: updates merge
    assert [s['type'] for s in busy['series']] == [s['type'] for s in idle['series']]
    commands, acks = busy['series'][2], busy['series'][3]
    saga, charger = [charts.STAGE_X['saga'], 50], charts.STAGE_X['charger']
    assert len(commands['data']) == 2 and all(line['coords'][0] == saga and line['coords'][1][0] == charger for line in commands['data'])
    assert len(acks['data']) == 2 and all(line['coords'][1] == saga and line['coords'][0][0] == charger for line in acks['data'])   # tx-002 has no acks yet
    assert commands['lineStyle']['color'] == charts.PALETTE['amber'] and acks['lineStyle']['color'] == charts.PALETTE['green']
    assert commands['lineStyle']['curveness'] and commands['effect']['symbol'] == 'arrow' and commands['effect']['show'] is True
    assert next(n for n in busy['series'][4]['data'] if n['name'] == 'Control saga')['label']['formatter'] == 'Control saga\n3 active'
    assert idle['series'][2]['data'] == [] and idle['series'][3]['data'] == []
    assert held['series'][2]['data'] == [] and held['series'][3]['data'] == []              # held: the loop is stopped
    node = next(n for n in held['series'][4]['data'] if n['name'] == 'Control saga')
    assert node['label']['formatter'] == 'Control saga\n1 active · safe hold' and node['itemStyle']['color'] == charts.PALETTE['red']


def test_every_line_is_silent_and_has_no_trail_in_every_state():
    for option in (charts.topology(fleet()), charts.topology(fleet(), actions=[a_action('tx-002', 'ACKS_PENDING', 3)]), charts.topology(fleet(), motion=False)):
        for series in option['series'][:4]:
            assert series['silent'] is True and series['effect']['trailLength'] == 0 and series['coordinateSystem'] == 'cartesian2d'


def test_topology_without_motion_has_no_particles_or_pulses():
    tx = {'tx-001': dict(utilization=99, net=99, capacity=100, band='critical', managed=True, incident=True)}
    option = charts.topology(tx, motion=False, actions=[a_action('tx-001', 'ACKS_PENDING', 3)])
    assert all(series['effect']['show'] is False for series in option['series'][:4]) and option['series'][5]['data'] == []
    assert [n['name'] for n in option['series'][4]['data'] if not n['name'].startswith('heading')] == ['Grid + Solar', 'tx-001', 'tx-001 chargers', 'Control saga']
    assert charts.topology({})['series'][4]['data'][-2]['name'] == 'Grid + Solar'                # empty fleet still renders


def test_compact_topology_uses_shorter_pills():
    normal = next(n for n in charts.topology(fleet())['series'][4]['data'] if n['name'] == 'tx-001')
    compact = next(n for n in charts.topology(fleet(), compact=True)['series'][4]['data'] if n['name'] == 'tx-001')
    assert compact['symbolSize'][1] < normal['symbolSize'][1]


def test_unchanged_topology_causes_no_chart_update():
    tx = {'tx-001': dict(utilization=50, net=50, capacity=100, band='normal', managed=True, incident=False)}
    store = {}
    assert changed(store, 'topology', charts.topology(tx)) is True
    assert changed(store, 'topology', charts.topology(dict(tx))) is False
    tx['tx-001'] = dict(tx['tx-001'], utilization=96, band='critical')
    assert changed(store, 'topology', charts.topology(tx)) is True
    assert changed(store, 'topology', charts.topology(tx)) is False
    assert changed(store, 'detail', charts.transformer_detail([], [], 'tx-001')) is True
    assert throttled_by_transformer([action(0, 'REDUCTION_CONFIRMED') | dict(acknowledged_device_count=3), action(0, 'COMPLETED', key='x') | dict(acknowledged_device_count=9)]) == {'tx-003': 3}


# --- business numbers: the confirmed throttle timeline -------------------------------------------------------------

T0 = 1_800_000_000.0


def iso(offset):
    return datetime.fromtimestamp(T0 + offset, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def action(offset, status, kw=44.0, tx='tx-003', key='a1'):
    return dict(action_id=key, transformer_id=tx, status=status, transition_time=iso(offset), confirmed_reduction_kw=kw,
                target_reduction_kw=50.0)


def loads_item(offset, net, revision=0, capacity=100.0):
    return dict(transformer_id='tx-003', window_id=f'w{offset}', window_start=iso(offset), window_end=iso(offset + 5),
                revision=revision, net_load_kw=net, capacity_kw=capacity)


def api_data(handler, now=T0):
    clock = [now]
    fleet = FleetData('http://prometheus', api='http://api/v1', clock=lambda: clock[0])
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return fleet, clock


def test_peak_avoided_equals_sustained_confirmed_kw_on_a_step_overload():
    fleet = FleetData('http://prometheus')
    fleet.track([action(0, 'ACKS_PENDING', 12.0)], now=T0)
    fleet.track([action(10, 'REDUCTION_CONFIRMED', 44.0)], now=T0 + 10)
    fleet.track([action(10, 'REDUCTION_CONFIRMED', 44.0)], now=T0 + 130)
    k = fleet.kpis(now=T0 + 130)
    assert k['shed_kw'] == 44.0 and round(k['peak_avoided_kw'], 6) == 44.0 and k['dollars'] == k['peak_avoided_kw'] * 25
    assert round(k['kwh_shifted'], 6) == round((12 * 10 + 44 * 120) / 3600, 6)
    assert fleet.segments['tx-003'] == [[T0, T0 + 10, 12.0], [T0 + 10, None, 44.0]]


def test_peak_avoided_is_a_rolling_mean_and_sums_transformers():
    fleet = FleetData('http://prometheus')
    fleet.track([action(0, 'REDUCTION_CONFIRMED', 60.0, key='a'), action(0, 'REDUCTION_CONFIRMED', 20.0, 'tx-004', 'b')], now=T0)
    fleet.track([action(0, 'REDUCTION_CONFIRMED', 60.0, key='a'), action(0, 'REDUCTION_CONFIRMED', 20.0, 'tx-004', 'b')], now=T0 + 30)
    k = fleet.kpis(now=T0 + 30)       # 30 s into a 60 s window: the mean has not reached the sustained value yet
    assert round(k['peak_avoided_kw'], 6) == 40.0
    assert round(fleet.kpis(now=T0 + 90)['peak_avoided_kw'], 6) == 80.0


def test_nothing_counts_after_restore_and_a_skipped_restore_poll_closes_the_segment():
    fleet = FleetData('http://prometheus')
    fleet.track([action(0, 'RECOVERY_MONITORING')], now=T0 + 5)
    fleet.track([action(100, 'RESTORE_DISPATCHED')], now=T0 + 101)
    assert fleet.segments['tx-003'] == [[T0, T0 + 100, 44.0]] and fleet.kpis(now=T0 + 500)['shed_kw'] == 0
    assert round(fleet.kpis(now=T0 + 500)['kwh_shifted'], 6) == round(44 * 100 / 3600, 6)
    fleet.track([action(100, 'COMPLETED')], now=T0 + 500)                      # nothing reopens
    assert fleet.kpis(now=T0 + 500)['shed_kw'] == 0
    skipped = FleetData('http://prometheus')
    skipped.track([action(0, 'REDUCTION_CONFIRMED')], now=T0 + 5)
    skipped.track([action(0, 'REDUCTION_CONFIRMED')], now=T0 + 60)             # last seen holding the reduction
    skipped.track([action(300, 'COMPLETED')], now=T0 + 310)
    assert skipped.segments['tx-003'] == [[T0, T0 + 60, 44.0]] and skipped.kpis(now=T0 + 400)['shed_kw'] == 0
    vanished = FleetData('http://prometheus')
    vanished.track([action(0, 'REDUCTION_CONFIRMED')], now=T0 + 5)
    vanished.track([], now=T0 + 20)
    assert vanished.segments['tx-003'][0][1] == T0 + 5


def test_held_states_are_not_counted_and_close_an_open_segment():
    for status in ('PARTIALLY_APPLIED', 'TIMED_OUT', 'NEEDS_OPERATOR_REVIEW', 'PLAN_CREATED', 'COMMANDS_DISPATCHED'):
        fleet = FleetData('http://prometheus')
        fleet.track([action(0, status, 30.0)], now=T0)
        assert fleet.segments == {} and fleet.kpis(now=T0 + 60) == dict(shed_kw=0.0, peak_avoided_kw=0.0, kwh_shifted=0.0, dollars=0.0)
    fleet = FleetData('http://prometheus')
    fleet.track([action(0, 'REDUCTION_CONFIRMED')], now=T0 + 2)
    fleet.track([action(20, 'NEEDS_OPERATOR_REVIEW')], now=T0 + 25)
    assert fleet.segments['tx-003'] == [[T0, T0 + 2, 44.0]]


def test_epoch_accepts_zones_and_rejects_naive_or_invalid_input():
    assert epoch('2026-11-26T21:00:00Z') == epoch('2026-11-26T21:00:00+00:00') == epoch('2026-11-26T13:00:00-08:00')
    assert epoch('2026-11-26T21:00:00') is None
    for bad in ('not a time', '', None, 12, {}):
        assert epoch(bad) is None


def test_bad_action_rows_are_skipped():
    fleet = FleetData('http://prometheus')
    fleet.track([dict(action_id='x', transformer_id='tx-001', status='REDUCTION_CONFIRMED', transition_time='nope', confirmed_reduction_kw=5),
                 dict(action_id='y', transformer_id='tx-001', status='REDUCTION_CONFIRMED', transition_time=iso(0)),
                 dict(action_id='z', transformer_id='tx-001', status='REDUCTION_CONFIRMED', transition_time='2026-11-26T21:00:00', confirmed_reduction_kw=5),
                 dict(transformer_id='tx-001'), None, 'junk',
                 action(0, 'REDUCTION_CONFIRMED', float('nan'), key='n'), action(0, 'REDUCTION_CONFIRMED', -3, key='m')], now=T0)
    assert fleet.segments == {} and fleet.kpis(now=T0 + 60)['shed_kw'] == 0


def test_shadow_series_adds_confirmed_kw_only_inside_the_segment():
    def handler(request):
        assert request.url.path == '/v1/transformers/tx-003/loads' and 'start' in request.url.params and 'end' in request.url.params
        items = [loads_item(-10, 100), loads_item(-5, 100), loads_item(0, 100, 0), loads_item(0, 70, 1), loads_item(5, 56), loads_item(10, 56),
                 dict(window_start='bad', window_end='bad', net_load_kw=1, revision=0), dict(window_start=iso(15))]
        return httpx.Response(200, json=dict(items=items, next_cursor=None, freshness='FRESH'))
    fleet, clock = api_data(handler, now=T0 + 20)
    fleet.track([action(4, 'REDUCTION_CONFIRMED', 44.0)], now=T0 + 20)         # throttle took effect at +4 s
    run(fleet.fetch_loads('tx-003'))
    series = fleet.shadow_series('tx-003')
    # window ends at -5, 0 (older revision replaced), 5 (starts at 0 < 4: before the throttle, no add), 10 and 15
    assert [(round(t - T0), net, shadow) for t, net, shadow, _ in series] == [(-5, 100, 100), (0, 100, 100), (5, 70, 70), (10, 56, 100), (15, 56, 100)]
    assert fleet.shadow_series('tx-404') == []


def test_shadow_series_stops_at_the_end_of_the_segment():
    fleet, _ = api_data(lambda r: httpx.Response(200, json=dict(items=[loads_item(o, 50) for o in (0, 5, 10, 15)], next_cursor=None, freshness='FRESH')), now=T0 + 30)
    fleet.segments['tx-003'] = [[T0, T0 + 12, 20.0]]
    run(fleet.fetch_loads('tx-003'))
    assert [round(shadow) for _, _, shadow, _ in fleet.shadow_series('tx-003')] == [70, 70, 50, 50]     # window 10-15 crosses the end


def test_loads_pages_are_followed_and_a_failure_keeps_the_kpis():
    seen = []
    def handler(request):
        seen.append(dict(request.url.params))
        if 'cursor' in request.url.params:
            return httpx.Response(200, json=dict(items=[loads_item(5, 60)], next_cursor=None, freshness='FRESH'))
        return httpx.Response(200, json=dict(items=[loads_item(0, 60)], next_cursor='c1', freshness='FRESH'))
    fleet, clock = api_data(handler)
    run(fleet.fetch_loads('tx-003'))
    assert len(seen) == 2 and seen[1]['cursor'] == 'c1' and len(fleet.loads['tx-003']['rows']) == 2
    failing, _ = api_data(lambda r: httpx.Response(503))
    failing.track([action(0, 'REDUCTION_CONFIRMED')], now=T0)
    assert run(failing.fetch_loads('tx-003')) is None and failing.errors['loads'] == 'unreachable'
    assert failing.kpis(now=T0 + 60)['shed_kw'] == 44.0
    garbage, _ = api_data(lambda r: httpx.Response(200, text='not json'))
    assert run(garbage.fetch_loads('tx-003')) is None and garbage.errors['loads'] == 'unreachable'


def test_concurrent_loads_fetches_make_one_request_and_respect_the_minimum_gap():
    calls = []
    async def handler(request):
        calls.append(request.url.path)
        await asyncio.sleep(.05)
        return httpx.Response(200, json=dict(items=[loads_item(0, 60)], next_cursor=None, freshness='FRESH'))
    fleet, clock = api_data(handler)
    async def burst():
        return await asyncio.gather(*[fleet.fetch_loads('tx-003') for _ in range(5)])
    results = run(burst())
    assert len(calls) == 1 and all(r is results[0] for r in results)
    run(fleet.fetch_loads('tx-003'))
    assert len(calls) == 1                                  # inside the 1 s gap: cached
    clock[0] += 1.5
    run(fleet.fetch_loads('tx-003'))
    assert len(calls) == 2


def test_a_stale_reply_for_another_transformer_does_not_change_the_selected_chart():
    async def scenario():
        gate = asyncio.Event()
        async def handler(request):
            tx = request.url.path.split('/')[3]
            if tx == 'tx-001':
                await gate.wait()
            return httpx.Response(200, json=dict(items=[dict(loads_item(0, 10 if tx == 'tx-001' else 99), transformer_id=tx)], next_cursor=None, freshness='FRESH'))
        fleet, _ = api_data(handler)
        slow = asyncio.ensure_future(fleet.fetch_loads('tx-001'))
        await fleet.fetch_loads('tx-002')                   # the user moved on and tx-002 is shown
        before = fleet.shadow_series('tx-002')
        gate.set()
        await slow
        return before, fleet.shadow_series('tx-002'), fleet.shadow_series('tx-001')
    before, after, other = run(scenario())
    assert before == after and after[0][1] == 99 and other[0][1] == 10


def test_selected_transformers_are_polled_only_for_sixty_seconds():
    calls = []
    def handler(request):
        calls.append(request.url.path.split('/')[3])
        return httpx.Response(200, json=dict(items=[], next_cursor=None, freshness='FRESH'))
    fleet, clock = api_data(handler)
    fleet.select('tx-001'); fleet.select(None)
    run(fleet.poll_loads())
    assert calls == ['tx-001']
    clock[0] += 30
    fleet.select('tx-001')                                  # a tab's refresh() renews the entry
    clock[0] += 45
    run(fleet.poll_loads())
    assert calls == ['tx-001', 'tx-001']
    clock[0] += 61
    run(fleet.poll_loads())
    assert calls == ['tx-001', 'tx-001'] and fleet.selected == {}


def test_empty_fleet_and_missing_capacity():
    fleet = FleetData('http://prometheus')
    assert fleet.kpis(now=T0) == dict(shed_kw=0.0, peak_avoided_kw=0.0, kwh_shifted=0.0, dollars=0.0)
    assert fleet.summary()['utilization'] is None and fleet.shadow_series('tx-001') == []
    fleet.track([], now=T0)
    fleet, _ = api_data(lambda r: httpx.Response(200, json=dict(items=[loads_item(0, 60, capacity=None)], next_cursor=None, freshness='FRESH')))
    run(fleet.fetch_loads('tx-003'))
    assert fleet.shadow_series('tx-003')[0][3] is None


def test_backfill_seeds_the_timeline_with_actions_active_at_startup():
    def handler(request):
        if request.url.path.endswith('/actions'):
            return httpx.Response(200, json=dict(items=[action(0, 'RECOVERY_MONITORING', 30.0, key='live'), action(0, 'COMPLETED', 99.0, 'tx-009', 'old')]))
        if request.url.path.endswith('/alerts'):
            return httpx.Response(200, json=dict(items=[]))
        return httpx.Response(200, json=dict(status='success', data=dict(resultType='matrix', result=[])))
    fleet, _ = api_data(handler, now=T0 + 40)
    run(fleet.backfill())
    assert fleet.segments == {'tx-003': [[T0, None, 30.0]]} and fleet.kpis(now=T0 + 40)['shed_kw'] == 30.0


def test_kpi_tiles_keep_kw_and_kwh_apart_and_label_the_dollar_figure():
    fleet = data()
    run(fleet.poll())
    acts = [action(0, 'REDUCTION_CONFIRMED', 44.0) | dict(acknowledged_device_count=18, selected_device_count=18),
            action(0, 'COMPLETED', 9.0, key='b') | dict(acknowledged_device_count=5),
            action(0, 'ACKS_PENDING', 4.0, key='c', tx='tx-004') | dict(acknowledged_device_count=2, selected_device_count=6)]
    business = dict(shed_kw=44.0, peak_avoided_kw=44.0, kwh_shifted=12.34, dollars=1100.0)
    tiles = {t['key']: t for t in kpi_tiles(fleet.summary(), dict(controllable=70, output_age_ms=6200), acts, business)}
    assert list(tiles) == ['load', 'throttled', 'peak', 'energy', 'fresh']
    assert (tiles['load']['unit'], tiles['load']['value']) == ('kW', '200') and '300 kW capacity' in tiles['load']['sub']
    assert tiles['throttled']['value'] == '20/70' and tiles['throttled']['unit'] == ''
    assert (tiles['peak']['unit'], tiles['peak']['value']) == ('kW', '44.0')
    assert tiles['peak']['sub'] == '$1,100 illustrative @ $25/kW-month'
    assert (tiles['energy']['unit'], tiles['energy']['value']) == ('kWh', '12.3')
    assert (tiles['fresh']['unit'], tiles['fresh']['value']) == ('s', '6.2')
    assert all(t['unit'] != 'kWh' for k, t in tiles.items() if k != 'energy') and 'kWh' not in tiles['peak']['sub']
    assert tile_value(tiles['energy']).endswith('<span class="u">kWh</span></div>')
    empty = {t['key']: t for t in kpi_tiles(FleetData('http://p').summary(), {}, [], FleetData('http://p').kpis(now=T0))}
    assert empty['load']['value'] == '–' and empty['throttled']['value'] == '0/–' and empty['fresh']['value'] == '–'


# --- decision log and chaos dock -----------------------------------------------------------------------------------

def test_every_action_status_has_a_sentence_and_the_example_reads_correctly():
    from dashboard.client import DEFINITIONS
    base = dict(action_id='a', transformer_id='tx-003', transition_time='2026-11-26T18:02:11Z', target_reduction_kw=46.0,
                confirmed_reduction_kw=44.0, selected_device_count=18, acknowledged_device_count=18, reason='ack timeout')
    sentences = {}
    for status in DEFINITIONS['ActionStatus']['enum']:
        sentences[status] = decision(dict(base, status=status), 97)
        assert 'status ' not in sentences[status].split('→')[1][:8] and sentences[status].startswith('18:02:11 · <b>tx-003</b> (triggered at 97 %) → ')
    assert len(set(sentences.values())) == len(sentences)
    assert sentences['REDUCTION_CONFIRMED'] == ('18:02:11 · <b>tx-003</b> (triggered at 97 %) → throttled 18 lowest-priority chargers · '
                                                '44/46 kW confirmed · acked 18/18')
    assert 'safe hold' in sentences['PARTIALLY_APPLIED'] and 'safe hold' in sentences['TIMED_OUT'] and 'safe hold' in sentences['NEEDS_OPERATOR_REVIEW']
    assert 'restoring' in sentences['RESTORE_DISPATCHED'] and 'restored' in sentences['COMPLETED']
    assert decision(dict(base, status='NEW_STATE'), None).endswith('status NEW_STATE') and 'at ' not in decision(dict(base, status='COMPLETED'))


def test_decision_log_escapes_every_dynamic_value():
    hostile = dict(action_id='a', transformer_id='<img src=x onerror=alert(1)>', status='NEEDS_OPERATOR_REVIEW', transition_time='<script>',
                   target_reduction_kw=1, confirmed_reduction_kw=0, selected_device_count=1, acknowledged_device_count=0,
                   reason='<script>alert(1)</script>' + 'x' * 500)
    html = decision_log([hostile, dict(hostile, status='<b>weird</b>', reason='r')], lambda a: 99)
    assert '<script' not in html and '<img' not in html and '<b>weird' not in html
    assert '&lt;script&gt;' in html and 'x' * 100 not in html
    assert 'No actions yet' in decision_log([])


def test_chaos_dock_sends_the_same_request_bodies_as_before():
    assert [(path, body) for _, path, body in DOCK] == [('/inject', {'kind': 'very_late', 'seconds': 30}),
                                                        ('/inject', {'kind': 'out_of_order', 'seconds': 30}),
                                                        ('/inject', {'kind': 'duplicates', 'seconds': 30}),
                                                        ('/inject', {'kind': 'cloud_cover', 'seconds': 30})]
    assert [label for label, _, _ in DOCK] == ['Delayed packets', 'Out of order', 'Duplicates', 'Cloud cover']
    assert rush_request('tx-004') == ('/overload', {'transformer': 'tx-004', 'seconds': 90})


def test_count_uses_the_singular_for_exactly_one():
    assert [count(v, 'command') for v in (0, 1, 1.0, 2, 12, None)] == ['0 commands', '1 command', '1 command', '2 commands', '12 commands', '– commands']


def test_detail_markers_sit_above_the_plot_and_alternate_rows():
    acts = [dict(transformer_id='tx-1', status='COMMANDS_DISPATCHED', transition_time='1970-01-01T00:20:00Z'),
            dict(transformer_id='tx-1', status='REDUCTION_CONFIRMED', transition_time='1970-01-01T00:20:03Z')]
    option = charts.transformer_detail([(t, 60, 80, 100) for t in range(1000, 1300, 5)], acts, 'tx-1')
    assert option['grid']['top'] >= 56
    labels = [m['label'] for m in option['series'][0]['markLine']['data'] if 'xAxis' in m]
    assert [l['formatter'] for l in labels] == ['throttled', 'confirmed'] and all(l['position'] == 'end' for l in labels)
    assert labels[0]['offset'] != labels[1]['offset']


def test_failed_controls_are_explained_without_the_internal_url():
    request = httpx.Request('POST', 'http://fleet:8003/overload')
    refused = httpx.HTTPStatusError('boom', request=request, response=httpx.Response(400, json={'error': 'transformer must be a managed one'}, request=request))
    assert explain(refused) == 'the fleet refused it (400): transformer must be a managed one'
    assert explain(httpx.HTTPStatusError('x', request=request, response=httpx.Response(502, text='bad gateway', request=request))) == 'the fleet answered 502'
    assert explain(RuntimeError('wait a few seconds between controls')) == 'wait a few seconds between controls'
    assert 'fleet:8003' not in explain(httpx.ConnectError('refused', request=request)) and 'not reachable' in explain(httpx.ConnectError('refused', request=request))


# --- audit fixes: P0 and P1 --------------------------------------------------------------------------------------

def test_topology_effect_has_no_trail_so_hidden_tab_redraws_cannot_fail():
    option = charts.topology({'tx-001': dict(utilization=99, net=99, capacity=100, band='critical', managed=True, incident=True)})
    assert option['series'][0]['effect']['trailLength'] == 0


def test_charts_use_utc_like_the_header_and_the_decision_log():
    assert charts.base('x')['useUTC'] is True and charts.base()['useUTC'] is True
    assert charts.spark([(1, 2)], 1, '#fff')['useUTC'] is True
    for option in (charts.fleet_load([]), charts.transformer_detail([], [], 'tx-001'), charts.throughput([])):
        assert option['useUTC'] is True


def test_throughput_leaves_room_for_a_wrapped_legend():
    assert charts.throughput([])['grid']['top'] >= 48


def test_business_selectors_skip_the_projection_gauge_copy():
    assert LIVE == 'exported_job!~"projection.*"'
    business = {k: v for k, v in TX.items() if 'grid_business_kw' in v or 'grid_window_devices' in v}
    assert set(business) == {'net', 'ev', 'meter', 'solar', 'capacity', 'good', 'estimated', 'missing', 'late'}
    assert all(LIVE in expr for expr in business.values())
    assert TX['incident'] == 'max by (transformer) (devices_fleet_overload_active)'
    seen = []
    def handler(request):
        seen.append(request.url.params['query'])
        return httpx.Response(200, json=dict(status='success', data=dict(resultType='matrix', result=[])))
    fleet, _ = api_data(handler)
    run(fleet.backfill())
    gauges = [q for q in seen if 'grid_business_kw' in q or 'grid_window_devices' in q]
    assert len(gauges) >= 10 and all(LIVE in q for q in gauges)


def trigger_items():
    return [dict(loads_item(0, 97), window_id='w-trigger', utilization_pct=97.0),
            dict(loads_item(5, 70), window_id='w-after', utilization_pct=70.0),
            dict(loads_item(0, 99), window_id='w-trigger', revision=1, utilization_pct=99.0),
            dict(loads_item(10, 50), window_id='w-bare')]            # no utilization_pct: derived from net / capacity


def test_decision_log_reports_the_utilization_of_the_trigger_window():
    fleet, _ = api_data(lambda r: httpx.Response(200, json=dict(items=trigger_items(), next_cursor=None, freshness='FRESH')))
    run(fleet.fetch_loads('tx-003'))
    fired = dict(action(0, 'REDUCTION_CONFIRMED'), trigger_window_id='w-trigger', trigger_revision=0)
    assert fleet.trigger_pct(fired) == 97.0                                           # not the 70 % that followed the throttle
    assert fleet.trigger_pct(dict(fired, trigger_revision=1)) == 99.0
    assert fleet.trigger_pct(dict(fired, trigger_window_id='w-bare')) == 50.0
    assert fleet.trigger_pct(dict(fired, trigger_window_id='w-unknown')) is None and fleet.trigger_pct({}) is None
    html = decision_log([fired], fleet.trigger_pct)
    assert 'at 97 %' in html and 'at 70 %' not in html
    assert ' at ' not in decision_log([dict(fired, trigger_window_id='w-unknown')], fleet.trigger_pct).split('→')[0]


def test_trigger_windows_survive_the_loads_window_moving_on_and_are_looked_up_for_other_transformers():
    requested = []
    def handler(request):
        tx = request.url.path.split('/')[3]
        requested.append(tx)
        return httpx.Response(200, json=dict(items=[dict(loads_item(0, 96), transformer_id=tx, window_id=f'w-{tx}', utilization_pct=96.0)], next_cursor=None, freshness='FRESH'))
    fleet, clock = api_data(handler)
    fleet.actions = [dict(action(0, 'REDUCTION_CONFIRMED', tx='tx-001', key='k1'), trigger_window_id='w-tx-001', trigger_revision=0),
                     dict(action(0, 'COMPLETED', tx='tx-002', key='k2'), trigger_window_id='w-never', trigger_revision=0)]
    for _ in range(5):
        run(fleet.poll_loads())
        clock[0] += 2
    assert fleet.trigger_pct(fleet.actions[0]) == 96.0                                # found without any tab selecting tx-001
    assert requested.count('tx-001') == 1                                             # and not requested again once known
    assert requested.count('tx-002') == 3                                             # a missing window gives up after three polls
    fleet.loads.clear()
    assert fleet.trigger_pct(fleet.actions[0]) == 96.0


def test_monitor_only_transformers_never_request_loads():
    calls = []
    def handler(request):
        if request.url.path.endswith('/loads'):
            calls.append(request.url.path)
            return httpx.Response(404, json={'error': 'not managed'})
        return prometheus(request)
    fleet = FleetData('http://prometheus', api='http://api/v1', clock=lambda: 1000.0)
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    run(fleet.poll())
    assert fleet.tx['tx_002']['managed'] and not fleet.tx['tx_001']['managed']
    assert fleet.loadable('tx_002') and not fleet.loadable('tx_001') and fleet.loadable('tx-never-seen') and not fleet.loadable(None)
    assert run(fleet.fetch_loads('tx_001')) is None and calls == [] and 'loads' not in fleet.errors
    fleet.select('tx_001')
    assert fleet.selected == {}
    run(fleet.poll_loads())
    assert calls == [] and 'loads' not in fleet.errors
    fleet.select('tx_002')
    run(fleet.poll_loads())
    assert calls == ['/v1/transformers/tx_002/loads'] and 'loads' in fleet.errors      # a managed one is still requested (this stub answers 404)


def test_rush_target_is_the_least_loaded_hero_without_an_incident():
    tx = {f'tx-00{i}': dict(managed=True, incident=False, band='normal', utilization=u) for i, u in enumerate((60, 40, 97, 55, 70), 1)}
    tx['tx-009'] = dict(managed=False, incident=False, band='normal', utilization=5)           # monitor-only is never a target
    assert rush_target(tx) == 'tx-002'
    tx['tx-002'] = dict(tx['tx-002'], incident=True)
    assert rush_target(tx) == 'tx-004'
    tx['tx-003']['incident'] = False
    for name in tx:
        tx[name]['incident'] = tx[name]['managed']
    assert rush_target(tx) == 'tx-002'                                                           # everything busy: still the least loaded
    assert rush_target({}) == 'tx-003' and rush_target({'tx-009': tx['tx-009']}) == 'tx-003'


# --- closed-loop stepper and pipeline strip ------------------------------------------------------------------------------

def steps(html):
    """[(state, text)] of the seven stepper cells."""
    return [(m[1], re.sub('<[^>]+>', ' ', m[2]).split()) for m in re.finditer(r'<div class="cr-step (\w+)"[^>]*>(.*?)</div>', html)]


def saga(status, acked=0, selected=10, confirmed=0.0, **extra):
    return dict(action_id='a', transformer_id='tx-003', status=status, selected_device_count=selected, acknowledged_device_count=acked,
                confirmed_reduction_kw=confirmed, target_reduction_kw=46.0, transition_time='2026-11-26T18:02:11Z') | extra


def states(html):
    return [state for state, _ in steps(html)]


def test_saga_steps_walk_the_whole_closed_loop():
    expected = dict(
        PLAN_CREATED=['done', 'active'] + ['todo'] * 5,
        COMMANDS_DISPATCHED=['done'] * 2 + ['active'] + ['todo'] * 4,
        ACKS_PENDING=['done'] * 3 + ['active'] + ['todo'] * 3,
        REDUCTION_CONFIRMED=['done'] * 4 + ['active', 'todo', 'todo'],
        RECOVERY_MONITORING=['done'] * 5 + ['active', 'todo'],
        RESTORE_DISPATCHED=['done'] * 6 + ['active'],
        COMPLETED=['done'] * 7)
    for status, want in expected.items():
        acked = 4 if status in ('PLAN_CREATED', 'COMMANDS_DISPATCHED', 'ACKS_PENDING') else 10
        html = saga_steps(saga(status, acked, confirmed=44.0), 97)
        assert states(html) == want, status
        assert html.count('aria-current="step"') == (0 if status == 'COMPLETED' else 1)
    html = saga_steps(saga('REDUCTION_CONFIRMED', 10, confirmed=44.0), 97.4)
    texts = [' '.join(t) for _, t in steps(html)]
    assert texts == ['✓ Overload at 97 %', '✓ Plan + outbox', '✓ Dispatched (CDC)', '✓ Acks 10/10', 'Reduction confirmed (44 kW)', 'Recovery monitoring', 'Restored']
    assert [label for label in SAGA] == ['Overload', 'Plan + outbox', 'Dispatched (CDC)', 'Acks', 'Reduction confirmed', 'Recovery monitoring', 'Restored']
    assert 'Acks 4/10' in saga_steps(saga('ACKS_PENDING', 4)) and 'Restoring' in saga_steps(saga('RESTORE_DISPATCHED'))


def test_saga_steps_mark_earlier_steps_done_when_a_poll_skipped_states():
    html = saga_steps(saga('RECOVERY_MONITORING', 10, confirmed=44.0))          # nothing was ever seen between plan and recovery
    assert states(html)[:5] == ['done'] * 5 and states(html)[5] == 'active'
    assert states(saga_steps(saga('COMPLETED')))[-1] == 'done' and 'Overload at' not in saga_steps(saga('COMPLETED'))     # no trigger % known: omitted cleanly


def test_saga_steps_show_a_red_safe_hold_for_every_held_status():
    for status, step in (('PARTIALLY_APPLIED', 3), ('TIMED_OUT', 3), ('NEEDS_OPERATOR_REVIEW', 3)):
        html = saga_steps(saga(status, 4))
        want = ['done'] * step + ['hold'] + ['todo'] * (6 - step)
        assert states(html) == want and 'safe hold' in html and 'no further commands' in html
        assert html.count('aria-current="step"') == 1
    assert states(saga_steps(saga('NEEDS_OPERATOR_REVIEW', 10, confirmed=3.0))) == ['done'] * 4 + ['hold', 'todo', 'todo']     # all acked: held at the reduction


def test_saga_steps_idle_state_and_unknown_status():
    idle = saga_steps(None, name='tx-004')
    assert states(idle) == ['todo'] * 7 and 'Monitoring tx-004: no active incident' in idle and 'aria-current' not in idle
    weird = saga_steps(saga('BRAND_NEW'))
    assert states(weird) == ['todo'] * 7 and 'status BRAND_NEW' in weird


def test_saga_steps_escape_every_dynamic_value():
    hostile = saga('NEEDS_OPERATOR_REVIEW', 1) | dict(transformer_id='<img src=x onerror=alert(1)>')
    for html in (saga_steps(hostile), saga_steps(saga('<script>alert(1)</script>') | dict(transformer_id='<b>x</b>')), saga_steps(None, name='<i>y</i>')):
        assert '<img' not in html and '<script' not in html and '<b>x' not in html and '<i>y' not in html
    assert '&lt;img' in saga_steps(hostile)


def test_saga_action_shows_active_held_and_just_finished_actions_for_one_transformer():
    now = epoch('2026-11-26T18:02:30Z')
    newest_first = [saga('COMPLETED') | dict(action_id='done', transition_time='2026-11-26T18:02:11Z'),
                    saga('COMPLETED', transition_time='2026-11-26T17:00:00Z') | dict(action_id='old')]
    assert saga_action(newest_first, 'tx-003', now)['action_id'] == 'done'          # finished 19 s ago
    assert saga_action(newest_first, 'tx-003', now + 300) is None                   # finished long ago: back to monitoring
    assert saga_action(newest_first, 'tx-004', now) is None
    assert saga_action([saga('PARTIALLY_APPLIED')] + newest_first, 'tx-003', now + 9999)['status'] == 'PARTIALLY_APPLIED'


def test_flow_strip_includes_outbox_gateway_and_the_return_rail():
    html = flow_html(dict(counts=dict(critical=2)), dict(devices=1000, kafka_in=198, kafka_out=5.0, flink_busy=18, controllable=70, commands=1), 3)
    for text in ('Outbox / CDC', 'Fenced gateway', '1 command applied in 10 min', 'Devices', 'Kafka', 'Flink windows', 'Action service', 'Chargers'):
        assert text in html
    assert html.index('Action service') < html.index('Outbox / CDC') < html.index('Fenced gateway') < html.index('Chargers')
    assert 'acks + lower measured kW re-enter as telemetry · loop ≈ 20 s' in html and 'class="cr-rail"' in html
    assert html.count('class="cr-arrow"') == 6 and '0 commands applied' in flow_html(dict(counts=dict(critical=0)), dict(commands=0), 0)


# --- full-fleet explorer: per-transformer history, chargers --------------------------------------------------------------

def test_backfill_builds_the_per_transformer_breakdown_from_four_range_queries():
    seen = []
    values = dict(net=100.0, ev=30.0, meter=80.0, solar=10.0, capacity=200.0)
    def handler(request):
        query = request.url.params['query']
        seen.append(query)
        signal = next((k for k, name in (('net', 'net_load_kw'), ('ev', 'ev_demand_kw'), ('meter', 'meter_demand_kw'), ('solar', 'solar_generation_kw'), ('capacity', 'capacity_kw'))
                       if f'signal="{name}"' in query), None)
        if request.url.path.endswith('/query_range') and signal:
            series = [('tx-001', 1.0), ('tx-002', 2.0)] if query.startswith('max by') else [(None, 1.0)]
            return httpx.Response(200, json=dict(status='success', data=dict(resultType='matrix', result=[
                dict(metric={} if name is None else {'transformer': name}, values=[[T0 - 20 + 10 * k, str(values[signal] * scale)] for k in range(3)]) for name, scale in series])))
        return httpx.Response(200, json=dict(status='success', data=dict(resultType='matrix', result=[])))
    fleet, _ = api_data(handler, now=T0)
    run(fleet.backfill())
    per_tx = [q for q in seen if q.startswith('max by (transformer) (grid_business_kw')]
    assert len(per_tx) == 5 and all(LIVE in q for q in per_tx)
    assert set(fleet.tx_history) == {'tx-001', 'tx-002'} and len(fleet.tx_history['tx-001']) == 3
    t, net, ev, meter, solar, capacity = fleet.tx_history['tx-002'][-1]
    assert (t, net, ev, meter, solar, capacity) == (T0, 200.0, 60.0, 160.0, 20.0, 400.0)
    assert [round(u) for _, u, *_ in fleet.util_history['tx-001']] == [50, 50, 50]


def test_poll_appends_to_the_per_transformer_history_every_five_seconds():
    fleet, clock = api_data(prometheus)
    for step in (0, 2, 5, 7, 10):
        clock[0] = 1000.0 + step
        run(fleet.poll())
    assert [row[0] for row in fleet.tx_history['tx_002']] == [1000.0, 1005.0, 1010.0]
    assert fleet.tx_history['tx_002'][0] == (1000.0, 110.0, 70.0, 0.0, 0.0, 100.0)


def view(key='ev-001', tx='tx-003', priority=0, power=7.2, rate=7.2, freshness='FRESH', quality='GOOD', controllable=True, history=(), departure='2026-11-26T20:30:00Z'):
    return dict(device=dict(device_id=key, transformer_id=tx, device_type='EV_CHARGER', controllable=controllable, minimum_rate_kw=1.4, maximum_rate_kw=7.2,
                            priority=priority, departure_time=departure, capability_version=1, updated_at='2026-11-26T18:00:00Z'),
                freshness=freshness, latest_telemetry=dict(power_kw=power, current_rate_kw=rate, quality=quality), command_history=list(history))


def command(action_id='a1', outcome='APPLIED', previous=7.2, applied=1.4, at='2026-11-26T18:02:12Z'):
    return dict(command_id='c', action_id=action_id, device_id='ev-001', outcome=outcome, previous_rate_kw=previous, applied_rate_kw=applied, result_time=at)


def test_chargers_are_throttled_spared_or_normal():
    from dashboard.fleet_data import charger_row, charger_state, throttling_actions
    throttling = throttling_actions([dict(action_id='a1', transformer_id='tx-003', status='REDUCTION_CONFIRMED'),
                                     dict(action_id='old', transformer_id='tx-004', status='COMPLETED'),
                                     dict(action_id='r', transformer_id='tx-005', status='RESTORE_DISPATCHED'),
                                     dict(action_id='p', transformer_id='tx-002', status='PLAN_CREATED')])
    assert throttling == {'tx-003': {'a1'}}
    state = lambda **kw: charger_state(charger_row(view(**kw)), throttling)
    assert state(history=[command()]) == 'Throttled'
    assert state(history=[command(outcome='ALREADY_APPLIED', previous=1.4)]) == 'Throttled'              # replayed command, still below the maximum
    assert state(priority=12) == 'Spared'                                                                   # online, charging, controllable, never commanded
    assert state(history=[command('older-action')]) == 'Spared'                                              # a command of another action does not count
    assert state(history=[command(outcome='REJECTED')]) == 'Normal' and state(history=[command(outcome='FAILED', applied=7.2)]) == 'Normal'
    assert state(history=[command(applied=7.2)]) == 'Normal'                                                 # commanded, but not lowered
    assert state(quality='INVALID') == 'Normal' and state(quality=None) == 'Normal'                         # no usable telemetry: protection is not the reason
    assert state(freshness='STALE') == 'Normal' and state(freshness='UNKNOWN') == 'Normal'                  # stale/unknown legacy devices are never Spared
    assert state(power=0.0, rate=0.0) == 'Normal' and state(rate=0.0) == 'Normal'                            # not charging
    assert state(controllable=False) == 'Normal'
    assert state(tx='tx-004', history=[command()]) == 'Normal' and state(tx='tx-005') == 'Normal' and state(tx='tx-002') == 'Normal'   # no throttle in force
    newest = [command('older', at='2026-11-26T18:00:00Z'), command('a1', at='2026-11-26T18:05:00Z')]
    assert state(history=newest) == 'Throttled' and state(history=newest[::-1]) == 'Throttled'               # the newest command decides


def test_charger_rows_skip_malformed_views():
    from dashboard.fleet_data import charger_row
    for bad in (None, 'x', {}, {'device': None}, {'device': {'device_id': 1, 'transformer_id': 'tx-001'}}, {'device': {'device_id': 'a'}}):
        assert charger_row(bad) is None
    row = charger_row(dict(device=dict(device_id='ev-9', transformer_id='tx-001', priority=True, controllable='yes'), freshness=None))
    assert row['priority'] is None and row['controllable'] is False and row['freshness'] == 'UNKNOWN' and row['last'] is None and row['rate'] is None


def devices_api(handler_items=None, now=T0):
    calls = []
    def handler(request):
        calls.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith('/devices'):
            return httpx.Response(200, json=dict(items=handler_items if handler_items is not None else [view(), view('ev-002', priority=1), 'junk'], next_cursor=None, freshness='FRESH'))
        return httpx.Response(404)
    fleet, clock = api_data(handler, now=now)
    return fleet, clock, calls


def test_chargers_are_fetched_in_one_page_and_only_while_the_devices_view_is_watched():
    fleet, clock, calls = devices_api()
    run(fleet.poll_devices())                                   # nobody watches: nothing is requested
    assert calls == [] and fleet.chargers == []
    fleet.watch_devices()
    run(fleet.poll_devices())
    assert calls == [('/v1/devices', dict(device_type='EV_CHARGER', limit='200'))]         # one page, no cursor, so the API writes no cursor rows
    assert [c['id'] for c in fleet.chargers] == ['ev-001', 'ev-002']                           # malformed entries are dropped
    clock[0] += 5
    fleet.watch_devices(); run(fleet.poll_devices())
    assert len(calls) == 1                                                                    # every 10 s, not every tick
    clock[0] += 6
    fleet.watch_devices(); run(fleet.poll_devices())
    assert len(calls) == 2
    clock[0] += 31                                                                            # the tab left: the watch expires
    run(fleet.poll_devices())
    assert len(calls) == 2
    run(fleet.poll_devices(force=True))                                                      # opening the tab fetches at once
    assert len(calls) == 3


def test_a_failing_devices_call_keeps_the_last_list_and_reports_it():
    fleet, clock, calls = devices_api()
    fleet.watch_devices(); run(fleet.poll_devices())
    fleet.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    clock[0] += 11; fleet.watch_devices(); run(fleet.poll_devices())
    assert len(fleet.chargers) == 2 and fleet.errors['devices'] == 'unreachable'
    garbage = devices_api(handler_items='nope')[0]
    garbage.watch_devices(); run(garbage.poll_devices())
    assert garbage.chargers == [] and garbage.errors['devices'] == 'unreachable'
    assert run(FleetData('http://p').poll_devices(force=True)) is None


def test_the_run_loop_polls_devices_only_when_watched():
    fleet, clock, calls = devices_api()
    async def loop():
        task = asyncio.ensure_future(fleet.run())
        await asyncio.sleep(.3)
        fleet.stopped = True
        task.cancel()
    run(loop())
    assert [c for c in calls if c[0].endswith('/devices')] == []


def test_transformer_rows_cover_all_columns_filters_and_saga_status():
    from dashboard.control_room import TX_COLUMNS, transformer_rows
    tx = {f'tx-{i:03d}': dict(band='normal', utilization=50.0 + i, net=50.0 + i, ev=10.0, meter=40.0, solar=5.0, capacity=100.0, managed=i <= 5,
                              good=38, estimated=1, missing=1, late=0) for i in range(1, 26)}
    tx['tx-002']['band'] = 'critical'
    actions = [dict(transformer_id='tx-001', status='REDUCTION_CONFIRMED', acknowledged_device_count=7),
               dict(transformer_id='tx-002', status='PARTIALLY_APPLIED', acknowledged_device_count=2)]
    rows = transformer_rows(tx, actions)
    assert len(rows) == 25 and [r['id'] for r in rows] == sorted(tx) and set(rows[0]) == {c['name'] for c in TX_COLUMNS}
    assert [c['label'] for c in TX_COLUMNS] == ['ID', 'Band', 'Utilization %', 'Net kW', 'EV kW', 'Meter kW', 'Solar kW', 'Capacity kW', 'Managed', 'Saga status', 'Throttled', 'Coverage %']
    assert all(c['sortable'] for c in TX_COLUMNS)
    by = {r['id']: r for r in rows}
    assert by['tx-001']['saga'] == 'reduced' and by['tx-001']['throttled'] == 7 and by['tx-002']['saga'] == 'safe hold'
    assert by['tx-003']['saga'] == 'idle' and by['tx-009']['saga'] == '–' and by['tx-009']['throttled'] is None and by['tx-009']['managed'] == 'Monitor-only'
    assert by['tx-001']['coverage'] == 97.5 and by['tx-001']['utilization'] == 51.0 and by['tx-001']['capacity'] == 100
    assert [r['id'] for r in transformer_rows(tx, actions, band='critical')] == ['tx-002']
    assert [r['id'] for r in transformer_rows(tx, actions, query=' TX-02 ')] == [f'tx-02{i}' for i in range(10)][:6]
    assert transformer_rows({}, []) == [] and json.dumps(rows, allow_nan=False)


def test_charger_rows_filter_by_transformer_priority_state_and_search():
    from dashboard.control_room import CHARGER_COLUMNS, charger_counts, charger_rows
    from dashboard.fleet_data import charger_row
    chargers = [charger_row(v) for v in (
        view('ev-001', 'tx-003', 0, history=[command()]), view('ev-002', 'tx-003', 1, history=[command()]), view('ev-003', 'tx-003', 12),
        view('ev-004', 'tx-003', 13, power=0.0, rate=0.0), view('ev-005', 'tx-001', 0))]
    actions = [dict(action_id='a1', transformer_id='tx-003', status='REDUCTION_CONFIRMED')]
    rows = charger_rows(chargers, actions)
    assert [r['id'] for r in rows] == ['ev-001', 'ev-002', 'ev-003', 'ev-004', 'ev-005'] and set(rows[0]) == {c['name'] for c in CHARGER_COLUMNS}
    assert [r['state'] for r in rows] == ['Throttled', 'Throttled', 'Spared', 'Normal', 'Normal']
    assert rows[0]['rate'] == '7.2 / 7.2' and rows[0]['departure'] == '20:30' and rows[0]['minimum'] == 1.4 and rows[0]['last'] == 'APPLIED · 1.4 kW · 18:02:12' and rows[2]['last'] == '–'
    assert rows[0]['freshness'] == 'Fresh'
    assert [r['id'] for r in charger_rows(chargers, actions, transformer='tx-001')] == ['ev-005']
    assert [r['id'] for r in charger_rows(chargers, actions, priority='0')] == ['ev-001', 'ev-005']
    assert [r['id'] for r in charger_rows(chargers, actions, state='Spared')] == ['ev-003']
    assert [r['id'] for r in charger_rows(chargers, actions, query='EV-00')] == ['ev-001', 'ev-002', 'ev-003', 'ev-004', 'ev-005']
    assert [r['id'] for r in charger_rows(chargers, actions, transformer='tx-003', state='Throttled', query='002')] == ['ev-002']
    assert charger_counts(chargers, actions) == dict(Throttled=2, Spared=1, Normal=2) and charger_counts(chargers, []) == dict(Throttled=0, Spared=0, Normal=5)
    assert charger_rows([], actions) == []


def test_history_falls_back_to_prometheus_when_the_loads_api_is_missing_or_behind():
    from collections import deque
    from dashboard.control_room import history_note
    fleet, clock = api_data(lambda r: httpx.Response(200, json=dict(items=[loads_item(-10, 70), loads_item(-5, 70)], next_cursor=None, freshness='STALE')), now=T0)
    fleet.tx = {'tx-003': dict(managed=True), 'tx-009': dict(managed=False)}
    fleet.util_history['tx-003'] = deque((T0 - 25 + 5 * k, 80.0, 80.0, 100.0) for k in range(6))      # five-second Prometheus samples to T0
    fleet.util_history['tx-009'] = deque([(T0, 50.0, 50.0, 100.0)])
    fleet.segments['tx-003'] = [[T0 - 14, None, 20.0]]
    rows, source = fleet.history('tx-003')                            # nothing fetched yet
    assert source == 'prometheus' and len(rows) == 6 and rows[0][2] == 80.0 and rows[-1][2] == 100.0      # shadow added where a window lies inside the segment
    run(fleet.fetch_loads('tx-003'))
    clock[0] = T0 + 120                                               # newest loads window ended 2 minutes ago
    assert fleet.history('tx-003')[1] == 'prometheus' and history_note('prometheus', True, {}) == 'Prometheus history (loads API behind)'
    clock[0] = T0 + 5
    rows, source = fleet.history('tx-003')
    assert source == 'loads' and [r[0] for r in rows] == [T0 - 5, T0] and history_note(source, True, {}) == ''
    fleet.unmanaged_history['tx-003'] = deque([(T0 - 5, 92.0), (T0, 0.0)])
    rows, source = fleet.history('tx-003')
    assert source == 'offered' and [r[2] for r in rows[-3:]] == [None, 92.0, 0.0]
    clock[0] = T0 + 120
    assert fleet.history('tx-003')[1] == 'prometheus'
    assert fleet.history('tx-009') == ([(T0, 50.0, None, 100.0)], 'monitor-only') and history_note('monitor-only', False, {}) == 'Prometheus history (monitor-only)'
    assert fleet.history('tx-404')[1] == 'prometheus' and fleet.history('tx-404')[0] == []
    assert history_note('prometheus', False, {}) == 'Loading history…' and history_note('prometheus', False, {'loads': 'x'}) == 'History unavailable'
    assert history_note('prometheus', True, {'loads': 'x'}) == 'Prometheus history (loads API unavailable)'


def test_production_package_never_references_test_code_and_the_image_context_excludes_it():
    from pathlib import Path
    for path in Path('dashboard').glob('*.py'):
        text = path.read_text()
        assert 'fake_backend' not in text and 'from tests' not in text and 'import tests' not in text, path
    assert '!tests/dashboard' not in Path('.dockerignore').read_text()


# --- review fixes ----------------------------------------------------------------------------------------------------

def test_stepper_does_not_tick_steps_that_never_happened():
    held = saga_steps(saga('NEEDS_OPERATOR_REVIEW', 0, selected=0))                  # plan found no flexible charger: nothing was sent
    assert states(held) == ['done', 'hold'] + ['todo'] * 5 and 'Dispatched (CDC)' in held
    insufficient = saga('NEEDS_OPERATOR_REVIEW', 0, selected=3) | {'reason': 'insufficient flexible capacity; safe hold'}
    assert states(saga_steps(insufficient)) == states(held)
    compensated = saga_steps(saga('RESTORE_DISPATCHED', 0, selected=6, confirmed=0.0))     # a held plan being compensated
    assert states(compensated) == ['done', 'done', 'done', 'skip', 'skip', 'skip', 'active']
    assert 'Acks 0/6' not in compensated and '(0 kW)' not in compensated and 'restoring without a confirmed reduction' in compensated
    assert states(saga_steps(saga('COMPLETED', 2, selected=6, confirmed=8.0))) == ['done'] * 4 + ['skip', 'skip', 'done']
    done = saga_steps(saga('COMPLETED', 0, selected=0, confirmed=0.0))
    assert states(done) == ['done', 'done', 'skip', 'skip', 'skip', 'skip', 'done']
    assert states(saga_steps(saga('COMPLETED', 6, selected=6, confirmed=40.0))) == ['done'] * 7                  # a normal run is unchanged


def test_stepper_caption_names_the_leader_epoch_when_known():
    assert 'leader epoch 229' in saga_steps(saga('REDUCTION_CONFIRMED', 6, confirmed=30.0, leader_epoch=229))
    assert 'leader epoch 229' in saga_steps(saga('TIMED_OUT', 2, leader_epoch=229))
    for bad in (None, True, 'x', 1.5):
        assert 'leader epoch' not in saga_steps(saga('REDUCTION_CONFIRMED', 6, confirmed=30.0, leader_epoch=bad))


def test_spared_waits_until_every_selected_charger_has_a_projected_result():
    from dashboard.control_room import charger_counts, charger_rows
    from dashboard.fleet_data import SETTLED, charger_row, charger_state, throttling_actions
    chargers = [charger_row(view('ev-001', 'tx-003', 0, history=[command()])), charger_row(view('ev-002', 'tx-003', 5)), charger_row(view('ev-003', 'tx-003', 6))]
    for status, spared in (('COMMANDS_DISPATCHED', 0), ('ACKS_PENDING', 0), ('REDUCTION_CONFIRMED', 2), ('RECOVERY_MONITORING', 2), ('RESTORE_DISPATCHED', 0)):
        actions = [dict(action_id='a1', transformer_id='tx-003', status=status)]
        assert charger_counts(chargers, actions)['Spared'] == spared, status
    pending = [dict(action_id='a1', transformer_id='tx-003', status='ACKS_PENDING')]
    assert [r['state'] for r in charger_rows(chargers, pending)] == ['Throttled', 'Normal', 'Normal']
    assert SETTLED == ('REDUCTION_CONFIRMED', 'RECOVERY_MONITORING')
    assert charger_state(chargers[1], throttling_actions(pending), throttling_actions(pending, SETTLED)) == 'Normal'


def test_rush_target_keeps_a_quiet_current_target_and_moves_only_when_it_gets_busy():
    tx = {f'tx-00{i}': dict(managed=True, incident=False, band='normal', utilization=u) for i, u in enumerate((60, 40, 97, 55, 70), 1)}
    assert rush_target(tx, current='tx-005') == 'tx-005'                                 # quiet: keep, even though tx-002 is lighter
    tx['tx-005']['incident'] = True
    assert rush_target(tx, current='tx-005') == 'tx-002'                                 # busy: move to the least loaded quiet one
    assert rush_target(tx, current='tx-009') == 'tx-002' and rush_target(tx) == 'tx-002'


def test_kpis_stop_growing_while_the_actions_api_fails():
    def handler(request):
        if fail[0]:
            return httpx.Response(503)
        return httpx.Response(200, json=dict(items=[action(0, 'REDUCTION_CONFIRMED', 44.0)], next_cursor=None, freshness='STALE'))
    fail = [False]
    fleet, clock = api_data(handler, now=T0 + 10)
    run(fleet.poll_api())
    assert fleet.actions_freshness == 'STALE' and fleet.api_good_at == T0 + 10 and 'api' not in fleet.errors
    grown = fleet.kpis(now=T0 + 100)
    fail[0] = True
    clock[0] = T0 + 100
    run(fleet.poll_api())
    assert fleet.errors['api'] == 'unreachable' and fleet.actions                         # last good rows are kept
    frozen = fleet.kpis()
    assert frozen['shed_kw'] == 44.0 and round(frozen['kwh_shifted'], 6) == round(44 * 10 / 3600, 6) < grown['kwh_shifted']      # clock stopped at the last good poll
    clock[0] = T0 + 500
    assert fleet.kpis() == frozen
    tiles = {t['key']: t for t in kpi_tiles(FleetData('http://p').summary(), {}, [], frozen, api_down=True)}
    assert 'unreachable' in tiles['throttled']['sub'] and 'unreachable' in tiles['peak']['sub'] and 'unreachable' in tiles['energy']['sub']
    fail[0] = False
    run(fleet.poll_api())
    assert 'api' not in fleet.errors and fleet.api_good_at == T0 + 500


def test_alerts_are_no_longer_fetched():
    paths = []
    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json=dict(items=[], next_cursor=None, freshness='FRESH'))
    fleet, _ = api_data(handler)
    run(fleet.poll_api())
    assert paths == ['/v1/actions'] and not hasattr(fleet, 'alerts')


def test_command_lines_only_while_commands_travel_and_tooltips_escape_names():
    for status, lines in (('PLAN_CREATED', 0), ('COMMANDS_DISPATCHED', 1), ('ACKS_PENDING', 1), ('REDUCTION_CONFIRMED', 0), ('RECOVERY_MONITORING', 0),
                          ('RESTORE_DISPATCHED', 1), ('COMPLETED', 0)):
        option = charts.topology(fleet(), actions=[a_action('tx-001', status, 2)])
        assert len(option['series'][2]['data']) == lines, status
    hostile = {'<img src=x onerror=alert(1)>': dict(utilization=50, net=50, capacity=100, band='normal', managed=True)}
    option = charts.topology(hostile)
    tips = ' '.join(n['tooltip']['formatter'] for n in option['series'][4]['data'] if 'tooltip' in n and 'formatter' in n['tooltip'])
    assert '<img' not in tips and '&lt;img' in tips


def test_solar_label_names_zero_output_and_monitor_only_detail_has_no_shadow_series():
    dark = charts.topology(fleet())
    assert next(n for n in dark['series'][4]['data'] if n['name'] == 'Grid + Solar')['label']['formatter'].endswith('Solar 250 kW')
    no_solar = {n: dict(t, solar=0.0) for n, t in fleet().items()}
    assert next(n for n in charts.topology(no_solar)['series'][4]['data'] if n['name'] == 'Grid + Solar')['label']['formatter'].endswith('Solar 0 kW (no output)')
    rows = [(t, 60, None, 100) for t in range(1000, 1300, 5)]
    managed, monitored = charts.transformer_detail(rows, [], 'tx-1'), charts.transformer_detail(rows, [dict(transformer_id='tx-9', status='COMPLETED', transition_time='1970-01-01T00:20:00Z')], 'tx-9', managed=False)
    assert [x['name'] for x in managed['series']] == ['Managed load', 'Without throttling']
    assert [x['name'] for x in monitored['series']] == ['Load'] and 'monitor-only' in monitored['title']['text']
    assert all('xAxis' not in m for m in monitored['series'][0]['markLine']['data'])          # no action markers either


def test_kafka_stage_shows_the_ingested_total():
    html = flow_html(dict(counts=dict(critical=0)), dict(kafka_in=200, telemetry_total=12_345_678), 0)
    assert '12.35 M events ingested' in html and '– events ingested' in flow_html(dict(counts=dict(critical=0)), {}, 0)


def test_correctness_scalars_use_five_minute_window_and_partition_metrics():
    from dashboard.fleet_data import LAG, PARTITION_LAG, PARTITION_RATE, SCALARS, SUBTASK_RATE
    assert 'increase(' in SCALARS['accepted_late'] and '[5m]' in SCALARS['accepted_late']
    assert 'watermark_delay_ms' in SCALARS['watermark_delay_ms']
    assert 'dlq' in LAG and 'partition' in PARTITION_RATE and 'partition' in PARTITION_LAG and 'subtask_index' in SUBTASK_RATE
    bar = charts.lag_bars({'integration-kind.projection-telemetry': 10, 'integration-kind.projection': 0,
                           'integration-kind.action.control.v1': 2, 'agg-fleet-v1-aggregation-v1': 5})
    assert bar['yAxis']['data'] == ['Flink aggregation', 'Action', 'Projection', 'Projection telemetry']
