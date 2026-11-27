"""Control-room data layer and chart builders, without a browser or a cluster."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json

import httpx

from dashboard import control_charts as charts
from dashboard.control_room import feed_rows, reduction
from dashboard.fleet_data import FleetData, band, by_label, number, scalar


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
    util = [(t, 50 + (t % 50), 60, 100) for t in range(1000, 1600, 5)]
    actions = [dict(transformer_id='tx_002', status='REDUCTION_CONFIRMED', transition_time='1970-01-01T00:20:00Z')]
    options = [charts.fleet_load(history), charts.heat_map(fleet.tx, 'tx_002'), charts.top_loaded(fleet.tx),
               charts.transformer_detail(util, actions, 'tx_002'), charts.throughput([(t, 1, 2, 3, 4, 5) for t in range(10)]),
               charts.lag_bars({'integration-kind.projection': 20, 'agg-fleet-v1-aggregation-v1': 3}), charts.gauge(40, 'busy'),
               charts.volumes({'data-postgres-0': 30, 'other': 1}), charts.spark(history, 1, '#fff')]
    for option in options:
        json.dumps(option, allow_nan=False)
    assert [s['name'] for s in options[0]['series']] == ['Households', 'EV charging', 'Solar generation', 'Net load', 'Capacity']
    assert all(point[1] <= 0 for point in options[0]['series'][2]['data'])          # generation plots below zero
    heat = options[1]['series'][0]['data']
    assert len(heat) == 3 and heat[1]['name'] == 'tx_002' and heat[1]['itemStyle']['borderColor'] == '#ffffff'
    assert options[2]['yAxis']['data'][-1] == 'tx_002'                              # most loaded last, drawn on top
    marks = options[3]['series'][0]['markLine']['data']
    assert any(m.get('xAxis') == 1200_000 for m in marks)
    assert options[7]['yAxis']['data'] == ['PostgreSQL']


def test_feed_escapes_and_formats_every_stage():
    rows = [dict(transformer_id='<b>x</b>', status='COMPLETED', transition_time='2026-11-26T21:00:00Z', target_reduction_kw=34.2,
                 confirmed_reduction_kw=0, acknowledged_device_count=6, selected_device_count=6),
            dict(transformer_id='tx-001', status='ACKS_PENDING', transition_time='2026-11-26T21:00:05Z', target_reduction_kw=30,
                 confirmed_reduction_kw=12.5, acknowledged_device_count=3, selected_device_count=7)]
    html = feed_rows(rows)
    assert '<b>x</b>' not in html and '&lt;b&gt;x&lt;/b&gt;' in html
    assert '34.2 kW shed' in html and '30.0 → 12.5 kW' in html and '3/7' in html
    assert reduction(rows[0]) == '34.2 kW shed'
    assert 'No actions yet' in feed_rows([])
