"""Disposable stand-in for Prometheus, the REST API and the fleet control port, for the /live browser tests.

Synthetic data only: 25 transformers, tx-001..tx-005 are managed, tx-003 is overloaded and has an active action.
Every request is recorded and readable at GET /_log so tests can assert what the dashboard sent.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NAMES = [f'tx-{i:03d}' for i in range(1, 26)]
CAPACITY = 100.0
NET = {name: 40.0 + (i * 7) % 45 for i, name in enumerate(NAMES)}
NET['tx-003'] = 97.0
NET['tx-012'] = 91.0
MANAGED = NAMES[:5]
OVERLOAD_KW = 120.0       # net load of a transformer during a forced overload
THROTTLE_KW = 44.0        # confirmed reduction the fake Action service achieves
CHARGERS, PER_TX = 70, 14   # chargers in the fleet and per hero transformer
THROTTLED = 10               # lowest-priority chargers the fake Action service throttles
NORMAL_RATE, THROTTLED_RATE = 7.2, 1.4
CONFIRM_AFTER = 8.0       # seconds from the overload to the confirmed reduction (window latency + acks)


def vector(rows):
    return {'status': 'success', 'data': {'resultType': 'vector', 'result': [
        {'metric': labels, 'value': [1, str(value)]} for labels, value in rows]}}


def by(values):
    return vector([({'transformer': name}, value) for name, value in values.items()])


def stamp(moment):
    return moment.strftime('%Y-%m-%dT%H:%M:%SZ')


def window_id(tx, end):
    """Stable id of the 5 s window that ends at `end` (epoch seconds), so an action can name its trigger window."""
    return f'{tx}-{int(end) // 5 * 5}'


def create_app(clock=time.time):
    app = FastAPI()
    log = []
    forced = {}               # transformer -> (start, seconds), set by POST /overload like the real fleet control port

    def phase(tx, t):
        """None, or (age, seconds) while `tx` is inside a forced overload at time `t`."""
        start = forced.get(tx)
        return (t - start[0], start[1]) if start and start[0] <= t < start[0] + start[1] else None

    def net_at(tx, t):
        """Load the dashboard would see: overloaded, then throttled once the reduction is confirmed."""
        p = phase(tx, t)
        if p is None:
            return NET.get(tx, 50.0)
        return OVERLOAD_KW if p[0] < CONFIRM_AFTER else OVERLOAD_KW - THROTTLE_KW
    delay = {'loads': 0.0}
    actions_available = True
    offered_available = False

    @app.middleware('http')
    async def record(request: Request, call_next):
        if request.url.path != '/_log':
            body = (await request.body()).decode() if request.method == 'POST' else ''
            log.append(dict(method=request.method, path=request.url.path, query=str(request.url.query), body=body))
        return await call_next(request)

    @app.get('/_log')
    async def get_log():
        return log

    @app.post('/_delay')
    async def set_delay(seconds: float):
        delay['loads'] = seconds
        return delay

    @app.post('/_reset')
    async def reset():
        nonlocal actions_available, offered_available
        forced.clear()
        delay['loads'] = 0.0
        actions_available = True
        offered_available = False
        return {'ok': True}

    @app.post('/_actions_available')
    async def set_actions_available(available: bool):
        nonlocal actions_available
        actions_available = available
        return {'available': available}

    @app.post('/_enable_offered')
    async def enable_offered():
        nonlocal offered_available
        offered_available = True
        return {'ok': True}

    @app.get('/healthz')
    async def healthz():
        return {'ok': True}

    @app.get('/api/v1/query')
    async def query(query: str):
        if 'devices_fleet_unmanaged_kw' in query:
            return by({'tx-003': 120.0} if offered_available else {})
        if 'signal="net_load_kw"' in query:
            return by({n: net_at(n, clock()) for n in NAMES})
        if 'signal="capacity_kw"' in query:
            return by({n: CAPACITY for n in NAMES})
        if 'signal="ev_demand_kw"' in query:
            return by({n: net_at(n, clock()) * .4 for n in NAMES})
        if 'signal="meter_demand_kw"' in query:
            return by({n: net_at(n, clock()) * .6 for n in NAMES})
        if 'signal="solar_generation_kw"' in query:
            return by({n: 0.0 for n in NAMES})
        if 'devices_fleet_overload_active' in query:
            return by({n: 1.0 if n == 'tx-003' or phase(n, clock()) else 0.0 for n in MANAGED})
        if 'kind="good"' in query:
            return by({n: 38 for n in NAMES})
        if 'kind="estimated"' in query:
            return by({n: 2 for n in NAMES})
        if 'kind="missing"' in query or 'kind="late"' in query:
            return by({n: 0 for n in NAMES})
        if 'output_age_ms' in query or 'latest_output_ms' in query:
            return vector([({}, 6000)])
        if 'devices_fleet_controllable_chargers' in query:
            return vector([({}, 70)])
        if 'devices_fleet_devices' in query:
            return vector([({}, 1000)])
        if 'devices_fleet_events_total' in query:
            return vector([({}, 200)])
        if 'watermark_delay_ms' in query:
            return vector([({}, 10100)])
        if 'flink_taskmanager_job_task_operator_input' in query:
            return vector([({'subtask_index': '0'}, 133), ({'subtask_index': '1'}, 67)])
        if 'busyTimeMsPerSecond' in query:
            return vector([({}, 200)])
        if 'operator_dlq' in query:
            return vector([])
        if 'kafka_topic_partition_current_offset' in query and 'oldest' not in query:
            if 'by (partition)' in query:
                return vector([({'partition': str(p)}, 66.0) for p in range(3)])
            return vector([({}, 198 if 'rate(' in query else 12_345_678)])
        if 'consumergroup' in query:
            if 'by (partition)' in query:
                return vector([({'partition': str(p)}, 10) for p in range(3)])
            return vector([({'consumergroup': 'agg-fleet-v1-aggregation-v1'}, 40)])
        if 'kubelet_volume' in query:
            return vector([({'persistentvolumeclaim': 'data-postgres-0'}, 12.0)])
        return vector([({}, 1)])

    @app.get('/api/v1/query_range')
    async def query_range():
        return {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}

    def action_items():
        now = datetime.now(timezone.utc)
        item = lambda key, tx, status, at, kw, acked, trigger: dict(
            action_id=key, transformer_id=tx, status=status, trigger_window_id=trigger, trigger_revision=0, policy_version=1,
            leader_epoch=1, target_reduction_kw=46.0, expected_reduction_kw=46.0, confirmed_reduction_kw=kw, selected_device_count=THROTTLED,
            acknowledged_device_count=acked, reason='fixture', transition_time=stamp(at))
        items = [item('action-1', 'tx-003', 'REDUCTION_CONFIRMED', now - timedelta(seconds=30), 44.0, THROTTLED, window_id('tx-003', now.timestamp() - 40)),
                 item('action-2', 'tx-002', 'COMPLETED', now - timedelta(seconds=900), 40.0, THROTTLED, window_id('tx-002', now.timestamp() - 1000))]
        t = clock()
        for tx, (start, seconds) in forced.items():
            begin = datetime.fromtimestamp(start, timezone.utc)
            trigger = window_id(tx, start + 7)                      # the first window that sees the overload
            if t < start + CONFIRM_AFTER:
                items.append(item(f'rush-{tx}-{int(start)}', tx, 'COMMANDS_DISPATCHED', begin + timedelta(seconds=2), 0.0, 0, trigger))
            elif t < start + seconds:
                items.append(item(f'rush-{tx}-{int(start)}', tx, 'REDUCTION_CONFIRMED', begin + timedelta(seconds=CONFIRM_AFTER), THROTTLE_KW, THROTTLED, trigger))
            else:
                items.append(item(f'rush-{tx}-{int(start)}', tx, 'COMPLETED', begin + timedelta(seconds=seconds), THROTTLE_KW, THROTTLED, trigger))
        return items

    @app.get('/api/v1/actions')
    async def actions():
        if not actions_available:
            return JSONResponse({'error': 'actions unavailable'}, status_code=503)
        return {'items': action_items(), 'next_cursor': None, 'freshness': 'FRESH'}

    @app.get('/api/v1/devices')
    async def devices(device_type: str = '', limit: int = 50, cursor: str = ''):
        """The 70 managed chargers, 14 per hero transformer: priority 0 is throttled first, 13 is idle, 12 reports invalid telemetry.

        While a transformer's reduction is confirmed its ten lowest-priority chargers carry an applied command of that
        action, so the others are the ones the priority rules spared. One page, like the real API for this fleet."""
        if device_type != 'EV_CHARGER' or not 1 <= limit <= 200 or cursor:
            return JSONResponse({'error': 'device_type=EV_CHARGER and limit 1..200 only'}, status_code=400)
        now = datetime.now(timezone.utc).replace(microsecond=0)
        confirmed = {a['transformer_id']: a for a in action_items() if a['status'] == 'REDUCTION_CONFIRMED'}
        views = []
        for i in range(CHARGERS):
            tx, j = MANAGED[i // PER_TX], i % PER_TX
            key, action = f'ev-{i + 1:03d}', confirmed.get(tx)
            throttled = bool(action) and j < THROTTLED
            rate = 0.0 if j == PER_TX - 1 else THROTTLED_RATE if throttled else NORMAL_RATE
            history = [dict(command_id=f'cmd-{key}', action_id=action['action_id'], device_id=key, outcome='APPLIED', previous_rate_kw=NORMAL_RATE,
                            applied_rate_kw=THROTTLED_RATE, result_time=action['transition_time'])] if throttled else []
            views.append(dict(
                device=dict(device_id=key, transformer_id=tx, device_type='EV_CHARGER', controllable=True, minimum_rate_kw=THROTTLED_RATE,
                            maximum_rate_kw=NORMAL_RATE, priority=j, departure_time=stamp(now + timedelta(hours=1 + j)), capability_version=1, updated_at=stamp(now)),
                freshness='STALE' if j == PER_TX - 2 else 'FRESH',
                latest_telemetry=dict(device_id=key, transformer_id=tx, device_type='EV_CHARGER', boot_id='boot', power_kw=rate,
                                      quality='INVALID' if j == PER_TX - 2 else 'GOOD',
                                      sequence_number=1, current_rate_kw=rate),
                command_history=history))
        return {'items': views, 'next_cursor': None, 'freshness': 'FRESH'}

    @app.get('/api/v1/alerts')
    async def alerts():
        return {'items': [], 'next_cursor': None, 'freshness': 'FRESH'}

    @app.get('/api/v1/transformers/{tx}/loads')
    async def loads(tx: str):
        await asyncio.sleep(delay['loads'])
        end = datetime.now(timezone.utc).replace(microsecond=0)
        end -= timedelta(seconds=end.second % 5)
        items = []
        for k in range(60, 0, -1):
            window_end = end - timedelta(seconds=5 * (k - 1))
            items.append(dict(transformer_id=tx, window_id=window_id(tx, window_end.timestamp()), window_start=stamp(window_end - timedelta(seconds=5)),
                              window_end=stamp(window_end), revision=0, net_load_kw=net_at(tx, window_end.timestamp()), capacity_kw=CAPACITY))
        return JSONResponse(dict(items=items, next_cursor=None, freshness='FRESH'))

    @app.post('/overload')
    async def overload(request: Request):
        body = await request.json()
        seconds = body.get('seconds')
        if body.get('transformer') not in MANAGED or isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not 30 <= seconds <= 180:
            return JSONResponse({'error': 'transformer must be a managed one and seconds 30..180'}, status_code=400)
        forced[body['transformer']] = (clock(), float(seconds))
        return {'overloads': {k: round(v[0] + v[1] - clock(), 1) for k, v in forced.items()}}

    @app.post('/inject')
    async def inject(request: Request):
        body = await request.json()
        if body.get('kind') not in ('out_of_order', 'duplicates', 'very_late', 'cloud_cover') or not 5 <= body.get('seconds', 0) <= 300:
            return JSONResponse({'error': 'invalid injection kind or duration'}, status_code=400)
        return {'ok': True}

    return app
