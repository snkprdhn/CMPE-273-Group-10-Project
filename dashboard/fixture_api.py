"""Explicit, disposable UI fixture server. No Kafka, SQL or control-service imports."""
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from dashboard.client import stamp, validate
from jsonschema import ValidationError

BASE = json.loads(Path(__file__).with_name('fixtures.json').read_text())
STEPS = [('Normal load', 'NORMAL', 72, None), ('Overload detected', 'CRITICAL', 96, 'PLAN_CREATED'),
         ('Throttling confirmed', 'NORMAL', 75, 'REDUCTION_CONFIRMED'),
         ('Recovery and restore', 'NORMAL', 72, 'COMPLETED')]


def fixture_error(status):
    code = {400:'INVALID_ARGUMENT', 401:'UNAUTHENTICATED', 403:'FORBIDDEN', 404:'NOT_FOUND',
            409:'CONFLICT', 422:'DOMAIN_INVALID'}.get(status, 'DEPENDENCY_UNAVAILABLE')
    return JSONResponse({'error': {'code': code, 'message': 'Fixture request failed',
                                  'request_id': 'fixture', 'details': []}}, status_code=status)


class Fixture:
    def __init__(self):
        self.step = 0
        self.fault = ''
        self.version = 0
        self.disconnect = 0
        self.gaps = 0
        self.active = None
        self.acknowledged = False
        self.operations = {}
        self.history = []
        self.changed()

    def changed(self):
        self.version += 1
        self.time = datetime.now(timezone.utc).replace(microsecond=0)
        transformers = self.payload()['transformers']
        if transformers:
            self.history.append(transformers[0]['load'])
        self.history = self.history[-200:]

    def payload(self):
        data = deepcopy(BASE)
        title, state, power, action = STEPS[self.step]
        t = data['transformers'][0]
        t.update(state=state, freshness='FRESH', data_age_seconds=1)
        end = self.time.replace(second=self.time.second//5*5)
        t['load'].update(window_start=stamp(end-timedelta(seconds=5)), window_end=stamp(end),
                         window_id=f'fixture-window-{self.version}', net_load_kw=power, utilization_pct=power,
                         meter_demand_kw=46 if self.step in (0, 3) else 70,
                         ev_demand_kw=7 if self.step == 2 else 28)
        if action:
            data['actions'][0].update(status=action, reason=title, transition_time=stamp(self.time),
                                      confirmed_reduction_kw=21 if self.step >= 2 else 0,
                                      acknowledged_device_count=1 if self.step >= 2 else 0)
            data['alerts'][0].update(status='RESOLVED' if self.step >= 2 else 'ACKNOWLEDGED' if self.acknowledged else 'OPEN',
                                    opened_at=stamp(self.time), message='EV demand exceeded the published critical threshold')
        else:
            data['actions'] = []
            data['alerts'] = []
        d = data['devices'][0]
        d['device']['updated_at'] = stamp(self.time)
        d['latest_telemetry'].update(power_kw=7 if self.step == 2 else 28, current_rate_kw=7 if self.step == 2 else 28)
        if self.step < 2:
            d['command_history'] = []
        else:
            d['command_history'][0]['result_time'] = stamp(self.time)
            if self.step == 3:
                d['command_history'].append(dict(command_id='restore-001', action_id='action-001', device_id='ev-001',
                    outcome='APPLIED', previous_rate_kw=7, applied_rate_kw=28, result_time=stamp(self.time)))
        data['loads'] = deepcopy(self.history) if self.history else [t['load']]
        if self.fault in ('stale', 'partial'):
            t['freshness'] = self.fault.upper()
            if self.fault == 'stale':
                t.update(state='STALE', data_age_seconds=45)
        if self.fault == 'empty':
            for name in data:
                data[name] = []
        return data


def create_fixture_app():
    app = FastAPI()
    state = Fixture()
    app.state.fixture = state

    @app.middleware('http')
    async def boundary(request, call_next):
        if request.url.path.startswith('/api/v1') and request.url.path not in ('/api/v1/health', '/api/v1/ready'):
            token = request.headers.get('Authorization', '')
            code = 403 if state.fault == 'denied' else 503 if state.fault == 'unavailable' else 500 if state.fault == 'error' else None
            if token not in ('Bearer fixture-operator-token', 'Bearer fixture-viewer-token'):
                code = 401
            if request.method == 'POST' and token == 'Bearer fixture-viewer-token':
                code = 403
            if code:
                return fixture_error(code)
            if state.fault == 'loading':
                await asyncio.sleep(2)
        response = await call_next(request)
        response.headers['X-Event-ID'] = f'fixture:{state.version}'
        response.headers['X-Snapshot-At'] = stamp(datetime.now(timezone.utc))
        return response

    @app.post('/fixture')
    async def configure(request: Request):
        body = await request.json()
        if body.get('clear_history'):
            state.history.clear()
            state.active = None
            state.acknowledged = False
            state.operations.clear()
        if 'step' in body:
            state.step = int(body['step']) % 4
        state.fault = body.get('fault', '')
        if body.get('gap'):
            state.gaps += 1
        if body.get('disconnect'):
            state.disconnect += 1
        state.changed()
        return {'step': state.step, 'version': state.version}

    @app.get('/api/v1/events')
    async def events(request: Request):
        async def stream():
            cursor = request.headers.get('last-event-id', '')
            disconnect = state.disconnect
            gap = state.gaps
            while True:
                if disconnect != state.disconnect or state.fault in ('unavailable', 'error', 'denied'):
                    return
                if gap != state.gaps:
                    event = dict(cursor=f'fixture:{state.version}', event_type='snapshot.required', entity_id='system',
                                 version=state.version, event_time=stamp(state.time), payload={'status':'degraded','observed_at':stamp(state.time)})
                    yield f'id: {event["cursor"]}\nevent: snapshot.required\ndata: {json.dumps(event)}\n\n'
                    return
                if cursor != f'fixture:{state.version}':
                    cursor = f'fixture:{state.version}'
                    event = dict(cursor=cursor, event_type='transformer.updated', entity_id='tx-001',
                                 version=state.version, event_time=stamp(state.time), payload=state.payload()['transformers'][0] if state.fault != 'empty' else BASE['transformers'][0])
                    yield f'id: {cursor}\nevent: transformer.updated\ndata: {json.dumps(event)}\n\n'
                else:
                    yield ': heartbeat\n\n'
                await asyncio.sleep(.2)
        return StreamingResponse(stream(), media_type='text/event-stream')

    @app.post('/api/v1/{path:path}')
    async def mutate(path: str, request: Request):
        key = request.headers.get('idempotency-key')
        body = await request.body()
        fingerprint = (path, body)
        if not key:
            return fixture_error(400)
        if key in state.operations:
            prior, value, status = state.operations[key]
            return JSONResponse(value, status_code=status) if prior == fingerprint else fixture_error(409)
        status = 200
        if path == 'scenario-runs':
            try:
                config = json.loads(body)
                validate('ScenarioStart', config)
            except (ValueError, ValidationError):
                return fixture_error(422)
            status = 202
            if config.get('scenario') not in ('normal', 'evening_overload', 'late_correction', 'leader_failover') or state.active:
                return fixture_error(409)
            state.active = dict(run_id='fixture-run', configuration=config, status='RUNNING', updated_at=stamp(state.time))
            value = state.active
        elif path == 'scenario-runs/fixture-run/stop' and state.active:
            value = dict(state.active, status='STOPPED')
            state.active = None
        elif path == 'alerts/alert-001/acknowledge' and state.step > 0:
            state.acknowledged = True
            value = state.payload()['alerts'][0]
        else:
            return fixture_error(404)
        state.operations[key] = (fingerprint, deepcopy(value), status)
        state.changed()
        return JSONResponse(value, status_code=status)

    @app.get('/api/v1/{path:path}')
    async def snapshot(path: str):
        data = state.payload()
        if path in ('health', 'ready'):
            return {'status': 'ok', 'observed_at': stamp(state.time)}
        if path == 'overview':
            ts = data['transformers']
            return dict(transformer_count=len(ts), critical_count=sum(t['state'] == 'CRITICAL' for t in ts),
                        total_net_load_kw=sum(t['load']['net_load_kw'] for t in ts), highest_risk=ts,
                        freshness=ts[0]['freshness'] if ts else 'UNKNOWN')
        if path == 'scenarios':
            return {'scenarios': ['normal', 'evening_overload', 'late_correction', 'leader_failover'], 'active_run': state.active}
        parts = path.split('/')
        if path.endswith('/loads'):
            items = data['loads']
        elif len(parts) == 2 and parts[0] in ('transformers', 'devices', 'actions'):
            field = {'transformers': 'transformer_id', 'devices': 'device_id', 'actions': 'action_id'}[parts[0]]
            return next((i for i in data[parts[0]] if (i.get('device') or i)[field] == parts[1]), fixture_error(404))
        else:
            if path not in data:
                return fixture_error(404)
            items = data[path]
        return {'items': items, 'next_cursor': None, 'freshness': data['transformers'][0]['freshness'] if data['transformers'] else 'UNKNOWN'}
    return app


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(create_fixture_app(), host='127.0.0.1', port=18085)
