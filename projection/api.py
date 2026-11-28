"""Standalone REST/SSE operations boundary with server-side authorization."""
import asyncio
from datetime import timedelta
import json
import re
import secrets
import time
from uuid import uuid4

import anyio
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from jsonschema import ValidationError
import psycopg
from psycopg.types.json import Jsonb
from starlette.exceptions import HTTPException

from observability import HTTPMiddleware, boundary, sse_reset

from projection.contracts import ApiError, BUNDLE, FORMATS, OPENAPI, TOPICS, instant, stamp, utc, validate
from projection.operations import ContractMocks
import projection.store as store_module
from projection.store import freshness

ROLES = ['Viewer', 'Operator', 'Administrator']


def frame(event):
    return f"id: {event['cursor']}\nevent: {event['event_type']}\ndata: {json.dumps(event, allow_nan=False)}\n\n"


def create_app(store, identities=None, mocks=None, oidc=None, *, live=False):
    if identities is None and oidc is None:
        from identity.auth import OIDC
        oidc = OIDC()
    if identities is not None and (not identities or oidc is not None):
        raise ValueError('Choose OIDC or explicit nonempty mock identities')
    for token, actor in (identities or {}).items():
        if not token or len(token) < 16:
            raise ValueError('Mock tokens must have at least 16 characters')
        validate('Id', actor['actor'])
        validate('Role', actor['role'])
    if mocks is None and not live:
        mocks = ContractMocks()
    app = FastAPI(title='Operations API', docs_url=None,
                  redoc_url=None, openapi_url=None)

    app.add_middleware(HTTPMiddleware)

    @app.middleware('http')
    async def request_context(request, call_next):
        request.state.request_id = uuid4().hex
        response = await call_next(request)
        response.headers['X-Request-ID'] = request.state.request_id
        response.headers['Cache-Control'] = 'no-store'
        return response

    @app.exception_handler(ApiError)
    async def api_error(request, exc):
        return JSONResponse(exc.body(request.state.request_id), status_code=exc.status)

    @app.exception_handler(ValidationError)
    @app.exception_handler(ValueError)
    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        return JSONResponse(ApiError(400, 'Invalid request').body(request.state.request_id), status_code=400)

    @app.exception_handler(psycopg.Error)
    async def dependency(request, exc):
        return JSONResponse(ApiError(503, 'Projection store unavailable').body(request.state.request_id), status_code=503)

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        code = exc.status_code if exc.status_code in ApiError.CODES else 400
        return JSONResponse(ApiError(code, 'Route or method unavailable').body(request.state.request_id), status_code=code)

    def authorize(request, minimum):
        authorization = request.headers.get('authorization', '')
        token = authorization[7:] if authorization.startswith('Bearer ') else ''
        if oidc is not None:
            from identity.auth import Denied, require
            try:
                return require(oidc.authenticate(token), minimum)
            except Denied as exc:
                raise ApiError(exc.status, exc.message) from exc
        actor = next((value for key, value in identities.items() if secrets.compare_digest(key.encode('utf-8'), token.encode('utf-8'))), None)
        if not actor:
            raise ApiError(401, 'Local mock bearer identity required')
        if ROLES.index(actor['role']) < ROLES.index(minimum):
            raise ApiError(403, 'Insufficient role')
        return actor

    def parameters(request, template, method):
        spec = OPENAPI['paths'][template][method]
        allowed = {p['name'] for p in spec['parameters'] if p['in'] == 'query'}
        if set(request.query_params) - allowed or any(len(request.query_params.getlist(k)) > 1 for k in allowed):
            raise ApiError(400, 'Unknown or repeated query parameter')
        from jsonschema import Draft202012Validator
        query = dict(request.query_params)
        for p in spec['parameters']:
            if p['in'] != 'query':
                continue
            value = query.get(p['name'])
            if value is None:
                if p.get('required'):
                    raise ApiError(400, 'Missing ' + p['name'])
                continue
            if p['schema'].get('type') == 'integer':
                value = int(value)
            schema = dict(p['schema'])
            if '$ref' in schema:
                schema['$ref'] = schema['$ref'].removeprefix('bundle.json')
            Draft202012Validator(dict(schema, **{'$defs': BUNDLE['$defs']}),
                                 format_checker=FORMATS).validate(value)
            query[p['name']] = value
        if 'start' in query and instant(query['start']) >= instant(query['end']):
            raise ApiError(422, 'Time range must be positive')
        return query

    def snapshot(request, template, entity=None):
        authorize(request, 'Viewer')
        q = parameters(request, template, 'get')
        with store.readonly(), store.transaction() as db:
            now = utc()
            cursor = store.cursor(db)
            scope = json.dumps([template, entity, {k: v for k, v in q.items() if k != 'cursor'}], sort_keys=True)
            if q.get('cursor'):
                row = db.execute('SELECT scope,response,cursor,created_at FROM projection_pages '
                                 'WHERE token=%s AND created_at > %s',
                                 (q['cursor'], now - timedelta(minutes=5))).fetchone()
                if not row or row[0] != scope:
                    raise ApiError(409, 'Page cursor expired or does not match query; reload snapshot')
                return JSONResponse(row[1], headers={'X-Event-ID': row[2], 'X-Snapshot-At': stamp(row[3])})
            if template == '/overview':
                ts = store.transformer_views(db, now)
                result = dict(transformer_count=len(ts), critical_count=sum(t['state'] == 'CRITICAL' for t in ts),
                              total_net_load_kw=sum(t['load']['net_load_kw'] for t in ts),
                              highest_risk=sorted(ts, key=lambda t: -(t['load']['utilization_pct'] or 0))[:20],
                              freshness=freshness([t['freshness'] for t in ts] + (['PARTIAL'] if store.incomplete_db(db) else [])))
            elif template == '/scenarios':
                runs = store._context(db, 'mock_scenario')
                result = dict(scenarios=BUNDLE['$defs']['ScenarioStart']['properties']['scenario']['enum'],
                              active_run=next((r for r in runs if r['status'] == 'RUNNING'), None))
            elif template == '/devices/{id}':
                result = store.device_view(db, entity, now)
                if result is None:
                    raise ApiError(404, 'Entity not found')
                return JSONResponse(result, headers={'X-Event-ID': cursor, 'X-Snapshot-At': stamp(now)})
            else:
                collection = template.split('/')[1]
                transformers = None
                if template.endswith('/loads'):
                    if not store._context(db, 'transformers', entity_id=entity):
                        raise ApiError(404, 'Transformer not found')
                    items = [p for p in store.load_views(db, transformer_id=entity)
                             if instant(q['start']) <= instant(p['window_start']) < instant(q['end'])]
                    items.sort(key=lambda p: (instant(p['window_start']), p['window_id'], p['revision']))
                elif collection == 'transformers':
                    items = store.transformer_views(db, now, transformer_id=entity)
                elif collection == 'devices':
                    items = store.device_views(db, now)
                elif collection == 'alerts':
                    items = store.alert_views(db, alert_id=entity)
                else:
                    items = store.action_views(db)
                if entity and not template.endswith('/loads'):
                    field = {'transformers': 'transformer_id', 'actions': 'action_id'}[collection]
                    result = next((i for i in items if i[field] == entity), None)
                    if result is None:
                        raise ApiError(404, 'Entity not found')
                    return JSONResponse(result, headers={'X-Event-ID': cursor, 'X-Snapshot-At': stamp(now)})
                for key, value in q.items():
                    if key in ('limit', 'cursor', 'start', 'end'):
                        continue
                    items = [i for i in items if (i['device'] if collection == 'devices' else i).get(key) == value]
                if collection == 'actions':
                    # Newest transitions first so page 1 (?limit=200) includes live/recent incidents even with 24h history.
                    items.sort(key=lambda a: (instant(a['transition_time']), a['action_id']), reverse=True)
                limit = q.get('limit', 50)
                fresh_seconds = store_module.FRESH_SECONDS
                def observed_freshness(item):
                    if 'freshness' in item:
                        return item['freshness']
                    observed = item.get('window_end') or item.get('transition_time') or item.get('resolved_at') or item.get('opened_at')
                    if not observed:
                        return 'UNKNOWN'
                    if (now-instant(observed)).total_seconds() > fresh_seconds or item.get('completeness') == 'STALE':
                        return 'STALE'
                    return 'PARTIAL' if item.get('completeness') == 'PARTIAL' else 'FRESH'
                if collection == 'actions':
                    transformers = store.transformer_views(db, now)
                    if transformers and any(t['data_age_seconds'] <= fresh_seconds for t in transformers):
                        # Action transitions are sparse (incidents only); judge projection lag from continuous transformer loads.
                        f = 'STALE' if any(t['data_age_seconds'] > fresh_seconds for t in transformers) else 'FRESH'
                    elif transformers and items:
                        f = 'STALE'
                    else:
                        f = freshness([observed_freshness(i) for i in items])
                else:
                    states = [observed_freshness(i) for i in items]
                    if collection in ('devices', 'transformers') and store.incomplete_db(db):
                        states.append('PARTIAL')
                    f = freshness(states)
                # ponytail: materialize finite local lists for stable pages; SQL keysets at measured scale.
                next_cursor = None
                for start in reversed(range(0, len(items), limit)):
                    page = dict(items=items[start:start+limit], next_cursor=next_cursor, freshness=f)
                    if start == 0:
                        break
                    token = uuid4().hex
                    db.execute('INSERT INTO projection_pages VALUES(%s,%s,%s,%s,%s)',
                               (token, scope, now, Jsonb(page), cursor))
                    next_cursor = token
                result = dict(items=items[:limit], next_cursor=next_cursor, freshness=f)
            return JSONResponse(result, headers={'X-Event-ID': cursor, 'X-Snapshot-At': stamp(now)})

    def mutate(request, template, body, entity=None):
        method = request.method.lower()
        spec = OPENAPI['paths'][template][method]
        actor = authorize(request, spec['x-minimum-role'])
        key = request.headers.get('idempotency-key')
        validate('Id', key)
        parameters(request, template, method)
        operation = ('policy' if template.endswith('/policy') else 'acknowledge' if template.endswith('/acknowledge')
                     else 'stop' if template.endswith('/stop') else 'start')
        fingerprint = [operation, entity, body]
        traceparent = request.headers.get('traceparent')
        if traceparent is not None and not re.fullmatch(r'00-[a-f0-9]{32}-[a-f0-9]{16}-0[01]', traceparent):
            raise ApiError(400, 'Invalid trace context')
        context = dict(actor, idempotency_key=key, request_id=request.state.request_id,
                       authorized_at=stamp(utc()),
                       traceparent=traceparent or ('00-' + uuid4().hex + '-' + uuid4().hex[:16] + '-01'),
                       service_identity='projection.api' if live else 'projection.api.mock', deadline_ms=3000)
        with store.transaction() as db:
            prior = db.execute('SELECT fingerprint,status,response FROM projection_operations WHERE actor=%s AND key=%s',
                               (actor['actor'], key)).fetchone()
            if prior:
                if prior[0] != fingerprint:
                    raise ApiError(409, 'Idempotency key reused for another request')
                return JSONResponse(prior[2], status_code=prior[1])
            now = utc()
            if operation == 'acknowledge':
                result = next((a for a in store.views(db, now)['alerts'] if a['alert_id'] == entity), None)
                if result is None:
                    raise ApiError(404, 'Alert not found')
                if result['status'] == 'OPEN':
                    result['status'] = 'ACKNOWLEDGED'
                status = 200
                store._emit(db, 'alert.updated', entity, result, now)
            else:
                if mocks is None:
                    raise ApiError(503, 'Live scenario and policy adapters are unavailable')
                status, result = mocks.call(db, operation, entity, body, context, now)
                validate('Policy' if operation == 'policy' else 'ScenarioRun', result)
                if operation == 'policy':
                    t = next(t for t in store.views(db, now)['transformers'] if t['transformer_id'] == entity)
                    store._emit(db, 'transformer.updated', entity, t, now)
                if operation in ('start', 'stop'):
                    store._emit(db, 'scenario.updated', result['run_id'], result, now)
            db.execute('INSERT INTO projection_operations VALUES(%s,%s,%s,%s,%s,%s)',
                       (actor['actor'], key, Jsonb(fingerprint), status, Jsonb(result), Jsonb(context)))
            return JSONResponse(result, status_code=status)

    async def bounded_body(request, schema=None):
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 65536:
                raise ApiError(400, 'Request exceeds 64KiB')
        if schema is None:
            if raw:
                raise ApiError(400, 'Request body is not allowed')
            return None
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise ApiError(400, 'Invalid JSON')
        try:
            validate(schema, value)
        except (ValueError, ValidationError, RecursionError):
            raise ApiError(422, 'Invalid ' + schema)
        return value

    @app.get('/api/v1/health')
    def health():
        return {'status': 'ok', 'observed_at': stamp(utc())}

    @app.get('/api/v1/ready')
    def ready():
        with store.readonly(), store.transaction() as db:
            store.cursor(db)
        return {'status': 'ok', 'observed_at': stamp(utc())}

    @app.get('/api/v1/events')
    async def events(request: Request):
        actor = await anyio.to_thread.run_sync(authorize, request, 'Viewer')
        cursor = request.headers.get('last-event-id')
        if cursor is not None:
            validate('Id', cursor)
        batch = await anyio.to_thread.run_sync(store.events_with_context, cursor)
        async def stream():
            nonlocal cursor, batch
            heartbeat = time.monotonic()
            while not await request.is_disconnected():
                if time.time() >= actor.get('expires_at', float('inf')):
                    return  # Reconnect with a renewed bearer; expiry applies to open streams too.
                for event, context in batch:
                    if event['event_type'] == 'snapshot.required':
                        sse_reset()
                    with boundary('sse.send', context, link=True):
                        encoded = frame(event)
                        yield encoded
                    cursor = event['cursor']
                if time.monotonic() - heartbeat >= 15:
                    yield ': heartbeat\n\n'
                    heartbeat = time.monotonic()
                await asyncio.sleep(.2)
                try:
                    batch = await anyio.to_thread.run_sync(store.events_with_context, cursor)
                except psycopg.Error:
                    return  # Force cursor-based retry after an established stream fails.
        return StreamingResponse(stream(), media_type='text/event-stream',
                                 headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    @app.get('/internal/investigator/projection')
    def investigator_projection(request: Request):
        authorize(request, 'Operator')
        q = dict(request.query_params)
        if len(q) != 1 or next(iter(q)) not in ('transformer_id', 'action_id'):
            raise ApiError(400, 'Exactly one entity scope required')
        field, entity = next(iter(q.items()))
        validate('Id', entity)
        kinds = ('Action', 'Alert') if field == 'transformer_id' else ('Action', 'Command', 'Result')
        topics = [topic for topic, kind in TOPICS.items() if kind in kinds]
        with store.readonly(), store.transaction() as db:
            # Read only the owned journal; never another service's domain tables.
            rows = db.execute("SELECT event FROM projection_events WHERE topic=ANY(%s) AND event->'payload'->>%s=%s "
                              "ORDER BY (event->>'event_time')::timestamptz LIMIT 201",
                              (topics, field, entity)).fetchall()
            if len(rows) > 200:
                raise ApiError(429, 'Projection history exceeds bounded read')
            events = [r[0] for r in rows]
            if not events:
                raise ApiError(404, 'Projection evidence unavailable')
            actions = [e for e in events if e['event_type'] == 'action.transition']
            action = max(actions, key=lambda e: e['saga_version'], default=None)
            if field == 'action_id' and action is None:
                raise ApiError(503, 'Confirmed reduction evidence unavailable')
            return dict(actions=[{k:v for k,v in e.items() if k in BUNDLE['$defs']['Action']['properties']} for e in actions], alerts=[{k:v for k,v in e.items() if k in BUNDLE['$defs']['Alert']['properties']} for e in events if e['event_type'].startswith('alert.')],
                        commands=[{k:v for k,v in e['payload'].items() if k in BUNDLE['$defs']['CommandPayload']['properties']} for e in events if e['event_type'] == 'charger.command'],
                        results=[{k:v for k,v in e['payload'].items() if k in BUNDLE['$defs']['ResultPayload']['properties']} for e in events if e['event_type'] == 'command.result'],
                        confirmed_reduction_kw=action['payload']['confirmed_reduction_kw'] if action else 0)

    # Register only the requested operations slice, preserving the frozen route shapes.
    def query_route(template):
        def handler(request: Request, id: str = None):
            if id is not None:
                validate('Id', id)
            return snapshot(request, template, id)
        app.add_api_route('/api/v1' + template, handler, methods=['GET'])

    def mutation_route(template, method, schema):
        async def handler(request: Request, id: str = None):
            await anyio.to_thread.run_sync(authorize, request,
                OPENAPI['paths'][template][method.lower()]['x-minimum-role'])
            if id is not None:
                validate('Id', id)
            body = await bounded_body(request, schema)
            return await anyio.to_thread.run_sync(mutate, request, template, body, id)
        app.add_api_route('/api/v1' + template, handler, methods=[method])

    for path in ('/overview', '/transformers', '/transformers/{id}', '/transformers/{id}/loads',
                 '/devices', '/devices/{id}', '/alerts', '/actions', '/actions/{id}', '/scenarios'):
        query_route(path)
    for path, method, schema in [('/transformers/{id}/policy', 'PATCH', 'PolicyWrite'),
                                 ('/alerts/{id}/acknowledge', 'POST', None),
                                 ('/scenario-runs', 'POST', 'ScenarioStart'),
                                 ('/scenario-runs/{id}/stop', 'POST', None)]:
        mutation_route(path, method, schema)
    return app
