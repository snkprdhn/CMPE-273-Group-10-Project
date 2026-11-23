"""Member_4 UI boundary: REST is authoritative; SSE invalidates cached snapshots."""
import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

import httpx


DEFINITIONS = json.loads((Path(__file__).resolve().parents[1] / 'schemas/v1/bundle.json').read_text())['$defs']

def validate(name, value):
    try:
        Draft202012Validator({'$ref': f'#/$defs/{name}', '$defs': DEFINITIONS}, format_checker=FormatChecker()).validate(value)
    except RecursionError as exc:
        raise ValueError('Response nesting exceeds validation limits') from exc


def decode_json(raw):
    def reject_constant(value):
        raise ValueError('Non-finite JSON number')
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError('Non-finite JSON number')
        return number
    try:
        return json.loads(raw, parse_constant=reject_constant, parse_float=finite_float)
    except RecursionError as exc:
        raise ValueError('Response nesting exceeds parsing limits') from exc


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def stamp(value):
    return value.isoformat(timespec='seconds').replace('+00:00', 'Z')


class Operations:
    def __init__(self, base_url, token, token_provider=None):
        self.http = httpx.AsyncClient(base_url=base_url, headers={'Authorization': f'Bearer {token}'},
                                     timeout=httpx.Timeout(5, read=25), trust_env=False)
        if token_provider:
            async def bearer(request):
                from identity.auth import Denied
                try:
                    request.headers['Authorization'] = 'Bearer ' + await token_provider()
                except Denied as exc:
                    response = httpx.Response(401, request=request)
                    raise httpx.HTTPStatusError('Login required', request=request, response=response) from exc
            self.http.event_hooks['request'].append(bearer)
        self.data = {}
        self.errors = {}
        self.cursor = None
        self.status = 'Loading'
        self.stream_connected = False
        self.observed = None
        self.versions = {}
        self.lock = asyncio.Lock()
        self.closed = False
        self.range_minutes = 15
        self.transformer = None
        self.health = {}
        self.retry_keys = {}

    async def get(self, path, params=None):
        response = await self.http.get(path, params=params)
        response.raise_for_status()
        return response

    async def collection(self, path, params=None):
        params = dict(params or {}, limit=200)
        items, cursors = [], set()
        while True:
            response = await self.get(path, params)
            page = decode_json(response.content)
            shape = {'transformers':'TransformerPage', 'devices':'DeviceViewPage', 'actions':'ActionPayloadPage', 'alerts':'AlertPayloadPage', 'loads':'LoadPayloadPage'}[path.split('/')[-1]]
            validate(shape, page)
            items.extend(page['items'])
            cursor = page.get('next_cursor')
            if not cursor:
                return dict(page, items=items)
            if cursor in cursors:
                raise ValueError('Repeated page cursor')
            cursors.add(cursor)
            params['cursor'] = cursor

    async def snapshot(self, reset=False):
        async with self.lock:
            # Anchor before all reads: replay closes races between separate REST responses.
            anchor = None
            observed = self.observed
            data = dict(self.data)
            minutes = self.range_minutes
            errors = {}
            paths = ['overview', 'transformers', 'alerts', 'actions', 'devices', 'scenarios']
            for name in paths:
                try:
                    if name in ('overview', 'scenarios'):
                        response = await self.get('/' + name)
                        value = decode_json(response.content)
                        validate('Overview' if name == 'overview' else 'ScenarioCatalog', value)
                        if name == 'overview':
                            anchor = response.headers.get('X-Event-ID')
                            observed = response.headers.get('X-Snapshot-At')
                    else:
                        value = await self.collection('/' + name)
                    data[name] = value
                except (httpx.HTTPError, ValueError, KeyError, ValidationError) as exc:
                    errors[name] = error_text(exc)
            target = self.transformer or next((t['transformer_id'] for t in data.get('overview', {}).get('highest_risk', [])), None)
            if target:
                try:
                    now = datetime.now(timezone.utc)
                    loads = await self.collection(f'/transformers/{target}/loads',
                        {'start': stamp(now-timedelta(minutes=minutes)), 'end': stamp(now+timedelta(seconds=5))})
                    data.update(loads=loads, history_transformer=target, history_minutes=minutes)
                except (httpx.HTTPError, ValueError, KeyError, ValidationError) as exc:
                    errors['loads'] = error_text(exc)
            else:
                data.pop('loads', None)
                data.pop('history_transformer', None)
            self.data = data
            self.observed = observed
            self.errors = errors
            if reset:
                self.cursor = anchor
                self.versions.clear()
                self.health.clear()
            if not self.data:
                self.status = 'Unavailable'
            elif self.stream_connected:
                self.status = 'Partial' if errors else 'Live'
            elif self.status != 'Loading':
                self.status = 'Reconnecting'
            elif errors:
                self.status = 'Partial'
            return not errors

    async def listen(self):
        delay = 1
        while not self.closed:
            try:
                await self.snapshot(reset=True)
                if not self.cursor:
                    raise ValueError('Snapshot cursor unavailable')
                headers = {'Last-Event-ID': self.cursor}
                async with self.http.stream('GET', '/events', headers=headers) as response:
                    response.raise_for_status()
                    if 'text/event-stream' not in response.headers.get('content-type', ''):
                        raise ValueError('Expected event stream')
                    self.stream_connected = True
                    self.status = 'Partial' if self.errors else 'Live'
                    delay = 1
                    fields = {}
                    async for line in response.aiter_lines():
                        if self.closed:
                            return
                        if not line:
                            if 'data' in fields:
                                event = decode_json(fields['data'])
                                validate('SseEvent', event)
                                if fields.get('id') != event['cursor'] or fields.get('event') != event['event_type']:
                                    raise ValueError('Inconsistent stream envelope')
                                if event['event_type'] == 'snapshot.required':
                                    break
                                key = (event['event_type'].split('.')[0], event['entity_id'])
                                if event['version'] > self.versions.get(key, -1):
                                    if event['event_type'] == 'system.health':
                                        self.health[event['entity_id']] = event['payload']
                                    if not await self.snapshot():
                                        raise ValueError('Snapshot refresh failed')
                                    self.versions[key] = event['version']
                                self.cursor = event['cursor']
                            fields = {}
                        elif not line.startswith(':'):
                            key, _, value = line.partition(':')
                            if key == 'data' and key in fields:
                                fields[key] += '\n' + value.lstrip()
                            else:
                                fields[key] = value.lstrip()
                self.status = 'Reconnecting'
            except (httpx.HTTPError, ValueError, KeyError, TypeError, ValidationError):
                self.status = 'Reconnecting' if self.data else 'Unavailable'
            finally:
                self.stream_connected = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)

    async def refresh(self):
        while not self.closed:
            await asyncio.sleep(10)
            await self.snapshot()

    async def mutate(self, path, body=None):
        # Preserve a key after uncertain failure; never blindly issue another logical command.
        fingerprint = json.dumps([path, body], sort_keys=True)
        key = self.retry_keys.setdefault(fingerprint, uuid4().hex)
        response = await self.http.post(path, json=body, headers={'Idempotency-Key': key}) if body is not None else await self.http.post(path, headers={'Idempotency-Key': key})
        response.raise_for_status()
        result = decode_json(response.content)
        validate('ScenarioRun' if 'scenario-runs' in path else 'AlertPayload', result)
        self.retry_keys.pop(fingerprint, None)
        await self.snapshot()
        return result

    async def close(self):
        self.closed = True
        await self.http.aclose()


def error_text(exc):
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return {401: 'Permission denied: authentication required', 403: 'Permission denied: Operator role required',
                404: 'Unavailable: entity not found', 409: 'Conflict: reload and inspect current state',
                429: 'Rate limited: try again later', 503: 'Unavailable: API dependency failed',
                504: 'Outcome uncertain: inspect history before retrying'}.get(code, f'Error: request failed ({code})')
    return 'Unavailable: connection or response failed'
