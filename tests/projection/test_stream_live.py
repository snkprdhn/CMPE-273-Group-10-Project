"""Real TCP streaming: independent API instances, reconnect, gap, heartbeat and reset."""
from contextlib import contextmanager
from copy import deepcopy
import socket
import threading
import time
from uuid import uuid4

import httpx
import uvicorn

from projection.api import create_app
from projection.contracts import utc, validate
from projection.store import Store
from tests.projection.conftest import IDENTITIES
from tests.projection.test_projection import feed

HEADERS = {'Authorization': 'Bearer viewer-local-token'}


@contextmanager
def server(store):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    config = uvicorn.Config(create_app(Store(store.dsn), IDENTITIES), log_level='error',
                            lifespan='off', timeout_graceful_shutdown=1)
    service = uvicorn.Server(config)
    thread = threading.Thread(target=service.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic()+5
        while not service.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                raise RuntimeError('API did not start')
            time.sleep(.01)
        yield 'http://127.0.0.1:'+str(port)+'/api/v1'
    finally:
        service.should_exit = True
        thread.join(5)
        listener.close()
        assert not thread.is_alive()


def next_event(lines):
    fields = {}
    for line in lines:
        if line.startswith(':'):
            continue
        if not line and fields:
            import json
            event = json.loads(fields['data'])
            validate('SseEvent', event)
            assert fields['id'] == event['cursor']
            assert fields['event'] == event['event_type']
            return event
        if ': ' in line:
            key, value = line.split(': ', 1)
            fields[key] = value
    raise AssertionError('SSE closed before expected event')


def test_real_disconnect_reconnect_other_replica_gap_and_heartbeat(store, records):
    store.context(records)
    feed(store, records['events'], now=utc())
    with server(store) as a, server(store) as b, httpx.Client(timeout=20) as client:
        response = client.get(a+'/overview', headers=HEADERS)
        cursor = response.headers['X-Event-ID']
        with client.stream('GET', a+'/events', headers=dict(HEADERS, **{'Last-Event-ID': cursor})) as stream:
            assert stream.status_code == 200
            load = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.transformer-load.v1'))
            load['event']['event_id'] = str(uuid4())
            load['event']['payload']['revision'] = 8
            feed(store, [load], now=utc())
            event = next_event(stream.iter_lines())
            assert event['event_type'] == 'transformer.updated'
            assert event['payload']['load']['revision'] == 8
            cursor = event['cursor']
        action = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.action.v1'))
        action['event'].update(event_id=str(uuid4()), saga_version=77)
        action['event']['payload']['status'] = 'COMPLETED'
        feed(store, [action], now=utc())
        with client.stream('GET', b+'/events', headers=dict(HEADERS, **{'Last-Event-ID': cursor})) as stream:
            lines = stream.iter_lines()
            event = next_event(lines)
            assert event['payload']['status'] == 'COMPLETED'
            assert event['event_type'] == 'action.updated'
            cursor = event['cursor']
            started = time.monotonic()
            assert next(line for line in lines if line.startswith(':')) == ': heartbeat'
            assert 10 <= time.monotonic()-started < 19
        with store.transaction() as db:
            db.execute("UPDATE projection_stream SET created_at=created_at-interval '6 minutes'")
        old = response.headers['X-Event-ID']
        with client.stream('GET', b+'/events', headers=dict(HEADERS, **{'Last-Event-ID': old})) as stream:
            reset = next_event(stream.iter_lines())
            assert reset['event_type'] == 'snapshot.required'
        # Snapshot cursor bridges snapshot->subscribe race, including on a different replica.
        snapshot = client.get(b+'/actions', headers=HEADERS)
        assert snapshot.json()['items'][0]['status'] == 'COMPLETED'
        snapshot_cursor = snapshot.headers['X-Event-ID']
        with client.stream('GET', a+'/events', headers=dict(HEADERS, **{'Last-Event-ID': snapshot_cursor})) as stream:
            action['event'].update(event_id=str(uuid4()), saga_version=78)
            action['event']['payload']['reason'] = 'post-snapshot evidence'
            feed(store, [action], now=utc())
            assert next_event(stream.iter_lines())['payload']['reason'] == 'post-snapshot evidence'
        store.rebuild()
        with client.stream('GET', a+'/events', headers=dict(HEADERS, **{'Last-Event-ID': cursor})) as stream:
            assert next_event(stream.iter_lines())['event_type'] == 'snapshot.required'
