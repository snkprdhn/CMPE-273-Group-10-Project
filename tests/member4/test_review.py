"""Regression cases found in the prepublication correctness/failure review."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
from jsonschema import ValidationError
import psycopg
import pytest

from member4.api import create_app
from member4.contracts import validate
from tests.member4.conftest import IDENTITIES
from tests.member4.test_projection import NOW, feed

HEADERS = {'Authorization': 'Bearer admin-local-token'}


@pytest.mark.parametrize('missing', [2, 3])
def test_sse_interior_or_tail_hole_requires_snapshot(store, missing):
    cursor = store.events(None, NOW)[0]['cursor']
    with store.transaction() as db:
        for n in range(3):
            store._emit(db, 'system.health', 'projection',
                        {'status': 'ok', 'observed_at': '2026-11-09T18:30:06Z'}, NOW)
        # A replica clock skew can make expiry pruning remove an interior row.
        db.execute('DELETE FROM projection_stream WHERE sequence=%s', (missing,))
    batch = store.events(cursor, NOW)
    assert [e['event_type'] for e in batch] == ['snapshot.required']


def test_snapshot_freshness_uses_time_after_lock_wait(store, records, monkeypatch):
    import member4.api as api
    store.context(records)
    entered = False
    original = store.transaction
    @contextmanager
    def delayed_transaction():
        nonlocal entered
        with original() as db:
            entered = True
            yield db
    monkeypatch.setattr(store, 'transaction', delayed_transaction)
    monkeypatch.setattr(api, 'utc', lambda: NOW + timedelta(seconds=20) if entered else NOW)
    with TestClient(create_app(store, IDENTITIES)) as c:
        response = c.get('/api/v1/transformers/tx-001', headers=HEADERS)
    assert response.status_code == 200
    assert response.json()['freshness'] == 'STALE'
    assert response.json()['data_age_seconds'] == 21


def test_cursor_expiry_uses_time_after_lock_wait(store, monkeypatch):
    import member4.store as storage
    cursor = store.events(None, NOW)[0]['cursor']
    with store.transaction() as db:
        store._emit(db, 'system.health', 'projection',
                    {'status': 'ok', 'observed_at': '2026-11-09T18:30:06Z'}, NOW)
    entered = False
    original = store.transaction
    @contextmanager
    def delayed_transaction():
        nonlocal entered
        with original() as db:
            entered = True
            yield db
    monkeypatch.setattr(store, 'transaction', delayed_transaction)
    monkeypatch.setattr(storage, 'utc', lambda: NOW+timedelta(minutes=5) if entered else NOW)
    assert store.events(cursor)[0]['event_type'] == 'snapshot.required'


def test_context_cannot_hide_conflicting_recorded_load(store, records):
    store.context(records)
    r = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.transformer-load.v1'))
    r['event']['payload']['net_load_kw'] += 1
    with pytest.raises(ValueError, match='context'):
        feed(store, [r])
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 0


def test_unknown_entities_mark_coverage_partial_and_publish_health(store, records, monkeypatch):
    import member4.api as api
    store.context(records)
    feed(store, records['events'])
    cursor = store.events(None, NOW)[0]['cursor']
    r = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.telemetry.v1'))
    r['event']['event_id'] = str(uuid4())
    r['event']['payload']['device_id'] = 'unconfigured-device'
    feed(store, [r])
    monkeypatch.setattr(api, 'utc', lambda: NOW)
    with TestClient(create_app(store, IDENTITIES)) as c:
        for path in ('/overview', '/devices', '/transformers'):
            response = c.get('/api/v1'+path, headers=HEADERS)
            assert response.status_code == 200
            assert response.json()['freshness'] == 'PARTIAL'
        assert len(c.get('/api/v1/devices', headers=HEADERS).json()['items']) == 1
    assert any(e['event_type'] == 'system.health' and e['payload']['status'] == 'degraded'
               for e in store.events(cursor, NOW))


def test_sse_initial_store_failure_has_consistent_error():
    from member4.store import Store
    store = Store('postgresql://projection:disposable@127.0.0.1:1/unavailable')
    with TestClient(create_app(store, IDENTITIES)) as c:
        response = c.get('/api/v1/events', headers=HEADERS)
        assert response.status_code == 503
        validate('HttpError', response.json())


@pytest.mark.parametrize('timestamp', ['2026-11-09 18:30:06Z', '20261109T183006Z'])
def test_non_rfc3339_dates_rejected(timestamp):
    with pytest.raises(ValidationError):
        validate('Timestamp', timestamp)


def test_nonascii_bearer_is_unauthenticated(store):
    with TestClient(create_app(store, IDENTITIES)) as c:
        response = c.get('/api/v1/overview', headers={b'authorization': b'Bearer \xff'})
        assert response.status_code == 401
        validate('HttpError', response.json())


def test_consumer_closes_when_subscribe_fails(store, monkeypatch):
    import member4.consumer as transport
    class BrokenConsumer:
        closed = False
        def __init__(self, config): pass
        def subscribe(self, *args, **kwargs): raise RuntimeError('subscribe failed')
        def close(self): self.closed = True
    consumer = BrokenConsumer({})
    monkeypatch.setattr(transport, 'Consumer', lambda config: consumer)
    monkeypatch.setattr(transport.signal, 'signal', lambda *args: None)
    with pytest.raises(RuntimeError, match='subscribe'):
        transport.consume(store)
    assert consumer.closed


@pytest.mark.parametrize('field', ['event_time', 'load_window'])
def test_future_events_cannot_appear_fresh(store, records, field):
    store.context(records)
    r = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.transformer-load.v1'))
    if field == 'event_time':
        r['event']['event_time'] = '2099-11-09T18:30:06Z'
    else:
        r['event']['payload']['window_end'] = '2099-11-09T18:30:05Z'
    with pytest.raises(ValueError, match='future'):
        feed(store, [r])
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 0


def test_event_then_conflicting_context_is_rejected(store, records):
    r = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.transformer-load.v1'))
    r['event']['payload']['net_load_kw'] += 1
    feed(store, [r])
    with pytest.raises(ValueError, match='context'):
        store.context(records)


def test_established_sse_store_failure_closes_and_resumes(store, records, monkeypatch):
    import httpx
    from member4.store import Store
    from tests.member4.test_stream_live import server, next_event
    store.context(records)
    feed(store, records['events'])
    cursor = store.events(None, NOW)[0]['cursor']
    original = Store.events_with_context
    failed = False
    def flaky(self, cursor, now=None):
        if failed:
            raise psycopg.OperationalError('injected store outage')
        return original(self, cursor, now)
    monkeypatch.setattr(Store, 'events_with_context', flaky)
    with server(store) as url, httpx.Client(timeout=5) as c:
        headers = dict(HEADERS, **{'Last-Event-ID': cursor})
        with c.stream('GET', url+'/events', headers=headers) as response:
            assert response.status_code == 200
            failed = True
            assert next(response.iter_lines(), None) is None
        failed = False
        r = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.action.v1'))
        r['event'].update(event_id=str(uuid4()), saga_version=9)
        r['event']['payload']['status'] = 'COMPLETED'
        # Use real wall time for transport retention, keeping the recorded event unchanged.
        from member4.contracts import utc
        feed(store, [r], now=utc())
        with c.stream('GET', url+'/events', headers=headers) as response:
            event = next_event(response.iter_lines())
            assert event['event_type'] == 'action.updated'
            assert event['payload']['status'] == 'COMPLETED'


def test_mutation_failure_after_notification_rolls_back_policy_and_cursor(store, records, monkeypatch):
    store.context(records)
    cursor = store.events(None, NOW)[0]['cursor']
    original = store._emit
    def fail_after_write(*args):
        original(*args)
        raise psycopg.OperationalError('injected failure after SSE insert')
    monkeypatch.setattr(store, '_emit', fail_after_write)
    policy = deepcopy(records['transformers'][0]['policy'])
    policy.update(version=2, warning_pct=81)
    with TestClient(create_app(store, IDENTITIES)) as c:
        response = c.patch('/api/v1/transformers/tx-001/policy',
                           headers=dict(HEADERS, **{'Idempotency-Key': 'rollback'}),
                           json={'expected_version': 1, 'policy': policy})
        assert response.status_code == 503
        assert c.get('/api/v1/transformers/tx-001', headers=HEADERS).json()['policy']['version'] == 1
    with store.transaction() as db:
        assert store.cursor(db) == cursor
        assert db.execute('SELECT count(*) FROM projection_operations').fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM projection_stream').fetchone()[0] == 0


def test_deeply_nested_request_has_safe_validation_error(store):
    with TestClient(create_app(store, IDENTITIES)) as c:
        response = c.post('/api/v1/scenario-runs', headers=HEADERS,
                          content='['*15000+'0'+']'*15000)
        assert response.status_code == 400
        validate('HttpError', response.json())
