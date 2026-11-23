from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from member4.api import create_app
from member4.contracts import BUNDLE, instant, utc, validate
from member4.operations import ContractMocks
from member4.store import Store
from tests.member4.conftest import IDENTITIES
from tests.member4.test_projection import feed

HEADERS = {'Authorization': 'Bearer admin-local-token', 'Idempotency-Key': 'request-1'}


@pytest.fixture
def client(store, records):
    store.context(records)
    feed(store, records['events'], now=utc())
    with TestClient(create_app(store, IDENTITIES)) as client:
        yield client


def test_snapshots_contracts_filters_errors(client):
    for path, schema in [('/overview', 'Overview'), ('/transformers', 'TransformerPage'),
                         ('/transformers/tx-001', 'Transformer'), ('/devices', 'DeviceViewPage'),
                         ('/devices/ev-001', 'DeviceView'), ('/alerts', 'AlertPayloadPage'),
                         ('/actions', 'ActionPayloadPage'), ('/scenarios', 'ScenarioCatalog'),
                         ('/transformers/tx-001/loads?start=2026-11-09T18:00:00Z&end=2026-11-09T19:00:00Z', 'LoadPayloadPage')]:
        r = client.get('/api/v1'+path, headers=HEADERS)
        assert r.status_code == 200, r.text
        validate(schema, r.json())
        assert r.headers['X-Event-ID']
    assert client.get('/api/v1/devices?device_type=SOLAR', headers=HEADERS).json()['items'] == []
    for path, code in [('/transformers?limit=0', 400), ('/alerts?status=bad', 400),
                       ('/alerts?limit=3&limit=4', 400), ('/alerts?evil=x', 400),
                       ('/devices/absent', 404), ('/missing', 404),
                       ('/transformers/tx-001/loads?start=no&end=no', 400),
                       ('/transformers/tx-001/loads', 400),
                       ('/transformers/tx-001/loads?start=2026-99-09T18:00:00Z&end=2026-11-09T19:00:00Z', 400)]:
        r = client.get('/api/v1'+path, headers=HEADERS)
        assert r.status_code == code, r.text
        validate('HttpError', r.json())
    assert client.get('/api/v1/overview').status_code == 401
    assert client.get('/api/v1/overview', headers={'Authorization': 'admin-local-token'}).status_code == 401
    assert client.get('/api/v1/overview', headers={'X-Actor-Role': 'Administrator'}).status_code == 401
    assert client.get('/api/v1/health').status_code == 200
    assert client.get('/api/v1/ready').status_code == 200


def test_stable_pages_across_replicas_and_expiry(store, records, client):
    more = deepcopy(records['devices'][0])
    more['device_id'] = 'ev-002'
    store.context({'devices': [more]})
    first = client.get('/api/v1/devices?limit=1', headers=HEADERS)
    token = first.json()['next_cursor']
    assert token
    third = deepcopy(more)
    third['device_id'] = 'ev-003'
    store.context({'devices': [third]})
    with TestClient(create_app(Store(store.dsn), IDENTITIES)) as second:
        r = second.get('/api/v1/devices?limit=1&cursor='+token, headers=HEADERS)
        assert [i['device']['device_id'] for i in r.json()['items']] == ['ev-002']
        assert r.headers['X-Event-ID'] == first.headers['X-Event-ID']
        assert r.headers['X-Snapshot-At'] == first.headers['X-Snapshot-At']
        assert r.json()['next_cursor'] is None
        assert second.get('/api/v1/devices?limit=2&cursor='+token, headers=HEADERS).status_code == 409
        with store.transaction() as db:
            db.execute("UPDATE projection_pages SET created_at=created_at-interval '6 minutes'")
        assert second.get('/api/v1/devices?limit=1&cursor='+token, headers=HEADERS).status_code == 409


def test_policy_idempotent_versioned_read_your_writes_and_roles(client, store):
    p = client.get('/api/v1/transformers/tx-001', headers=HEADERS).json()['policy']
    p.update(version=2, warning_pct=81)
    body = {'expected_version': 1, 'policy': p}
    path = '/api/v1/transformers/tx-001/policy'
    r = client.patch(path, json=body, headers=HEADERS)
    assert r.status_code == 200, r.text
    validate('Policy', r.json())
    assert client.patch(path, json=body, headers=HEADERS).json() == r.json()
    assert client.get('/api/v1/transformers/tx-001', headers=HEADERS).json()['policy'] == p
    assert client.patch(path, json=body, headers=dict(HEADERS, **{'Idempotency-Key': 'second'})).status_code == 409
    body['policy']['warning_pct'] = 99
    assert client.patch(path, json=body, headers=HEADERS).status_code == 409
    assert client.patch(path, json=body, headers=dict(HEADERS, **{'Authorization': 'Bearer viewer-local-token'})).status_code == 403
    assert client.patch(path, json=body, headers={'Authorization': HEADERS['Authorization']}).status_code == 400
    assert client.patch(path, content='{broken', headers=HEADERS).status_code == 400
    assert client.patch(path, content='x'*65537, headers=HEADERS).status_code == 400
    assert client.patch(path, json={}, headers=HEADERS).status_code == 422
    with store.transaction() as db:
        context = db.execute('SELECT context FROM projection_operations').fetchone()[0]
        assert context['actor'] == 'admin'
        assert context['deadline_ms'] == 3000
        assert context['traceparent'].startswith('00-')


@pytest.mark.parametrize('failure,status', [('unavailable',503), ('timeout',504), ('conflict',409)])
def test_internal_failure_mapping(store, records, failure, status):
    store.context(records)
    with TestClient(create_app(store, IDENTITIES, ContractMocks(failure))) as c:
        r = c.post('/api/v1/scenario-runs', headers=HEADERS,
                   json={'scenario':'normal', 'seed':0, 'scale':1, 'duration_seconds':60})
        assert r.status_code == status
        validate('HttpError', r.json())
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_operations').fetchone()[0] == 0


def test_live_scenario_does_not_report_mock_success(store):
    with TestClient(create_app(store, IDENTITIES, live=True)) as client:
        response = client.post('/api/v1/scenario-runs', headers=HEADERS,
                               json={'scenario': 'normal', 'seed': 0, 'scale': 1, 'duration_seconds': 60})
        assert response.status_code == 503
        validate('HttpError', response.json())
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_operations').fetchone()[0] == 0


def test_scenario_concurrent_replicas_and_acknowledgement(store, records, client):
    body = {'scenario':'normal', 'seed':0, 'scale':1, 'duration_seconds':60}
    def start(_):
        with TestClient(create_app(Store(store.dsn), IDENTITIES)) as c:
            r = c.post('/api/v1/scenario-runs', headers=HEADERS, json=body)
            assert r.status_code == 202, r.text
            return r.json()
    with ThreadPoolExecutor(max_workers=4) as pool:
        runs = list(pool.map(start, range(8)))
    assert all(r == runs[0] for r in runs)
    run = runs[0]
    validate('ScenarioRun', run)
    assert client.post('/api/v1/scenario-runs', headers=dict(HEADERS, **{'Idempotency-Key':'another'}), json=body).status_code == 409
    r = client.post('/api/v1/scenario-runs/'+run['run_id']+'/stop',
                    headers=dict(HEADERS, **{'Idempotency-Key':'stop'}))
    assert r.status_code == 200 and r.json()['status'] == 'STOPPED'
    assert client.get('/api/v1/scenarios', headers=HEADERS).json()['active_run'] is None
    alert = client.get('/api/v1/alerts', headers=HEADERS).json()['items'][0]
    r = client.post('/api/v1/alerts/'+alert['alert_id']+'/acknowledge',
                    headers=dict(HEADERS, **{'Idempotency-Key':'ack'}))
    assert r.status_code == 200
    assert r.json()['status'] == 'ACKNOWLEDGED'
    assert client.get('/api/v1/alerts?status=ACKNOWLEDGED', headers=HEADERS).json()['items']
    store.rebuild()
    assert client.get('/api/v1/alerts?status=ACKNOWLEDGED', headers=HEADERS).json()['items']


def test_database_outage_safe_errors():
    with TestClient(create_app(Store('postgresql://projection:disposable@127.0.0.1:1/unavailable'), IDENTITIES)) as c:
        assert c.get('/api/v1/health').status_code == 200
        for path in ('/ready', '/overview'):
            response = c.get('/api/v1'+path, headers=HEADERS)
            assert response.status_code == 503
            validate('HttpError', response.json())
            assert 'disposable' not in response.text
