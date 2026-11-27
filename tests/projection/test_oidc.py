from copy import deepcopy

from fastapi.testclient import TestClient
from tests.identity.test_auth import signing
from projection.api import create_app


def test_oidc_allowed_changes_audited_and_forbidden_changes_absent(store, records, signing):
    oidc, token = signing
    store.context(records)
    with TestClient(create_app(store, oidc=oidc)) as client:
        def headers(role, key):
            return {'Authorization': 'Bearer '+token(role), 'Idempotency-Key': key}
        assert client.get('/api/v1/overview', headers=headers('Viewer', 'read')).status_code == 200
        body = dict(scenario='normal', seed=0, scale=1, duration_seconds=60)
        assert client.post('/api/v1/scenario-runs', json=body, headers=headers('Viewer', 'denied')).status_code == 403
        response = client.post('/api/v1/scenario-runs', json=body, headers=headers('Operator', 'start'))
        assert response.status_code == 202, response.text
        assert client.post('/api/v1/scenario-runs/'+response.json()['run_id']+'/stop', headers=headers('Operator', 'stop')).status_code == 200
        policy = deepcopy(records['transformers'][0]['policy'])
        policy['version'] += 1
        body = dict(policy=policy, expected_version=policy['version']-1)
        path = '/api/v1/transformers/tx-001/policy'
        assert client.patch(path, json=body, headers=headers('Operator', 'deny-policy')).status_code == 403
        assert client.patch(path, json=body, headers=headers('Administrator', 'policy')).status_code == 200
    with store.transaction() as db:
        rows = db.execute('SELECT actor,key,context FROM projection_operations ORDER BY key').fetchall()
        assert len(rows) == 3
        assert {row[1] for row in rows} == {'start', 'stop', 'policy'}
        assert all(row[0] == 'subject-123' and row[2]['actor'] == 'subject-123' for row in rows)
        assert all('token' not in str(row[2]).lower() for row in rows)


def test_open_stream_stops_at_token_expiry(store, records, signing, monkeypatch):
    oidc, token = signing
    store.context(records)
    bearer = token()
    monkeypatch.setattr('projection.api.time.time', lambda: 9999999999)
    with TestClient(create_app(store, oidc=oidc)) as client:
        response = client.get('/api/v1/events', headers={'Authorization': 'Bearer '+bearer})
        assert response.status_code == 200
        assert response.content == b''
