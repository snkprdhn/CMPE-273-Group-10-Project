"""Called by real-browser acceptance with transient server-side test tokens."""
import json
import os
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from identity.auth import OIDC
from member4.api import create_app

pytestmark = pytest.mark.skipif(os.environ.get('IDENTITY_LIVE') != '1', reason='requires generated real test tokens')


def test_real_tokens_direct_api_and_audit(store, records):
    tokens = json.loads(Path('.tools/identity/test-tokens.json').read_text())
    store.context(records)
    with TestClient(create_app(store, oidc=OIDC())) as client:
        def headers(role, key):
            return {'Authorization': 'Bearer '+tokens[role], 'Idempotency-Key': key}
        assert client.get('/api/v1/overview', headers=headers('Viewer', 'read')).status_code == 200
        body = dict(scenario='normal', seed=0, scale=1, duration_seconds=60)
        assert client.post('/api/v1/scenario-runs', json=body, headers=headers('Viewer', 'denied')).status_code == 403
        response = client.post('/api/v1/scenario-runs', json=body, headers=headers('Operator', 'start'))
        assert response.status_code == 202
        assert client.post('/api/v1/scenario-runs/'+response.json()['run_id']+'/stop', headers=headers('Operator', 'stop')).status_code == 200
        policy = dict(records['transformers'][0]['policy'], version=2)
        body = dict(policy=policy, expected_version=1)
        path = '/api/v1/transformers/tx-001/policy'
        assert client.patch(path, json=body, headers=headers('Operator', 'denied-policy')).status_code == 403
        assert client.patch(path, json=body, headers=headers('Administrator', 'policy')).status_code == 200
        assert client.post('/api/v1/scenario-runs', json={}, headers={'X-Actor-Role': 'Administrator'}).status_code == 401
    with store.transaction() as db:
        rows = db.execute('SELECT context FROM projection_operations').fetchall()
        assert len(rows) == 3
        assert {r[0]['role'] for r in rows} == {'Operator', 'Administrator'}
        assert all(r[0]['actor'] not in ('viewer', 'operator', 'administrator') for r in rows)
