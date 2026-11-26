import time
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
import jwt
import pytest

from identity.auth import AUDIENCE, CLIENT, ISSUER, Denied, OIDC, authorize_tool


@pytest.fixture
def signing():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    oidc = OIDC()
    oidc.keys = SimpleNamespace(get_signing_key_from_jwt=lambda token: SimpleNamespace(key=private.public_key()))
    def token(role='Operator', **changes):
        claims = dict(iss=ISSUER, aud=AUDIENCE, azp=CLIENT, typ='Bearer', sub='subject-123',
                      iat=int(time.time()), exp=int(time.time())+120, realm_access={'roles': [role]})
        claims.update(changes)
        return jwt.encode(claims, private, algorithm='RS256', headers={'kid': 'local-test'})
    return oidc, token


@pytest.mark.parametrize('changes', [dict(iss='https://evil'), dict(aud='other'), dict(exp=1),
    dict(iat=int(time.time())+1000), dict(nbf=int(time.time())+1000), dict(typ='ID'),
    dict(azp='other'), dict(sub=''), dict(exp=None)])
def test_bad_claims(signing, changes):
    oidc, token = signing
    with pytest.raises(Denied) as failure:
        oidc.authenticate(token(**changes))
    assert failure.value.status == 401


def test_role_claims_fail_closed(signing):
    oidc, token = signing
    for value in (None, [], 'Administrator', {'roles': 'Administrator'}, {'roles': []}):
        with pytest.raises(Denied) as failure:
            oidc.authenticate(token(realm_access=value))
        assert failure.value.status == 403


def test_signature_roles_and_tool_guard(signing):
    oidc, token = signing
    for role in ('Viewer', 'Operator', 'Administrator'):
        assert oidc.authenticate(token(role))['role'] == role
    for bad in ('garbage', token()[:-10]+'xxxxxxxxxx', jwt.encode({'sub': 'fake'}, 's'*32, algorithm='HS256')):
        with pytest.raises(Denied):
            oidc.authenticate(bad)
    with pytest.raises(Denied):
        oidc.authenticate(token(realm_access={'roles': ['admin']}))
    for role in ('Operator', 'Administrator'):
        assert authorize_tool(oidc, token(role), 'get_transformer_snapshot')['role'] == role
        for name in ('write_policy', [], {'tool': 'get_transformer_snapshot'}, None):
            with pytest.raises(Denied):
                authorize_tool(oidc, token(role), name)
    with pytest.raises(Denied) as failure:
        authorize_tool(oidc, token('Viewer'), 'get_transformer_snapshot')
    assert failure.value.status == 403


def test_direct_api_denies_before_store_or_body(signing):
    from member4.api import create_app
    oidc, token = signing
    with TestClient(create_app(object(), oidc=oidc)) as client:
        assert client.get('/api/v1/overview', headers={'X-Actor-Role': 'Administrator'}).status_code == 401
        for method, path, role in [('post', '/scenario-runs', 'Viewer'),
             ('post', '/scenario-runs/run/stop', 'Viewer'), ('post', '/alerts/a/acknowledge', 'Viewer'),
             ('patch', '/transformers/t/policy', 'Operator')]:
            response = getattr(client, method)('/api/v1'+path, content='{invalid',
                headers={'Authorization': 'Bearer '+token(role), 'X-Actor-Role': 'Administrator'})
            assert response.status_code == 403
        assert client.get('/api/v1/overview', headers={'Authorization': 'Bearer '+token(exp=1)}).status_code == 401


def test_public_demo_client_is_limited_to_viewer(signing):
    from identity.auth import DEMO_CLIENT, require
    oidc, token = signing
    actor = oidc.authenticate(token('Viewer', azp=DEMO_CLIENT))
    assert actor['role'] == 'Viewer'
    with pytest.raises(Denied):
        require(actor, 'Operator')
    for role in ('Operator', 'Administrator'):
        with pytest.raises(Denied):
            oidc.authenticate(token(role, azp=DEMO_CLIENT))


@pytest.mark.parametrize('expiry', [str(int(time.time())+3600), float('inf')])
def test_review_rejects_non_numeric_or_nonfinite_expiry(signing, expiry):
    oidc, token = signing
    with pytest.raises(Denied) as failure:
        oidc.authenticate(token(exp=expiry))
    assert failure.value.status == 401
