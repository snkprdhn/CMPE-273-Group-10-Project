import pytest

from identity.local_routes import allowed


@pytest.mark.parametrize(('path', 'method', 'expected'), [
    ('/realms/smart-grid/protocol/openid-connect/auth', 'GET', True),
    ('/realms/smart-grid/login-actions/authenticate', 'POST', True),
    ('/resources/a1/login/keycloak/css/login.css', 'GET', True),
    ('/realms/master/protocol/openid-connect/auth', 'GET', False),
    ('/realms/smart-grid/protocol/openid-connect/auth', 'POST', False),
    ('/admin/master/console/', 'GET', False),
    ('/resources/a1/../admin.css', 'GET', False),
    ('/resources/a1/%2e%2e/admin.css', 'GET', False),
])
def test_public_identity_routes(path, method, expected):
    assert allowed(path, method) is expected


def test_proxy_forwards_only_idp_cookies(monkeypatch):
    from contextlib import asynccontextmanager

    import httpx
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from identity.local_routes import install

    calls = []
    def upstream(request):
        calls.append(request)
        return httpx.Response(302, headers=[
            ('location', 'http://127.0.0.1:18086/auth/callback'),
            ('set-cookie', 'AUTH_SESSION_ID=next; HttpOnly'),
            ('set-cookie', 'KC_RESTART=restart; HttpOnly'),
            ('x-internal-header', 'private'),
        ])

    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    monkeypatch.setattr('identity.local_routes.httpx.AsyncClient', lambda **_: http)
    shutdown = []
    @asynccontextmanager
    async def lifespan(_):
        yield
        for callback in shutdown:
            await callback()
    app = FastAPI(lifespan=lifespan)
    app.on_shutdown = shutdown.append
    install(app)
    with TestClient(app) as test:
        response = test.get('/realms/smart-grid/protocol/openid-connect/auth?client_id=smart-grid-ui',
                            headers={'Cookie': 'grid_session=private; AUTH_SESSION_ID=local-idp',
                                     'Authorization': 'Bearer private'}, follow_redirects=False)
        assert response.status_code == 302
        assert len(response.headers.get_list('set-cookie')) == 2
        assert 'x-internal-header' not in response.headers
        assert calls[0].url.host == 'keycloak'
        assert calls[0].headers['cookie'] == 'AUTH_SESSION_ID=local-idp'
        assert 'authorization' not in calls[0].headers
        assert test.get('/realms/master/protocol/openid-connect/auth').status_code == 404
        assert len(calls) == 1
