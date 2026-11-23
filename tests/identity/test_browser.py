import asyncio
import time
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
import pytest

from identity.auth import CLIENT, Denied
from identity.browser import BrowserLogin, COOKIE
from tests.identity.test_auth import signing


def test_pkce_callback_state_replay_and_logout(signing):
    oidc, token = signing
    app = FastAPI()
    login = BrowserLogin(app)
    login.oidc = oidc
    requests = []
    with TestClient(app) as client:
        response = client.get('/auth/login', follow_redirects=False)
        query = parse_qs(urlsplit(response.headers['location']).query)
        state = query['state'][0]
        pending = login.pending[state]
        assert query['code_challenge_method'] == ['S256']
        assert len(query['code_challenge'][0]) == 43
        assert 'HttpOnly' in response.headers['set-cookie']
        assert client.get('/auth/callback?state=evil&code=x').status_code == 400
        assert client.get('/auth/callback?state=λ&code=x').status_code == 400
        assert state in login.pending
        nonce = pending['nonce']
        def exchange(request):
            requests.append(parse_qs(request.content.decode()))
            return httpx.Response(200, json={'access_token': token(), 'refresh_token': 'refresh-private',
                'id_token': token(aud=CLIENT, nonce=nonce)})
        login.http = httpx.AsyncClient(transport=httpx.MockTransport(exchange))
        response = client.get('/auth/callback?state='+state+'&code=code', follow_redirects=False)
        assert response.status_code == 303
        assert requests[0]['code_verifier'] == [pending['verifier']]
        assert 'refresh-private' not in str(response.headers)
        sid = client.cookies[COOKIE]
        assert client.get('/auth/callback?state='+state+'&code=code').status_code == 400
        assert client.get('/auth/logout?csrf=forged').status_code == 400
        assert client.get('/auth/logout?csrf=λ').status_code == 400
        csrf = login.sessions[sid]['csrf']
        response = client.get('/auth/logout?csrf='+csrf, follow_redirects=False)
        assert response.status_code == 307
        assert '/protocol/openid-connect/logout?' in response.headers['location']
        assert sid not in login.sessions


def test_refresh_serialized_rotation_and_failure(signing):
    oidc, token = signing
    async def run():
        login = BrowserLogin(FastAPI())
        login.oidc = oidc
        calls = []
        def refresh(request):
            calls.append(parse_qs(request.content.decode()))
            return httpx.Response(200, json={'access_token': token(), 'refresh_token': 'rotated'})
        await login.http.aclose()
        login.http = httpx.AsyncClient(transport=httpx.MockTransport(refresh))
        login.sessions['sid'] = dict(actor={'actor': 'subject-123', 'role': 'Operator', 'expires_at': time.time()},
            tokens={'access_token': 'old', 'refresh_token': 'first', 'id_token': 'id'},
            until=time.time()+300, lock=asyncio.Lock())
        results = await asyncio.gather(*(login.token('sid') for _ in range(5)))
        assert len(set(results)) == 1 and len(calls) == 1
        assert calls[0]['refresh_token'] == ['first']
        assert login.sessions['sid']['tokens']['refresh_token'] == 'rotated'
        login.sessions['sid']['actor']['expires_at'] = 0
        login.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(400)))
        with pytest.raises(Denied):
            await login.token('sid')
        assert 'sid' not in login.sessions
        await login.http.aclose()
    asyncio.run(run())


def test_bad_nonce_and_expired_state_never_create_session(signing):
    oidc, token = signing
    app = FastAPI()
    login = BrowserLogin(app)
    login.oidc = oidc
    login.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={
        'access_token': token(), 'refresh_token': 'secret', 'id_token': token(aud=CLIENT, nonce='forged')})))
    with TestClient(app) as client:
        response = client.get('/auth/login', follow_redirects=False)
        state = parse_qs(urlsplit(response.headers['location']).query)['state'][0]
        assert client.get('/auth/callback?state='+state+'&code=c').status_code == 401
        assert not login.sessions
        response = client.get('/auth/login', follow_redirects=False)
        state = parse_qs(urlsplit(response.headers['location']).query)['state'][0]
        login.pending[state]['until'] = 0
        assert client.get('/auth/callback?state='+state+'&code=c').status_code == 400
        assert not login.sessions


def test_dashboard_hook_renews_every_request_and_fails_closed():
    from dashboard.client import Operations
    async def run():
        from identity.auth import Denied
        calls = []
        current = ['first']
        async def provider():
            if not current:
                raise Denied(401, 'Login required')
            return current[0]
        source = Operations('http://127.0.0.1:1/api/v1', '', token_provider=provider)
        await source.http.aclose()
        hooks = source.http.event_hooks
        source.http = httpx.AsyncClient(base_url='http://127.0.0.1:1/api/v1', event_hooks=hooks,
            transport=httpx.MockTransport(lambda request: calls.append(request.headers['Authorization']) or httpx.Response(200)))
        await source.get('/overview')
        current[0] = 'renewed'
        await source.get('/overview')
        current.clear()
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await source.get('/overview')
        assert failure.value.response.status_code == 401
        assert calls == ['Bearer first', 'Bearer renewed']
        await source.close()
    asyncio.run(run())


@pytest.mark.parametrize('invalidation', ['logout', 'expiry'])
def test_review_logout_during_refresh_cannot_return_a_bearer(signing, invalidation):
    oidc, token = signing
    async def run():
        login = BrowserLogin(FastAPI())
        login.oidc = oidc
        started, release = asyncio.Event(), asyncio.Event()
        async def exchange(data):
            started.set()
            await release.wait()
            return {'access_token': token(), 'refresh_token': 'rotated'}, oidc.authenticate(token())
        login.exchange = exchange
        session = dict(actor={'actor': 'subject-123', 'role': 'Operator', 'expires_at': 0},
            tokens={'refresh_token': 'first', 'id_token': 'id'}, until=time.time()+300, lock=asyncio.Lock())
        login.sessions['sid'] = session
        task = asyncio.create_task(login.token('sid'))
        await started.wait()
        if invalidation == 'logout':
            login.sessions.pop('sid')  # Exactly the invalidation performed by logout.
        else:
            session['until'] = 0
        release.set()
        with pytest.raises(Denied):
            await task
        assert 'sid' not in login.sessions
        await login.http.aclose()
    asyncio.run(run())


def test_review_refresh_checks_new_id_token(signing):
    oidc, token = signing
    async def run():
        login = BrowserLogin(FastAPI())
        login.oidc = oidc
        login.sessions['sid'] = dict(actor={'actor': 'subject-123', 'role': 'Operator', 'expires_at': 0},
            tokens={'refresh_token': 'first', 'id_token': 'id'}, until=time.time()+300, lock=asyncio.Lock())
        async def exchange(data):
            return {'access_token': token(), 'refresh_token': 'rotated', 'id_token': 'forged'}, oidc.authenticate(token())
        login.exchange = exchange
        with pytest.raises(Denied):
            await login.token('sid')
        assert 'sid' not in login.sessions
        await login.http.aclose()
    asyncio.run(run())


def test_review_malformed_provider_payload_is_a_login_failure():
    login = BrowserLogin(FastAPI())
    login.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=[])))
    async def run():
        with pytest.raises(Denied):
            await login.exchange({'grant_type': 'refresh_token', 'refresh_token': 'test'})
        await login.http.aclose()
    asyncio.run(run())
