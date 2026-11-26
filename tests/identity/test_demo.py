"""Public demo tokens stay server-side and refresh without browser login."""
import asyncio
import time

import httpx
import pytest

from identity.auth import Denied
from identity.demo import DemoIdentity


def test_demo_identity_caches_refreshes_and_rejects_privileged_tokens(monkeypatch):
    monkeypatch.setenv('GRID_DEMO_CLIENT_SECRET', 'test-only-secret')
    async def check():
        identity = DemoIdentity()
        calls = []
        def exchange(request):
            calls.append(request)
            assert b'grant_type=client_credentials' in request.content
            return httpx.Response(200, json={'access_token': 'signed-viewer-token'})
        await identity.http.aclose()
        identity.http = httpx.AsyncClient(transport=httpx.MockTransport(exchange))
        identity.oidc.authenticate = lambda _: dict(role='Viewer', expires_at=time.time()+120)
        assert await identity.token() == await identity.token() == 'signed-viewer-token'
        assert len(calls) == 1
        identity.expires = 0
        assert await identity.token() == 'signed-viewer-token'
        assert len(calls) == 2
        identity.expires = 0
        identity.oidc.authenticate = lambda _: dict(role='Operator', expires_at=time.time()+120)
        with pytest.raises(Denied):
            await identity.token()
        await identity.http.aclose()
    asyncio.run(check())
