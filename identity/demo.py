"""Server-held, read-only OIDC identity for the explicitly enabled public demo."""
import asyncio
import os
import time

import httpx

from identity.auth import DEMO_CLIENT, Denied, OIDC


class DemoIdentity:
    def __init__(self):
        self.secret = os.environ['GRID_DEMO_CLIENT_SECRET']
        if not self.secret:
            raise ValueError('Demo client secret must not be empty')
        self.oidc = OIDC()
        self.http = httpx.AsyncClient(timeout=5, trust_env=False)
        self.lock = asyncio.Lock()
        self.bearer, self.expires = '', 0

    async def token(self):
        async with self.lock:
            if self.expires <= time.time() + 30:
                response = await self.http.post(self.oidc.backchannel + '/protocol/openid-connect/token',
                    data=dict(grant_type='client_credentials', client_id=DEMO_CLIENT,
                              client_secret=self.secret))
                response.raise_for_status()
                bearer = response.json()['access_token']
                actor = await asyncio.to_thread(self.oidc.authenticate, bearer)
                if actor['role'] != 'Viewer':
                    raise Denied(403, 'Demo identity must be read-only')
                self.bearer, self.expires = bearer, actor['expires_at']
            return self.bearer
