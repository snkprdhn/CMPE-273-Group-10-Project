"""Single-process local BFF: opaque HttpOnly cookies, server-held rotating tokens."""
import asyncio
import base64
import hashlib
import logging
import secrets
import time
from urllib.parse import urlencode

import httpx
from fastapi import Request
from fastapi.responses import RedirectResponse, PlainTextResponse, HTMLResponse

from identity.auth import CLIENT, ISSUER, Denied, OIDC

ORIGIN = 'http://127.0.0.1:18086'
COOKIE = 'grid_session'


class BrowserLogin:
    def __init__(self, app):
        self.oidc = OIDC()
        self.sessions = {}
        self.pending = {}
        self.http = httpx.AsyncClient(timeout=5, trust_env=False)
        app.add_api_route('/auth/login', self.login, methods=['GET'])
        app.add_api_route('/auth/callback', self.callback, methods=['GET'])
        app.add_api_route('/auth/logout', self.logout, methods=['GET'])
        app.add_api_route('/auth/signed-out', self.signed_out, methods=['GET'])

        @app.middleware('http')
        async def private_pages(request, call_next):
            response = await call_next(request)
            if request.url.path == '/' or request.url.path.startswith('/auth/'):
                response.headers['Cache-Control'] = 'no-store'
                response.headers['Referrer-Policy'] = 'no-referrer'
            return response

    def prune(self):
        now = time.time()
        for mapping in (self.sessions, self.pending):
            for key, value in list(mapping.items()):
                if value['until'] <= now:
                    mapping.pop(key, None)

    async def login(self, request: Request):
        self.prune()
        if len(self.pending) >= 100:
            return PlainTextResponse('Too many pending logins', status_code=429)
        state, verifier, nonce = (secrets.token_urlsafe(32) for _ in range(3))
        self.pending[state] = dict(verifier=verifier, nonce=nonce, until=time.time()+300)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
        response = RedirectResponse(ISSUER + '/protocol/openid-connect/auth?' + urlencode(dict(
            client_id=CLIENT, redirect_uri=ORIGIN+'/auth/callback', response_type='code',
            scope='openid', state=state, nonce=nonce, code_challenge=challenge, code_challenge_method='S256')))
        response.set_cookie('grid_login', state, httponly=True, samesite='lax', max_age=300, path='/auth')
        return response

    async def exchange(self, data):
        response = await self.http.post(ISSUER+'/protocol/openid-connect/token', data=dict(client_id=CLIENT, **data))
        response.raise_for_status()
        tokens = response.json()
        if not isinstance(tokens, dict) or any(
            not isinstance(tokens.get(name), str) or not 0 < len(tokens[name]) <= 16384
            for name in ('access_token', 'refresh_token')
        ):
            raise Denied(401, 'Invalid token response')
        if 'id_token' in tokens:
            if not isinstance(tokens['id_token'], str) or not 0 < len(tokens['id_token']) <= 16384:
                raise Denied(401, 'Invalid ID token')
        actor = await asyncio.to_thread(self.oidc.authenticate, tokens['access_token'])
        return tokens, actor

    async def callback(self, request: Request):
        self.prune()
        state = request.query_params.get('state', '')
        if not state or not secrets.compare_digest(state.encode(), request.cookies.get('grid_login', '').encode()):
            return PlainTextResponse('Invalid login state', status_code=400)
        pending = self.pending.pop(state, None)
        if pending is None:
            return PlainTextResponse('Login expired or already used', status_code=400)
        try:
            tokens, actor = await self.exchange(dict(grant_type='authorization_code',
                code=request.query_params.get('code', ''), redirect_uri=ORIGIN+'/auth/callback',
                code_verifier=pending['verifier']))
            claims = await asyncio.to_thread(self.oidc.claims, tokens['id_token'], CLIENT)
            if claims.get('nonce') != pending['nonce'] or claims['sub'] != actor['actor'] or claims.get('azp', CLIENT) != CLIENT:
                raise Denied(401, 'Invalid login identity')
        except (httpx.HTTPError, Denied, ValueError, KeyError) as exc:
            logging.getLogger(__name__).warning('OIDC login rejected: %s / %s / %s',
                type(exc).__name__, type(exc.__cause__).__name__, getattr(exc.__cause__, 'claim', ''))
            return PlainTextResponse('Login failed; start again', status_code=401)
        self.sessions.pop(request.cookies.get(COOKIE), None)
        if len(self.sessions) >= 100:
            return PlainTextResponse('Session capacity reached', status_code=429)
        sid = secrets.token_urlsafe(32)
        self.sessions[sid] = dict(tokens=tokens, actor=actor, until=time.time()+1800,
                                  lock=asyncio.Lock(), csrf=secrets.token_urlsafe(32))
        response = RedirectResponse('/', status_code=303)
        response.set_cookie(COOKIE, sid, httponly=True, samesite='lax', max_age=1800)
        response.delete_cookie('grid_login', path='/auth')
        response.headers['Cache-Control'] = 'no-store'
        return response

    def session(self, sid):
        self.prune()
        return self.sessions.get(sid)

    async def token(self, sid):
        session = self.session(sid)
        if session is None:
            raise Denied(401, 'Login required')
        async with session['lock']:
            if self.session(sid) is not session:
                raise Denied(401, 'Login required')
            if session['actor']['expires_at'] <= time.time()+30:
                try:
                    tokens, actor = await self.exchange(dict(grant_type='refresh_token',
                        refresh_token=session['tokens']['refresh_token']))
                    if actor['actor'] != session['actor']['actor']:
                        raise Denied(401, 'Identity changed')
                    if 'id_token' in tokens:
                        claims = await asyncio.to_thread(self.oidc.claims, tokens['id_token'], CLIENT)
                        if claims['sub'] != actor['actor'] or claims.get('azp', CLIENT) != CLIENT:
                            raise Denied(401, 'Invalid renewed identity')
                    if self.session(sid) is not session:
                        raise Denied(401, 'Login required')
                    # Refresh responses may omit id_token; retain it for RP logout.
                    session['tokens'].update(tokens)
                    session['actor'] = actor
                except (httpx.HTTPError, Denied, ValueError, KeyError) as exc:
                    self.sessions.pop(sid, None)
                    raise Denied(401, 'Login required') from exc
            return session['tokens']['access_token']

    async def signed_out(self):
        return HTMLResponse('<h1>Signed out</h1><p>Your local session ended.</p><a href="/auth/login">Log in</a>')

    async def logout(self, request: Request):
        sid = request.cookies.get(COOKIE)
        session = self.session(sid)
        if not session or not secrets.compare_digest(request.query_params.get('csrf', '').encode(), session['csrf'].encode()):
            return PlainTextResponse('Invalid logout request', status_code=400)
        self.sessions.pop(sid, None)
        response = RedirectResponse(ISSUER+'/protocol/openid-connect/logout?'+urlencode(dict(
            client_id=CLIENT, id_token_hint=session['tokens']['id_token'], post_logout_redirect_uri=ORIGIN+'/')))
        response.delete_cookie(COOKIE)
        response.headers['Cache-Control'] = 'no-store'
        return response
