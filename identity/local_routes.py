"""Loopback UI's public Keycloak routes. The Keycloak admin surface stays internal."""
import asyncio
import re

import httpx
from fastapi import Request, Response
from identity.realm import public_origin

KEYCLOAK = 'http://keycloak:8080'
REALM = '/realms/smart-grid'
OIDC = REALM + '/protocol/openid-connect'
ROUTES = {
    OIDC + '/auth': {'GET'},
    OIDC + '/logout': {'GET', 'POST'},
    OIDC + '/token': {'POST'},
    OIDC + '/certs': {'GET'},
    REALM + '/.well-known/openid-configuration': {'GET'},
}
SEGMENTS = r'[A-Za-z0-9_~.-]+'
ASSET = re.compile(r'/resources/' + SEGMENTS + r'(?:/' + SEGMENTS + r')+')
LOGIN = re.compile(REALM + r'/login-actions/(?:authenticate|restart|required-action)')
REQUEST_HEADERS = {'accept', 'accept-language', 'content-type', 'origin', 'referer', 'user-agent'}
RESPONSE_HEADERS = {'cache-control', 'content-type', 'content-security-policy', 'location',
                    'pragma', 'referrer-policy', 'set-cookie', 'x-content-type-options', 'x-frame-options'}


def allowed(path, method):
    if method in ROUTES.get(path, ()):
        return True
    if LOGIN.fullmatch(path):
        return method in ('GET', 'POST')
    return method == 'GET' and ASSET.fullmatch(path) is not None and not {'.', '..'}.intersection(path.split('/'))


def install(app):
    http = httpx.AsyncClient(timeout=5, trust_env=False)
    origin = public_origin(None)
    app.on_shutdown(http.aclose)

    @app.get('/healthz')
    async def health():
        return {'status': 'ok'}

    @app.get('/readyz')
    async def ready():
        try:
            upstream = await asyncio.gather(http.get('http://api:18084/api/v1/ready'),
                                           http.get(KEYCLOAK + OIDC + '/certs'))
            return Response(status_code=200 if all(r.is_success for r in upstream) else 503)
        except httpx.RequestError:
            return Response(status_code=503)

    @app.api_route('/realms/{rest:path}', methods=['GET', 'POST'], include_in_schema=False)
    @app.api_route('/resources/{rest:path}', methods=['GET'], include_in_schema=False)
    async def proxy(rest: str, request: Request):
        path = request.url.path
        if not allowed(path, request.method):
            return Response(status_code=404)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 65536:
                return Response(status_code=413)
        headers = {k: v for k, v in request.headers.items() if k in REQUEST_HEADERS}
        if origin:
            headers.update({'x-forwarded-proto': 'https', 'x-forwarded-host': origin.removeprefix('https://')})
        # Never send the dashboard's session or PKCE state to the IdP.
        cookies = [part.strip() for part in request.headers.get('cookie', '').split(';')
                   if part.strip().partition('=')[0] in
                   {'AUTH_SESSION_ID', 'AUTH_SESSION_ID_LEGACY', 'KC_RESTART', 'KEYCLOAK_IDENTITY',
                    'KEYCLOAK_SESSION', 'KEYCLOAK_REMEMBER_ME', 'KC_AUTH_SESSION_HASH'}]
        if cookies:
            headers['cookie'] = '; '.join(cookies)
        try:
            # An explicit Request avoids sharing one httpx cookie jar among browsers.
            upstream = await http.send(httpx.Request(request.method, KEYCLOAK + path,
                                       params=request.query_params, headers=headers, content=bytes(body)))
        except httpx.RequestError:
            return Response(status_code=502)
        response = Response(upstream.content, status_code=upstream.status_code)
        response.raw_headers.extend((k.lower(), v) for k, v in upstream.headers.raw
                                    if k.decode().lower() in RESPONSE_HEADERS)
        return response
