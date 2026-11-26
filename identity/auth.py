"""Validate Keycloak JWTs using a fixed issuer, audience and signing algorithm."""
import math
import os

import jwt
from jwt import PyJWKClient

from identity.realm import CLIENT, AUDIENCE, public_origin

ROLES = ('Viewer', 'Operator', 'Administrator')
DEMO_CLIENT = 'smart-grid-demo'
ISSUER = 'http://127.0.0.1:18080/realms/smart-grid'


class Denied(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message
        super().__init__(message)


def require(actor, minimum):
    if actor.get('role') not in ROLES or ROLES.index(actor['role']) < ROLES.index(minimum):
        raise Denied(403, 'Insufficient role')
    return actor


class OIDC:
    def __init__(self):
        # The named kind profile keeps the browser issuer exact and fetches keys by cluster DNS.
        kind = os.getenv('GRID_LOCAL_KIND') == '1'
        self.issuer = public_origin('http://127.0.0.1:18086') + '/realms/smart-grid' if kind else ISSUER
        self.backchannel = 'http://keycloak:8080/realms/smart-grid' if kind else self.issuer
        self.keys = PyJWKClient(self.backchannel + '/protocol/openid-connect/certs', timeout=3)

    def claims(self, token, audience=AUDIENCE):
        try:
            if not isinstance(token, str) or len(token) > 16384:
                raise ValueError('Invalid token size')
            key = self.keys.get_signing_key_from_jwt(token)
            claims = jwt.decode(token, key.key, algorithms=['RS256'], audience=audience,
                                issuer=self.issuer, options={'require': ['exp', 'iat', 'sub', 'iss', 'aud']})
            for name in ('exp', 'iat', 'nbf'):
                if name in claims and (type(claims[name]) not in (int, float) or not math.isfinite(claims[name])):
                    raise ValueError('Invalid NumericDate')
            if not isinstance(claims['sub'], str) or not claims['sub']:
                raise ValueError('Missing subject')
            return claims
        except (jwt.PyJWTError, ValueError, TypeError, KeyError, RecursionError, OverflowError) as exc:
            raise Denied(401, 'Valid OIDC bearer token required') from exc

    def authenticate(self, token):
        claims = self.claims(token)
        if claims.get('typ') != 'Bearer' or claims.get('azp') not in (CLIENT, DEMO_CLIENT):
            raise Denied(401, 'Invalid access token type or client')
        realm_access = claims.get('realm_access', {})
        if not isinstance(realm_access, dict):
            raise Denied(403, 'No application role')
        roles = realm_access.get('roles', [])
        if not isinstance(roles, list):
            raise Denied(403, 'No application role')
        role = next((r for r in reversed(ROLES) if r in roles), None)
        if role is None:
            raise Denied(403, 'No application role')
        if claims['azp'] == DEMO_CLIENT and role != 'Viewer':
            raise Denied(403, 'Demo client must be read-only')
        return dict(actor=claims['sub'], role=role, expires_at=claims['exp'])


TOOLS = frozenset(('get_transformer_snapshot', 'get_load_history', 'get_action_timeline',
                   'get_command_results', 'get_stream_health', 'get_metric_window', 'get_trace_summary'))


def authorize_tool(oidc, token, name):
    """Revalidate the caller at tool dispatch; model-provided roles are never inputs."""
    actor = require(oidc.authenticate(token), 'Operator')
    if not isinstance(name, str) or name not in TOOLS:
        raise Denied(403, 'Tool unavailable')
    return actor
