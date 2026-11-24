"""Local Keycloak realm and client configuration shared by deployment profiles."""

import os
from urllib.parse import urlsplit

CLIENT = "smart-grid-ui"
AUDIENCE = "smart-grid-api"


def public_origin(default):
    origin = os.getenv('GRID_PUBLIC_ORIGIN')
    if origin is None:
        return default
    parsed = urlsplit(origin)
    if parsed.scheme != 'https' or not parsed.hostname or origin != 'https://' + parsed.hostname:
        raise ValueError('GRID_PUBLIC_ORIGIN must be an HTTPS hostname without a path or port')
    return origin


def realm(passwords, origin='http://127.0.0.1:18086', review=False):
    roles = [dict(name='Viewer'), dict(name='Operator', composite=True,
             composites={'realm': ['Viewer']}), dict(name='Administrator', composite=True,
             composites={'realm': ['Operator']})]
    accounts = [('professor', 'Operator')] if review else [
        ('viewer', 'Viewer'), ('operator', 'Operator'), ('administrator', 'Administrator')]
    return dict(realm='smart-grid', enabled=True, sslRequired='external' if review else 'none', registrationAllowed=False,
        resetPasswordAllowed=False, bruteForceProtected=True, accessTokenLifespan=120,
        ssoSessionIdleTimeout=1800, ssoSessionMaxLifespan=3600,
        revokeRefreshToken=True, refreshTokenMaxReuse=0, roles={'realm': roles},
        clients=[dict(clientId=CLIENT, enabled=True, publicClient=True,
            standardFlowEnabled=True, directAccessGrantsEnabled=False, implicitFlowEnabled=False,
            serviceAccountsEnabled=False, fullScopeAllowed=False,
            redirectUris=[origin + '/auth/callback'],
            attributes={'pkce.code.challenge.method': 'S256',
                        'post.logout.redirect.uris': origin + '/'},
            defaultClientScopes=['roles', 'basic'], protocol='openid-connect',
            protocolMappers=[dict(name='api-audience', protocol='openid-connect',
                protocolMapper='oidc-audience-mapper', config={'included.custom.audience': AUDIENCE,
                    'access.token.claim': 'true', 'id.token.claim': 'false'})])],
        clientScopes=[dict(name='basic', protocol='openid-connect', protocolMappers=[
            dict(name='subject', protocol='openid-connect', protocolMapper='oidc-sub-mapper',
                 config={'access.token.claim': 'true', 'introspection.token.claim': 'true'})]),
            dict(name='roles', protocol='openid-connect',
            protocolMappers=[dict(name='realm-roles', protocol='openid-connect',
                protocolMapper='oidc-usermodel-realm-role-mapper',
                config={'claim.name': 'realm_access.roles', 'jsonType.label': 'String',
                        'multivalued': 'true', 'access.token.claim': 'true'})])],
        scopeMappings=[{'client': CLIENT, 'roles': ['Viewer', 'Operator', 'Administrator']}],
        users=[dict(username=name, enabled=True, realmRoles=[role], firstName='Local',
            lastName=role, email=name+'@example.invalid', emailVerified=True,
            credentials=[dict(type='password', value=passwords[name], temporary=False)])
            for name, role in accounts])
