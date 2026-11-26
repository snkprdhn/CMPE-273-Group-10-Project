"""Provision the read-only review client using a temporary internal bootstrap identity.

Run inside the authorized review cluster with GRID_BOOTSTRAP_SECRET and
GRID_DEMO_CLIENT_SECRET from a Kubernetes Secret. Never print token responses.
Member_4 owns identity; this does not modify application roles or existing users.
"""
import os

import httpx

from identity.auth import DEMO_CLIENT
from identity.realm import AUDIENCE


def main():
    origin = 'http://keycloak:8080'
    with httpx.Client(base_url=origin, timeout=15, trust_env=False) as http:
        response = http.post('/realms/master/protocol/openid-connect/token', data={
            'grant_type': 'client_credentials', 'client_id': 'review-bootstrap',
            'client_secret': os.environ['GRID_BOOTSTRAP_SECRET']})
        response.raise_for_status()
        http.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
        base = '/admin/realms/smart-grid'
        def request(method, path, **kwargs):
            response = http.request(method, path, **kwargs)
            response.raise_for_status()
            return response
        clients = request('GET', base+'/clients', params={'clientId': DEMO_CLIENT}).json()
        client = dict(clientId=DEMO_CLIENT, enabled=True, protocol='openid-connect',
            publicClient=False, serviceAccountsEnabled=True, standardFlowEnabled=False,
            directAccessGrantsEnabled=False, implicitFlowEnabled=False, fullScopeAllowed=False,
            secret=os.environ['GRID_DEMO_CLIENT_SECRET'], defaultClientScopes=['roles', 'basic'],
            protocolMappers=[dict(name='api-audience', protocol='openid-connect',
                protocolMapper='oidc-audience-mapper', config={'included.custom.audience': AUDIENCE,
                    'access.token.claim': 'true', 'id.token.claim': 'false'})])
        if clients:
            cid = clients[0]['id']
            request('PUT', base+'/clients/'+cid, json=client)
        else:
            response = request('POST', base+'/clients', json=client)
            cid = response.headers['Location'].rsplit('/', 1)[-1]
        viewer = request('GET', base+'/roles/Viewer').json()
        account = request('GET', base+'/clients/'+cid+'/service-account-user').json()['id']
        request('POST', base+'/users/'+account+'/role-mappings/realm', json=[viewer])
        request('POST', base+'/clients/'+cid+'/scope-mappings/realm', json=[viewer])
        bootstrap = request('GET', '/admin/realms/master/clients',
                            params={'clientId': 'review-bootstrap'}).json()
        if bootstrap:
            request('DELETE', '/admin/realms/master/clients/'+bootstrap[0]['id'])
        print('Read-only demo client configured; temporary bootstrap client removed.')


if __name__ == '__main__':
    main()
