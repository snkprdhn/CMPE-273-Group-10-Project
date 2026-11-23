"""Opt-in real Keycloak/Chromium check; never prints generated passwords or tokens."""
import base64
import hashlib
import secrets
from urllib.parse import parse_qs, urlencode, urlsplit
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import httpx
import pytest
from playwright.sync_api import sync_playwright

from identity.auth import OIDC, authorize_tool, Denied, ISSUER, CLIENT

pytestmark = pytest.mark.skipif(os.environ.get('IDENTITY_LIVE') != '1', reason='requires local Keycloak and free port 18086')


def test_real_login_roles_refresh_logout():
    config = json.loads(Path('.tools/identity/local.json').read_text())
    env = {k: v for k, v in os.environ.items() if k not in ('PYTEST_CURRENT_TEST', 'MEMBER4_AUTH_MODE', 'DASHBOARD_API_TOKEN')}
    with open('.tools/identity/ui-test.log', 'w') as log:
        proc = subprocess.Popen([sys.executable, '-m', 'dashboard'], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(100):
                assert proc.poll() is None, 'UI failed; inspect .tools/identity/ui-test.log'
                try:
                    if httpx.get('http://127.0.0.1:18086', trust_env=False).status_code == 307:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(.1)
            else:
                pytest.fail('UI did not start')
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch()
                tokens = {}
                try:
                    for user, role in [('viewer', 'Viewer'), ('operator', 'Operator'), ('administrator', 'Administrator')]:
                        context = browser.new_context()
                        page = context.new_page()
                        page.goto('http://127.0.0.1:18086')
                        page.locator('#username').fill(user)
                        page.locator('#password').fill(config[user])
                        page.locator('#kc-login').click()
                        assert urlsplit(page.url).path == '/' and urlsplit(page.url).port == 18086, page.locator('body').inner_text()
                        page.get_by_text(role, exact=True).wait_for()
                        cookies = context.cookies('http://127.0.0.1:18086')
                        assert all('token' not in c['name'] for c in cookies)
                        assert next(c for c in cookies if c['name'] == 'grid_session')['httpOnly']
                        other = context.new_page()
                        other.goto('http://127.0.0.1:18086')
                        other.get_by_text(role, exact=True).wait_for()
                        page.get_by_role('link', name='Log out', exact=True).click()
                        page.locator('#username').wait_for()
                        other.get_by_role('heading', name='Signed out', exact=True).wait_for()
                        assert 'Grid overview' not in other.locator('body').inner_text()
                        context.close()
                        # A separate PKCE client obtains tokens for direct API calls,
                        # never reading browser app storage or enabling password grants.
                        context = browser.new_context()
                        page = context.new_page()
                        verifier = secrets.token_urlsafe(32)
                        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
                        codes = []
                        def capture(request):
                            if urlsplit(request.url).path == '/auth/callback':
                                codes.append(parse_qs(urlsplit(request.url).query)['code'][0])
                        page.on('request', capture)
                        page.goto(ISSUER+'/protocol/openid-connect/auth?'+urlencode(dict(client_id=CLIENT,
                            redirect_uri='http://127.0.0.1:18086/auth/callback', response_type='code',
                            scope='openid', state=secrets.token_urlsafe(24), code_challenge=challenge, code_challenge_method='S256')))
                        page.locator('#username').fill(user)
                        page.locator('#password').fill(config[user])
                        page.locator('#kc-login').click()
                        # This independent test client has no BFF state cookie, so the
                        # application correctly refuses its callback without consuming code.
                        page.get_by_text('Invalid login state', exact=True).wait_for()
                        assert codes
                        with httpx.Client(trust_env=False) as http:
                            response = http.post(ISSUER+'/protocol/openid-connect/token', data=dict(
                                client_id=CLIENT, grant_type='authorization_code', code=codes[0],
                                redirect_uri='http://127.0.0.1:18086/auth/callback', code_verifier=verifier))
                            assert response.status_code == 200
                            initial = response.json()
                            assert OIDC().authenticate(initial['access_token'])['role'] == role
                            refresh = http.post(ISSUER+'/protocol/openid-connect/token', data=dict(
                                client_id=CLIENT, grant_type='refresh_token', refresh_token=initial['refresh_token']))
                            assert refresh.status_code == 200
                            tokens[role] = refresh.json()['access_token']
                            if role == 'Viewer':
                                with pytest.raises(Denied):
                                    authorize_tool(OIDC(), tokens[role], 'get_transformer_snapshot')
                            else:
                                assert authorize_tool(OIDC(), tokens[role], 'get_transformer_snapshot')['role'] == role
                        context.close()
                    path = Path('.tools/identity/test-tokens.json')
                    try:
                        path.write_text(json.dumps(tokens))
                        path.chmod(0o600)
                        result = subprocess.run(['.tools/member4-venv/bin/python', '-m', 'pytest', '-q',
                            'tests/member4/test_keycloak_live.py', 'tests/identity/test_bootstrap.py'], env=dict(env, IDENTITY_LIVE='1',
                            MEMBER4_TEST_DSN='postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection'),
                            capture_output=True, text=True, timeout=30)
                        assert result.returncode == 0, result.stdout+result.stderr
                    finally:
                        path.unlink(missing_ok=True)
                finally:
                    browser.close()
        finally:
            proc.terminate()
            proc.wait(timeout=15)
