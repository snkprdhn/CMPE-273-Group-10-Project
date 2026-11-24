"""Check live Viewer PKCE sign-in, read-only REST and SSE, and the rendered action."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import secrets
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from playwright.sync_api import expect, sync_playwright
from playwright.sync_api import Error as BrowserError

from identity.realm import CLIENT
from tools.integration import verify_http

ORIGIN = 'http://127.0.0.1:18086'
CALLBACK = ORIGIN + '/auth/callback'
ISSUER = ORIGIN + '/realms/smart-grid'
OIDC = ISSUER + '/protocol/openid-connect'
API = 'http://127.0.0.1:18094/api/v1'


def login(page, password, username='viewer'):
    try:
        page.locator('#username').fill(username)
        page.locator('#password').fill(password)
        page.locator('#kc-login').click()
    except BrowserError:
        raise RuntimeError('Unable to submit the local login form') from None


def token(browser, http, password, username='viewer'):
    # A separate public PKCE client tests the API without reading BFF tokens.
    verifier = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
    state = secrets.token_urlsafe(24)
    with browser.new_context() as context:
        page = context.new_page()
        callbacks = []
        def capture(request):
            url = urlsplit(request.url)
            if url.path == '/auth/callback' and request.url.startswith(CALLBACK + '?'):
                callbacks.append(parse_qs(url.query))
        page.on('request', capture)
        page.goto(OIDC + '/auth?' + urlencode({'client_id': CLIENT, 'redirect_uri': CALLBACK, 'response_type': 'code',
                  'scope': 'openid', 'state': state, 'code_challenge': challenge, 'code_challenge_method': 'S256'}))
        login(page, password, username)
        expect(page.get_by_text('Invalid login state', exact=True)).to_be_visible()
        assert len(callbacks) == 1 and callbacks[0].get('state') == [state] and 'code' in callbacks[0]
    response = http.post(OIDC + '/token', data={'client_id': CLIENT, 'grant_type': 'authorization_code',
                         'code': callbacks[0]['code'][0], 'redirect_uri': CALLBACK, 'code_verifier': verifier})
    assert response.status_code == 200, 'The public PKCE client could not exchange its code'
    return response.json()['access_token']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    password = json.loads((args.directory / 'local.json').read_text())['viewer']
    evidence_file = args.directory / 'evidence.json'
    evidence = json.loads(evidence_file.read_text())
    with httpx.Client(trust_env=False, timeout=5) as http, sync_playwright() as playwright:
        assert http.get(ISSUER + '/.well-known/openid-configuration').json()['issuer'] == ISSUER
        for path in ('/admin/master/console/', '/realms/master/.well-known/openid-configuration', '/metrics'):
            assert http.get(ORIGIN + path).status_code == 404, 'Unexpected public route'
        assert http.get(API + '/transformers').status_code == 401
        with playwright.chromium.launch() as browser:
            final = verify_http(API, evidence, token(browser, http, password), evidence['projection_generation'])
            page = browser.new_page(viewport={'width': 1440, 'height': 1000})
            page.goto(ORIGIN)
            login(page, password)
            expect(page.get_by_text('Viewer', exact=True)).to_be_visible()
            cookie = next(c for c in page.context.cookies(ORIGIN) if c['name'] == 'grid_session')
            assert cookie['httpOnly']
            page.get_by_role('button', name='Alerts & actions', exact=True).click(timeout=30000)
            page.get_by_role('button', name=final['action_id'], exact=True).click(timeout=30000)
            detail = page.get_by_role('dialog')
            expect(detail.get_by_text('COMPLETED', exact=True)).to_be_visible()
            expect(detail.get_by_text('APPLIED', exact=True).first).to_be_visible()
            page.screenshot(path=args.directory / 'browser.png', full_page=True)
            evidence['ui_action_equal'] = True
            evidence['oidc_viewer'] = True
            evidence_file.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
