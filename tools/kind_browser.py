"""Check live Viewer PKCE sign-in, read-only REST and SSE, and the rendered action."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import secrets
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from playwright.sync_api import expect, sync_playwright
from playwright.sync_api import Error as BrowserError

from identity.realm import CLIENT
from tools.integration import verify_http

ORIGIN = 'http://127.0.0.1:18086'
API = 'http://127.0.0.1:18094/api/v1'
MOCK_SUMMARY = 'AI-generated MOCK investigation of published evidence.'


def login(page, password, username='viewer'):
    try:
        page.locator('#username').fill(username)
        page.locator('#password').fill(password)
        page.locator('#kc-login').click()
    except BrowserError:
        raise RuntimeError('Unable to submit the local login form') from None


def token(browser, http, password, username='viewer', origin=ORIGIN):
    # A separate public PKCE client tests the API without reading BFF tokens.
    callback = origin + '/auth/callback'
    oidc = origin + '/realms/smart-grid/protocol/openid-connect'
    verifier = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
    state = secrets.token_urlsafe(24)
    with browser.new_context() as context:
        page = context.new_page()
        callbacks = []
        def capture(request):
            url = urlsplit(request.url)
            if url.path == '/auth/callback' and request.url.startswith(callback + '?'):
                callbacks.append(parse_qs(url.query))
        page.on('request', capture)
        page.goto(oidc + '/auth?' + urlencode({'client_id': CLIENT, 'redirect_uri': callback, 'response_type': 'code',
                  'scope': 'openid', 'state': state, 'code_challenge': challenge, 'code_challenge_method': 'S256'}))
        login(page, password, username)
        expect(page.get_by_text('Invalid login state', exact=True)).to_be_visible()
        assert len(callbacks) == 1 and callbacks[0].get('state') == [state] and 'code' in callbacks[0]
    response = http.post(oidc + '/token', data={'client_id': CLIENT, 'grant_type': 'authorization_code',
                         'code': callbacks[0]['code'][0], 'redirect_uri': callback, 'code_verifier': verifier})
    assert response.status_code == 200, 'The public PKCE client could not exchange its code'
    return response.json()['access_token']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--origin', default=ORIGIN)
    parser.add_argument('--professor', action='store_true')
    parser.add_argument('--after-restart', action='store_true')
    args = parser.parse_args()
    username, role = ('professor', 'Operator') if args.professor else ('viewer', 'Viewer')
    password = json.loads((args.directory / 'local.json').read_text())[username]
    issuer = args.origin + '/realms/smart-grid'
    evidence_file = args.directory / 'evidence.json'
    evidence = json.loads(evidence_file.read_text())
    with httpx.Client(trust_env=False, timeout=5) as http, sync_playwright() as playwright:
        assert http.get(issuer + '/.well-known/openid-configuration').json()['issuer'] == issuer
        for path in ('/admin/master/console/', '/realms/master/.well-known/openid-configuration', '/metrics'):
            assert http.get(args.origin + path).status_code == 404, 'Unexpected public route'
        assert http.get(API + '/transformers').status_code == 401
        with playwright.chromium.launch() as browser:
            bearer = token(browser, http, password, username, args.origin)
            if args.after_restart:
                final = next(event['payload'] for event in reversed(evidence['actions'])
                             if event['payload']['status'] == 'COMPLETED')
                headers = {'Authorization': 'Bearer ' + bearer}
                response = http.get(API + '/actions/' + final['action_id'], headers=headers)
                response.raise_for_status()
                assert response.json() == final, 'Completed action changed after restart'
                assert http.get(API + '/overview', headers=headers).status_code == 200
                with urlopen(Request(API + '/events', headers=headers), timeout=20) as stream:
                    assert stream.headers.get_content_type() == 'text/event-stream' and stream.readline()
            else:
                final = verify_http(API, evidence, bearer, evidence['projection_generation'])
            page = browser.new_page(viewport={'width': 1440, 'height': 1000})
            page.goto(args.origin)
            login(page, password, username)
            expect(page.get_by_text(role, exact=True)).to_be_visible()
            cookie = next(c for c in page.context.cookies(args.origin) if c['name'] == 'grid_session')
            assert cookie['httpOnly'] and (not args.professor or cookie['secure'])
            if args.professor and args.after_restart:
                prior = evidence['professor_investigation_path']
                page.goto(args.origin + prior)
                expect(page.locator('body')).to_contain_text(MOCK_SUMMARY)
                page.goto(args.origin)
            page.get_by_role('button', name='Alerts & actions', exact=True).click(timeout=30000)
            page.get_by_role('button', name=final['action_id'], exact=True).click(timeout=30000)
            detail = page.get_by_role('dialog')
            expect(detail.get_by_text('COMPLETED', exact=True)).to_be_visible(timeout=15000)
            expect(detail.get_by_text('APPLIED', exact=True).first).to_be_visible()
            if args.professor:
                detail.get_by_role('button', name='Investigate action', exact=True).click()
                investigation = page.get_by_role('dialog', name='Incident investigator')
                investigation.get_by_role('button', name='Run investigation', exact=True).click()
                expect(investigation.get_by_text(MOCK_SUMMARY, exact=True)).to_be_visible(timeout=35000)
                expect(investigation.get_by_text('Published confirmed reduction', exact=False)).to_be_visible()
                if not args.after_restart:
                    details = investigation.get_by_text(re.compile(r'^\S+ · get_command_results · '))
                    details.click()
                    path = investigation.get_by_role('link', name='Authorized investigation view').first.get_attribute('href')
                    assert path and path.startswith('/investigations/')
                    evidence['professor_investigation_path'] = path
                evidence['professor_investigator'] = True
            filename = 'browser-after-restart.png' if args.after_restart else 'browser.png'
            page.screenshot(path=args.directory / filename, full_page=True)
            evidence['ui_action_equal'] = True
            evidence['oidc_viewer'] = not args.professor
            if args.professor:
                evidence['oidc_professor'] = True
            evidence_file.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
