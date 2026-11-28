"""An empty COPILOT_URL hides every Investigate button (the AKS demo has no Investigator)."""
import os
from pathlib import Path
import subprocess
import sys
import time

import httpx
import pytest
from playwright.sync_api import expect

from tests.dashboard.conftest import API

PORT = 18190


@pytest.mark.parametrize('hosted', [False, True])
def test_empty_copilot_url_hides_investigate_buttons(page, fixture_api, hosted):
    env = {**{k: v for k, v in os.environ.items() if k != 'PYTEST_CURRENT_TEST'}, 'COPILOT_URL': ''}
    if hosted:
        env.update(DASHBOARD_HOME='live', AUTH_MODE='mock', DASHBOARD_API_TOKEN='fixture-operator-token')
    else:
        env.pop('DASHBOARD_HOME', None)
    command = [sys.executable, '-m', 'dashboard', '--port', str(PORT), '--api-url', API + '/api/v1']
    if not hosted:
        command.append('--fixture')
    url = f'http://127.0.0.1:{PORT}/operator'
    with Path('.tools/dashboard-evidence/no-copilot.log').open('w') as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(100):
                try:
                    if httpx.get(url, timeout=.3, trust_env=False).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(.1)
            fixture_api.post('/fixture', json={'step': 3, 'fault': 'historical'}).raise_for_status()
            page.set_viewport_size({'width': 1440, 'height': 1000})
            page.goto(url)
            expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=15000)
            expect(page.get_by_role('heading', name='Grid overview')).to_be_visible()
            expect(page.get_by_role('button', name='Investigate', exact=True)).to_have_count(0)
            for name in ('Flow Map', 'Scenarios'):
                expect(page.get_by_role('button', name=name, exact=True)).to_have_count(0 if hosted else 1)
            page.get_by_role('button', name='Alerts & actions', exact=True).click()
            page.get_by_role('button', name='action-001', exact=True).click()
            expect(page.get_by_role('dialog', name='Action explanation').get_by_text('COMPLETED', exact=True)).to_be_visible()
            expect(page.get_by_role('button', name='Investigate action', exact=True)).to_have_count(0)
            expect(page.get_by_role('button', name='Investigate alert')).to_have_count(0)
        finally:
            process.terminate()
            process.wait(timeout=10)
