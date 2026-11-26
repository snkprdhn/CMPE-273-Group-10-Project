import pytest
from playwright.sync_api import expect

from tests.dashboard.conftest import UI


def enter(page):
    page.set_viewport_size({'width': 1440, 'height': 1000})
    page.goto(UI)
    expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=15000)


def test_completed_action_remains_accessible_after_review_week(page, fixture_api):
    fixture_api.post('/fixture', json={'step': 3, 'fault': 'historical'}).raise_for_status()
    enter(page)
    page.get_by_role('button', name='Alerts & actions', exact=True).click()
    page.get_by_role('button', name='action-001', exact=True).click()
    dialog = page.get_by_role('dialog', name='Action explanation')
    expect(dialog.get_by_text('COMPLETED', exact=True)).to_be_visible()
    expect(dialog.get_by_role('button', name='Investigate action', exact=True)).to_be_visible()


def test_business_flow_detail_and_investigation(page, fixture_api):
    enter(page)
    expect(page.get_by_text('72.0 kW', exact=True).first).to_be_visible()
    open_button = page.get_by_role('button', name='Open tx-001', exact=True)
    open_button.focus()
    fixture_api.post('/fixture', json={'step': 1})
    expect(page.get_by_role('status').first).to_contain_text('1 critical', timeout=10000)
    expect(page.get_by_text('Live updates pending while focus is in the data panel.', exact=False)).to_be_visible()
    expect(open_button).to_be_focused()
    page.keyboard.press('Enter')
    expect(page.get_by_role('heading', name='Transformer tx-001')).to_be_visible()
    fixture_api.post('/fixture', json={'step': 1})
    expect(page.get_by_text('CRITICAL', exact=True)).to_be_visible(timeout=10000)
    expect(page.get_by_text('Overload detected', exact=True)).to_be_visible()
    page.mouse.wheel(0, -10000)
    page.wait_for_timeout(350)
    page.screenshot(path='.tools/dashboard-evidence/desktop-overload.png', full_page=True, animations='disabled')
    fixture_api.post('/fixture', json={'step': 2})
    expect(page.get_by_text('REDUCTION CONFIRMED', exact=True).first).to_be_visible(timeout=10000)
    page.get_by_role('button', name='action-001', exact=True).click()
    expect(page.get_by_role('dialog').get_by_text('command-001', exact=True)).to_be_visible()
    expect(page.get_by_role('dialog').get_by_text('APPLIED', exact=True)).to_be_visible()
    page.get_by_role('button', name='Close details').click()
    page.get_by_role('button', name='Investigate', exact=True).focus()
    fixture_api.post('/fixture', json={'step': 3})
    expect(page.get_by_text('Recovery and restore', exact=True)).to_be_visible(timeout=10000)
    expect(page.get_by_text('COMPLETED', exact=True).first).to_be_visible()
    page.get_by_role('button', name='Investigate', exact=True).click()
    expect(page.get_by_text('Scope: tx-001', exact=False)).to_be_visible()
    expect(page.get_by_text('AI-generated · Provider mode: MOCK · Read-only', exact=False)).to_be_visible()
    page.get_by_role('button', name='Run investigation', exact=True).click()
    expect(page.get_by_text('Investigation unavailable or forbidden.', exact=False)).to_be_visible(timeout=10000)
    page.screenshot(path='.tools/dashboard-evidence/investigator.png', full_page=True, animations='disabled')
    page.get_by_role('button', name='Close investigator').click()
    expect(page.get_by_text('Scope: tx-001', exact=False)).not_to_be_visible()
    page.screenshot(path='.tools/dashboard-evidence/desktop-recovery.png', full_page=True)
    # A same-document context survives every SSE update and drawer interaction.
    expect(page.get_by_role('heading', name='Transformer tx-001')).to_be_visible()


def test_sse_reconnect_keeps_navigation_and_keyboard_focus(page, fixture_api):
    enter(page)
    page.get_by_role('button', name='Devices', exact=True).click()
    search = page.get_by_label('Search IDs or text')
    search.fill('ev-001')
    search.focus()
    fixture_api.post('/fixture', json={'disconnect': True, 'step': 1})
    expect(page.get_by_role('status').first).to_contain_text('RECONNECTING', timeout=5000)
    expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=15000)
    expect(page.get_by_role('heading', name='Devices', exact=True)).to_be_visible()
    expect(search).to_have_value('ev-001')
    expect(search).to_be_focused()
    fixture_api.post('/fixture', json={'gap': True, 'step': 2})
    expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=15000)
    expect(search).to_have_value('ev-001')
    expect(search).to_be_focused()
    page.keyboard.press('Tab')
    expect(search).not_to_be_focused()
    page.screenshot(path='.tools/dashboard-evidence/reconnected.png', full_page=True)


@pytest.mark.parametrize('fault,expected', [('stale','STALE data'),('partial','PARTIAL data'),
    ('denied','Permission denied'),('unavailable','Unavailable: API dependency failed'),('error','Error: request failed (500)'),('empty','No transformers available.')])
def test_explicit_states_and_recovery(page, fixture_api, fault, expected):
    fixture_api.post('/fixture', json={'fault': fault})
    page.goto(UI)
    expect(page.get_by_text(expected, exact=False).first).to_be_visible(timeout=15000)
    page.screenshot(path=f'.tools/dashboard-evidence/state-{fault}.png', full_page=True)
    fixture_api.post('/fixture', json={'fault':'', 'step':1})
    expect(page.get_by_text('CRITICAL', exact=True).first).to_be_visible(timeout=20000)


def test_completed_history_explains_stale_telemetry_on_live_connection(page, fixture_api):
    fixture_api.post('/fixture', json={'step': 3, 'fault': 'grid-history'}).raise_for_status()
    page.goto(UI)
    expect(page.get_by_text('Live connection · Showing retained readings', exact=False)).to_be_visible()
    expect(page.get_by_text('No fresh telemetry is arriving', exact=False)).to_be_visible()
    expect(page.get_by_text('Missing or old readings cannot establish recovery', exact=False)).to_have_count(0)


def test_loading_tablet_and_scenario_mutations(page, fixture_api):
    fixture_api.post('/fixture', json={'fault':'loading'})
    page.set_viewport_size({'width':834,'height':1194})
    page.goto(UI)
    expect(page.get_by_text('Loading authoritative snapshots…')).to_be_visible()
    fixture_api.post('/fixture', json={'fault':''})
    expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=25000)
    page.get_by_role('button', name='Toggle navigation').click()
    page.get_by_role('button', name='Scenarios', exact=True).click()
    page.get_by_role('button', name='Close navigation', exact=True).click()
    expect(page.get_by_role('heading', name='Run an approved scenario')).to_be_visible()
    page.screenshot(path='.tools/dashboard-evidence/tablet-overview.png', full_page=True, animations='disabled')
    page.get_by_label('Scale', exact=True).fill('0')
    page.get_by_role('button', name='Start scenario', exact=True).click()
    expect(page.get_by_text('Enter whole numbers within the labeled limits')).to_be_visible()
    page.get_by_label('Scale', exact=True).fill('1')
    page.get_by_role('button', name='Start scenario', exact=True).click()
    page.get_by_role('button', name='Cancel', exact=True).click()
    expect(page.get_by_text('No active scenario.')).to_be_visible()
    page.get_by_role('button', name='Start scenario', exact=True).click()
    page.get_by_role('button', name='Confirm operation').click()
    expect(page.get_by_text('RUNNING', exact=True)).to_be_visible(timeout=10000)
    expect(page.get_by_role('dialog')).not_to_be_visible()
    page.screenshot(path='.tools/dashboard-evidence/tablet-scenarios.png', full_page=True, animations='disabled')
    page.get_by_role('button', name='Start scenario', exact=True).click()
    page.get_by_role('button', name='Confirm operation').click()
    expect(page.get_by_text('Conflict: reload and inspect current state')).to_be_visible()
    page.get_by_role('button', name='Close error').click()
    page.get_by_role('button', name='Stop scenario', exact=True).click()
    page.get_by_role('button', name='Confirm operation').click()
    page.get_by_role('button', name='Investigate', exact=True).focus()
    expect(page.get_by_text('No active scenario.')).to_be_visible(timeout=10000)


def test_alert_acknowledgement_and_mobile_layout(page, fixture_api):
    fixture_api.post('/fixture', json={'step':1})
    page.set_viewport_size({'width':390, 'height':844})
    page.goto(UI)
    expect(page.get_by_text('CRITICAL', exact=True).first).to_be_visible(timeout=15000)
    page.get_by_role('button', name='Acknowledge alert').click()
    page.get_by_role('button', name='Investigate', exact=True).focus()
    expect(page.get_by_text('ACKNOWLEDGED', exact=False).first).to_be_visible(timeout=10000)
    main = page.locator('#main').bounding_box()
    assert main['x'] >= 0 and main['x']+main['width'] <= 391
    page.mouse.wheel(0, -10000)
    page.wait_for_timeout(350)  # Allow native scroll and toast/dialog transitions before visual QA.
    page.screenshot(path='.tools/dashboard-evidence/mobile-overload.png', full_page=True, animations='disabled')


def test_review_open_action_updates_and_investigator_mobile_keyboard(page, fixture_api):
    fixture_api.post('/fixture', json={'step': 1}).raise_for_status()
    enter(page)
    page.get_by_role('button', name='action-001', exact=True).click()
    dialog = page.get_by_role('dialog', name='Action explanation')
    expect(dialog.get_by_text('PLAN CREATED', exact=True)).to_be_visible()
    fixture_api.post('/fixture', json={'step': 2}).raise_for_status()
    expect(dialog.get_by_text('REDUCTION CONFIRMED', exact=True)).to_be_visible(timeout=10000)
    expect(dialog.get_by_text('command-001', exact=True)).to_be_visible()
    page.get_by_role('button', name='Close details').click()
    expect(dialog).not_to_be_visible()
    page.set_viewport_size({'width': 390, 'height': 844})
    launcher = page.get_by_role('button', name='Investigate', exact=True)
    launcher.focus()
    page.keyboard.press('Enter')
    investigation = page.get_by_role('dialog', name='Incident investigator')
    expect(investigation).to_be_visible()
    expect(investigation.get_by_label('Focused question')).to_be_enabled()
    close = page.get_by_role('button', name='Close investigator')
    expect(close).to_be_focused()
    expect(close).to_be_in_viewport(ratio=1)
    box = close.bounding_box()
    assert box['x'] >= 0 and box['x']+box['width'] <= 390
    page.keyboard.press('Tab')
    expect(investigation.locator(':focus')).to_have_count(1)
    page.screenshot(path='.tools/dashboard-evidence/review-mobile-investigator.png', animations='disabled')
    page.keyboard.press('Escape')
    expect(investigation).not_to_be_visible()
    expect(launcher).to_be_focused()


def test_review_keyboard_reading_keeps_expansion_and_applies_latest_on_exit(page, fixture_api):
    enter(page)
    page.get_by_role('button', name='Open tx-001', exact=True).click()
    expect(page.get_by_role('img', name='Net load history', exact=False)).to_be_visible()
    history = page.get_by_text('Load history values', exact=True)
    history.click()
    sorter = page.get_by_role('button', name='Sort by Net kW', exact=True)
    expect(sorter).to_be_visible()
    sorter.focus()
    page.keyboard.press('Enter')
    expect(page.get_by_role('columnheader', name='Net kW')).to_have_attribute('aria-sort', 'ascending')
    fixture_api.post('/fixture', json={'step': 1}).raise_for_status()
    expect(page.get_by_text('Live updates pending while focus is in the data panel.', exact=False)).to_be_visible(timeout=10000)
    expect(page.get_by_role('status').first).to_contain_text('1 critical')
    expect(page.get_by_role('columnheader', name='Net kW')).to_be_visible()
    page.get_by_role('button', name='Investigate', exact=True).focus()
    expect(page.get_by_text('CRITICAL', exact=True)).to_be_visible(timeout=10000)
    expect(page.get_by_role('columnheader', name='Net kW')).to_be_visible()


def test_grounded_mock_investigation(page,fixture_api,copilot_server):
    enter(page)
    page.get_by_role('button',name='Open tx-001',exact=True).click()
    page.get_by_role('button',name='Investigate',exact=True).click()
    dialog=page.get_by_role('dialog',name='Incident investigator')
    dialog.get_by_role('button',name='Run investigation',exact=True).click()
    expect(dialog.get_by_text('AI-generated MOCK investigation of published evidence.',exact=True)).to_be_visible(timeout=15000)
    expect(dialog.get_by_text('Published transformer state is NORMAL;',exact=False)).to_be_visible()
    expect(dialog.get_by_text('Unknown:',exact=False).first).to_be_visible()
    page.screenshot(path='.tools/dashboard-evidence/grounded-investigation.png',animations='disabled')


def test_single_site_shows_all_tabs_without_login(page, fixture_api):
    import os
    from pathlib import Path
    import subprocess
    import sys
    import time
    import httpx
    env = {**{k: v for k, v in os.environ.items() if k != 'PYTEST_CURRENT_TEST'}, 'GRID_DEMO_MODE': '1'}
    with Path('.tools/dashboard-evidence/mvp.log').open('w') as log:
        process = subprocess.Popen([sys.executable, '-m', 'dashboard', '--fixture', '--port', '18188',
            '--api-url', 'http://127.0.0.1:18185/api/v1'], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(100):
                try:
                    if httpx.get('http://127.0.0.1:18188', timeout=.3).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(.1)
            page.goto('http://127.0.0.1:18188')
            # One open site for everyone: no login gate, and every tab is present.
            for name in ('Overview', 'Flow Map', 'Transformers', 'Alerts & actions',
                         'Devices', 'Scenarios', 'System health', 'Settings'):
                expect(page.get_by_role('button', name=name, exact=True)).to_have_count(1)
            page.get_by_role('button', name='Flow Map', exact=True).click()
            page.get_by_role('button', name='Run simulation', exact=True).click()
            expect(page.get_by_text('Simulation playback', exact=True)).to_be_visible()
        finally:
            process.terminate()
            process.wait(timeout=10)
