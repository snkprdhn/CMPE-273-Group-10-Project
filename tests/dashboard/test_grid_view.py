from pathlib import Path
import re

from playwright.sync_api import expect

from tests.dashboard.conftest import UI

EVIDENCE = Path('.tools/dashboard-evidence')


def test_grid_view_overload_ack_measurement_and_retained_restore(page, fixture_api):
    def stage(step, fault='grid-live'):
        fixture_api.post('/fixture', json={'step': step, 'fault': fault}).raise_for_status()

    stage(1)
    page.set_viewport_size({'width': 1440, 'height': 950})
    page.goto(UI)
    page.get_by_role('button', name='Open tx-001', exact=True).click()
    grid = page.get_by_label('Grid View', exact=True)
    expect(grid.get_by_text('Net load 96.0 kW / capacity', exact=False)).to_be_visible()
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('28.0 kW')
    expect(grid.get_by_text('Requested charging rate · Unknown', exact=False)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'grid-overload.png', full_page=True)

    stage(1, 'grid-ack')
    expect(grid.get_by_text('Throttle acknowledgement', exact=False)).to_be_visible()
    expect(grid.get_by_text('Requested charging rate 7.0 kW', exact=False)).to_be_visible()
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('28.0 kW')
    expect(grid.get_by_text('Post-throttle measured reduction unavailable', exact=False)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'grid-throttle-ack.png', full_page=True)

    stage(2)
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('7.0 kW')
    expect(grid.get_by_text('Historical measured EV reduction · 21.0 kW', exact=False)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'grid-throttling.png', full_page=True)

    stage(3, 'grid-history')
    expect(grid.get_by_text('Stale reading', exact=False)).to_be_visible()
    expect(grid.get_by_text('Restore acknowledgement', exact=False)).to_be_visible()
    expect(grid.get_by_text('Historical EV draw after restore · 28.0 kW', exact=False)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'grid-recovery.png', full_page=True)
    grid.get_by_text('EV charger readings · tx-001', exact=True).click()
    expect(grid.get_by_text('ev-001 · Throttle participant · Measured draw 28.0 kW', exact=False)).to_be_visible()
    grid.get_by_role('button', name='Open action action-001').click()
    expect(page.get_by_role('dialog', name='Action explanation').get_by_role('button', name='Investigate action')).to_be_visible()


def test_grid_view_scopes_2500_device_fixture_to_selected_transformer(page, fixture_api):
    fixture_api.post('/fixture', json={'step': 0, 'fault': 'grid-scale'}).raise_for_status()
    page.goto(UI)
    page.get_by_role('button', name='Flow Map', exact=True).click()
    overview = page.get_by_label('Grid electricity map', exact=True)
    expect(overview.get_by_role('button', name='Open tx-', exact=False)).to_have_count(25, timeout=30000)
    tile = overview.locator('.grid-map-tile').filter(has=page.get_by_role('button', name='Open tx-025', exact=True))
    for kind, count in [('EV_CHARGER', 35), ('SOLAR', 10), ('SMART_METER', 55)]:
        group = tile.get_by_test_id(f'grid-{kind}')
        group.get_by_role('button').click()
        expect(group.get_by_text('Unknown · UNKNOWN', exact=True).filter(visible=True)).to_have_count(count)
    expect(tile.get_by_text('tx-024-device', exact=False)).to_have_count(0)
    page.screenshot(path=EVIDENCE / 'flow-map-expanded.png', full_page=False, animations='disabled')
    tile.get_by_role('button', name='Open tx-025', exact=True).click()
    grid = page.get_by_label('Grid View', exact=True)
    expect(grid.get_by_test_id('grid-SMART_METER')).to_contain_text('55 listed')
    expect(grid.get_by_test_id('grid-SOLAR')).to_contain_text('10 listed')
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('35 listed')
    grid.get_by_text('EV charger readings · tx-025', exact=True).click()
    expect(grid.get_by_text('Measured draw Unknown', exact=False).filter(visible=True)).to_have_count(35)
    expect(grid.get_by_text('tx-024-device', exact=False)).to_have_count(0)


def test_flow_map_shows_applied_throttle_and_restores_normal_color(page, fixture_api):
    fixture_api.post('/fixture', json={'step': 1, 'fault': 'grid-live'}).raise_for_status()
    page.set_viewport_size({'width': 1440, 'height': 1000})
    page.goto(UI)
    expect(page.get_by_label('Grid electricity map', exact=True)).to_have_count(0)
    page.get_by_role('button', name='Flow Map', exact=True).click()
    grid = page.get_by_label('Grid electricity map', exact=True)
    expect(grid.get_by_text('Live connection', exact=True)).to_be_visible()
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('28.0 kW')
    expect(grid.locator('.grid-branch.moving')).to_have_count(3)
    expect(grid.get_by_role('img', name='Into transformer')).to_have_count(1)
    solar = grid.get_by_test_id('grid-SOLAR').bounding_box()
    hub = grid.locator('.grid-hub-circle').bounding_box()
    ev = grid.get_by_test_id('grid-EV_CHARGER').bounding_box()
    assert solar['x'] + solar['width'] <= hub['x'] and hub['x'] + hub['width'] <= ev['x']
    expect(grid.get_by_role('img', name='Into transformer')).to_have_class(
        re.compile(r'\bout\b.*grid-input-line'))

    page.screenshot(path=EVIDENCE / 'flow-map.png', full_page=True, animations='disabled')

    group = grid.get_by_test_id('grid-EV_CHARGER')
    group.get_by_role('button').click()
    expect(group.get_by_text('ev-001', exact=True)).to_be_visible()
    grid.get_by_role('heading').click()
    expect(group).not_to_have_class('throttled')
    fixture_api.post('/fixture', json={'step': 1, 'fault': 'grid-ack'}).raise_for_status()
    expect(group.locator('.grid-device.throttled')).to_have_count(1)
    expect(group.get_by_text('Throttle applied · 7.0 kW rate', exact=True)).to_be_visible()
    expect(group.get_by_text('→ 28.0 kW · FRESH', exact=True)).to_be_visible()
    expect(grid.locator('.grid-branch[style*="#b42332"]')).to_have_count(1)
    fixture_api.post('/fixture', json={'step': 2, 'fault': 'grid-live'}).raise_for_status()
    expect(group).to_contain_text('7.0 kW')
    expect(group.get_by_text('ev-001', exact=True)).to_be_visible()
    expect(group.get_by_text('→ 7.0 kW · FRESH', exact=True)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'flow-map-live-group.png', full_page=True, animations='disabled')
    fixture_api.post('/fixture', json={'step': 3, 'fault': 'grid-live'}).raise_for_status()
    expect(group.locator('.grid-device.throttled')).to_have_count(0)
    expect(grid.locator('.grid-branch[style*="#b42332"]')).to_have_count(0)
    expect(group.get_by_text('→ 28.0 kW · FRESH', exact=True)).to_be_visible()
    page.screenshot(path=EVIDENCE / 'flow-map-restored.png', full_page=True, animations='disabled')
    fixture_api.post('/fixture', json={'step': 3, 'fault': 'grid-history'}).raise_for_status()
    expect(grid.get_by_test_id('grid-EV_CHARGER')).to_contain_text('28.0 kW')
    expect(grid.locator('.grid-branch.moving')).to_have_count(0)
    expect(grid.get_by_text('STALE · Window end', exact=False)).to_be_visible()

    page.set_viewport_size({'width': 390, 'height': 844})
    expect(grid).to_be_visible()
    assert grid.bounding_box()['x'] + grid.bounding_box()['width'] <= 391


def test_flow_map_demo_playback_is_separate_and_can_switch_scenarios(page, fixture_api):
    page.goto(UI)
    page.get_by_role('button', name='Flow Map', exact=True).click()
    grid = page.get_by_label('Grid electricity map', exact=True)
    expect(grid.get_by_text('Live connection', exact=True)).to_be_visible()
    page.get_by_role('button', name='Run simulation', exact=True).click()
    expect(grid.get_by_text('Simulation playback', exact=True)).to_be_visible()
    expect(page.get_by_text('EV charging surge', exact=False)).to_be_visible(timeout=7000)
    ev = grid.get_by_test_id('grid-EV_CHARGER')
    expect(ev).to_contain_text('7.2 kW')
    page.get_by_role('button', name='Next phase', exact=True).click()
    expect(page.get_by_text('Throttle acknowledged', exact=False)).to_be_visible()
    expect(ev).to_contain_text('7.2 kW')
    expect(ev).to_contain_text('throttle applied')
    page.get_by_role('button', name='Next phase', exact=True).click()
    expect(ev).to_contain_text('2.0 kW')
    page.get_by_role('button', name='Next phase', exact=True).click()
    expect(ev).to_contain_text('2.0 kW')
    expect(ev).not_to_contain_text('throttle applied')
    page.get_by_role('button', name='Next phase', exact=True).click()
    expect(page.get_by_text('Simulation complete', exact=True)).to_be_visible()
    expect(ev).to_contain_text('7.2 kW')

    page.get_by_label('Demo scenario').click()
    page.get_by_text('Sudden solar drop', exact=True).click()
    page.get_by_role('button', name='Run simulation', exact=True).click()
    solar = grid.get_by_test_id('grid-SOLAR')
    expect(solar).to_contain_text('5.0 kW')
    page.get_by_role('button', name='Next phase', exact=True).click()
    expect(solar).to_contain_text('0.0 kW')
    page.get_by_role('button', name='Return to live grid', exact=True).click()
    expect(grid.get_by_text('Live connection', exact=True)).to_be_visible()
    expect(grid.get_by_role('button', name='Open tx-001', exact=True)).to_be_visible()
