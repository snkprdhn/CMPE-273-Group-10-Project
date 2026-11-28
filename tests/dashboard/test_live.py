"""Browser smoke tests for /live against the disposable backend in tests/dashboard/fake_backend.py."""
from playwright.sync_api import expect

import json
import re

import httpx

from dashboard import control_charts as charts
from tests.dashboard.conftest import BACKEND, UI


import pytest


@pytest.fixture(autouse=True)
def clean_backend(servers):
    """A forced overload lasts 90 s on the fake: never let one test see another's incident."""
    httpx.post(BACKEND + '/_reset', trust_env=False)
    yield
    httpx.post(BACKEND + '/_reset', trust_env=False)


def live(page):
    page.set_viewport_size({'width': 1600, 'height': 1100})
    page.goto(UI + '/live')
    expect(page.get_by_text('GridBalance', exact=True)).to_be_visible()
    expect(page.get_by_text('LIVE', exact=False).first).to_be_visible(timeout=15000)


def test_tabs_switch_between_grid_operations_and_pipeline_health(page, servers):
    live(page)
    expect(page.get_by_role('tab', name='Grid operations')).to_be_visible()
    expect(page.get_by_role('heading', name='Closed-loop topology')).to_be_visible()
    expect(page.get_by_text('Correctness, last 5 minutes')).not_to_be_visible()
    page.get_by_role('tab', name='Pipeline health').click()
    expect(page.get_by_text('Correctness, last 5 minutes')).to_be_visible()
    expect(page.get_by_text('Flink busy time', exact=True)).to_be_visible()
    links = {a.inner_text(): a.get_attribute('href') for a in page.locator('a:visible').all() if 'dashboard' in a.inner_text()}
    assert links == {'Pipeline dashboard': '/grafana/d/grid-pipeline', 'Kafka dashboard': '/grafana/d/grid-kafka'}
    page.get_by_role('tab', name='Grid operations').click()
    expect(page.get_by_text('Correctness, last 5 minutes')).not_to_be_visible()
    expect(page.get_by_role('heading', name='Closed-loop topology')).to_be_visible()


def click_node(page, chart, x, y):
    box, g = chart.bounding_box(), charts.GRID
    # the plot sits inside fixed pixel margins and the axes are fixed: x 0..100, y 0..Y_MAX
    page.mouse.click(box['x'] + g['left'] + x / 100 * (box['width'] - g['left'] - g['right']),
                     box['y'] + g['top'] + (1 - y / charts.Y_MAX) * (box['height'] - g['top'] - g['bottom']))


def backend_log():
    return httpx.get(BACKEND + '/_log', trust_env=False).json()


def chart_title(page, index):
    return page.locator('.nicegui-echart').nth(index).get_attribute('aria-label')


def test_clicking_a_node_selects_it_and_fetches_its_loads_immediately(page, servers):
    live(page)
    topology, detail = page.locator('.nicegui-echart').nth(1), page.locator('.nicegui-echart').nth(2)
    page.wait_for_timeout(1500)
    expect(detail).to_have_attribute('aria-label', re.compile('tx-003 +load and actions'))
    httpx.post(BACKEND + '/_delay', params=dict(seconds=1.5), trust_env=False)
    before = len([r for r in backend_log() if r['path'].endswith('/tx-004/loads')])
    click_node(page, topology, charts.STAGE_X['transformer'], 94 - 3 * 88 / 7)        # tx-004: fourth of eight nodes
    expect(page.get_by_text('Loading history…')).to_be_visible(timeout=1500)
    expect(detail).to_have_attribute('aria-label', re.compile('tx-004 +load and actions'))
    assert len([r for r in backend_log() if r['path'].endswith('/tx-004/loads')]) == before + 1     # fetched on the click, not on a later tick
    expect(page.get_by_text('Loading history…')).not_to_be_visible(timeout=5000)
    httpx.post(BACKEND + '/_delay', params=dict(seconds=0), trust_env=False)
    page.get_by_role('button', name='Follow incidents').click()
    expect(detail).to_have_attribute('aria-label', re.compile('tx-003 +load and actions'), timeout=5000)


def test_ev_rush_overloads_the_chosen_transformer_and_the_pipeline_reacts(page, servers):
    httpx.post(BACKEND + '/_reset', trust_env=False)
    live(page)
    page.get_by_role('tab', name='Pipeline health').click()            # the dock stays visible on both tabs
    expect(page.get_by_text('Chaos dock')).to_be_visible()
    for name in ('Delayed packets', 'Out of order', 'Duplicates'):
        expect(page.get_by_role('button', name=name, exact=True)).to_be_visible()
    page.get_by_role('tab', name='Grid operations').click()
    expect(page.locator('#kpi-throttled')).to_contain_text('10/70')
    page.get_by_label('Rush transformer').click()
    page.get_by_role('option', name='tx-001').click()
    page.get_by_role('button', name='6 PM EV rush (tx-001)').click()
    # Immediate feedback: status line, toast, and the chosen transformer is shown in the detail chart.
    expect(page.get_by_text('Sent: 6 PM EV rush on tx-001 for 90 s.')).to_be_visible(timeout=5000)
    expect(page.get_by_text('accepted. The grid reacts in about 10 to 15 s.')).to_be_visible()
    detail = page.locator('.nicegui-echart').nth(2)
    expect(detail).to_have_attribute('aria-label', re.compile('tx-001 +load and actions'), timeout=5000)
    sent = [r for r in backend_log() if r['method'] == 'POST' and r['path'] in ('/overload', '/inject')]
    assert sent[-1]['path'] == '/overload' and json.loads(sent[-1]['body']) == {'transformer': 'tx-001', 'seconds': 90}
    # A second control inside the rate-limit window is refused, visibly.
    page.get_by_role('button', name='Delayed packets', exact=True).click()
    expect(page.get_by_text('Not sent: wait a few seconds between controls')).to_be_visible(timeout=5000)
    assert len([r for r in backend_log() if r['path'] == '/inject']) == 0
    # The overload shows up as a critical node, then the Action service throttles it and the numbers move.
    topology = page.locator('.nicegui-echart').nth(1)
    expect(topology).to_have_attribute('aria-label', re.compile(r'the data for tx-001 is 37, [\d.]+, 120'), timeout=15000)
    expect(page.get_by_text('tx-001 (triggered at 120 %)').first).to_be_visible(timeout=15000)      # the trigger window's utilization, not the post-throttle 76 %
    stepper = page.locator('.cr-saga')
    expect(stepper.locator('.cr-step.done', has_text='Overload at 120 %')).to_be_visible(timeout=15000)
    expect(stepper.locator('.cr-step', has_text='Dispatched (CDC)')).to_be_visible()
    expect(stepper.locator('.cr-step', has_text='Reduction confirmed (44 kW)')).to_be_visible(timeout=15000)
    expect(page.locator('#kpi-throttled')).to_contain_text('20/70', timeout=25000)
    expect(page.get_by_text('throttled 10 lowest-priority chargers · 44/46 kW confirmed · acked 10/10').first).to_be_visible()
    expect(topology).to_have_attribute('aria-label', re.compile(r'the data for tx-001 is 37, [\d.]+, 76'), timeout=15000)   # load fell by the confirmed 44 kW
    httpx.post(BACKEND + '/_reset', trust_env=False)


def test_decision_log_is_in_the_right_rail(page, servers):
    live(page)
    expect(page.get_by_role('heading', name='Decision log')).to_be_visible()
    expect(page.get_by_text('throttled 10 lowest-priority chargers · 44/46 kW confirmed · acked 10/10').first).to_be_visible(timeout=8000)


def test_cloud_cover_button_sends_a_bounded_fleet_injection(page, servers):
    live(page)
    button = page.get_by_role('button', name='Cloud cover')
    expect(button).not_to_be_visible()
    httpx.post(BACKEND + '/_enable_offered', trust_env=False).raise_for_status()
    expect(button).to_be_visible(timeout=12000)
    button.click()
    expect(page.get_by_text('Sent: Cloud cover for 30 s.')).to_be_visible(timeout=8000)
    sent = [row for row in backend_log() if row['path'] == '/inject']
    assert json.loads(sent[-1]['body']) == {'kind': 'cloud_cover', 'seconds': 30}


def test_actions_outage_and_recovery_are_visible(page, servers):
    live(page)
    header = page.locator('.q-header')
    expect(header.get_by_text('LIVE · 25 transformers', exact=False)).to_be_visible(timeout=8000)
    httpx.post(BACKEND + '/_actions_available', params={'available': False}, trust_env=False).raise_for_status()
    expect(header.get_by_text('ACTIONS API UNREACHABLE · 25 transformers', exact=False)).to_be_visible(timeout=12000)
    expect(page.locator('#kpi-energy')).to_contain_text('frozen')
    httpx.post(BACKEND + '/_actions_available', params={'available': True}, trust_env=False).raise_for_status()
    expect(header.get_by_text('LIVE · 25 transformers', exact=False)).to_be_visible(timeout=12000)


def canvas_pixels(chart):
    """Number of non-transparent pixels over every canvas layer of one chart."""
    return chart.evaluate('''el => [...el.querySelectorAll('canvas')].reduce((sum, c) => {
        const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
        for (let i = 3; i < d.length; i += 4) if (d[i] > 0) sum++;
        return sum;
    }, 0)''')


def test_topology_survives_switching_tabs_without_console_errors(page, servers):
    errors = []
    page.on('console', lambda m: errors.append(m.text) if m.type == 'error' else None)
    page.on('pageerror', lambda e: errors.append(str(e)))
    live(page)
    topology = page.locator('.nicegui-echart').nth(1)
    page.wait_for_timeout(1500)
    before = canvas_pixels(topology)
    assert before > 500
    for _ in range(2):
        for name in ('Pipeline health', 'Devices (1,000)', 'Transformers (25)'):
            page.get_by_role('tab', name=name).click()
            page.wait_for_timeout(1200)        # two refresh ticks happen while the topology is hidden
        page.get_by_role('tab', name='Grid operations').click()
        page.wait_for_timeout(1200)
    expect(topology).to_have_attribute('aria-label', re.compile('tx-003'))
    assert canvas_pixels(topology) > 500
    assert errors == []


def test_saga_stepper_follows_the_selected_transformer_and_idles_otherwise(page, servers):
    live(page)
    stepper = page.locator('.cr-saga')
    expect(stepper).to_have_attribute('aria-label', 'Control saga for tx-003', timeout=8000)
    expect(stepper.locator('.cr-step')).to_have_count(7)
    expect(stepper.locator('.cr-step.active')).to_have_count(1)                 # tx-003 holds a confirmed reduction
    expect(stepper.locator('.cr-step.done')).to_have_count(4)
    page.get_by_role('combobox', name='Transformer', exact=True).click()
    page.get_by_role('option', name='tx-004').click()
    expect(stepper).to_have_attribute('aria-label', 'Control saga for tx-004', timeout=8000)
    expect(stepper.locator('.cr-step.todo')).to_have_count(7)
    expect(page.get_by_text('Monitoring tx-004: no active incident')).to_be_visible()


def fits(page, width, height, columns):
    page.set_viewport_size({'width': width, 'height': height})
    page.goto(UI + '/live')
    expect(page.get_by_text('LIVE', exact=False).first).to_be_visible(timeout=15000)
    expect(page.locator('.cr-saga .cr-step')).to_have_count(7, timeout=8000)
    page.wait_for_timeout(1500)
    dock = page.locator('.q-footer').bounding_box()['y']
    bottoms = page.eval_on_selector_all('.cr-stage-grid > .cr-col', 'cols => cols.map(c => [...c.querySelectorAll(":scope > .cr-card")].map(k => k.getBoundingClientRect().bottom).pop())')
    return dock, bottoms[:columns]


def test_closed_loop_fits_above_the_fold_at_1440x900(page, servers):
    dock, (left, right) = fits(page, 1440, 900, 2)
    assert left <= dock and right <= dock, (left, right, dock)       # topology, stepper, detail, decision log and fleet load


def test_core_view_fits_above_the_fold_at_1280x720(page, servers):
    dock, (left,) = fits(page, 1280, 720, 1)
    assert left <= dock, (left, dock)                                  # KPIs, topology, stepper and the managed-vs-shadow chart
    kpi = page.locator('.cr-kpis').bounding_box()
    assert kpi['y'] + kpi['height'] < page.locator('.cr-stage-grid').bounding_box()['y']


def test_the_four_tabs_share_the_header_and_the_dock(page, servers):
    live(page)
    tabs = page.get_by_role('tab')
    expect(tabs).to_have_text(['Grid operations', 'Transformers (25)', 'Devices (1,000)', 'Pipeline health'])
    for name in ('Transformers (25)', 'Devices (1,000)', 'Pipeline health', 'Grid operations'):
        tabs.filter(has_text=name).click()
        expect(page.locator('#kpi-load')).to_be_visible()
        expect(page.locator('#kpi-throttled')).to_contain_text('/70')
        expect(page.get_by_text('Chaos dock')).to_be_visible()


def row_ids(page):
    return page.locator('.q-table tbody tr:visible td:first-child').all_inner_texts()


def test_view_all_link_opens_the_transformer_table_and_a_row_opens_its_detail(page, servers):
    live(page)
    page.get_by_role('button', name='View all 25 transformers →').click()
    expect(page.get_by_role('tab', name='Transformers (25)')).to_have_attribute('aria-selected', 'true')
    expect(page.get_by_role('heading', name='All transformers')).to_be_visible()
    for label in ('ID', 'Band', 'Utilization %', 'Net kW', 'EV kW', 'Meter kW', 'Solar kW', 'Capacity kW', 'Managed', 'Saga status', 'Throttled', 'Coverage %'):
        expect(page.locator('th', has_text=label).first).to_be_visible()
    expect(page.locator('.q-table tbody tr')).to_have_count(10, timeout=8000)          # paginated: 10 of 25
    expect(page.get_by_text('1-10 of 25')).to_be_visible()
    assert row_ids(page)[:2] == ['tx-001', 'tx-002']
    page.locator('th', has_text='Utilization %').click()
    page.locator('th', has_text='Utilization %').click()                                 # descending
    assert row_ids(page)[0] == 'tx-003'
    page.get_by_role('textbox', name='Search transformers').fill('tx-00')
    expect(page.get_by_text('1-9 of 9')).to_be_visible()
    # a managed transformer opens the detail panel with its three views and only its own decisions
    page.locator('.q-table tbody tr', has_text='tx-003').click()
    panel = page.locator('.cr-tx-grid')
    expect(page.get_by_text('tx-003 · critical · 97 % of 100 kW')).to_be_visible(timeout=8000)
    expect(panel.get_by_text('throttled 10 lowest-priority chargers', exact=False)).to_be_visible(timeout=8000)
    expect(page.locator('.cr-tx-chart').nth(1)).to_have_attribute('aria-label', re.compile('tx-003 +load and actions'))
    expect(page.locator('.cr-tx-chart').nth(0)).to_have_attribute('aria-label', re.compile('Households'))
    assert 'tx-002' not in panel.inner_text()
    # a monitor-only transformer never polls /loads
    before = len([r for r in backend_log() if r['path'].endswith('/loads') and '/tx-012/' in r['path']])
    page.get_by_role('textbox', name='Search transformers').fill('tx-012')
    page.locator('.q-table tbody tr', has_text='tx-012').click()
    expect(page.get_by_text('Prometheus history (monitor-only)')).to_be_visible(timeout=8000)
    page.wait_for_timeout(6500)
    assert len([r for r in backend_log() if r['path'].endswith('/loads') and '/tx-012/' in r['path']]) == before == 0
    expect(page.get_by_text('History unavailable')).not_to_be_visible()
    page.get_by_role('button', name='Close').click()
    expect(panel).not_to_be_visible()


def test_devices_tab_lists_the_70_chargers_and_proves_priority_protection(page, servers):
    live(page)
    page.get_by_role('tab', name='Devices (1,000)').click()
    expect(page.get_by_text('930 monitor-only devices are aggregated per transformer; 70 controllable EV chargers are tracked individually below.')).to_be_visible(timeout=8000)
    expect(page.get_by_text('Throttled 10 · Spared 2 · Normal 58 · 70 chargers')).to_be_visible(timeout=15000)
    for label in ('Device ID', 'Transformer', 'Priority', 'Departure (UTC)', 'Rate (now / max kW)', 'Min kW', 'Last command', 'State', 'Freshness'):
        expect(page.locator('th', has_text=label).first).to_be_visible()
    expect(page.locator('th', has_text='Good')).to_be_visible()
    state = page.get_by_role('combobox', name='State')
    state.click(); page.get_by_role('option', name='Spared').click()
    expect(page.get_by_text('Showing 2 of 70')).to_be_visible()
    assert set(row_ids(page)) >= {'ev-039', 'ev-040'}
    expect(page.locator('.q-table tbody tr', has_text='ev-039')).to_contain_text('Spared')
    expect(page.locator('.q-table tbody tr', has_text='ev-039')).to_contain_text('7.2 / 7.2')
    state.click(); page.get_by_role('option', name='Throttled').click()
    expect(page.get_by_text('Showing 10 of 70')).to_be_visible()
    expect(page.locator('.q-table tbody tr', has_text='ev-029')).to_contain_text('APPLIED · 1.4 kW')
    state.click(); page.get_by_role('option', name='All states').click()
    page.get_by_role('textbox', name='Search devices').fill('ev-070')
    expect(page.get_by_text('Showing 1 of 70')).to_be_visible()
    devices = [r for r in backend_log() if r['path'] == '/api/v1/devices']
    assert devices and all(r['query'] == 'device_type=EV_CHARGER&limit=200' for r in devices)      # a single page, never a cursor



def test_the_rush_button_keeps_naming_the_transformer_it_just_hit(page, servers):
    live(page)
    page.get_by_label('Rush transformer').click()
    page.get_by_role('option', name='tx-004').click()
    expect(page.get_by_role('button', name='6 PM EV rush (tx-004)')).to_be_visible()
    page.evaluate("document.querySelector('[aria-label=\"Rush transformer\"]')")        # the dropdown choice is manual: nothing may retarget it
    page.get_by_role('button', name='6 PM EV rush (tx-004)').click()
    expect(page.get_by_text('Sent: 6 PM EV rush on tx-004 for 90 s.')).to_be_visible(timeout=5000)
    page.wait_for_timeout(9000)                                                              # several refresh ticks while tx-004 is busy
    expect(page.get_by_role('button', name='6 PM EV rush (tx-004)')).to_be_visible()


def test_the_automatic_rush_target_stays_after_the_click_and_a_row_opens_in_view(page, servers):
    live(page)
    button = page.get_by_role('button', name=re.compile(r'6 PM EV rush \(tx-00\d\)'))
    expect(button).to_have_text(re.compile('tx-001'))
    button.click()
    expect(page.get_by_text('Sent: 6 PM EV rush on tx-001 for 90 s.')).to_be_visible(timeout=5000)
    page.wait_for_timeout(9000)
    expect(button).to_have_text(re.compile('tx-001'))                                       # tx-001 is busy now, but the button must not relabel
    page.set_viewport_size({'width': 1440, 'height': 900})
    page.get_by_role('tab', name='Transformers (25)').click()
    page.locator('.q-table tbody tr', has_text='tx-004').click()
    panel = page.locator('#tx-panel')
    expect(panel).to_be_visible(timeout=8000)
    page.wait_for_timeout(900)
    box, dock = panel.bounding_box(), page.locator('.q-footer').bounding_box()
    assert box['y'] < dock['y'] - 60 and box['y'] > 0, (box, dock)                          # scrolled into view, not hidden behind the dock


def test_monitor_only_detail_hides_the_shadow_chart_everywhere(page, servers):
    live(page)
    page.get_by_role('tab', name='Transformers (25)').click()
    page.get_by_role('textbox', name='Search transformers').fill('tx-012')
    page.locator('.q-table tbody tr', has_text='tx-012').click()
    expect(page.get_by_role('heading', name='Managed vs. without throttling')).not_to_be_visible(timeout=8000)
    expect(page.get_by_text('Monitor-only: no actions are planned for this transformer.')).to_be_visible()
    expect(page.get_by_role('heading', name='Demand breakdown')).to_be_visible()
    page.get_by_role('tab', name='Grid operations').click()
    page.get_by_role('combobox', name='Transformer', exact=True).click()
    page.get_by_role('option', name='tx-012').click()
    expect(page.locator('.cr-detail')).to_have_attribute('aria-label', re.compile(r'tx-012 +load, kW \(monitor-only\)'), timeout=8000)


def test_kafka_stage_shows_the_ingested_total_and_pipeline_health_details(page, servers):
    live(page)
    expect(page.locator('.cr-note')).to_contain_text('leader epoch 1', timeout=8000)
    page.get_by_role('tab', name='Pipeline health').click()
    expect(page.get_by_text('12.35 M events ingested')).to_be_visible(timeout=8000)
    expect(page.get_by_text('Watermark delay')).to_be_visible()
    expect(page.get_by_text('10.1 s')).to_be_visible()
    expect(page.get_by_text('P0: 66.0/s (lag 10)')).to_be_visible()
    expect(page.get_by_text('S0: 133.0/s')).to_be_visible()
    expect(page.locator('tr', has_text='Rejected (DLQ)')).to_contain_text('0')
