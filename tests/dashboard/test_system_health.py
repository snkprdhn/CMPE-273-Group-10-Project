"""Playwright tests for the System health tab.

These tests use the same fixture server as the other dashboard browser tests.
The fixture API does not serve Prometheus or Flink metrics, so both polling
sources are unreachable during the test. The tests verify that:

1. The tab renders without crashing when both sources are unavailable.
2. All four KPI card labels are present in the DOM.
3. Both ECharts elements are rendered (throughput and lag charts).

No live metrics are asserted — that would require a running Prometheus/Flink,
which is not part of the local dashboard fixture environment.
"""
import pytest
from playwright.sync_api import expect

from tests.dashboard.conftest import UI


def _navigate_to_system_health(page):
    page.set_viewport_size({'width': 1440, 'height': 900})
    page.goto(UI)
    # Wait for initial data load before navigating.
    expect(page.get_by_role('status').first).to_contain_text('LIVE', timeout=15000)
    page.get_by_role('button', name='System health', exact=True).click()
    # The heading or the KPI section must appear; give up to 8 s for
    # the tab to render (render() runs synchronously on navigate()).
    expect(page.get_by_text('Records In/s', exact=True).first).to_be_visible(timeout=8000)


def test_system_health_tab_renders(page, fixture_api):
    """Tab navigates cleanly and shows the section heading."""
    fixture_api.post('/fixture', json={'step': 0}).raise_for_status()
    _navigate_to_system_health(page)
    # The eyebrow label rendered by render_system_health.
    expect(page.get_by_text('SYSTEM HEALTH', exact=True)).to_be_visible()
    expect(page.get_by_text(
        'Kafka throughput · Flink aggregation · Consumer lag', exact=True,
    )).to_be_visible()


def test_system_health_kpi_cards_present(page, fixture_api):
    """All four KPI card eyebrow labels are present."""
    fixture_api.post('/fixture', json={'step': 0}).raise_for_status()
    _navigate_to_system_health(page)
    for label in ('Records In/s', 'Records Out/s', 'Consumer Lag', 'Last Checkpoint'):
        expect(page.get_by_text(label, exact=True)).to_be_visible()


def test_system_health_charts_rendered(page, fixture_api):
    """Both ECharts card headings are present — charts render inside their cards."""
    fixture_api.post('/fixture', json={'step': 0}).raise_for_status()
    _navigate_to_system_health(page)
    # Assert the card eyebrow labels that wrap each chart are visible.
    # The ECharts canvas itself loads asynchronously; the heading is synchronous.
    expect(page.get_by_text('Flink throughput — records/s', exact=True)).to_be_visible()
    expect(page.get_by_text('Kafka consumer lag — per partition', exact=True)).to_be_visible()
