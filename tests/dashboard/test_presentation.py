from copy import deepcopy

from dashboard.app import chart, ordered_records, table_sort_key
from dashboard.fixture_api import BASE


def test_recent_records_compare_fractional_instants_before_limiting():
    before = dict(BASE['actions'][0], action_id='before', transition_time='2026-11-09T18:30:06Z')
    after = dict(before, action_id='after', transition_time='2026-11-09T18:30:06.5Z')
    assert ordered_records('actions', [before, after])[0]['action_id'] == 'after'
    old = dict(BASE['alerts'][0], alert_id='old', opened_at='2026-11-09T18:30:06Z')
    new = dict(old, alert_id='new', resolved_at='2026-11-09T18:30:06.5Z')
    assert ordered_records('alerts', [old, new])[0]['alert_id'] == 'new'


def test_export_load_is_not_clipped_and_chart_has_an_accessible_description(monkeypatch):
    from dashboard import app
    captured = []
    class Chart:
        def classes(self, value):
            return self
        def props(self, value):
            return self
    monkeypatch.setattr(app.ui, 'echart', lambda options: (captured.append(options), Chart())[1])
    load = deepcopy(BASE['loads'][0])
    load.update(net_load_kw=-12, meter_demand_kw=10, ev_demand_kw=0, solar_generation_kw=22)
    chart([load], BASE['transformers'][0]['policy'], 100)
    assert captured[0]['yAxis']['min'] == -12
    assert captured[0]['series'][0]['data'] == [-12]
    assert 'kilowatts' in captured[0]['aria']['label']['description']


def test_table_sort_uses_instants_and_numeric_power_values():
    rows = [{'time':'2026-11-09T18:30:06Z'}, {'time':'2026-11-09T18:30:06.5Z'}]
    assert sorted(rows, key=lambda r:table_sort_key(r, 'time')) == rows
    powers = [{'power':'80.0 kW'}, {'power':'9.0 kW'}, {'power':'Unknown'}]
    assert [r['power'] for r in sorted(powers, key=lambda r:table_sort_key(r, 'power'))] == ['9.0 kW','80.0 kW','Unknown']
