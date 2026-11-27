"""Fleet identity and reporting-clock regressions for the real backend probe."""
from collections import Counter
from devices.common import utc
from tools.backend_scale import fleet, reporting_transformers


def test_scale_fleet_has_distinct_devices_and_declared_types():
    devices=fleet('acceptance',utc())
    assert len({d['device_id'] for d in devices})==2500
    assert len({d['transformer_id'] for d in devices})==25
    assert Counter(d['device_type'] for d in devices)=={
        'SMART_METER':1375,'EV_CHARGER':875,'SOLAR':250}
    assert sum(d['minimum_rate_kw']==1 for d in devices)==25


def test_foreground_reports_on_every_frame_despite_timer_jitter():
    foreground={'hot'};due={'hot':4.001,'background':16.0}
    assert reporting_transformers(foreground,due,3.999)=={'hot'}
    assert reporting_transformers(foreground,due,4.002)=={'hot'}
    assert reporting_transformers(foreground,due,16.0)=={'hot','background'}
