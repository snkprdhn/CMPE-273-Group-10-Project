from tools.load_test import percentile, summarize


def test_missing_measurements_are_not_zero_latency():
    report = summarize({})
    assert report['window_delay_seconds']['p95'] is None
    assert report['acknowledgement_seconds'] == []
    assert report['edge_rate'] is None


def test_stage_boundaries_and_achieved_rate():
    report = summarize({'load_samples': [dict(elapsed=t,count=3,bytes=1800) for t in (0,59.9,60,120)]})
    assert [s['offered_count'] for s in report['stages']] == [6,3,3]
    assert report['stages'][0]['offered_eps'] == .1
    assert percentile([1,2,100], .95) == 100
    assert percentile([], .5) is None


def test_kubectl_adjacent_objects_and_list():
    import pytest
    from tools.load_test import resources
    assert resources(' {"kind":"Job"}\n{"kind":"Job"}\n') == [{'kind':'Job'}, {'kind':'Job'}]
    assert resources('{"kind":"List","items":[{"kind":"Job"}]}') == [{'kind':'Job'}]
    with pytest.raises(ValueError):
        resources('{"kind":"Job"}broken')


def test_business_reduction_uses_contract_float_tolerance():
    evidence=dict(loads=[dict(event_id='a',payload=dict(net_load_kw=20.2,ev_demand_kw=7.2,window_end='2026-11-23T00:00:00Z')),dict(event_id='b',payload=dict(net_load_kw=15,ev_demand_kw=2.000000000000001,window_end='2026-11-23T00:00:05Z'))],received=[dict(event_id=e,observed_at=t) for e,t in [('a','2026-11-23T00:00:11Z'),('b','2026-11-23T00:00:16Z')]])
    business=summarize(evidence)['business']
    assert abs(business['observed_ev_reduction_kw']-5.2)<1e-9
    assert business['critical_to_safe_observed_seconds']==5
