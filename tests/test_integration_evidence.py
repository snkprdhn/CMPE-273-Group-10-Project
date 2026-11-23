"""Acceptance assertions reject missing or contradictory business evidence."""
from copy import deepcopy

import pytest

from tools.integration import GATES, evaluate, stable_id


def test_aggregation_gate_checks_committed_totals_and_identity():
    loads = []
    for second, meter in [(0, 8), (5, 15)]:
        start = f'2026-11-23T03:12:{second:02}Z'
        window = stable_id(f'window/tx-001/{start}/5')
        loads.append(dict(event_id=stable_id(f'load/{window}/0'), payload=dict(
            transformer_id='tx-001', window_start=start, window_id=window, revision=0,
            capacity_kw=20, expected_device_count=3, completeness='COMPLETE',
            meter_demand_kw=meter, ev_demand_kw=7.2, solar_generation_kw=2,
            net_load_kw=meter + 5.2)))
    evidence = dict(loads=loads, checkpoint_completed=1)
    evaluate(GATES[0], evidence)
    wrong = deepcopy(evidence)
    wrong['loads'][1]['event_id'] = wrong['loads'][0]['event_id']
    with pytest.raises(AssertionError, match='unstable load identity'):
        evaluate(GATES[0], wrong)
    with pytest.raises(AssertionError, match='checkpoint'):
        evaluate(GATES[0], dict(evidence, checkpoint_completed=0))


def test_projection_gate_requires_sse_and_chromium_even_with_matching_rest():
    evidence = dict(projection_equal=True, api_action_equal=True)
    with pytest.raises(AssertionError, match='SSE action'):
        evaluate(GATES[3], evidence)
    evidence['sse_action_equal'] = True
    with pytest.raises(AssertionError, match='browser UI'):
        evaluate(GATES[3], evidence)
