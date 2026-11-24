"""Frozen golden outputs exercise the production engine, not the reference replay."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import pickle

import pytest

from member2.aggregation import Aggregator, instant, stamp, validate, LOAD, DLQ, LATE

ROOT = Path(__file__).resolve().parents[2]
CASES = json.loads((ROOT / 'schemas/v1/golden.scenarios.json').read_text())
STREAM_CASES = [c for c in CASES if all(s['op'] in ('reading', 'evaluate', 'checkpoint_replay') for s in c['steps'])]


@pytest.mark.parametrize('case', STREAM_CASES, ids=lambda c: c['name'])
def test_golden_and_snapshot_restore(case):
    engine = Aggregator(case['topology'])
    results = []
    for step in case['steps']:
        if step['op'] == 'reading':
            results += engine.reading(step['event'], step['key'], instant(step['event']['ingested_at']))
        elif step['op'] == 'evaluate':
            results += engine.evaluate(instant(step['now']))
        else:
            results.append([r for r in results if r[0] == LOAD][step['index']])
        engine = pickle.loads(pickle.dumps(engine))
    assert [r[0] for r in results] == case['expected']['topics']
    assert [dict(event_id=e['event_id'], payload=e['payload']) for t, _, e in results if t == LOAD] == case['expected']['loads']
    assert dict(engine.metrics) == case['expected']['metrics']
    for topic, key, event in results:
        validate({LOAD: 'Load', DLQ: 'Dlq', LATE: 'Late'}[topic], event)


@pytest.mark.parametrize('mutation', ['key', 'unknown', 'type', 'future', 'sign', 'rate', 'nan', 'json'])
def test_invalid_does_not_advance_watermark_or_poison_healthy_data(mutation):
    engine = Aggregator(CASES[0]['topology'])
    e = deepcopy(CASES[0]['steps'][0]['event'])
    key = 'tx-001'
    if mutation == 'key': key = 'bad'
    if mutation == 'unknown': e['payload']['device_id'] = 'other'
    if mutation == 'type': e['payload']['device_type'] = 'SOLAR'
    if mutation == 'future': e['event_time'] = '2026-11-09T18:30:07Z'
    if mutation == 'sign': e['payload']['power_kw'] = -1
    if mutation == 'rate': e['payload'].update(minimum_rate_kw=2, current_rate_kw=1)
    if mutation == 'nan': e['payload']['power_kw'] = float('nan')
    if mutation == 'json': e = '{broken'
    assert engine.reading(e, key)[0][0] == DLQ
    assert engine.greatest is None and not engine.seen
    for step in CASES[0]['steps']:
        if step['op'] == 'reading': engine.reading(step['event'], step['key'])
    assert engine.evaluate(instant(CASES[0]['steps'][-1]['now']))[0][2]['payload']['net_load_kw'] == 15


def test_superseded_late_duplicate_boot_order_and_run_limit():
    engine = Aggregator(CASES[0]['topology'], max_events=2)
    e = deepcopy(CASES[0]['steps'][0]['event'])
    engine.reading(e, 'tx-001')
    engine.reading(e, 'tx-001')
    newer = deepcopy(e)
    newer['event_id'] = '00000000-0000-4000-8000-000000000001'
    newer['payload'].update(boot_id='new-boot', sequence_number=0, power_kw=20)
    newer['event_time'] = '2026-11-09T18:30:02Z'
    engine.reading(newer, 'tx-001')
    assert next(iter(engine.windows.values()))['devices']['meter-001']['payload']['power_kw'] == 20
    newer['event_id'] = '00000000-0000-4000-8000-000000000002'
    with pytest.raises(RuntimeError, match='RUN_EVENT_LIMIT'):
        engine.reading(newer, 'tx-001')


def test_duplicate_topology_and_empty_expected_set_rejected():
    topology = deepcopy(CASES[0]['topology'])
    topology['transformers'][0]['devices'] = []
    with pytest.raises(ValueError): Aggregator(topology)
    topology = deepcopy(CASES[0]['topology'])
    topology['transformers'] *= 2
    with pytest.raises(ValueError): Aggregator(topology)


def test_superseded_late_count_waits_for_selected_correction():
    case = deepcopy(CASES[0])
    engine = Aggregator(case['topology'])
    for step in case['steps'][:-1]: engine.reading(step['event'], step['key'])
    now = instant(case['steps'][-1]['now'])
    assert engine.evaluate(now)[0][2]['payload']['revision'] == 0
    older = deepcopy(case['steps'][1]['event'])
    older['event_id'] = '00000000-0000-4000-8000-000000000004'
    older['event_time'] = '2026-11-09T18:30:01Z'
    older['ingested_at'] = '2026-11-09T18:30:20Z'
    engine.reading(older, 'tx-001')
    engine.reading(older, 'tx-001')
    assert engine.evaluate(now) == []
    newer = deepcopy(older)
    newer['event_id'] = '00000000-0000-4000-8000-000000000005'
    newer['event_time'] = '2026-11-09T18:30:04Z'
    engine.reading(newer, 'tx-001')
    p = engine.evaluate(now)[0][2]['payload']
    assert p['revision'] == 1 and p['late_event_count'] == 2


def test_valid_very_late_event_dedup_and_no_watermark_advance():
    engine = Aggregator(CASES[0]['topology'])
    e = deepcopy(CASES[0]['steps'][0]['event'])
    e['ingested_at'] = '2026-11-09T18:30:35.001Z'
    result = engine.reading(e, 'tx-001', partition=2, offset=30)
    assert result[0][0] == LATE
    assert result[0][2]['payload']['original_offset'] == 30
    assert engine.reading(e, 'tx-001') == []
    assert engine.greatest is None and not engine.windows
    assert engine.metrics['very_late'] == engine.metrics['duplicates'] == 1


def test_global_watermark_keeps_transformer_windows_isolated():
    topology = deepcopy(CASES[0]['topology'])
    second = deepcopy(topology['transformers'][0])
    second['transformer_id'] = 'tx-002'
    for d in second['devices']: d['device_id'] += '-2'
    topology['transformers'].append(second)
    engine = Aggregator(topology)
    first = deepcopy(CASES[0]['steps'][0]['event'])
    engine.reading(first, 'tx-001')
    future = deepcopy(CASES[0]['steps'][3]['event'])
    future['payload'].update(transformer_id='tx-002', device_id='meter-001-2')
    engine.reading(future, 'tx-002')
    result = engine.evaluate(instant(future['ingested_at']))
    assert len(result) == 1 and result[0][1] == 'tx-001'
    assert result[0][2]['payload']['net_load_kw'] == 10


def test_unrepresentable_window_is_dlq_and_minimum_timestamp_can_be_audited():
    engine = Aggregator(CASES[0]['topology'])
    e = deepcopy(CASES[0]['steps'][0]['event'])
    e['event_time'] = e['ingested_at'] = '9999-12-31T23:59:59Z'
    assert engine.reading(e, 'tx-001')[0][0] == DLQ
    assert engine.greatest is None and not engine.seen
    e['event_time'] = '0001-01-01T00:00:00Z'
    assert engine.reading(e, 'tx-001')[0][0] == LATE
    e['ingested_at'] = e['event_time']
    e['event_id'] = '00000000-0000-4000-8000-000000000099'
    assert engine.reading(e, 'tx-001') == []
    assert engine.evaluate(instant('0001-01-01T00:00:00Z')) == []


def test_unrepresentable_load_is_dlq_without_mutating_selected_state():
    topology = deepcopy(CASES[0]['topology'])
    topology['transformers'][0]['capacity_kw'] = 1
    engine = Aggregator(topology)
    e = deepcopy(CASES[0]['steps'][0]['event'])
    e['payload']['power_kw'] = 1e308
    assert engine.reading(e, 'tx-001')[0][0] == DLQ
    assert not engine.windows and not engine.seen
    # Very-late finite power is valid audit data; it never enters a load sum.
    e['ingested_at'] = '2026-11-09T18:30:36Z'
    assert engine.reading(e, 'tx-001')[0][0] == LATE


def test_category_or_net_overflow_cannot_poison_a_healthy_window():
    engine = Aggregator(CASES[0]['topology'])
    first = deepcopy(CASES[0]['steps'][0]['event'])
    first['payload']['power_kw'] = 1e308
    assert engine.reading(first, 'tx-001') == []
    second = deepcopy(CASES[0]['steps'][1]['event'])
    second['payload']['power_kw'] = 1e308
    assert engine.reading(second, 'tx-001')[0][0] == DLQ
    assert len(engine.seen) == 1
    # A rejected reading has not reserved its ID or replaced the accepted sample.
    second['payload']['power_kw'] = 7
    assert engine.reading(second, 'tx-001') == []
    assert len(engine.seen) == 2


@pytest.mark.parametrize('field', ['event_time', 'ingested_at', 'departure_time'])
@pytest.mark.parametrize('value', ['2026-11-09Z', '2026-11-09 18:30:01Z', 'not-a-timeZ', '2026-02-30T18:30:01Z'])
def test_rfc3339_validation_does_not_depend_on_optional_jsonschema_extras(field, value):
    engine = Aggregator(CASES[0]['topology'])
    e = deepcopy(CASES[0]['steps'][0]['event'])
    (e['payload'] if field == 'departure_time' else e)[field] = value
    assert engine.reading(e, 'tx-001')[0][0] == DLQ
    assert not engine.windows and not engine.seen


def test_non_string_timestamp_is_dead_lettered_even_with_trace_extraction():
    from member2.tracing import remember

    engine = Aggregator(CASES[0]['topology'])
    event = deepcopy(CASES[0]['steps'][0]['event'])
    event['event_time'] = 123
    raw = json.dumps(event)
    # The managed-state operator parses the timestamp before dispatching to the engine.
    with pytest.raises(ValueError, match='Timestamp must be a string'):
        instant(event['event_time'])
    assert engine.reading(raw, 'tx-001')[0][0] == DLQ
    remember(engine, raw, {})
    assert not engine.windows
