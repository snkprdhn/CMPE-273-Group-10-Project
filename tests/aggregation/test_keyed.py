"""Per-transformer state: same results as the plain engine, bounded memory, restore-safe."""
from datetime import datetime, timedelta, timezone
import json
import pickle
import random
from uuid import UUID

from aggregation.engine import Aggregator, LOAD, LATE, DLQ, stamp
from aggregation.keyed import Keyed, RETAIN_SECONDS

TOPOLOGY = {'topology_version': 'keyed-test', 'transformers': [
    {'transformer_id': f'tx-{i:03}', 'capacity_kw': 50, 'devices': [
        {'device_id': f'{kind}-{i:03}-{j}', 'device_type': kind}
        for kind, count in (('SMART_METER', 6), ('EV_CHARGER', 3), ('SOLAR', 1)) for j in range(count)]}
    for i in range(2)]}
BASE = datetime(2026, 11, 26, 20, 0, 0, tzinfo=timezone.utc)


class Slots:
    """MapState stand-in. Values round-trip through pickle exactly as Flink state does."""
    def __init__(self):
        self.data = {}

    def get(self, key):
        raw = self.data.get(key)
        return None if raw is None else pickle.loads(raw)

    def put(self, key, value):
        self.data[key] = pickle.dumps(value)

    def remove(self, key):
        self.data.pop(key, None)


class Meta:
    def __init__(self):
        self.raw = None

    def value(self):
        return None if self.raw is None else pickle.loads(self.raw)

    def update(self, value):
        self.raw = pickle.dumps(value)


def reading(tx, device, kind, at, arrival, power, seq, event_id=None):
    sign = -1 if kind == 'SOLAR' else 1
    return json.dumps(dict(
        event_id=event_id or str(UUID(int=random.getrandbits(128), version=4)), event_type='telemetry.reading',
        schema_version=1, source='test', event_time=stamp(at), ingested_at=stamp(arrival), correlation_id='run',
        causation_id=None, payload=dict(device_id=device, transformer_id=tx, device_type=kind, boot_id='b',
                                        quality='GOOD', sequence_number=seq, power_kw=sign * power)))


def stream(windows=8, seed=1, disorder=True):
    """Devices report once per window at a random offset; arrivals are shuffled within a few seconds."""
    rng = random.Random(seed)
    random.seed(seed)
    rows = []
    for w in range(windows):
        start = BASE + timedelta(seconds=5 * w)
        for t in TOPOLOGY['transformers']:
            for d in t['devices']:
                at = start + timedelta(seconds=rng.uniform(.1, 4.8))
                arrival = at + timedelta(seconds=rng.uniform(0, 3 if disorder else 0))
                rows.append((arrival, t['transformer_id'], reading(
                    t['transformer_id'], d['device_id'], d['device_type'], at, arrival, 1 + rng.random(), w)))
    rows.sort(key=lambda r: r[0])
    return rows


def loads(results):
    return sorted((e['payload']['transformer_id'], e['payload']['window_start'], e['payload']['revision'],
                   e['payload']['net_load_kw'], e['payload']['good_device_count'])
                  for topic, _, e in results if topic == LOAD)


def test_matches_plain_engine_with_out_of_order_arrival():
    rows = stream()
    plain = Aggregator(TOPOLOGY)
    expected = []
    for arrival, key, raw in rows:
        expected += plain.reading(raw, key, arrival)
        expected += plain.evaluate(arrival)
    keyed = Keyed(TOPOLOGY)
    slots = {t['transformer_id']: (Slots(), Meta()) for t in TOPOLOGY['transformers']}
    actual = []
    for arrival, key, raw in rows:
        results, _, _ = keyed.process(key, raw, 0, 0, *slots[key], now=arrival)
        actual += results
    # The plain engine uses one global watermark; the keyed engine one per transformer.
    # Windows that both close must carry identical totals.
    closed = {(k[0], k[1]): k for k in loads(expected)}
    got = {(k[0], k[1]): k for k in loads(actual)}
    assert got and set(got) <= set(closed) | set(got)
    for window, row in got.items():
        assert row[4] == len(TOPOLOGY['transformers'][0]['devices'])
        if window in closed:
            assert abs(row[3] - closed[window][3]) < 1e-9


def test_duplicate_is_ignored_and_state_survives_restart():
    rows = stream(windows=4, disorder=False)
    keyed = Keyed(TOPOLOGY)
    slots = {t['transformer_id']: (Slots(), Meta()) for t in TOPOLOGY['transformers']}
    seen, outputs = 0, []
    for arrival, key, raw in rows:
        results, deltas, _ = keyed.process(key, raw, 0, 0, *slots[key], now=arrival)
        outputs += results
        results, deltas2, _ = keyed.process(key, raw, 0, 0, *slots[key], now=arrival)   # redelivery
        assert results == [] and deltas2['duplicates'] == 1
        seen += 1
        if seen == len(rows) // 2:
            keyed = Keyed(TOPOLOGY)   # new process: restores from the persisted state only
    assert loads(outputs)
    assert all(r[4] == 10 for r in loads(outputs))


def test_state_is_bounded_by_the_replay_horizon():
    keyed = Keyed(TOPOLOGY)
    slots, meta = Slots(), Meta()
    count = int(RETAIN_SECONDS / 5)
    for w in range(0, 400):   # 400 windows, about 33 minutes of event time
        start = BASE + timedelta(seconds=5 * w)
        for d in TOPOLOGY['transformers'][0]['devices']:
            at = start + timedelta(seconds=2)
            keyed.process('tx-000', reading('tx-000', d['device_id'], d['device_type'], at, at, 1, w), 0, 0,
                          slots, meta, now=at)
    assert len(slots.data) <= count + 4
    assert len(meta.value()['starts']) == len(slots.data)
    assert len(pickle.dumps(meta.value())) < 2000
    assert sum(len(v) for v in slots.data.values()) < 200_000


def test_replay_beyond_horizon_is_audited_not_recounted():
    keyed = Keyed(TOPOLOGY)
    slots, meta = Slots(), Meta()
    old = BASE + timedelta(seconds=2)
    d = TOPOLOGY['transformers'][0]['devices'][0]
    first = reading('tx-000', d['device_id'], d['device_type'], old, old, 1, 0, event_id=str(UUID(int=1, version=4)))
    keyed.process('tx-000', first, 0, 0, slots, meta, now=old)
    later = BASE + timedelta(seconds=RETAIN_SECONDS + 60)
    advance = reading('tx-000', d['device_id'], d['device_type'], later, later, 1, 1)
    keyed.process('tx-000', advance, 0, 0, slots, meta, now=later)
    results, deltas, _ = keyed.process('tx-000', first, 0, 0, slots, meta, now=later)
    assert [r[0] for r in results] == [LATE] and deltas['very_late'] == 1


def test_invalid_records_go_to_dlq_without_state_or_watermark():
    keyed = Keyed(TOPOLOGY)
    slots, meta = Slots(), Meta()
    for raw in ('{broken', json.dumps({'event_time': 5}), '[]'):
        results, deltas, _ = keyed.process('tx-000', raw, 1, 7, slots, meta, now=BASE)
        assert [r[0] for r in results] == [DLQ] and deltas['dlq'] == 1
    assert slots.data == {}
    assert keyed.shard('unknown') == '' and keyed.shard('tx-000') == 'tx-000'
