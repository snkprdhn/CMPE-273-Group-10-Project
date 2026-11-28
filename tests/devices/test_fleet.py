"""Fleet model: shape, load behavior, schedule and fault knobs, without Kafka or PostgreSQL."""
from datetime import datetime, timezone
import json

import pytest

from devices.common import METRICS, validate
from devices.fleet import (CYCLE_SECONDS, EV_MAX, Knobs, counts, fleet, frac, offered_kw, overload_active,
                           reading, record_offered, schedule, sun, WINDOW)
from aggregation.engine import Aggregator, DLQ, LOAD
from aggregation.keyed import Keyed


def totals(topology, capabilities, heroes, t, window):
    result = {}
    for tx in topology['transformers']:
        index, hero = int(tx['transformer_id'][3:]), tx['transformer_id'] in heroes
        result[tx['transformer_id']] = sum(
            offered_kw(d['device_id'], d['device_type'], index, hero, t, window) for d in tx['devices'])
    return result


def test_shape_and_registry_candidates():
    topology, capabilities, heroes = fleet(25, 40, 5)
    validate('Topology', topology)
    Aggregator(topology)
    assert len(topology['transformers']) == 25 and len(capabilities) == 1000
    assert counts(40) == {'SMART_METER': 22, 'EV_CHARGER': 14, 'SOLAR': 4}
    assert heroes == ['tx-001', 'tx-002', 'tx-003', 'tx-004', 'tx-005']
    controllable = [c for c in capabilities if c['controllable']]
    assert len(controllable) == 5 * 14 and all(c['transformer_id'] in heroes for c in controllable)
    for c in capabilities:
        validate('Device', dict(c, updated_at='2026-11-26T20:00:00Z'))
    assert len({c['device_id'] for c in capabilities}) == 1000


def test_offered_gauge_publishes_complete_windows_independently_of_limits(monkeypatch):
    metric = 'fleet_unmanaged_kw{transformer="tx-001"}'
    monkeypatch.setitem(METRICS, metric, 0)
    total = {}
    for limit in (2, EV_MAX):
        offered = offered_kw('ev-001-01', 'EV_CHARGER', 1, True, 100, 20, forced=True)
        record_offered(total, 'tx-001', offered, 2)
        assert METRICS[metric] == (0 if limit == 2 else 6.2)
        record_offered(total, 'tx-001', -1.0, 2)
        assert METRICS[metric] == 6.2 and not total
        assert METRICS[metric] - (min(offered, limit) - 1) == pytest.approx(offered - limit)


def test_cloud_cover_is_bounded_to_heroes_and_expires():
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen
    from devices.fleet import serve_control

    now = [100.0]
    knobs = Knobs(clock=lambda: now[0])
    knobs.heroes = tuple(f'tx-{i:03d}' for i in range(1, 7))
    server = serve_control(knobs, 'test-token', 0, host='127.0.0.1')

    def inject(seconds, token='test-token'):
        request = Request(f'http://127.0.0.1:{server.server_port}/inject',
                          data=json.dumps({'kind': 'cloud_cover', 'seconds': seconds}).encode(),
                          headers={'Authorization': f'Bearer {token}'})
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    try:
        with pytest.raises(HTTPError) as failure:
            inject(30, token='wrong')
        assert failure.value.code == 401
        for seconds in (4, 301, True, '30', float('nan')):
            with pytest.raises(HTTPError) as failure:
                inject(seconds)
            assert failure.value.code == 400
        assert inject(5)['active']['cloud_cover'] == 5
    finally:
        server.shutdown()
        server.server_close()
    assert knobs.status()['active']['cloud_cover'] == 5
    args = ('solar-005-01', 'SOLAR', 5, False, 100, 20)
    assert offered_kw(*args, solar_scale=knobs.solar_scale('tx-005')) == pytest.approx(.2 * offered_kw(*args), abs=.001)
    assert knobs.solar_scale('tx-006') == knobs.solar_scale('tx-025') == 1
    now[0] += 5
    assert knobs.solar_scale('tx-005') == 1 and 'cloud_cover' not in knobs.status()['active']


@pytest.mark.parametrize('t', [0, 311, 777, 1500, 5000, 9000])
def test_hero_overload_crosses_critical_and_baseline_stays_healthy(t):
    topology, capabilities, heroes = fleet()
    capacity = {tx['transformer_id']: tx['capacity_kw'] for tx in topology['transformers']}
    for window in range(t // 5, t // 5 + 48):   # four minutes of windows, a full overload cycle
        now = window * 5 + 2.5
        loads = totals(topology, capabilities, heroes, now, window)
        for index, tx in enumerate(heroes):
            pct = loads[tx] / capacity[tx] * 100
            if overload_active(index, now):
                assert pct >= 97, (tx, now, pct)
            else:
                assert pct <= 80, (tx, now, pct)
        for tx, load in loads.items():
            if tx not in heroes:
                assert load / capacity[tx] * 100 < 93   # warning band at most; Action must not review it


def test_some_hero_is_always_within_an_incident_cycle():
    active = [any(overload_active(i, t) for i in range(5)) for t in range(0, CYCLE_SECONDS * 3)]
    assert sum(active) / len(active) > .8


def test_restore_leaves_headroom_below_critical():
    """Action restores only if baseline net + confirmed reduction stays under 95% of capacity."""
    topology, capabilities, heroes = fleet()
    for index, tx in enumerate(topology['transformers'][:5]):
        name, capacity = tx['transformer_id'], tx['capacity_kw']
        quiet = [totals(topology, capabilities, heroes, t + .5, t // 5)[name] for t in range(0, 7200, 5)
                 if not overload_active(index, t + .5)]
        busy = [totals(topology, capabilities, heroes, t + .5, t // 5)[name] for t in range(0, 7200, 5)
                if overload_active(index, t + .5)]
        reduction = sorted(busy)[len(busy) // 2] - .75 * capacity
        typical = sorted(quiet)[len(quiet) // 2]
        assert typical + reduction < .93 * capacity, (name, typical, reduction, capacity)
        assert max(quiet) + reduction < 1.0 * capacity   # even the worst quiet moment restores


def test_schedule_places_each_device_once_inside_its_window():
    topology, capabilities, _ = fleet(3, 8, 1)
    phases = {c['device_id']: .2 + 4.4 * frac(c['device_id'], 'phase') for c in capabilities}
    devices = [d for t in topology['transformers'] for d in t['devices']]
    rows = schedule(devices, 1234, phases)
    assert [d['device_id'] for _, d in rows].count(rows[0][1]['device_id']) == 1 and len(rows) == len(devices)
    assert all(1234 * WINDOW < at < 1235 * WINDOW for at, _ in rows) and rows == sorted(rows, key=lambda r: r[0])


def test_generated_readings_are_valid_and_complete_the_windows():
    topology, capabilities, heroes = fleet(3, 40, 1)
    phases = {c['device_id']: .2 + 4.4 * frac(c['device_id'], 'phase') for c in capabilities}
    kinds = {c['device_id']: (c['device_type'], c['transformer_id']) for c in capabilities}
    keyed = Keyed(topology)
    state = {t['transformer_id']: ({}, [None]) for t in topology['transformers']}

    class Slots:
        def __init__(self, data): self.data = data
        def get(self, k): return self.data.get(k)
        def put(self, k, v): self.data[k] = v
        def remove(self, k): self.data.pop(k, None)

    class Meta:
        def __init__(self, box): self.box = box
        def value(self): return self.box[0]
        def update(self, v): self.box[0] = v

    devices = [d for t in topology['transformers'] for d in t['devices']]
    outputs = []
    base = 5_000_000
    for window in range(base, base + 8):
        for at, d in schedule(devices, window, phases):
            kind, tx = kinds[d['device_id']]
            when = datetime.fromtimestamp(at, timezone.utc)
            extra = dict(current_rate_kw=1, minimum_rate_kw=1, priority=1) if kind == 'EV_CHARGER' else None
            power = offered_kw(d['device_id'], kind, int(tx[3:]), tx in heroes, at, window)
            value = reading(d['device_id'], kind, tx, 'boot', window, when, when, 'run', power, extra)
            validate('Telemetry', value, tx)
            slots, meta = state[tx]
            results, _, _ = keyed.process(tx, json.dumps(value), 0, 0, Slots(slots), Meta(meta), now=when)
            outputs += results
    assert not [r for r in outputs if r[0] == DLQ]
    loads = [r[2]['payload'] for r in outputs if r[0] == LOAD]
    assert loads and all(p['completeness'] == 'COMPLETE' and p['good_device_count'] == 40 for p in loads)
    assert {p['transformer_id'] for p in loads} == {'tx-001', 'tx-002', 'tx-003'}


def test_knobs_validate_and_bursts_expire():
    now = [0.0]
    knobs = Knobs(clock=lambda: now[0])
    assert knobs.current()['late'] == .01
    knobs.inject('out_of_order', 30)
    assert knobs.current()['late'] == .25 and knobs.status()['active']['out_of_order'] == 30
    now[0] = 31
    assert knobs.current()['late'] == .01 and knobs.status()['active'] == {}
    knobs.set({'drop': .02})
    assert knobs.current()['drop'] == .02
    for bad in ({'late': .9}, {'late': True}, {'late': 'x'}):
        with pytest.raises(ValueError):
            knobs.set(bad)
    for kind, seconds in (('unknown', 30), ('duplicates', 1), ('duplicates', 9999), ('duplicates', True)):
        with pytest.raises(ValueError):
            knobs.inject(kind, seconds)


def test_demand_is_deterministic_and_solar_is_generation():
    args = ('solar-001-01', 'SOLAR', 1, True, 1000.0, 200)
    assert offered_kw(*args) == offered_kw(*args) < 0
    assert .1 <= sun(0) <= 1 and all(sun(t) >= .1 for t in range(0, 3600, 60))


def test_committed_fleet_topology_matches_the_generator():
    from pathlib import Path
    expected = fleet()[0]
    stored = json.loads((Path(__file__).resolve().parents[2] / 'devices/fixtures/fleet-topology.json').read_text())
    assert stored == expected


class Message:
    def __init__(self, key, offset, partition=0, value=b'{}'):
        self._key, self._offset, self._partition, self._value = key, offset, partition, value

    def error(self): return None
    def key(self): return self._key.encode()
    def value(self): return self._value
    def headers(self): return None
    def topic(self): return 'prefix.grid.charger-command.v1'
    def partition(self): return self._partition
    def offset(self): return self._offset


class FakeEdge:
    def __init__(self):
        self.applied, self.rows, self.acknowledged = [], [], []

    def apply(self, command, key, now, headers=None):
        self.applied.append(key)
        self.rows.append((f'id-{key}', json.dumps({'event_type': 'command.result', 'payload': {'device_id': key}}), '[]'))

    def pending(self): return list(self.rows)
    def acknowledge(self, identity): self.acknowledged.append(identity)


def test_command_batch_publishes_once_then_commits_each_partition_offset():
    from devices.fleet import apply_commands
    sent, events, edge = [], [], FakeEdge()

    class Publisher:
        def send(self, topic, key, value, headers=None): sent.append((topic, key))
        def drain(self): events.append('drain')

    class Receiver:
        def commit(self, offsets, asynchronous):
            events.append(('commit', sorted((o.partition, o.offset) for o in offsets), asynchronous))

    messages = [Message('ev-001-01', 10, 0), Message('ev-001-02', 11, 0), Message('old-device', 3, 1), Message('ev-002-01', 4, 2)]
    apply_commands(messages, {'ev-001-01': 1, 'ev-001-02': 1, 'ev-002-01': 1}, edge, Publisher(), Receiver())
    assert edge.applied == ['ev-001-01', 'ev-001-02', 'ev-002-01']      # the unknown device is skipped, not applied
    assert sent == [('grid.command-result.v1', k) for k in edge.applied]
    assert events == ['drain', ('commit', [(0, 12), (1, 4), (2, 5)], False)]   # one flush, one commit, positions advance past skips
    assert edge.acknowledged == [f'id-{k}' for k in edge.applied]


def test_command_batch_does_not_commit_when_an_effect_fails():
    from devices.fleet import apply_commands

    class Failing(FakeEdge):
        def apply(self, command, key, now, headers=None):
            raise ValueError('WRONG_SHARD')

    class Receiver:
        def commit(self, **kwargs): raise AssertionError('offset must not be committed')

    with pytest.raises(ValueError):
        apply_commands([Message('ev-001-01', 1)], {'ev-001-01': 1}, Failing(), object(), Receiver())


def test_forced_overload_is_bounded_validated_and_expires():
    now = [0.0]
    knobs = Knobs(clock=lambda: now[0])
    knobs.heroes = ('tx-001', 'tx-002')
    assert not knobs.is_forced('tx-001')
    knobs.force_overload('tx-001', 60)
    assert knobs.is_forced('tx-001') and not knobs.is_forced('tx-002') and knobs.status()['overloads'] == {'tx-001': 60.0}
    now[0] = 61
    assert not knobs.is_forced('tx-001') and knobs.status()['overloads'] == {}
    for transformer, seconds in (('tx-009', 60), ('tx-001', 5), ('tx-001', 900), ('tx-001', True)):
        with pytest.raises(ValueError):
            knobs.force_overload(transformer, seconds)
    quiet = sum(offered_kw(f'ev-001-{i:02}', 'EV_CHARGER', 1, True, 100.5, 20) for i in range(1, 15) if not overload_active(0, 100.5))
    forced = sum(offered_kw(f'ev-001-{i:02}', 'EV_CHARGER', 1, True, 100.5, 20, forced=True) for i in range(1, 15))
    assert forced > quiet or overload_active(0, 100.5)
