from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import random
from uuid import uuid4

import pytest

from projection.contracts import TOPICS, instant, validate
from projection.store import Store
from tests.projection.conftest import FIXTURES

NOW = instant('2026-11-09T18:30:06Z')


def feed(store, records, now=NOW):
    for r in records:
        store.ingest(r['topic'], r['key'], r['event'], now=now)


def view(store, now=NOW):
    with store.transaction() as db:
        return store.views(db, now)


def test_recorded_contracts(records):
    for r in records['events']:
        validate(TOPICS[r['topic']], r['event'])


def test_duplicate_reorder_restart_rebuild_converge(store, records):
    store.context(records)
    events = records['events']
    for name in ('Load', 'Action', 'Telemetry'):
        record = deepcopy(next(r for r in events if TOPICS[r['topic']] == name))
        record['event']['event_id'] = str(uuid4())
        if name == 'Load':
            record['event']['payload'].update(revision=2, net_load_kw=19)
        elif name == 'Action':
            record['event'].update(saga_version=9)
            record['event']['payload']['status'] = 'REDUCTION_CONFIRMED'
        else:
            record['event']['payload'].update(sequence_number=999, power_kw=8)
        events.append(record)
    feed(store, events)
    expected = view(store)
    assert expected['loads'][0]['revision'] == 2
    assert expected['actions'][0]['status'] == 'REDUCTION_CONFIRMED'
    assert expected['devices'][0]['latest_telemetry']['power_kw'] == 8
    with store.transaction() as db:
        cursor = store.cursor(db)
    rng = random.Random(42)
    for _ in range(4):
        rng.shuffle(events)
        with store.transaction() as db:
            db.execute('DELETE FROM projection_current')
            db.execute('DELETE FROM projection_events')
        feed(Store(store.dsn), events + events)
        assert view(store) == expected
    store.rebuild()
    assert view(Store(store.dsn)) == expected
    assert store.events(cursor, NOW)[0]['event_type'] == 'snapshot.required'


def test_conflict_invalid_key_atomic_rollback_and_offsets(store, records):
    store.context(records)
    r = next(r for r in records['events'] if TOPICS[r['topic']] == 'Load')
    store.ingest(r['topic'], r['key'], r['event'], partition=0, offset=4, now=NOW)
    changed = deepcopy(r['event'])
    changed['payload']['net_load_kw'] = 999
    with pytest.raises(ValueError, match='identity'):
        store.ingest(r['topic'], r['key'], changed, partition=0, offset=5)
    with pytest.raises(ValueError, match='key'):
        store.ingest(r['topic'], 'wrong', r['event'], partition=0, offset=6)
    changed['event_id'] = str(uuid4())
    with pytest.raises(ValueError, match='version'):
        store.ingest(r['topic'], r['key'], changed, partition=0, offset=7)
    with store.transaction() as db:
        assert db.execute('SELECT offset_value FROM projection_offsets').fetchone()[0] == 4
        assert db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 1
    assert store.ingest(r['topic'], r['key'], r['event'], partition=0, offset=9) is False
    with store.transaction() as db:
        assert db.execute('SELECT offset_value FROM projection_offsets').fetchone()[0] == 9


def test_fresh_partial_stale_no_domain_inference(store, records):
    store.context(records)
    load = deepcopy(next(r for r in records['events'] if TOPICS[r['topic']] == 'Load'))
    load['event']['payload'].update(revision=1, completeness='PARTIAL')
    feed(store, [load])
    t = view(store)['transformers'][0]
    assert t['freshness'] == 'PARTIAL'
    assert t['state'] == 'STALE'  # no classification for the new revision
    assert view(store, NOW+timedelta(seconds=15))['transformers'][0]['freshness'] == 'STALE'
    assert view(store)['devices'][0]['freshness'] == 'UNKNOWN'


def test_sse_reconnect_gap_expiry_generation_and_retention(store, records):
    store.context(records)
    reset = store.events(None, NOW)[0]
    validate('SseEvent', reset)
    cursor = reset['cursor']
    feed(store, records['events'])
    batch = store.events(cursor, NOW)
    assert batch
    for e in batch:
        validate('SseEvent', e)
    assert store.events(batch[-1]['cursor'], NOW) == []
    assert store.events(batch[0]['cursor'], NOW) == batch[1:]
    assert store.events('bogus', NOW)[0]['event_type'] == 'snapshot.required'
    assert store.events(cursor, NOW+timedelta(minutes=5))[0]['event_type'] == 'snapshot.required'
    assert store.events('wrong:0', NOW)[0]['event_type'] == 'snapshot.required'
    with store.transaction() as db:
        for i in range(1005):
            store._emit(db, 'system.health', 'projection', {'status': 'ok', 'observed_at': '2026-11-09T18:30:06Z'}, NOW)
        assert db.execute('SELECT count(*) FROM projection_stream').fetchone()[0] == 1000
    assert store.events(batch[-1]['cursor'], NOW)[0]['event_type'] == 'snapshot.required'


def test_concurrent_replicas_one_logical_event(store, records):
    store.context(records)
    r = next(r for r in records['events'] if TOPICS[r['topic']] == 'Load')
    def write(_):
        return Store(store.dsn).ingest(r['topic'], r['key'], r['event'], now=NOW)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(write, range(12))) == 1
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM projection_stream').fetchone()[0] == 1


def test_fixed_expected_snapshot(store, records):
    import json
    from pathlib import Path
    store.context(records)
    feed(store, list(reversed(records['events'])) + records['events'])
    assert view(store) == json.loads(Path('projection/expected.json').read_text())


def test_migration_repeatable_and_source_checkpoints_durable(store, records):
    store.migrate()
    store.context(records)
    r = next(r for r in records['events'] if TOPICS[r['topic']] == 'Load')
    store.ingest(r['topic'], r['key'], r['event'], partition=0, offset=10, now=NOW)
    store.rebuild()
    store.migrate()
    with store.transaction() as db:
        assert db.execute('SELECT version FROM projection_migrations').fetchall() == [(1,)]
        assert db.execute('SELECT offset_value FROM projection_offsets').fetchone()[0] == 10


def test_positive_normal_classification_requires_exact_owner_marker_and_revision(store, records):
    store.context(records)
    events = records['events']
    load = deepcopy(next(r for r in events if TOPICS[r['topic']] == 'Load'))
    load['event']['event_id'] = str(uuid4())
    load['event']['payload']['revision'] = 1
    feed(store, [load])
    assert view(store)['transformers'][0]['state'] == 'STALE'

    alert = deepcopy(next(r for r in events if TOPICS[r['topic']] == 'Alert'))
    alert['event']['event_id'] = str(uuid4())
    alert['event']['event_time'] = '2026-11-09T18:30:05Z'
    alert['event']['payload'].update(alert_id='classification-fixture', status='RESOLVED',
                                      load_revision=1, message='Operator resolved alert')
    feed(store, [alert])
    assert view(store)['transformers'][0]['state'] == 'STALE'

    alert['event']['event_id'] = str(uuid4())
    alert['event']['event_time'] = '2026-11-09T18:30:06Z'
    alert['event']['payload']['message'] = 'Authoritative load classification: NORMAL'
    feed(store, [alert])
    assert view(store)['transformers'][0]['state'] == 'NORMAL'
    assert view(store, NOW + timedelta(seconds=16))['transformers'][0]['state'] == 'STALE'


@pytest.mark.parametrize('kind', ['Telemetry', 'Result', 'Load', 'Alert', 'Action', 'Command'])
def test_device_notification_matches_snapshot_without_fleet_scan(store, records, monkeypatch, kind):
    store.context(records)
    feed(store, records['events'])
    record = deepcopy(next(r for r in records['events'] if TOPICS[r['topic']] == kind))
    record['event']['event_id'] = str(uuid4())
    if kind == 'Telemetry':
        record['event']['payload']['sequence_number'] = 1000
    elif kind == 'Load':
        record['event']['payload']['revision'] = 1
    elif kind == 'Alert':
        record['event']['event_time'] = '2026-11-09T18:30:07Z'
        record['event']['payload']['status'] = 'RESOLVED'
    elif kind == 'Action':
        record['event']['saga_version'] = 99
        record['event']['payload']['status'] = 'COMPLETED'
    elif kind == 'Command':
        record['event']['saga_version'] = 99
        record['event']['payload']['saga_version'] = 99
    snapshot = store.views
    def no_fleet_scan(*args):
        raise AssertionError('event ingestion must not rebuild the complete fleet')
    monkeypatch.setattr(store, 'views', no_fleet_scan)
    with store.transaction() as db:
        cursor = store.cursor(db)
    feed(store, [record])
    notifications = store.events(cursor, NOW)
    if kind in ('Telemetry', 'Result'):
        with store.transaction() as db:
            expected = next(d for d in snapshot(db, NOW)['devices']
                            if d['device']['device_id'] == record['event']['payload']['device_id'])
        device_notes = [e for e in notifications if e['event_type'] == 'device.updated']
        assert device_notes[-1]['payload'] == expected
    elif kind == 'Load':
        with store.transaction() as db:
            expected = next(t for t in snapshot(db, NOW)['transformers']
                            if t['transformer_id'] == record['event']['payload']['transformer_id'])
        tx_notes = [e for e in notifications if e['event_type'] == 'transformer.updated']
        assert tx_notes[-1]['payload'] == expected
    elif kind == 'Alert':
        alert_notes = [e for e in notifications if e['event_type'] == 'alert.updated']
        assert alert_notes[-1]['payload']['status'] == 'RESOLVED'
    elif kind == 'Action':
        action_notes = [e for e in notifications if e['event_type'] == 'action.updated']
        assert action_notes[-1]['payload']['status'] == 'COMPLETED'
    else:
        assert notifications == []


def test_worker_owned_connection_rolls_back_and_remains_usable(store, records):
    import psycopg
    with psycopg.connect(store.dsn, autocommit=True) as connection:
        writer = Store(store.dsn, connection=connection)
        writer.context(records)
        record = next(r for r in records['events'] if TOPICS[r['topic']] == 'Telemetry')
        feed(writer, [record])
        bad = deepcopy(record)
        bad['event']['payload']['power_kw'] += 1
        with pytest.raises(ValueError, match='identity'):
            feed(writer, [bad])
        assert view(writer) == view(store)
        assert connection.closed is False


def test_scope_skips_unmanaged_inventory_and_prune_keeps_a_rolling_window(store, records):
    from projection.consumer import Scope, process

    store.context(records)
    transformers, devices = store.inventory()
    assert transformers and devices
    scope = Scope(store, ttl=0)
    load = next(r for r in records['events'] if TOPICS[r['topic']] == 'Load')
    telemetry = next(r for r in records['events'] if TOPICS[r['topic']] == 'Telemetry')

    class Message:
        def __init__(self, record, offset):
            import json
            self.record, self._offset, self.data = record, offset, json.dumps(record['event']).encode()
        def error(self): return None
        def value(self): return self.data
        def topic(self): return self.record['topic']
        def key(self): return self.record['key'].encode()
        def headers(self): return None
        def partition(self): return 0
        def offset(self): return self._offset

    assert scope.allows('Load', load['event']['payload']) and scope.allows('Telemetry', telemetry['event']['payload'])
    ticks = [100.0]
    paced = Scope(store, ttl=0, clock=lambda: ticks[0], telemetry_interval=10)
    assert paced.allows('Telemetry', telemetry['event']['payload'])
    ticks[0] += 4
    assert not paced.allows('Telemetry', telemetry['event']['payload'])   # same device inside the interval
    ticks[0] += 7
    assert paced.allows('Telemetry', telemetry['event']['payload'])
    stranger = deepcopy(telemetry)
    stranger['event']['payload']['device_id'] = 'fleet-device-that-is-not-managed'
    assert not scope.allows('Telemetry', stranger['event']['payload'])
    other = deepcopy(load['event']['payload'])
    other['transformer_id'] = 'tx-unmanaged'
    assert not scope.allows('Load', other)
    assert process(store, Message(stranger, 5), scope=scope) == 'skipped'
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 0
    store.advance({(stranger['topic'], 0): 5})
    store.advance({(stranger['topic'], 0): 3})
    with store.transaction() as db:
        assert db.execute('SELECT offset_value FROM projection_offsets').fetchone()[0] == 5   # never moves back

    feed(store, records['events'])
    with store.transaction() as db:
        before = db.execute('SELECT count(*) FROM projection_events').fetchone()[0]
        telemetry_rows = db.execute("SELECT count(*) FROM projection_current WHERE kind='Telemetry'").fetchone()[0]
    assert before
    assert store.prune(now=NOW + timedelta(minutes=5), minutes=30) == {'events': 0, 'loads': 0}
    removed = store.prune(now=NOW + timedelta(hours=2), minutes=30)
    assert removed['events'] and removed['loads'] and removed['alert']          # alerts keep an hour
    with store.transaction() as db:
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Load'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Alert'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Telemetry'").fetchone()[0] == telemetry_rows
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Action'").fetchone()[0] >= 1   # actions keep a day
    assert store.prune(now=NOW + timedelta(days=3), minutes=30)['action']
    with store.transaction() as db:
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Action'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM projection_current WHERE kind='Telemetry'").fetchone()[0] == telemetry_rows


def test_previous_classification_is_carried_only_when_configured(store, records, monkeypatch):
    import projection.store as module
    store.context(records)
    first = next(r for r in records['events'] if TOPICS[r['topic']] == 'Load')
    alert = next(r for r in records['events'] if TOPICS[r['topic']] == 'Alert')
    feed(store, [first, alert])
    second = deepcopy(first)
    p = second['event']['payload']
    p.update(window_id=str(uuid4()), window_start='2026-11-09T18:30:05Z', window_end='2026-11-09T18:30:10Z')
    second['event']['event_id'] = str(uuid4())
    second['event']['event_time'] = '2026-11-09T18:30:10Z'
    feed(store, [second], now=instant('2026-11-09T18:30:12Z'))
    at = instant('2026-11-09T18:30:12Z')
    def state():
        with store.transaction() as db:
            return next(t for t in store.views(db, at)['transformers'] if t['transformer_id'] == first['key'])
    monkeypatch.setattr(module, 'ALERT_GRACE_SECONDS', 0)
    assert state()['state'] == 'STALE' and state()['freshness'] == 'PARTIAL'     # missing evidence is never normal
    monkeypatch.setattr(module, 'ALERT_GRACE_SECONDS', 8)
    carried = state()
    assert carried['state'] == 'CRITICAL' and carried['freshness'] == 'FRESH'
    monkeypatch.setattr(module, 'ALERT_GRACE_SECONDS', 3)
    assert state()['state'] == 'STALE'                                          # the older classification is too old to carry
    monkeypatch.setattr(module, 'FRESH_SECONDS', 1)
    assert state()['freshness'] == 'STALE'
