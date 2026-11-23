from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import random
from uuid import uuid4

import pytest

from member4.contracts import TOPICS, instant, validate
from member4.store import Store
from tests.member4.conftest import FIXTURES

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
    assert view(store) == json.loads(Path('member4/expected.json').read_text())


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
