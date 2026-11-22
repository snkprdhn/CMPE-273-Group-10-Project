from datetime import timedelta
import json

import pytest

from member1.common import instant, validate
from member1.kafka import flush_results, handle_message, producer_config, Publisher
from member1.simulator import devices, EdgeState, scenario, topology

NOW = instant('2026-11-22T18:00:01Z')


def command(identity='command-001', epoch=42, target=3):
    value = next(x['value'] for x in json.load(open('schemas/v1/examples.valid.json')) if x['schema'] == 'Command')
    value['payload'].update(command_id=identity, target_rate_kw=target, leader_epoch=epoch,
                            issued_at='2026-11-22T18:00:01Z', expires_at='2026-11-22T18:00:11Z')
    value['leader_epoch'] = epoch
    return value


def state(tmp_path, **kwargs):
    return EdgeState(tmp_path / 'edge.sqlite', devices(NOW)[0], **kwargs)


def test_repeatable_scenario_and_contracts():
    first = list(scenario(273, NOW))
    assert first == list(scenario(273, NOW))
    assert first != list(scenario(274, NOW))
    assert len(first) == 90
    validate('Topology', topology(NOW))
    for tick in range(30):
        triplet = first[tick * 3:tick * 3 + 3]
        total = sum(e['payload']['power_kw'] for e in triplet)
        assert (total > 20 * .95) if 10 <= tick < 20 else (total < 20 * .75)
        for e in triplet:
            validate('Telemetry', e, 'tx-001')
            assert e['payload']['sequence_number'] == tick
    assert {x['payload']['boot_id'] for x in scenario(273, NOW, boot_id='new-boot')} == {'new-boot'}
    with pytest.raises(ValueError):
        list(scenario(1, NOW, steps=2))


def test_restart_persists_effect_dedup_epoch_and_result(tmp_path):
    edge = state(tmp_path)
    boot = edge.boot_id
    result = edge.apply(command(), 'ev-001', NOW)
    assert result['payload']['outcome'] == 'APPLIED'
    assert edge.current() == (3, '42', 1)
    assert len(edge.pending()) == 2
    reading = json.loads(edge.pending()[1][1])
    assert reading['causation_id'] == 'command-001'
    assert reading['payload']['power_kw'] == 3
    edge.db.close()
    edge = state(tmp_path)
    assert edge.boot_id != boot
    assert json.loads(edge.pending()[0][1]) == result
    duplicate = edge.apply(command(), 'ev-001', NOW + timedelta(days=1))
    assert duplicate['payload']['outcome'] == 'ALREADY_APPLIED'
    assert edge.current() == (3, '42', 1)
    stale = edge.apply(command('old', epoch=41), 'ev-001', NOW)
    assert stale['payload']['reason'] == 'STALE_EPOCH'
    assert edge.current() == (3, '42', 1)
    edge.apply(command('new', epoch=43, target=4), 'ev-001', NOW)
    assert edge.apply(command(), 'ev-001', NOW)['payload']['outcome'] == 'ALREADY_APPLIED'
    assert edge.current() == (4, '43', 2)
    restore = command('restore', epoch=43, target=7.2)
    restore['payload']['command_type'] = 'RESTORE'
    assert edge.apply(restore, 'ev-001', NOW)['payload']['outcome'] == 'APPLIED'
    assert edge.current() == (7.2, '43', 3)


@pytest.mark.parametrize('case,expected', [('expiry', 'EXPIRED'), ('rate', 'REJECTED'),
                                          ('assignment', 'REJECTED'), ('future', 'REJECTED')])
def test_command_rejections_do_not_advance_fence(tmp_path, case, expected):
    edge = state(tmp_path)
    value = command()
    now = NOW
    if case == 'expiry':
        now += timedelta(seconds=10)
    if case == 'rate':
        value['payload']['target_rate_kw'] = 99
    if case == 'assignment':
        value['payload']['transformer_id'] = 'tx-other'
    if case == 'future':
        now -= timedelta(seconds=1)
    assert edge.apply(value, 'ev-001', now)['payload']['outcome'] == expected
    assert edge.current() == (7.2, '0', 0)
    edge.db.close()
    assert state(tmp_path).apply(value, 'ev-001', now)['payload']['outcome'] == expected


def test_conflict_validation_and_outbox_bound(tmp_path):
    edge = state(tmp_path, pending_limit=4)
    edge.apply(command(), 'ev-001', NOW)
    changed = command(target=4)
    assert edge.apply(changed, 'ev-001', NOW)['payload']['reason'] == 'COMMAND_ID_CONFLICT'
    with pytest.raises(BufferError):
        edge.apply(command('next'), 'ev-001', NOW)
    assert edge.current()[2] == 1
    for invalid in [command(target=float('nan')), command()]:
        invalid['leader_epoch'] = 0
        with pytest.raises(ValueError):
            edge.apply(invalid, 'ev-001', NOW)
    with pytest.raises(ValueError):
        edge.apply(command(), 'other', NOW)
    assert edge.current()[2] == 1


class FakeProducer:
    def __init__(self, full=False, error=False):
        self.messages, self.full, self.error = [], full, error
    def produce(self, topic, **kwargs):
        if self.full:
            raise BufferError('full')
        self.messages.append((topic, kwargs))
    def poll(self, timeout):
        pass
    def flush(self, timeout):
        while self.messages:
            _, args = self.messages.pop(0)
            args['on_delivery']('failure' if self.error else None, None)
        return 0
    def __len__(self):
        return len(self.messages)


def test_producer_bounds_delivery_and_headers():
    config = producer_config('localhost:19092', capacity=2)
    assert config['enable.idempotence'] and config['acks'] == 'all'
    assert config['queue.buffering.max.messages'] == 2
    assert config['retries'] == 3 and config['delivery.timeout.ms'] == 10000
    fake = FakeProducer()
    pub = Publisher('', producer=fake)
    value = next(scenario(273, NOW))
    pub.send('grid.telemetry.v1', 'tx-001', value, [('traceparent', b'00-test')])
    topic, sent = fake.messages[0]
    assert topic == 'local-dev.grid.telemetry.v1' and sent['key'] == b'tx-001'
    assert ('traceparent', b'00-test') in sent['headers']
    pub.drain()
    assert pub.healthy
    full = Publisher('', producer=FakeProducer(full=True))
    full.timeout = .001
    with pytest.raises(TimeoutError):
        full.send('grid.telemetry.v1', 'tx-001', value)
    assert not full.healthy
    failed = Publisher('', producer=FakeProducer(error=True))
    failed.send('grid.telemetry.v1', 'tx-001', value)
    with pytest.raises(RuntimeError):
        failed.drain()


class Message:
    def error(self): return None
    def value(self): return json.dumps(command()).encode()
    def key(self): return b'ev-001'
    def headers(self): return [('traceparent', b'00-example')]


class Receiver:
    def __init__(self): self.commits = 0
    def commit(self, **kwargs): self.commits += 1


def test_crash_window_delivery_failure_and_replay(tmp_path):
    edge, receiver = state(tmp_path), Receiver()
    failed = Publisher('', producer=FakeProducer(error=True))
    with pytest.raises(RuntimeError):
        handle_message(Message(), edge, failed, receiver, NOW)
    assert receiver.commits == 0
    assert edge.current()[2] == 1 and len(edge.pending()) == 2
    original_ids = [x[0] for x in edge.pending()]
    edge.db.close()
    edge = state(tmp_path)
    assert [x[0] for x in edge.pending()] == original_ids
    good = Publisher('', producer=FakeProducer())
    flush_results(edge, good)
    handle_message(Message(), edge, good, receiver, NOW)
    assert receiver.commits == 1 and edge.pending() == []
    assert edge.current()[2] == 1


def test_abrupt_process_exit_keeps_effect_outbox_and_fence(tmp_path):
    import subprocess
    import sys
    path = str(tmp_path / 'edge.sqlite')
    # Exit without closing SQLite or running finally/atexit handlers, after commit.
    script = '''
import os, sys
from tests.member1.test_edge import command, NOW
from member1.simulator import EdgeState, devices
s = EdgeState(sys.argv[1], devices(NOW)[0])
s.apply(command(), 'ev-001', NOW, [('traceparent', b'00-persisted')])
os._exit(0)
'''
    subprocess.run([sys.executable, '-c', script, path], check=True)
    edge = state(tmp_path)
    assert edge.current() == (3, '42', 1)
    assert len(edge.pending()) == 2
    assert json.loads(edge.pending()[0][2]) == [['traceparent', '00-persisted']]
    assert edge.apply(command(), 'ev-001', NOW)['payload']['outcome'] == 'ALREADY_APPLIED'
    assert edge.current()[2] == 1
    edge.db.close()


def test_publication_crash_after_ack_replays_stable_result_id(tmp_path):
    edge = state(tmp_path)
    original = edge.apply(command(), 'ev-001', NOW)
    fake = FakeProducer()
    pub = Publisher('', producer=fake)
    row = edge.pending()[0]
    pub.send('grid.command-result.v1', 'ev-001', json.loads(row[1]))
    pub.drain()
    # Crash before acknowledge(): broker may have accepted the result already.
    edge.db.close()
    restarted = state(tmp_path)
    assert json.loads(restarted.pending()[0][1])['event_id'] == original['event_id']
    flush_results(restarted, pub)
    assert restarted.pending() == [] and restarted.current()[2] == 1


def test_invalid_poison_command_has_no_effect_or_commit(tmp_path):
    from jsonschema.exceptions import ValidationError
    edge, receiver = state(tmp_path), Receiver()
    for raw in (b'{', b'{}'):
        class InvalidMessage(Message):
            def value(self): return raw
        with pytest.raises((ValueError, ValidationError)):
            handle_message(InvalidMessage(), edge, Publisher('', producer=FakeProducer()), receiver, NOW)
    assert receiver.commits == 0 and edge.pending() == []
    assert edge.current()[2] == 0
    edge.db.close()


def test_changed_scenario_cannot_reuse_event_ids_for_different_readings():
    original = list(scenario(273, NOW))
    later = list(scenario(273, NOW + timedelta(seconds=1)))
    shorter = list(scenario(273, NOW, steps=3))
    assert {e['event_id'] for e in original}.isdisjoint(e['event_id'] for e in later + shorter)
    assert original == list(scenario(273, NOW, interval=1.0))


def test_failed_outbox_insert_rolls_back_effect_fence_and_inbox(tmp_path):
    import sqlite3
    edge = state(tmp_path)
    edge.db.execute("CREATE TRIGGER disk_failure BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT, 'injected disk failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        edge.apply(command(), 'ev-001', NOW)
    assert edge.current() == (7.2, '0', 0)
    assert edge.db.execute('SELECT count(*) FROM commands').fetchone()[0] == 0
    assert edge.pending() == []
    edge.db.close()
    edge = state(tmp_path)
    assert edge.current() == (7.2, '0', 0)
    edge.db.execute('DROP TRIGGER disk_failure')
    assert edge.apply(command(), 'ev-001', NOW)['payload']['outcome'] == 'APPLIED'
    edge.db.close()
