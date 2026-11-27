"""Worker input/cleanup boundaries without live dependencies."""
from io import StringIO
import threading

import pytest

from action import worker


class EmptyMessage:
    def __bool__(self):
        return False

    def error(self):
        return None

    def value(self):
        return b''

    def key(self):
        return b'tx'

    def topic(self):
        return 'local-delivery.grid.transformer-load.v1'


def setup_worker(monkeypatch, empty=False, revoke_fails=False):
    calls = []
    stopped = threading.Event()
    monkeypatch.setenv('CONTROL_DSN', 'unused')
    monkeypatch.setenv('CONTROL_SNAPSHOTS', 'test-snapshots.json')
    monkeypatch.setattr(worker, 'open', lambda _: StringIO('[]'), raising=False)
    monkeypatch.setattr(worker.threading, 'Event', lambda: stopped)
    monkeypatch.setattr(worker.signal, 'signal', lambda *_: None)

    class Lease:
        def __init__(self, **kwargs):
            pass
        def step(self):
            return True

    class Authority:
        def __init__(self, *args):
            pass
        def activate(self):
            return 1
        def stop(self):
            calls.append('revoke')
            if revoke_fails:
                raise RuntimeError('database unavailable')

    class Consumer:
        def __init__(self, config):
            pass
        def subscribe(self, topics):
            pass
        def poll(self, timeout):
            if empty:
                return EmptyMessage()
            stopped.set()
            return None
        def commit(self, **kwargs):
            calls.append('commit')
        def close(self):
            calls.append('close')

    class Control:
        def __init__(self, *args, **kwargs):
            pass
        def reconcile(self, now):
            pass
        def tick(self, now):
            # Also bound the old implementation when it silently skips empties.
            stopped.set()

    for name, value in [('Lease', Lease), ('Leadership', Authority), ('Consumer', Consumer), ('Control', Control)]:
        monkeypatch.setattr(worker, name, value)
    return calls


def test_empty_record_stops_without_committing(monkeypatch):
    calls = setup_worker(monkeypatch, empty=True)
    with pytest.raises(ValueError):
        worker.main()
    assert calls == ['revoke', 'close']


def test_shutdown_revokes_before_leaving_group(monkeypatch):
    calls = setup_worker(monkeypatch)
    worker.main()
    assert calls == ['revoke', 'close']


def test_consumer_closed_even_when_revocation_fails(monkeypatch):
    calls = setup_worker(monkeypatch, empty=True, revoke_fails=True)
    with pytest.raises(RuntimeError, match='database unavailable'):
        worker.main()
    assert calls == ['revoke', 'close']


def test_maintenance_is_bounded_instead_of_running_per_poll(monkeypatch):
    calls = setup_worker(monkeypatch)
    stopped = worker.threading.Event()
    clock = [0.0]
    monkeypatch.setattr(worker.time, 'monotonic', lambda: clock[0])
    class Lease:
        duration = 15
        def __init__(self, **kwargs): pass
        def step(self):
            calls.append('lease')
            return True
    class Authority:
        def __init__(self, *args): pass
        def activate(self):
            calls.append('activate')
            return 1
        def stop(self): calls.append('revoke')
    class Consumer:
        def __init__(self, config): pass
        def subscribe(self, topics): pass
        def poll(self, timeout):
            clock[0] += .25
            if clock[0] == 10:
                stopped.set()
            return None
        def close(self): calls.append('close')
    class Control:
        def __init__(self, *args, **kwargs): pass
        def reconcile(self, now): pass
        def tick(self, now): calls.append('tick')
    for name, value in [('Lease', Lease), ('Leadership', Authority), ('Consumer', Consumer), ('Control', Control)]:
        monkeypatch.setattr(worker, name, value)
    worker.main()
    assert calls.count('lease') == calls.count('activate') == 2
    assert calls.count('tick') == 20
    assert calls[-2:] == ['revoke', 'close']


@pytest.mark.parametrize('fail_last', [False, True])
def test_batched_offsets_follow_durable_processing_and_exclude_failed_record(monkeypatch, fail_last):
    calls = setup_worker(monkeypatch)
    stopped = worker.threading.Event()
    monkeypatch.setattr(worker.time, 'monotonic', lambda: 0)
    state = dict(polled=0, durable=0, stored=0, committed=0)
    class Message:
        def error(self): return None
        def value(self): return b'{}'
        def key(self): return b'tx'
        def topic(self): return 'test.grid.transformer-load.v1'
        def headers(self): return None
    class Consumer:
        def __init__(self, config):
            assert config['enable.auto.offset.store'] is False
        def subscribe(self, topics): pass
        def poll(self, timeout):
            state['polled'] += 1
            if state['polled'] == 101: stopped.set()
            return Message()
        def store_offsets(self, message):
            assert state['durable'] == state['polled']
            state['stored'] += 1
        def commit(self, *, asynchronous):
            assert asynchronous is False
            assert state['stored'] == state['durable'] == 100
            state['committed'] = 100
        def close(self): calls.append('close')
    class Control:
        def __init__(self, *args, **kwargs): pass
        def reconcile(self, now): pass
        def tick(self, now): pass
        def load(self, *args):
            if fail_last and state['polled'] == 101: raise ValueError('durable write failed')
            state['durable'] += 1
    monkeypatch.setattr(worker, 'Consumer', Consumer)
    monkeypatch.setattr(worker, 'Control', Control)
    if fail_last:
        with pytest.raises(ValueError, match='durable write failed'): worker.main()
    else:
        worker.main()
    assert state['committed'] == 100
    assert state['stored'] == (100 if fail_last else 101)
    assert calls[-2:] == ['revoke', 'close']


def test_malformed_telemetry_is_skipped_and_offset_stored(monkeypatch):
    """A hostile or corrupt charger report must not crash-loop the Action service."""
    calls = setup_worker(monkeypatch)
    stopped = worker.threading.Event()

    class Poison:
        def error(self):
            return None
        def value(self):
            return b'e2e-000000 {not json'
        def key(self):
            return b'tx-001'
        def topic(self):
            return 'local-delivery.grid.telemetry.v1'
        def headers(self):
            return None

    class Consumer:
        def __init__(self, config):
            self.sent = False
        def subscribe(self, topics):
            pass
        def poll(self, timeout):
            if not self.sent:
                self.sent = True
                return Poison()
            stopped.set()
            return None
        def store_offsets(self, message):
            calls.append('store')
        def commit(self, **kwargs):
            calls.append('commit')
        def close(self):
            calls.append('close')

    monkeypatch.setattr(worker.threading, 'Event', lambda: stopped)
    monkeypatch.setattr(worker, 'Consumer', Consumer)
    worker.main()
    assert calls[0] == 'store' and calls[-2:] == ['revoke', 'close']


@pytest.mark.parametrize('topic,key', [('local-delivery.grid.telemetry.v1', b'tx-009'), ('local-delivery.grid.transformer-load.v1', b'tx-010')])
def test_records_of_uncontrolled_transformers_are_skipped_without_decoding(monkeypatch, topic, key):
    calls = setup_worker(monkeypatch)
    monkeypatch.setenv('CONTROL_TRANSFORMERS', 'tx-001,tx-002')
    decoded = []

    class Outside:
        def error(self): return None
        def value(self): return b'not json at all'
        def key(self): return key
        def topic(self): return topic
        def headers(self): return None

    class Consumer:
        def __init__(self, config):
            self.sent = False
        def subscribe(self, topics): pass
        def poll(self, timeout):
            if not self.sent:
                self.sent = True
                return Outside()
            return None
        def store_offsets(self, message): calls.append('store')
        def commit(self, **kwargs): calls.append('commit')
        def close(self): calls.append('close')

    original = worker.json.loads

    def spy(raw, *args, **kwargs):
        if raw == b'not json at all':
            decoded.append(raw)
        return original(raw, *args, **kwargs)

    monkeypatch.setattr(worker.json, 'loads', spy)
    monkeypatch.setattr(worker, 'Consumer', Consumer)
    worker.main()    # the stub Control stops the loop after the first pass
    assert calls[0] == 'store' and decoded == []     # no JSON decode, no handler, no failure


@pytest.mark.parametrize('reason,survives', [('EVENT_ID_REUSED', True), ('SAGA_VERSION', False)])
def test_reemitted_load_revision_is_skipped_but_other_conflicts_stop(monkeypatch, reason, survives):
    from action.control import Conflict
    calls = setup_worker(monkeypatch)

    class Message:
        def error(self): return None
        def value(self): return b'{"event_id": "e", "payload": {}}'
        def key(self): return b'tx-001'
        def topic(self): return 'local-delivery.grid.transformer-load.v1'
        def headers(self): return None

    class Consumer:
        def __init__(self, config):
            self.sent = False
        def subscribe(self, topics): pass
        def poll(self, timeout):
            if not self.sent:
                self.sent = True
                return Message()
            return None
        def store_offsets(self, message): calls.append('store')
        def commit(self, **kwargs): calls.append('commit')
        def close(self): calls.append('close')

    class Control:
        def __init__(self, *args, **kwargs): pass
        def reconcile(self, now): pass
        def tick(self, now): pass
        def load(self, event, key, now): raise Conflict(reason)

    monkeypatch.setattr(worker, 'Consumer', Consumer)
    monkeypatch.setattr(worker, 'Control', Control)
    stopped = worker.threading.Event()
    monkeypatch.setattr(worker.time, 'monotonic', lambda: 0.0)
    if survives:
        stop_after = Consumer.poll
        def poll(self, timeout):
            message = stop_after(self, timeout)
            if message is None:
                stopped.set()
            return message
        monkeypatch.setattr(Consumer, 'poll', poll)
        worker.main()
        assert calls[0] == 'store'
    else:
        with pytest.raises(Conflict):
            worker.main()
        assert 'store' not in calls
