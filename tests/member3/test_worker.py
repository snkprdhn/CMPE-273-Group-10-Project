"""Worker input/cleanup boundaries without live dependencies."""
from io import StringIO
import threading

import pytest

from member3 import worker


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
        return 'local-m3.grid.transformer-load.v1'


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
