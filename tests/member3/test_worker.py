"""Worker input/cleanup boundaries without live dependencies."""
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
    monkeypatch.delenv('CONTROL_SNAPSHOTS', raising=False)
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
        def __init__(self, *args):
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
