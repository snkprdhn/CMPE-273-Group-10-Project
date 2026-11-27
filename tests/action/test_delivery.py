"""Epoch/gateway and CDC failure boundaries against real local dependencies."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import time
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from action.control import Conflict, Control
from action.leadership import Leadership, Lease
from action.fixtures import FakeRegistry, replay
from tests.action.test_control import control, CASES, load_step


class ObservedLease:
    """Only Lease transport/time is fake; durable authority is real PostgreSQL."""
    def __init__(self, generation=1):
        self.token, self.holder = ('fixed-test-uid', generation), str(uuid4())
        self.deadline = time.monotonic() + 10
        self.valid = True

    def release(self):
        self.valid = False

    def verify(self):
        if not self.valid or time.monotonic() >= self.deadline:
            raise Conflict('LEASE_LOST')


def activate(control, generation=1):
    lease = ObservedLease(generation)
    authority = Leadership(control.dsn, lease)
    epoch = authority.activate()
    return Control(control.dsn, control.registry, epoch, authority), authority


def test_epoch_is_durable_and_standalone_cannot_bypass(control):
    first, a = activate(control)
    assert a.activate() == 1  # renewal is not another acquisition
    second, b = activate(control, 2)
    assert b.epoch == 2
    with pytest.raises(Conflict, match='STALE_EPOCH'):
        first.tick(datetime.now(timezone.utc))
    with pytest.raises(Conflict, match='LEADERSHIP_REQUIRED'):
        control.tick(datetime.now(timezone.utc))
    with pytest.raises(Conflict, match='LEASE_GENERATION'):
        a.activate()  # delayed old acquisition cannot mint epoch 3
    assert b.activate() == 2
    b.stop()
    with pytest.raises(Conflict):
        second.tick(datetime.now(timezone.utc))


def test_lease_loss_rolls_back_work_including_outbox(control, monkeypatch):
    worker, authority = activate(control)
    step = load_step()
    worker.registry.values = deepcopy(CASES[0]['steps'][0]['values'])
    original = worker.outbox

    def lose(*args, **kwargs):
        original(*args, **kwargs)
        authority.lease.valid = False
    monkeypatch.setattr(worker, 'outbox', lose)
    with pytest.raises(Conflict, match='LEASE_LOST'):
        worker.load(step['event'], step['event']['payload']['transformer_id'],
                    __import__('action.domain', fromlist=['instant']).instant(step['now']))
    state = control.inspect()
    assert not state['outbox'] and not state['commands'] and state['inbox'] == 0


def test_partial_ack_restart_holds_and_never_reissues(control):
    # Use fixture steps up to the first acknowledgement, retaining another pending.
    worker, a = activate(control)
    case = next(c for c in CASES if c['name'] == 'partial acknowledgement timeout')
    steps = []
    for step in case['steps']:
        if step['op'] == 'restart':
            continue
        steps.append(step)
        if step['op'] == 'result':
            break
    replay(worker, dict(steps=steps))
    before = control.inspect()
    newer, b = activate(control, 2)
    newer.reconcile(datetime.now(timezone.utc))
    after = control.inspect()
    assert len(after['commands']) == len(before['commands'])
    assert all(c['status'] != 'PENDING' for c in after['commands'])
    assert all(a['status'] == 'NEEDS_OPERATOR_REVIEW' for a in after['actions'])
    count = len(after['outbox'])
    newer.reconcile(datetime.now(timezone.utc))
    assert len(control.inspect()['outbox']) == count


def test_gateway_rejects_unseen_old_epoch_without_new_device_command(control, tmp_path):
    from devices.simulator import EdgeState
    from devices.fencing import FencedEdgeState
    from action.domain import instant
    worker, a = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    commands = control.inspect()['commands']
    assert commands
    event = commands[0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == event['payload']['device_id'])
    edge = FencedEdgeState(control.dsn, device)
    now = instant(event['payload']['issued_at'])
    first = edge.apply(event, device['device_id'], now)
    assert first['payload']['outcome'] == 'APPLIED'
    _, b = activate(control, 2)
    assert edge.apply(event, device['device_id'], now)['payload']['outcome'] == 'ALREADY_APPLIED'
    # A different device never observed epoch 2 but must reject the old command.
    event = commands[1]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == event['payload']['device_id'])
    other = FencedEdgeState(control.dsn, device)
    result = other.apply(event, device['device_id'], now)
    assert result['payload']['reason'] == 'STALE_EPOCH'
    assert other.current()[2] == 0
    edge.db.close()
    other.db.close()


def test_gateway_unavailable_has_no_effect(control, tmp_path):
    from devices.simulator import EdgeState, devices, command_fixture
    from devices.fencing import FencedEdgeState
    now = datetime.now(timezone.utc)
    with pytest.raises(psycopg.OperationalError):
        FencedEdgeState('postgresql://invalid@127.0.0.1:1/unavailable', devices(now)[0])


def test_process_death_before_and_after_commit(control):
    # os._exit simulates actual process death, without context-manager cleanup.
    program = '''
import os, psycopg
from psycopg.types.json import Jsonb
with psycopg.connect(os.environ['CRASH_DSN']) as db:
    db.execute("INSERT INTO action_outbox(id,aggregatetype,aggregateid,type,payload) VALUES (%s,'grid.action.v1','tx','test',%s)",
               (os.environ['CRASH_ID'],Jsonb({'event_id':os.environ['CRASH_ID']})))
    if os.environ['CRASH_COMMIT']=='yes': db.commit()
    os._exit(73)
'''
    for committed in (False, True):
        eid = str(uuid4())
        env = dict(os.environ, CRASH_DSN=control.dsn, CRASH_ID=eid, CRASH_COMMIT='yes' if committed else 'no')
        result = subprocess.run([sys.executable, '-c', program], env=env)
        assert result.returncode == 73
        with psycopg.connect(control.dsn) as db:
            assert db.execute('SELECT count(*) FROM action_outbox WHERE id=%s', (eid,)).fetchone()[0] == int(committed)


@pytest.mark.skipif(not os.getenv('ACTION_KIND_TEST'), reason='set ACTION_KIND_TEST=1 for real kind Lease test')
def test_kind_leader_death_and_delayed_old_owner(control):
    # Dedicated Lease; never replace the application's Lease or any other workload.
    name = 'test-action-' + uuid4().hex[:12]
    first = Lease(name=name, duration=5)
    second = Lease(name=name, duration=5)
    program = """
import json, os, time
from action.leadership import Lease, Leadership
lease=Lease(name=os.environ['TEST_LEASE'],duration=5)
assert lease.step()
a=Leadership(os.environ['TEST_DSN'],lease)
print(json.dumps(dict(epoch=a.activate(),holder=lease.holder,token=lease.token,deadline=lease.deadline)),flush=True)
while True:
    time.sleep(.25)
    assert lease.step()
    a.activate()
"""
    child = subprocess.Popen([sys.executable, '-c', program], stdout=subprocess.PIPE, text=True,
                             env=dict(os.environ, TEST_LEASE=name, TEST_DSN=control.dsn))
    try:
        data = json.loads(child.stdout.readline())
        assert data['epoch'] == 1
        first.holder, first.token, first.deadline = data['holder'], tuple(data['token']), data['deadline']
        a = Leadership(control.dsn, first)
        a.epoch = 1
        assert not second.step()
        child.kill()  # actual leader process death: no graceful release/cleanup
        child.wait(timeout=3)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and not second.step():
            time.sleep(.25)
        b = Leadership(control.dsn, second)
        assert b.activate() == 2
        with pytest.raises(Conflict):
            a.activate()
        assert not first.step()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        child.stdout.close()
        subprocess.run(first.command + ['delete', 'lease', name, '--ignore-not-found'], check=True, capture_output=True)


def test_gateway_process_crash_is_atomic_with_effect(control):
    from devices.fencing import FencedEdgeState
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id'])
    program = '''
import json, os
from devices.fencing import FencedEdgeState
from devices.common import instant
edge=FencedEdgeState(os.environ['CRASH_DSN'],json.loads(os.environ['CRASH_DEVICE']))
command=json.loads(os.environ['CRASH_COMMAND'])
if os.environ['CRASH_COMMIT']=='no':
    edge.reading=lambda *_: os._exit(74) # after rate UPDATE, before transaction commit
edge.apply(command, command['payload']['device_id'], instant(command['payload']['issued_at']))
os._exit(74) # after effect/result/outbox commit, before result publication
'''
    env = dict(os.environ, CRASH_DSN=control.dsn, CRASH_DEVICE=json.dumps(device), CRASH_COMMAND=json.dumps(command))
    for committed in (False, True):
        result = subprocess.run([sys.executable, '-c', program], env=dict(env, CRASH_COMMIT='yes' if committed else 'no'))
        assert result.returncode == 74
        edge = FencedEdgeState(control.dsn, device)
        assert edge.current()[2] == int(committed)
        assert len(edge.pending()) == (2 if committed else 0)
        edge.db.close()


def test_gateway_and_epoch_advance_serialize(control):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from devices.fencing import FencedEdgeState
    from devices.common import instant
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id'])
    edge = FencedEdgeState(control.dsn, device)
    updated, release = threading.Event(), threading.Event()
    reading = edge.reading
    def paused(*args):
        updated.set()
        assert release.wait(3)
        return reading(*args)
    edge.reading = paused
    try:
        with ThreadPoolExecutor(2) as pool:
            effect = pool.submit(edge.apply, command, device['device_id'], instant(command['payload']['issued_at']))
            assert updated.wait(3)
            acquired = pool.submit(activate, control, 2)
            # Query the actual database lock wait rather than infer blocking from time.
            with psycopg.connect(control.dsn) as db:
                end = time.monotonic() + 2
                while time.monotonic() < end:
                    waiting = db.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objid=273003").fetchone()[0]
                    if waiting:
                        break
                    time.sleep(.01)
                assert waiting and not acquired.done()
            release.set()
            assert effect.result()['payload']['outcome'] == 'APPLIED'
            assert acquired.result()[1].epoch == 2
        assert edge.current()[2] == 1
    finally:
        release.set()
        edge.db.close()


def test_initial_authority_fences_historical_standalone_epochs(control):
    standalone = Control(control.dsn, control.registry, 42)
    replay(standalone, dict(steps=CASES[0]['steps'][:2]))
    worker, authority = activate(control)
    assert authority.epoch == 43
    worker.reconcile(datetime.now(timezone.utc))
    assert control.inspect()['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'


@pytest.mark.parametrize('case, outcome', [('expired', 'EXPIRED'), ('future', 'REJECTED'), ('capability', 'REJECTED')])
def test_fenced_gateway_failure_has_no_effect(control, case, outcome):
    from datetime import timedelta
    from devices.fencing import FencedEdgeState
    from devices.common import instant
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = deepcopy(next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id']))
    now = instant(command['payload']['issued_at'])
    if case == 'expired':
        now = instant(command['payload']['expires_at'])
    elif case == 'future':
        now -= timedelta(seconds=1)
    else:
        device['minimum_rate_kw'] = command['payload']['target_rate_kw'] + .1
    edge = FencedEdgeState(control.dsn, device)
    try:
        assert edge.apply(command, device['device_id'], now)['payload']['outcome'] == outcome
        assert edge.current()[2] == 0
        assert len(edge.pending()) == 1
    finally:
        edge.db.close()


def test_gateway_session_loss_rolls_back_effect_and_releases_epoch_lock(control):
    from devices.fencing import FencedEdgeState
    from devices.common import instant
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id'])
    edge = FencedEdgeState(control.dsn, device)
    reading = edge.reading
    def disconnected(*args):
        edge.db.execute("SET LOCAL idle_in_transaction_session_timeout = '100ms'")
        time.sleep(.2)  # DB terminates a paused session after the rate UPDATE
        return reading(*args)
    edge.reading = disconnected
    try:
        with pytest.raises((psycopg.OperationalError, psycopg.errors.IdleInTransactionSessionTimeout)):
            edge.apply(command, device['device_id'], instant(command['payload']['issued_at']))
    finally:
        edge.db.close()
    newer, b = activate(control, 2)
    edge = FencedEdgeState(control.dsn, device)
    try:
        assert edge.current()[2] == 0 and not edge.pending()
        assert edge.apply(command, device['device_id'], instant(command['payload']['issued_at']))['payload']['reason'] == 'STALE_EPOCH'
    finally:
        edge.db.close()


@pytest.mark.parametrize('delay_at', ['device_lock', 'result_write', 'final_authorization'])
def test_command_expiry_counts_database_waits(control, monkeypatch, delay_at):
    from devices.fencing import FencedEdgeState
    from devices.common import instant
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id'])
    edge = FencedEdgeState(control.dsn, device)
    # Advance only the gateway elapsed clock, never the Lease's clock.
    class Clock:
        elapsed = 0
        @classmethod
        def monotonic(cls):
            return cls.elapsed
    monkeypatch.setattr('devices.fencing.time', Clock, raising=False)
    original = edge.db.execute
    authorization_calls = 0
    def delayed(query, params=None, **kwargs):
        nonlocal authorization_calls
        if 'SELECT authorize_command' in query:
            authorization_calls += 1
        row = original(query, params, **kwargs)
        if (delay_at == 'device_lock' and 'FOR UPDATE' in query
                or delay_at == 'result_write' and 'INSERT INTO edge_outbox' in query
                or delay_at == 'final_authorization' and authorization_calls == 2):
            Clock.elapsed = 11  # past the original 10 second command TTL
        return row
    monkeypatch.setattr(edge.db, 'execute', delayed)
    try:
        if delay_at != 'device_lock':
            with pytest.raises(TimeoutError, match='expired'):
                edge.apply(command, device['device_id'], instant(command['payload']['issued_at']))
            assert not edge.pending()
        else:
            result = edge.apply(command, device['device_id'], instant(command['payload']['issued_at']))
            assert result['payload']['outcome'] == 'EXPIRED'
        assert edge.current()[2] == 0
    finally:
        edge.db.close()


def test_authority_expiry_during_effect_rolls_back(control, monkeypatch):
    from devices.fencing import FencedEdgeState
    from devices.common import instant
    worker, authority = activate(control)
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    command = control.inspect()['commands'][0]['event']
    device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == command['payload']['device_id'])
    edge = FencedEdgeState(control.dsn, device)
    reading = edge.reading
    def expire(*args):
        # Compress the real server-clock expiry after authorization, then wait.
        edge.db.execute("UPDATE action_authority SET valid_until=clock_timestamp()+interval '100 milliseconds'")
        time.sleep(.2)
        return reading(*args)
    monkeypatch.setattr(edge, 'reading', expire)
    try:
        with pytest.raises(PermissionError, match='authority expired'):
            edge.apply(command, device['device_id'], instant(command['payload']['issued_at']))
        assert edge.current()[2] == 0 and not edge.pending()
    finally:
        edge.db.close()
