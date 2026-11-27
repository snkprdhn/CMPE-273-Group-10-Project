"""Real Debezium crash/restart and duplicate-safe simulated effects.

Opt in: ACTION_CDC_TEST=1; only stops/kills the new Connect container.
Never resets slots, offsets, schemas, broker data or pre-existing workloads.
"""
from copy import deepcopy
import os
import subprocess
import time
from uuid import uuid4

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from action.cdc import request
from action.domain import instant
from action.fixtures import replay
from devices.fencing import FencedEdgeState
from tests.action.test_control import control, CASES
from tests.action.test_delivery import activate

pytestmark = pytest.mark.skipif(not os.getenv('ACTION_CDC_TEST'), reason='set ACTION_CDC_TEST=1 for real Connect crash test')
COMPOSE = ['docker', 'compose', '-p', 'smart-grid-local-dev', '-f', 'infra/compose/compose.yaml', '-f', 'infra/compose/connect.yaml']


def container(*args):
    subprocess.run(COMPOSE + list(args), check=True, capture_output=True, timeout=45)


def running():
    end = time.monotonic() + 40
    while time.monotonic() < end:
        try:
            status = request('connectors/delivery-outbox/status')
            if status['connector']['state'] == 'RUNNING' and status['tasks'] and all(t['state'] == 'RUNNING' for t in status['tasks']):
                return
        except OSError:
            pass
        time.sleep(.25)
    raise AssertionError('Connect task did not recover')


def test_committed_rows_survive_connect_death_and_duplicates(control):
    from confluent_kafka import Consumer, TopicPartition
    worker, authority = activate(control)
    authority.lease.deadline = time.monotonic() + 120
    authority.activate()
    replay(worker, dict(steps=CASES[0]['steps'][:2]))
    rows = control.inspect()['outbox']
    assert len([r for r in rows if r['topic'] == 'grid.charger-command.v1']) == 2
    dsn = make_conninfo(os.environ['ACTION_TEST_DSN'], options='-c search_path=delivery')
    receiver = Consumer({'bootstrap.servers': '127.0.0.1:19092', 'group.id': 'local-delivery.test.' + uuid4().hex,
        'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    topics = ['local-delivery.grid.action.v1', 'local-delivery.grid.charger-command.v1']
    positions = []
    for topic in topics:
        tp = TopicPartition(topic, 0)
        _, high = receiver.get_watermark_offsets(tp, timeout=5)
        positions.append(TopicPartition(topic, 0, high))
    receiver.assign(positions)
    rollback_id = str(uuid4())

    def insert(db, row, event=None):
        db.execute('INSERT INTO action_outbox(id,aggregatetype,aggregateid,type,payload) VALUES (%s,%s,%s,%s,%s)',
                   (str(uuid4()), row['topic'], row['key'], row['event']['event_type'], Jsonb(event or row['event'])))

    def collect(expected_count):
        import json
        result = []
        end = time.monotonic() + 30
        seen = set()
        while len(seen) < expected_count and time.monotonic() < end:
            message = receiver.poll(.25)
            if message is None:
                continue
            assert not message.error(), message.error()
            value = json.loads(message.value())
            assert value['event_id'] != rollback_id  # uncommitted insert never published
            result.append((message, value))
            seen.add(value['event_id'])
        assert len(seen) == expected_count
        return result

    edges = {}
    try:
        running()
        container('kill', '-s', 'SIGKILL', 'connect')  # no flush on publisher death
        # Both rollback and commit happen while CDC is dead.
        with psycopg.connect(dsn) as db:
            uncommitted = deepcopy(rows[0]['event'])
            uncommitted['event_id'] = rollback_id
            insert(db, rows[0], uncommitted)
            db.rollback()
            for row in rows:
                insert(db, row)
        container('start', 'connect')
        running()
        first = collect(len(rows))
        expected = {(r['topic'], r['key'], r['event']['event_id']) for r in rows}
        assert {(m.topic().removeprefix('local-delivery.'), m.key().decode(), e['event_id']) for m, e in first} == expected
        expected_payloads = {row['event']['event_id']: row['event'] for row in rows}
        assert all(e == expected_payloads[e['event_id']] for _, e in first)
        for m, e in first:
            header = dict(m.headers())['id']
            # Router header identifies a physical row; logical identity is payload.event_id.
            from uuid import UUID
            UUID(header.decode() if isinstance(header, bytes) else header)
        # Publication occurred; crash before relying on Connect offset flush.
        container('kill', '-s', 'SIGKILL', 'connect')
        container('start', 'connect')
        running()
        # Force a CDC redelivery even if this runtime flushed its offset first.
        # Retry rows have new physical ids but identical logical ids and bytes.
        with psycopg.connect(dsn) as db:
            for row in rows:
                insert(db, row)
        second = collect(len(rows))
        all_messages = first + second
        for _, envelope in all_messages:
            if envelope['event_type'] != 'charger.command':
                continue
            key = envelope['payload']['device_id']
            if key not in edges:
                device = next(v['device'] for v in CASES[0]['steps'][0]['values'] if v['device']['device_id'] == key)
                edges[key] = FencedEdgeState(control.dsn, device)
            edges[key].apply(envelope, key, instant(envelope['payload']['issued_at']))
        assert len(edges) == 2
        assert all(edge.current()[2] == 1 for edge in edges.values())
        # A result loss/restart replays the durable result without another effect.
        for key, edge in list(edges.items()):
            device = edge.device
            edge.db.close()
            edges[key] = FencedEdgeState(control.dsn, device)
            command = next(e for _, e in first if e['event_type'] == 'charger.command' and e['payload']['device_id'] == key)
            assert edges[key].apply(command, key, instant(command['payload']['issued_at']))['payload']['outcome'] == 'ALREADY_APPLIED'
            assert edges[key].current()[2] == 1
    finally:
        for edge in edges.values():
            edge.db.close()
        receiver.close()
        container('start', 'connect')


@pytest.mark.skipif(not os.getenv('ACTION_KIND_TEST'), reason='worker test also requires kind')
def test_worker_graceful_loss_and_restart_reconciliation(control, tmp_path):
    import signal
    import sys
    from action.control import Control
    name = 'test-worker-' + uuid4().hex[:12]
    replay(control, dict(steps=CASES[0]['steps'][:2]))
    # This restart test deliberately uses the standalone fixture boundary.
    snapshots = tmp_path / 'snapshots.json'
    snapshots.write_text('[]')
    env = dict(os.environ, CONTROL_DSN=control.dsn, LEASE_NAME=name, CONTROL_SNAPSHOTS=str(snapshots))
    child = None
    try:
        for expected_epoch in (2, 3):
            child = subprocess.Popen([sys.executable, '-m', 'action.worker'], env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            end = time.monotonic() + 20
            while time.monotonic() < end:
                assert child.poll() is None, child.stderr.read().decode()
                state = control.inspect()
                if state['actions'][0]['epoch'] == expected_epoch:
                    break
                time.sleep(.1)
            assert state['actions'][0]['epoch'] == expected_epoch
            assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'
            assert len(state['commands']) == 2
            child.send_signal(signal.SIGTERM)
            child.wait(timeout=15)
            assert child.returncode == 0, child.stderr.read().decode()
            child.stderr.close()
            with psycopg.connect(control.dsn) as db:
                assert db.execute('SELECT valid_until <= clock_timestamp() FROM action_authority').fetchone()[0]
    finally:
        if child and child.poll() is None:
            child.kill()
            child.wait(timeout=3)
        if child and not child.stderr.closed:
            child.stderr.close()
        subprocess.run(['.venv/bin/kubectl', '--context', 'kind-smart-grid-dev', '-n', 'smart-grid-control',
                        'delete', 'lease', name, '--ignore-not-found'], check=True, capture_output=True)
