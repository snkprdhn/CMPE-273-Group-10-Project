"""Real gRPC + PostgreSQL. Enable only with MEMBER1_TEST_DSN."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
import time
from uuid import uuid4

import grpc
import psycopg
import pytest

from member1.common import utc
from member1.registry import Client, device_message, pb, Registry, rpc, serve
from member1.simulator import devices

DSN = os.getenv('MEMBER1_TEST_DSN')
pytestmark = pytest.mark.skipif(not DSN, reason='set MEMBER1_TEST_DSN for real PostgreSQL/gRPC tests')


@pytest.fixture
def running():
    prefix = 'test-' + uuid4().hex
    service = Registry(DSN, {'simulator': 'sim-token', 'action': 'read-token'}, 'test-cursor-secret-273')
    service.migrate()
    server, port = serve(service, '127.0.0.1:0')
    client = Client(f'127.0.0.1:{port}', 'simulator', 'sim-token')
    try:
        yield prefix, service, client
    finally:
        client.channel.close()
        server.stop(0).wait()
        with psycopg.connect(DSN) as db:
            db.execute('DELETE FROM member1_operations WHERE id LIKE %s', (prefix + '%',))
            db.execute('DELETE FROM member1_devices WHERE id LIKE %s', (prefix + '%',))


def register(client, prefix, now, suffix='ev', **overrides):
    d = devices(now)[0]
    d.update(device_id=prefix + '-' + suffix, transformer_id=prefix + '-tx', **overrides)
    req = pb.RegisterDeviceRequest(operation_id=prefix + '-' + uuid4().hex, device=device_message(d))
    return req, client.call('RegisterDevice', req)


def fails(code, function):
    with pytest.raises(grpc.RpcError) as exc:
        function()
    assert exc.value.code() == getattr(grpc.StatusCode, code)


def test_all_rpcs_versions_idempotency_and_restart(running):
    prefix, service, client = running
    now = utc()
    req, first = register(client, prefix, now)
    assert client.call('RegisterDevice', req) == first
    conflict = pb.RegisterDeviceRequest()
    conflict.CopyFrom(req)
    conflict.device.maximum_rate_kw = 8
    fails('ALREADY_EXISTS', lambda: client.call('RegisterDevice', conflict))
    conflict.operation_id = prefix + '-conflict'
    fails('ALREADY_EXISTS', lambda: client.call('RegisterDevice', conflict))
    newer = pb.RegisterDeviceRequest()
    newer.CopyFrom(req)
    newer.operation_id = prefix + '-newer'
    newer.device.capability_version = 2
    client.call('RegisterDevice', newer)
    older = pb.RegisterDeviceRequest()
    older.CopyFrom(req)
    older.operation_id = prefix + '-older'
    fails('FAILED_PRECONDITION', lambda: client.call('RegisterDevice', older))
    heartbeat = pb.HeartbeatRequest(operation_id=prefix + '-hb', device_id=first.device_id,
                                    capability_version=2)
    heartbeat.observed_at.FromDatetime(now + timedelta(seconds=1))
    seen = client.call('Heartbeat', heartbeat)
    assert seen.updated_at == heartbeat.observed_at
    assert client.call('Heartbeat', heartbeat) == seen
    heartbeat.operation_id = prefix + '-bad-hb'
    heartbeat.capability_version = 1
    fails('FAILED_PRECONDITION', lambda: client.call('Heartbeat', heartbeat))
    # A second instance reads the exact same owned persistence; operation IDs too.
    replica = Registry(DSN, service.tokens, 'test-cursor-secret-273')
    replica.migrate()
    server, port = serve(replica, '127.0.0.1:0')
    restarted = Client(f'127.0.0.1:{port}', 'simulator', 'sim-token')
    try:
        assert restarted.call('GetDevice', pb.GetDeviceRequest(device_id=first.device_id)) == seen
        assert restarted.call('RegisterDevice', req) == first
    finally:
        restarted.channel.close()
        server.stop(0).wait()


def test_eligibility_freshness_departure_and_cursor(running):
    prefix, _, client = running
    now = utc()
    for suffix in ('a', 'b', 'c'):
        register(client, prefix, now - timedelta(seconds=15), suffix)
    register(client, prefix, now - timedelta(seconds=15, microseconds=1), 'stale')
    departure = device_message(devices(now)[0]).updated_at
    departure.FromDatetime(now + timedelta(minutes=5))
    register(client, prefix, now, 'leaving', departure_time=departure.ToJsonString())
    register(client, prefix, now + timedelta(seconds=1), 'future')
    register(client, prefix, now, 'passive', controllable=False)
    query = pb.ListEligibleChargersRequest(transformer_id=prefix + '-tx', policy_version=1, limit=2)
    query.evaluated_at.FromDatetime(now)
    first = client.call('ListEligibleChargers', query)
    assert [d.device_id for d in first.devices] == [prefix + '-a', prefix + '-b']
    assert first.next_cursor
    query.cursor = first.next_cursor
    second = client.call('ListEligibleChargers', query)
    assert [d.device_id for d in second.devices] == [prefix + '-c']
    assert second.next_cursor == ''
    query.policy_version = 2
    fails('INVALID_ARGUMENT', lambda: client.call('ListEligibleChargers', query))
    query.cursor = 'broken'
    fails('INVALID_ARGUMENT', lambda: client.call('ListEligibleChargers', query))
    query.cursor, query.limit = '', 0
    assert len(client.call('ListEligibleChargers', query).devices) == 3
    query.limit = 201
    fails('INVALID_ARGUMENT', lambda: client.call('ListEligibleChargers', query))


def test_validation_permissions_and_missing(running):
    prefix, service, client = running
    fails('INVALID_ARGUMENT', lambda: client.call('GetDevice', pb.GetDeviceRequest()))
    fails('NOT_FOUND', lambda: client.call('GetDevice', pb.GetDeviceRequest(device_id=prefix)))
    bad = device_message(devices(utc())[0])
    bad.minimum_rate_kw = 99
    fails('INVALID_ARGUMENT', lambda: client.call('RegisterDevice', pb.RegisterDeviceRequest(operation_id=prefix, device=bad)))
    req, registered = register(client, prefix, utc())
    read = Client(client.channel._channel.target().decode(), 'action', 'read-token')
    invalid = rpc.DeviceRegistryStub(client.channel)
    try:
        assert read.call('GetDevice', pb.GetDeviceRequest(device_id=registered.device_id)) == registered
        fails('PERMISSION_DENIED', lambda: read.call('RegisterDevice', req))
        fails('UNAUTHENTICATED', lambda: invalid.GetDevice(pb.GetDeviceRequest(device_id=registered.device_id), timeout=1))
    finally:
        read.channel.close()


def test_database_lock_deadline_and_unavailable(running):
    prefix, _, client = running
    req, d = register(client, prefix, utc())
    with psycopg.connect(DSN) as blocker:
        blocker.execute('SELECT pg_advisory_xact_lock(273001)')
        started = time.monotonic()
        fails('DEADLINE_EXCEEDED', lambda: client.call('GetDevice', pb.GetDeviceRequest(device_id=d.device_id), timeout=.1))
        assert time.monotonic() - started < .6
    assert client.call('GetDevice', pb.GetDeviceRequest(device_id=d.device_id)) == d
    unavailable = Registry('postgresql://unused:unused@127.0.0.1:1/unused',
                           {'simulator': 'sim-token'}, 'test-cursor-secret-273')
    server, port = serve(unavailable, '127.0.0.1:0')
    bad_client = Client(f'127.0.0.1:{port}', 'simulator', 'sim-token')
    try:
        fails('UNAVAILABLE', lambda: bad_client.call('GetDevice', pb.GetDeviceRequest(device_id=prefix)))
    finally:
        bad_client.channel.close()
        server.stop(0).wait()


def test_concurrent_registration_and_timed_out_write(running):
    prefix, _, client = running
    now = utc()
    d = devices(now)[0]
    d.update(device_id=prefix + '-race', transformer_id=prefix + '-tx')
    requests = []
    for i in (1, 2):
        candidate = dict(d, priority=i)
        requests.append(pb.RegisterDeviceRequest(operation_id=prefix + f'-race-{i}',
                                                  device=device_message(candidate)))
    def attempt(req):
        try:
            client.call('RegisterDevice', req)
            return 'OK'
        except grpc.RpcError as exc:
            return exc.code().name
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, requests)) == ['ALREADY_EXISTS', 'OK']
    timed = pb.RegisterDeviceRequest(operation_id=prefix + '-timed-op',
                                     device=device_message(dict(d, device_id=prefix + '-timed')))
    with psycopg.connect(DSN) as blocker:
        blocker.execute('SELECT pg_advisory_xact_lock(273001)')
        fails('DEADLINE_EXCEEDED', lambda: client.call('RegisterDevice', timed, timeout=.05))
    fails('NOT_FOUND', lambda: client.call('GetDevice', pb.GetDeviceRequest(device_id=prefix + '-timed')))
