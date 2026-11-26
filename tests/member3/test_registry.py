"""Generated-wire adapter tests, with an isolated Member_3 PostgreSQL schema."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import os
from uuid import uuid4

import grpc
from google.protobuf import json_format
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from member3 import registry as module
from member3.control import Control
from member3.domain import candidates, instant, stamp
from member3.fixtures import scenarios


@pytest.fixture
def adapter():
    dsn = os.getenv('MEMBER3_TEST_DSN')
    if not dsn:
        pytest.skip('set MEMBER3_TEST_DSN for adapter persistence tests')
    schema = 'test_action_' + uuid4().hex
    with psycopg.connect(dsn) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    scoped = make_conninfo(dsn, options=f'-c search_path={schema}')
    client = module.Registry('127.0.0.1:1', 'test-token', scoped)
    try:
        yield client
    finally:
        client.close()
        with psycopg.connect(dsn) as db:
            db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def inputs():
    case = scenarios()[0]
    snapshots = deepcopy(case['steps'][0]['values'])
    step = next(s for s in case['steps'] if s['op'] == 'load')
    return snapshots, instant(step['now']), deepcopy(step['event'])


def report(device, now, rate=20, sequence=1):
    return dict(event_id=str(uuid4()), event_type='telemetry.reading', schema_version=1,
        source='test.simulator', event_time=stamp(now), ingested_at=stamp(now),
        correlation_id='live-test', causation_id=None,
        payload=dict(device_id=device['device_id'], transformer_id=device['transformer_id'],
                     device_type='EV_CHARGER', boot_id='boot-test', power_kw=rate,
                     quality='GOOD', sequence_number=sequence, current_rate_kw=rate))


def page(devices, now, cursor=''):
    result = module.pb.ChargerPage(devices=[json_format.ParseDict(d, module.pb.Device()) for d in devices], next_cursor=cursor)
    result.observed_at.FromDatetime(now)
    return result


def wire(adapter, pages):
    calls = []
    def call(request, **kwargs):
        calls.append((module.pb.ListEligibleChargersRequest.FromString(request.SerializeToString()), kwargs))
        result = pages.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    adapter.stub.ListEligibleChargers = call
    return calls


def test_pages_observations_restart_and_allocation(adapter):
    snaps, now, load = inputs()
    for s in snaps:
        adapter.observe(report(s['device'], now), 'tx-fixture', now)
    calls = wire(adapter, [page([snaps[0]['device']], now, 'next'), page([snaps[1]['device']], now)])
    values = adapter.snapshots('tx-fixture', now, 1)
    assert len(candidates(values, 'tx-fixture', now)) == 2
    assert [c[0].cursor for c in calls] == ['', 'next']
    assert all(c[0].limit == 50 and c[0].policy_version == 1 for c in calls)
    assert calls[1][1]['timeout'] <= calls[0][1]['timeout'] <= 2
    assert dict(calls[0][1]['metadata']) == {'x-service-identity': 'action', 'authorization': 'Bearer test-token'}
    restarted = module.Registry('127.0.0.1:1', 'test-token', adapter.dsn)
    try:
        wire(restarted, [page([s['device'] for s in snaps], now)])
        control = Control(adapter.dsn, restarted)
        control.migrate()
        control.load(load, 'tx-fixture', now)
        assert len(control.inspect()['commands']) == 2
    finally:
        restarted.close()


@pytest.mark.parametrize('failure', ['missing', 'stale', 'estimated', 'invalid', 'no_rate', 'assignment', 'rate_mismatch'])
def test_unusable_observations_hold_without_commands(adapter, failure):
    snaps, now, load = inputs()
    for s in snaps:
        event = report(s['device'], now)
        if failure == 'missing':
            continue
        if failure == 'stale':
            event['event_time'] = stamp(now - timedelta(seconds=16))
        elif failure in ('estimated', 'invalid'):
            event['payload']['quality'] = failure.upper()
        elif failure == 'no_rate':
            del event['payload']['current_rate_kw']
        elif failure == 'assignment':
            event['payload']['transformer_id'] = 'another'
        elif failure == 'rate_mismatch':
            event['payload']['power_kw'] = 1
        adapter.observe(event, event['payload']['transformer_id'], now)
    wire(adapter, [page([s['device'] for s in snaps], now)])
    control = Control(adapter.dsn, adapter)
    control.migrate()
    control.load(load, 'tx-fixture', now)
    state = control.inspect()
    assert state['commands'] == []
    assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'


def test_observation_order_validation_invalidation_and_zero_restore(adapter):
    snaps, now, _ = inputs()
    d = snaps[0]['device']
    event = report(d, now, rate=0)
    adapter.observe(event, 'tx-fixture', now)
    adapter.observe(event, 'tx-fixture', now)  # duplicate is harmless
    adapter.observe(report(d, now-timedelta(seconds=1)), 'tx-fixture', now)
    wire(adapter, [page([d], now)])
    values = adapter.snapshots('tx-fixture', now, 1)
    assert values[0]['telemetry']['current_rate_kw'] == 0
    assert candidates(values, 'tx-fixture', now) == []
    assert len(candidates(values, 'tx-fixture', now, restoring=True)) == 1
    with pytest.raises(ValueError, match='CONFLICTING'):
        adapter.observe(report(d, now), 'tx-fixture', now)
    with pytest.raises(ValueError, match='PARTITION_KEY'):
        adapter.observe(event, 'wrong', now)
    with pytest.raises(ValueError, match='FUTURE'):
        adapter.observe(report(d, now+timedelta(seconds=1)), 'tx-fixture', now)
    newer = report(d, now, sequence=2)
    newer['payload']['quality'] = 'INVALID'
    adapter.observe(newer, 'tx-fixture', now)
    wire(adapter, [page([d], now)])
    assert 'telemetry' not in adapter.snapshots('tx-fixture', now, 1)[0]


class RpcFailure(grpc.RpcError):
    def __init__(self, code):
        self.status = code
    def code(self):
        return self.status


@pytest.mark.parametrize('code,attempts', [(grpc.StatusCode.UNAVAILABLE, 3),
    (grpc.StatusCode.DEADLINE_EXCEEDED, 1), (grpc.StatusCode.UNAUTHENTICATED, 1)])
def test_rpc_errors_no_partial_success_and_rollback(adapter, code, attempts):
    snaps, now, load = inputs()
    calls = wire(adapter, [RpcFailure(code) for _ in range(attempts)])
    control = Control(adapter.dsn, adapter)
    control.migrate()
    with pytest.raises(grpc.RpcError):
        control.load(load, 'tx-fixture', now)
    assert len(calls) == attempts
    assert control.inspect()['inbox'] == 0
    assert control.inspect()['actions'] == []


@pytest.mark.parametrize('failure', ['duplicate', 'assignment', 'cursor', 'missing_time', 'wrong_time', 'invalid_device'])
def test_invalid_pages_fail_closed(adapter, failure):
    snaps, now, _ = inputs()
    d = snaps[0]['device']
    if failure == 'assignment':
        d['transformer_id'] = 'wrong'
    elif failure == 'invalid_device':
        d['minimum_rate_kw'] = d['maximum_rate_kw'] + 1
    first = page([d], now, 'next')
    second = page([d], now)
    if failure == 'cursor':
        second = page([snaps[1]['device']], now, 'next')
    elif failure == 'missing_time':
        first.ClearField('observed_at')
    elif failure == 'wrong_time':
        first.observed_at.FromDatetime(now-timedelta(seconds=1))
    wire(adapter, [first, second])
    with pytest.raises(ValueError):
        adapter.snapshots('tx-fixture', now, 1)


def test_shared_deadline_across_pages_and_retry(adapter, monkeypatch):
    snaps, now, _ = inputs()
    wire(adapter, [page([snaps[0]['device']], now, 'next')])
    clock = iter([0, .1, .2, 2.01])
    monkeypatch.setattr(module.time, 'monotonic', lambda: next(clock))
    with pytest.raises(TimeoutError):
        adapter.snapshots('tx-fixture', now, 1)


def test_real_grpc_wire_auth_and_generated_serialization(adapter):
    snaps, now, _ = inputs()
    class Service(module.rpc.DeviceRegistryServicer):
        def ListEligibleChargers(self, request, context):
            assert dict(context.invocation_metadata())['authorization'] == 'Bearer test-token'
            assert context.time_remaining() <= 2
            assert request.transformer_id == 'tx-fixture'
            return page([snaps[0]['device']], now)
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    module.rpc.add_DeviceRegistryServicer_to_server(Service(), server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    adapter.channel.close()
    adapter.channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    adapter.stub = module.rpc.DeviceRegistryStub(adapter.channel)
    try:
        values = adapter.snapshots('tx-fixture', now, 1)
        assert values == [{'device': snaps[0]['device']}]
    finally:
        server.stop(0).wait()


def test_observation_order_preserves_python_microsecond_precision(adapter):
    snaps, now, _ = inputs()
    d = snaps[0]['device']
    base = stamp(now.replace(microsecond=0))[:-1]
    older = report(d, now, rate=20, sequence=1)
    older['event_time'] = older['ingested_at'] = base + '.0000009Z'
    newer = report(d, now, rate=19, sequence=2)
    newer['event_time'] = newer['ingested_at'] = base + '.0000001Z'
    adapter.observe(older, d['transformer_id'], now)
    adapter.observe(newer, d['transformer_id'], now)
    wire(adapter, [page([d], now)])
    assert adapter.snapshots(d['transformer_id'], now, 1)[0]['telemetry']['current_rate_kw'] == 19
