import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

import httpx
import pytest

from dashboard.client import Operations
from dashboard.fixture_api import BASE, create_fixture_app


def test_snapshot_pages_and_errors_retain_data():
    async def check():
        fixture = create_fixture_app()
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture), base_url='http://fixture/api/v1', headers={'Authorization':'Bearer fixture-operator-token'})
        assert await source.snapshot(reset=True)
        assert source.cursor.startswith('fixture:')
        assert source.data['overview']['total_net_load_kw'] == 72
        fixture.state.fixture.fault = 'denied'
        assert not await source.snapshot()
        assert 'Permission denied' in source.errors['overview']
        assert source.data['overview']['total_net_load_kw'] == 72
        fixture.state.fixture.fault = ''
        await source.mutate('/scenario-runs', {'scenario':'normal','scale':1,'seed':42,'duration_seconds':60})
        assert source.data['scenarios']['active_run']['status'] == 'RUNNING'
        await source.mutate('/scenario-runs/fixture-run/stop')
        assert source.data['scenarios']['active_run'] is None
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_mutation_uncertainty_preserves_key_and_authorization():
    async def check():
        keys = []
        def transport(request):
            keys.append(request.headers['idempotency-key'])
            if len(keys) == 1:
                raise httpx.ReadTimeout('uncertain', request=request)
            return httpx.Response(403, json={})
        source = Operations('http://fixture/api/v1', 'fixture-viewer-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.MockTransport(transport), base_url='http://fixture/api/v1')
        with pytest.raises(httpx.ReadTimeout):
            await source.mutate('/scenario-runs', {'scenario':'normal'})
        with pytest.raises(httpx.HTTPStatusError):
            await source.mutate('/scenario-runs', {'scenario':'normal'})
        assert keys[0] == keys[1]
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_pagination_repeated_cursor_fails_instead_of_looping():
    async def check():
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(base_url='http://fixture/api/v1', transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={'items':[], 'next_cursor':'repeated','freshness':'FRESH'})))
        with pytest.raises(ValueError, match='Repeated'):
            await source.collection('/devices')
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_recorded_fixture_business_accounting():
    state = create_fixture_app().state.fixture
    for step, power in enumerate([72,96,75,72]):
        state.step = step
        state.changed()
        data = state.payload()
        load = data['transformers'][0]['load']
        assert load['net_load_kw'] == power == load['meter_demand_kw'] + load['ev_demand_kw'] - load['solar_generation_kw']
        assert load['ev_demand_kw'] == data['devices'][0]['latest_telemetry']['power_kw']
    assert data['actions'][0]['status'] == 'COMPLETED'
    assert data['devices'][0]['command_history'][-1]['applied_rate_kw'] == 28


def test_fixture_payloads_match_executable_v1():
    definitions = json.loads(Path('schemas/v1/bundle.json').read_text())['$defs']
    def validate(name, value):
        Draft202012Validator({'$ref': f'#/$defs/{name}', '$defs': definitions}, format_checker=FormatChecker()).validate(value)
    state = create_fixture_app().state.fixture
    for step in range(4):
        state.step = step
        state.changed()
        data = state.payload()
        for collection, shape in [('transformers','Transformer'),('devices','DeviceView'),('alerts','AlertPayload'),('actions','ActionPayload'),('loads','LoadPayload')]:
            for record in data[collection]:
                validate(shape, record)


def test_sse_versions_ignore_duplicates_and_old_events():
    async def check():
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        calls = []
        async def snapshot(reset=False):
            calls.append(reset)
            source.data = {'present': True}
            if reset:
                source.cursor = 'fixture:0'
            return True
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                for cursor, version in enumerate([2, 2, 1, 3], 1):
                    event = dict(cursor=f'fixture:{cursor}', event_type='transformer.updated', entity_id='tx-001',
                                 version=version, event_time='2026-11-09T18:30:06Z', payload=BASE['transformers'][0])
                    yield f'id: {event["cursor"]}\nevent: transformer.updated\ndata: {json.dumps(event)}\n\n'.encode()
                await asyncio.Event().wait()
        def transport(request):
            assert request.headers['Last-Event-ID'] == 'fixture:0'
            return httpx.Response(200, headers={'content-type':'text/event-stream'}, stream=Stream())
        source.snapshot = snapshot
        source.http = httpx.AsyncClient(base_url='http://fixture/api/v1', transport=httpx.MockTransport(transport))
        task = asyncio.create_task(source.listen())
        for _ in range(100):
            if source.cursor == 'fixture:4':
                break
            await asyncio.sleep(.01)
        assert source.cursor == 'fixture:4'
        assert calls == [True, False, False]
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_sse_min_interval_coalesces_bursts_and_bumps_revision():
    async def check():
        source = Operations('http://fixture/api/v1', 'fixture-operator-token', sse_min_interval=5.0)
        await source.http.aclose()
        calls = []
        async def snapshot(reset=False):
            calls.append(reset)
            source.data = {'present': True}
            source.revision += 1
            from time import monotonic
            source._last_snapshot_at = monotonic()
            source._sse_pending = False
            if reset:
                source.cursor = 'fixture:0'
            return True
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                for cursor, version in enumerate([2, 3, 4], 1):
                    event = dict(cursor=f'fixture:{cursor}', event_type='device.updated', entity_id=f'ev-{cursor}',
                                 version=version, event_time='2026-11-09T18:30:06Z', payload=BASE['devices'][0])
                    yield f'id: {event["cursor"]}\nevent: device.updated\ndata: {json.dumps(event)}\n\n'.encode()
                await asyncio.Event().wait()
        source.snapshot = snapshot
        source.http = httpx.AsyncClient(base_url='http://fixture/api/v1',
                                        transport=httpx.MockTransport(lambda r: httpx.Response(200, headers={'content-type': 'text/event-stream'}, stream=Stream())))
        task = asyncio.create_task(source.listen())
        for _ in range(100):
            if source.cursor == 'fixture:3':
                break
            await asyncio.sleep(.01)
        assert source.cursor == 'fixture:3' and calls == [True] and source._sse_pending is True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_invalid_success_payload_is_reported_as_error():
    async def check():
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(base_url='http://fixture/api/v1', transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={'unexpected': True})))
        assert not await source.snapshot(reset=True)
        assert source.status == 'Unavailable'
        assert not source.data
        assert len(source.errors) == 6
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_viewer_cannot_mutate_fixture_boundary():
    async def check():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_fixture_app()), base_url='http://fixture') as client:
            response = await client.post('/api/v1/scenario-runs', headers={'Authorization':'Bearer fixture-viewer-token','Idempotency-Key':'attempt'},
                json={'scenario':'normal','scale':1,'seed':42,'duration_seconds':60})
            assert response.status_code == 403
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_failed_history_switch_keeps_the_original_scope():
    async def check():
        fixture = create_fixture_app()
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture), base_url='http://fixture/api/v1', headers={'Authorization':'Bearer fixture-operator-token'})
        assert await source.snapshot(reset=True)
        previous = source.data['loads']
        collection = source.collection
        async def fail_new_history(path, params=None):
            if '/tx-002/loads' in path:
                raise httpx.ConnectError('unavailable')
            return await collection(path, params)
        source.collection = fail_new_history
        source.transformer = 'tx-002'
        assert not await source.snapshot()
        assert source.data['history_transformer'] == 'tx-001'
        assert source.data['loads'] == previous
        assert 'loads' in source.errors
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_review_disconnected_refresh_never_reports_live():
    async def check():
        fixture = create_fixture_app()
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture), base_url='http://fixture/api/v1', headers={'Authorization':'Bearer fixture-operator-token'})
        await source.snapshot()
        source.status = 'Reconnecting'
        fixture.state.fixture.fault = 'denied'
        await source.snapshot()
        fixture.state.fixture.fault = ''
        await source.snapshot()
        assert source.status == 'Reconnecting'
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_review_uncertain_key_survives_an_intervening_operation():
    async def check():
        requests = []
        def transport(request):
            requests.append((request.url.path, request.headers['idempotency-key']))
            raise httpx.ReadTimeout('uncertain', request=request)
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.MockTransport(transport), base_url='http://fixture/api/v1')
        for path in ['/scenario-runs', '/alerts/alert-001/acknowledge', '/scenario-runs']:
            with pytest.raises(httpx.ReadTimeout):
                await source.mutate(path)
        assert requests[0][1] == requests[2][1]
        assert requests[0][1] != requests[1][1]
        await source.close()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_review_snapshot_does_not_publish_half_finished_reads():
    async def check():
        fixture = create_fixture_app()
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture), base_url='http://fixture/api/v1', headers={'Authorization':'Bearer fixture-operator-token'})
        await source.snapshot()
        fixture.state.fixture.step = 1
        fixture.state.fixture.changed()
        reached, release = asyncio.Event(), asyncio.Event()
        collection = source.collection
        async def delayed(path, params=None):
            if path == '/devices':
                reached.set()
                await release.wait()
            return await collection(path, params)
        source.collection = delayed
        task = asyncio.create_task(source.snapshot())
        try:
            await asyncio.wait_for(reached.wait(), timeout=2)
            assert source.data['overview']['critical_count'] == 0
        finally:
            release.set()
            await task
            await source.close()
        assert source.data['overview']['critical_count'] == 1
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)


def test_review_nonfinite_and_excessively_nested_json_fail_visibly():
    async def check(raw):
        source = Operations('http://fixture/api/v1', 'fixture-operator-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(base_url='http://fixture/api/v1', transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=raw) if request.url.path.endswith('/overview') else httpx.Response(503)))
        assert not await source.snapshot()
        assert 'overview' not in source.data
        assert source.status == 'Unavailable'
        await source.close()
    overview = dict(transformer_count=1, critical_count=1, total_net_load_kw=float('nan'), highest_risk=BASE['transformers'], freshness='FRESH')
    for raw in [json.dumps(overview).encode(), b'['*2000+b'0'+b']'*2000]:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(asyncio.run, check(raw)).result(timeout=10)


def test_review_empty_fixture_configuration_returns_success_and_recovers():
    async def check():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_fixture_app()), base_url='http://fixture') as client:
            result = await client.post('/fixture', json={'fault':'empty'})
            assert result.status_code == 200
            response = await client.get('/api/v1/transformers', headers={'Authorization':'Bearer fixture-operator-token'})
            assert response.json()['items'] == []
            assert (await client.post('/fixture', json={'fault':''})).status_code == 200
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(asyncio.run, check()).result(timeout=10)
