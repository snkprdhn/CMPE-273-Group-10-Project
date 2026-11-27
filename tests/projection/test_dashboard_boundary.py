"""Actual PostgreSQL/API boundary consumed by the isolated dashboard client."""
import asyncio

import httpx
import pytest

from dashboard.client import Operations
from projection.api import create_app
from projection.contracts import utc
from tests.projection.conftest import IDENTITIES
from tests.projection.test_projection import feed


def test_dashboard_client_with_real_projection_api(store, records):
    store.context(records)
    feed(store, records['events'], now=utc())

    async def check():
        source = Operations('http://api/api/v1', 'admin-local-token')
        await source.http.aclose()
        source.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(store, IDENTITIES)),
            base_url='http://api/api/v1', headers={'Authorization':'Bearer admin-local-token'})
        try:
            assert await source.snapshot(reset=True), source.errors
            assert source.cursor
            assert source.data['overview']['total_net_load_kw'] == 96
            assert source.data['transformers']['items'][0]['freshness'] == 'STALE'
            assert source.data['loads']['items'] == []  # Recording is outside the current UI range.
            configuration = {'scenario':'normal','scale':1,'seed':42,'duration_seconds':60}
            source.http.headers['Authorization'] = 'Bearer viewer-local-token'
            with pytest.raises(httpx.HTTPStatusError) as denied:
                await source.mutate('/scenario-runs', configuration)
            assert denied.value.response.status_code == 403
            source.http.headers['Authorization'] = 'Bearer admin-local-token'
            run = await source.mutate('/scenario-runs', configuration)
            assert source.data['scenarios']['active_run']['run_id'] == run['run_id']
            await source.mutate(f'/scenario-runs/{run["run_id"]}/stop')
            assert source.data['scenarios']['active_run'] is None
            alert = source.data['alerts']['items'][0]
            result = await source.mutate(f'/alerts/{alert["alert_id"]}/acknowledge')
            assert result['status'] == 'ACKNOWLEDGED'
        finally:
            await source.close()
    asyncio.run(check())
