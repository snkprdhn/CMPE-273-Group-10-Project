"""Seeded real PostgreSQL projections through real gateway and mock provider."""
import asyncio
import json
import httpx
import pytest
from projection.api import create_app
from tests.projection.conftest import IDENTITIES
from tests.projection.test_projection import feed
from projection.contracts import utc
from copilot.tools import Tools
from copilot.service import Service
from copilot.contracts import validate
from identity.auth import Denied

class Identity:
    def authenticate(self, token):
        if token not in IDENTITIES:
            raise Denied(401,'Unauthenticated')
        return IDENTITIES[token]


def test_seeded_projection_investigation(store,records,tmp_path,monkeypatch):
    store.context(records)
    feed(store,records['events'],now=utc())
    # A normal demo publishes more than 200 readings. Only action/alert
    # records should count against the transformer's action evidence limit.
    with store.transaction() as db:
        db.execute("""WITH ids AS (SELECT gen_random_uuid()::text AS id FROM generate_series(1,201))
            INSERT INTO projection_events
            SELECT id, topic, event || jsonb_build_object('event_id', id) FROM ids CROSS JOIN
            (SELECT topic,event FROM projection_events WHERE topic='grid.telemetry.v1' LIMIT 1) AS reading""")
    app=create_app(store,IDENTITIES)
    original=httpx.AsyncClient
    def client(*args,**kwargs):
        kwargs['transport']=httpx.ASGITransport(app=app)
        return original(*args,**kwargs)
    monkeypatch.setattr('copilot.tools.httpx.AsyncClient',client)
    async def check():
        identity=Identity()
        tools=Tools('http://projection',identity)
        service=Service(tools,str(tmp_path/'audit.sqlite'))
        request=dict(question='Why is the transformer critical?',transformer_id='tx-001',
                     start='2026-11-09T18:00:00Z',end='2026-11-09T19:00:00Z')
        actor=identity.authenticate('operator-local-token')
        id=service.start(actor,'operator-local-token',request,'seeded')
        await service.tasks[id]
        state=service.get(actor,id)
        validate('Investigation',state)
        assert state['status']=='PARTIAL'
        assert state['result']['observations']
        assert len(state['evidence']) >= 4, state
        assert any(e['source_type']=='get_command_results' and e['data']['commands'] for e in state['evidence']), state
        audit=json.loads(service.db.execute('SELECT audit FROM investigations WHERE id=?',(id,)).fetchone()[0])
        assert all(row['outcome']=='OK' for row in audit if 'tool' in row), audit
        async with client(base_url='http://projection') as http:
            assert (await http.get('/internal/investigator/projection',params={'transformer_id':'tx-001'},headers={'Authorization':'Bearer viewer-local-token'})).status_code==403
            assert (await http.get('/api/v1/ready')).status_code==200
        service.db.close()
    asyncio.run(check())
