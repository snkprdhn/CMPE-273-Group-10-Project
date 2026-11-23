"""Seeded real PostgreSQL projections through real gateway and mock provider."""
import asyncio
import json
import httpx
import pytest
from member4.api import create_app
from tests.member4.conftest import IDENTITIES
from tests.member4.test_projection import feed
from member4.contracts import utc
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
