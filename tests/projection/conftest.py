from copy import deepcopy
import json
import os
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from projection.store import Store

DSN = os.getenv('PROJECTION_TEST_DSN')
FIXTURES = json.loads(Path('projection/fixtures.json').read_text())
IDENTITIES = {'viewer-local-token': {'actor': 'viewer', 'role': 'Viewer'},
              'operator-local-token': {'actor': 'operator', 'role': 'Operator'},
              'admin-local-token': {'actor': 'admin', 'role': 'Administrator'}}


@pytest.fixture
def records():
    return deepcopy(FIXTURES)


@pytest.fixture
def store():
    if not DSN:
        pytest.skip('set PROJECTION_TEST_DSN for real PostgreSQL acceptance')
    schema = 'test_projection_' + uuid4().hex
    with psycopg.connect(DSN) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    service = Store(make_conninfo(DSN, options=f'-c search_path={schema}'))
    service.migrate()
    try:
        yield service
    finally:
        with psycopg.connect(DSN) as db:
            db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
