"""In-cluster database initialization and deterministic simulator acceptance."""
import argparse
import base64
import gzip
import json
import os
import signal
from types import SimpleNamespace

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from member3.control import Control
from member3.fixtures import FakeRegistry
from member4.store import Store
from tools.integration import GATES, Runner, evaluate, http, require, wait

SCHEMA = 'integration_kind'
PREFIX = 'integration-kind.'
BOOTSTRAP = 'grid-kafka-bootstrap:9092'
DATABASES = {'registry': 'local_dev_registry', 'control': 'local_dev_control',
             'projection': 'local_dev_projection', 'keycloak': 'local_dev_keycloak'}


def bootstrap():
    with psycopg.connect(os.environ['POSTGRES_ADMIN_DSN'], autocommit=True) as db:
        for role in (*DATABASES, 'member3_cdc', 'member1_gateway'):
            if not db.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
                db.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {} {}').format(
                    sql.Identifier(role), sql.Literal(os.environ[role.upper() + '_PASSWORD']),
                    sql.SQL('REPLICATION' if role == 'member3_cdc' else 'NOREPLICATION')))
        for role, name in DATABASES.items():
            if not db.execute('SELECT 1 FROM pg_database WHERE datname=%s', (name,)).fetchone():
                db.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(name), sql.Identifier(role)))
            db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(name)))
        db.execute('REVOKE CONNECT ON DATABASE postgres FROM PUBLIC')
        db.execute('GRANT CONNECT ON DATABASE local_dev_control TO member3_cdc, member1_gateway')
    with psycopg.connect(os.environ['CONTROL_DSN'], options='-c search_path=public') as db:
        db.execute('CREATE SCHEMA IF NOT EXISTS integration_kind AUTHORIZATION control')
    Control(os.environ['CONTROL_DSN'], FakeRegistry()).migrate()
    with psycopg.connect(make_conninfo(os.environ['POSTGRES_ADMIN_DSN'], dbname=DATABASES['control'])) as db:
        db.execute('CREATE SCHEMA IF NOT EXISTS integration_edge AUTHORIZATION member1_gateway')
        db.execute('GRANT USAGE ON SCHEMA integration_kind TO member3_cdc, member1_gateway')
        db.execute('GRANT SELECT ON integration_kind.action_outbox TO member3_cdc')
        db.execute('GRANT EXECUTE ON FUNCTION integration_kind.authorize_command(UUID,BIGINT,JSONB) TO member1_gateway')
        if not db.execute("SELECT 1 FROM pg_publication WHERE pubname='integration_kind'").fetchone():
            db.execute('CREATE PUBLICATION integration_kind FOR TABLE integration_kind.action_outbox')
    with psycopg.connect(os.environ['PROJECTION_DSN'], options='-c search_path=public') as db:
        db.execute('CREATE SCHEMA IF NOT EXISTS integration_kind AUTHORIZATION projection')
    Store(os.environ['PROJECTION_DSN']).migrate()
    print('Service databases, migrations and scoped outbox publication are ready')


class KindRunner(Runner):
    def __init__(self):
        self.args = SimpleNamespace(run='kind', prefix=PREFIX, bootstrap=BOOTSTRAP, duration=180,
            registry='registry:50051', flink_url='http://aggregation-rest:8081',
            connect_url='http://grid-connect-connect-api:8083', connector='integration-kind',
            control_dsn=os.environ['CONTROL_DSN'], edge_dsn=os.environ['EDGE_DSN'],
            projection_dsn=os.environ['PROJECTION_DSN'], start_flink=False)
        self.evidence = {key: [] for key in ('loads', 'actions', 'commands', 'results', 'telemetry')}
        self.children = []

    def start_workers(self):
        self.store = Store(self.args.projection_dsn)

    def finish_projection(self):
        self.projected_journal()
        # Rebuild a transactionally consistent snapshot in a temporary schema.
        # The deployed consumer and its SSE generation continue serving.
        with self.store.transaction() as db:
            reads = 'SELECT kind,id,event FROM {}.projection_current ORDER BY kind,id'
            before = list(db.execute(reads.format(SCHEMA)))
            generation = db.execute('SELECT generation FROM projection_meta').fetchone()[0]
            db.execute('CREATE SCHEMA kind_replay')
            for table in ('projection_events', 'projection_current', 'projection_stream',
                          'projection_pages', 'projection_meta', 'projection_trace'):
                db.execute(sql.SQL('CREATE TABLE kind_replay.{} AS TABLE integration_kind.{}').format(
                    sql.Identifier(table), sql.Identifier(table)))
            db.execute('ALTER TABLE kind_replay.projection_current ADD PRIMARY KEY(kind,id)')
            db.execute('ALTER TABLE kind_replay.projection_stream ADD PRIMARY KEY(sequence)')
            db.execute('ALTER TABLE kind_replay.projection_trace ADD FOREIGN KEY(sequence) REFERENCES kind_replay.projection_stream(sequence) ON DELETE CASCADE')
            # Reuse the same replay logic inside the locked snapshot transaction.
            db.execute('SET LOCAL search_path TO kind_replay')
            self.store._rebuild(db)
            after = list(db.execute(reads.format('kind_replay')))
            db.execute('DROP SCHEMA kind_replay CASCADE')
        self.evidence['projection_equal'] = bool(before) and before == after
        self.evidence['projection_generation'] = generation

    def simulate(self):
        # Unlike the host preflight, KafkaTopics and the connector are managed
        # declaratively. Refuse to generate data if provisioning is incomplete.
        with psycopg.connect(self.args.control_dsn) as db:
            for table in ('action_inbox', 'action_outbox', 'action_plans'):
                require(db.execute('SELECT count(*) FROM ' + table).fetchone()[0] == 0,
                        'control history exists; use a fresh kind cluster for another smoke')
        config = http(self.args.connect_url + '/connectors/' + self.args.connector + '/config')
        require(config['table.include.list'] == SCHEMA + '.action_outbox' and
                config['transforms.outbox.route.topic.replacement'] == PREFIX + '${routedByValue}', 'CDC routing mismatch')
        status = http(self.args.connect_url + '/connectors/' + self.args.connector + '/status')
        self.evidence['connector_running'] = status['connector']['state'] == 'RUNNING' and bool(status['tasks']) and all(
            task['state'] == 'RUNNING' for task in status['tasks'])
        require(self.evidence['connector_running'], 'CDC not RUNNING')
        self.evidence['live_registry'] = True
        self.initial_jobs = wait(lambda: [j for j in http(self.args.flink_url + '/jobs/overview')['jobs']
                                          if j['name'] == 'member2-kind' and j['state'] == 'RUNNING'],
                                 'running operator-managed Flink job', 30)
        self.flink()
        self.flow()
        for gate in GATES[:3]:
            evaluate(gate, self.evidence)
        require(self.evidence['projection_equal'], 'projection replay mismatch')


def main():
    from observability import setup
    setup('member1.simulator')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['bootstrap', 'simulate'])
    args = parser.parse_args()
    if args.phase == 'bootstrap':
        bootstrap()
    else:
        runner = KindRunner()
        def stop(*_):
            raise SystemExit('Simulator received SIGTERM')
        signal.signal(signal.SIGTERM, stop)
        try:
            runner.simulate()
        finally:
            payload = gzip.compress(json.dumps(runner.evidence, separators=(',', ':')).encode())
            encoded = base64.b64encode(payload).decode()
            for start in range(0, len(encoded), 4096):
                print('KIND_EVIDENCE_CHUNK ' + encoded[start:start + 4096])


if __name__ == '__main__':
    main()
