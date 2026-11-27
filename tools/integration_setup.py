"""Provision isolated additive local integration state; never reset prior runs."""
import json
import os
from pathlib import Path
import re
import argparse
from urllib.error import HTTPError
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from action.control import Control
from action.fixtures import FakeRegistry
from action.cdc import configuration, request
from tools.integration import wait, http


def provision(run, bootstrap='127.0.0.1:19092', partitions=1, replication=1, connect_url='http://127.0.0.1:18083'):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_]{0,24}', run):
        raise ValueError('run must be 1..25 lowercase letters/digits/underscores')
    schema, edge = 'integration_' + run, 'integration_edge_' + run
    control = os.environ.get('CONTROL_DSN', 'postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control')
    admin = os.environ.get('ACTION_ADMIN_DSN', 'postgresql://local_admin:local_admin_disposable@127.0.0.1:15432/local_dev_control')
    projection = os.environ.get('PROJECTION_DSN', 'postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection')
    with psycopg.connect(control) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    scoped = make_conninfo(control, options=f'-c search_path={schema}')
    Control(scoped, FakeRegistry()).migrate()
    with psycopg.connect(admin) as db:
        db.execute(sql.SQL('CREATE SCHEMA {} AUTHORIZATION gateway').format(sql.Identifier(edge)))
        db.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO cdc,gateway').format(sql.Identifier(schema)))
        db.execute(sql.SQL('GRANT SELECT ON {}.action_outbox TO cdc').format(sql.Identifier(schema)))
        db.execute(sql.SQL('GRANT EXECUTE ON FUNCTION {}.authorize_command(UUID,BIGINT,JSONB) TO gateway').format(sql.Identifier(schema)))
        db.execute(sql.SQL('CREATE PUBLICATION {} FOR TABLE {}.action_outbox').format(sql.Identifier(schema), sql.Identifier(schema)))
    with psycopg.connect(projection) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    config = configuration()
    config.update({'table.include.list':schema+'.action_outbox', 'slot.name':schema,
                   'publication.name':schema, 'topic.prefix':schema,
                   'transforms.outbox.route.topic.replacement':'integration-'+run+'.${routedByValue}'})
    from confluent_kafka.admin import AdminClient, NewTopic
    from devices.common import BUNDLE
    kafka = AdminClient({'bootstrap.servers': bootstrap})
    names = ['integration-' + run + '.' + topic for topic in BUNDLE['x-events']]
    names.append('__debezium-heartbeat.' + schema)
    for future in kafka.create_topics([NewTopic(name, partitions, replication, config={'min.insync.replicas': '2' if replication == 3 else '1'}) for name in names]).values():
        future.result(10)
    http(connect_url + '/connectors/' + schema + '/config', 'PUT', config, headers={'Content-Type': 'application/json'}, timeout=30)
    def streaming():
        try:
            status = http(connect_url + '/connectors/' + schema + '/status')
        except HTTPError as exc:
            if exc.code == 404:
                return False  # Connect has accepted the config but has not assigned the task.
            raise
        if any(task['state'] == 'FAILED' for task in status['tasks']):
            raise RuntimeError('Integration Debezium task failed. Inspect Connect logs.')
        return status['tasks'] and all(task['state'] == 'RUNNING' for task in status['tasks'])
    wait(streaming, 'integration Debezium task', 30)
    values = {'INTEGRATION_CONTROL_DSN':scoped,
              'INTEGRATION_EDGE_DSN':make_conninfo('postgresql://gateway:gateway_disposable@127.0.0.1:15432/local_dev_control',options=f'-c search_path={edge},{schema}'),
              'INTEGRATION_PROJECTION_DSN':make_conninfo(projection,options=f'-c search_path={schema}'),
              'INTEGRATION_CONNECTOR':schema}
    directory = Path('.tools/integration-config')
    directory.mkdir(exist_ok=True, mode=0o700)
    path = directory / (run + '.json')
    with path.open('x') as output:
        path.chmod(0o600)
        json.dump(values, output)
    print(path)
    return values

if __name__=='__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    provision(parser.parse_args().run)
