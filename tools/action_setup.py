"""Additive disposable CDC/gateway roles and isolated delivery schema."""
import os
import psycopg
from psycopg.conninfo import make_conninfo

from action.control import Control
from action.fixtures import FakeRegistry


def main():
    admin = os.environ['ACTION_ADMIN_DSN']
    control = os.environ['CONTROL_DSN']
    with psycopg.connect(admin) as db:
        for role, password, replication in [
            ('cdc', 'cdc_disposable', 'REPLICATION'),
            ('gateway', 'gateway_disposable', 'NOREPLICATION')]:
            if not db.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
                from psycopg import sql
                db.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {} {}').format(
                    sql.Identifier(role), sql.Literal(password), sql.SQL(replication)))
        db.execute('GRANT CONNECT ON DATABASE local_dev_control TO cdc, gateway')
    with psycopg.connect(control) as db:
        db.execute('CREATE SCHEMA IF NOT EXISTS delivery AUTHORIZATION control')
    scoped = make_conninfo(control, options='-c search_path=delivery')
    Control(scoped, FakeRegistry()).migrate()
    with psycopg.connect(admin) as db:
        db.execute('GRANT USAGE ON SCHEMA delivery TO cdc, gateway')
        db.execute('GRANT SELECT ON delivery.action_outbox TO cdc')
        db.execute('CREATE SCHEMA IF NOT EXISTS gateway AUTHORIZATION gateway')
        db.execute('GRANT EXECUTE ON FUNCTION delivery.authorize_command(UUID,BIGINT,JSONB) TO gateway')
        if not db.execute("SELECT 1 FROM pg_publication WHERE pubname='delivery_outbox'").fetchone():
            db.execute('CREATE PUBLICATION delivery_outbox FOR TABLE delivery.action_outbox')
    from confluent_kafka.admin import AdminClient, NewTopic
    client = AdminClient({'bootstrap.servers': '127.0.0.1:19092'})
    existing = client.list_topics(timeout=5).topics
    names = ['local-delivery.' + topic for topic in ('grid.action.v1', 'grid.charger-command.v1', 'grid.transformer-load.v1',
             'grid.command-result.v1', 'grid.telemetry.v1', 'grid.alert.v1')]
    names += ['__debezium-heartbeat.local-dev.cdc']
    missing = [NewTopic(n, num_partitions=1, replication_factor=1) for n in names if n not in existing]
    if missing:
        for future in client.create_topics(missing).values():
            future.result(timeout=10)
    print('Delivery schema, publication and restricted local roles ready')


if __name__ == '__main__':
    main()
