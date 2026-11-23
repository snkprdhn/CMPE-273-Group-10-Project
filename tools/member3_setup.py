"""Additive disposable CDC/gateway roles and isolated Member_3 delivery schema."""
import os
import psycopg
from psycopg.conninfo import make_conninfo

from member3.control import Control
from member3.fixtures import FakeRegistry


def main():
    admin = os.environ['MEMBER3_ADMIN_DSN']
    control = os.environ['CONTROL_DSN']
    with psycopg.connect(admin) as db:
        for role, password, replication in [
            ('member3_cdc', 'member3_cdc_disposable', 'REPLICATION'),
            ('member1_gateway', 'member1_gateway_disposable', 'NOREPLICATION')]:
            if not db.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
                from psycopg import sql
                db.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {} {}').format(
                    sql.Identifier(role), sql.Literal(password), sql.SQL(replication)))
        db.execute('GRANT CONNECT ON DATABASE local_dev_control TO member3_cdc, member1_gateway')
    with psycopg.connect(control) as db:
        db.execute('CREATE SCHEMA IF NOT EXISTS member3_delivery AUTHORIZATION control')
    scoped = make_conninfo(control, options='-c search_path=member3_delivery')
    Control(scoped, FakeRegistry()).migrate()
    with psycopg.connect(admin) as db:
        db.execute('GRANT USAGE ON SCHEMA member3_delivery TO member3_cdc, member1_gateway')
        db.execute('GRANT SELECT ON member3_delivery.action_outbox TO member3_cdc')
        db.execute('CREATE SCHEMA IF NOT EXISTS member1_gateway AUTHORIZATION member1_gateway')
        db.execute('GRANT EXECUTE ON FUNCTION member3_delivery.authorize_command(UUID,BIGINT,JSONB) TO member1_gateway')
        if not db.execute("SELECT 1 FROM pg_publication WHERE pubname='member3_outbox'").fetchone():
            db.execute('CREATE PUBLICATION member3_outbox FOR TABLE member3_delivery.action_outbox')
    from confluent_kafka.admin import AdminClient, NewTopic
    client = AdminClient({'bootstrap.servers': '127.0.0.1:19092'})
    existing = client.list_topics(timeout=5).topics
    names = ['local-m3.' + topic for topic in ('grid.action.v1', 'grid.charger-command.v1', 'grid.transformer-load.v1',
             'grid.command-result.v1', 'grid.telemetry.v1', 'grid.alert.v1')]
    names += ['__debezium-heartbeat.local-dev.member3.cdc']
    missing = [NewTopic(n, num_partitions=1, replication_factor=1) for n in names if n not in existing]
    if missing:
        for future in client.create_topics(missing).values():
            future.result(timeout=10)
    print('Member_3 delivery schema, publication and restricted local roles ready')


if __name__ == '__main__':
    main()
