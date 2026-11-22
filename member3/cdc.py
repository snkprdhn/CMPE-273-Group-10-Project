"""Pinned Debezium configuration and bounded local Connect REST operations."""
import json
from urllib.request import Request, urlopen


def configuration(schema='member3_delivery', prefix='local-m3.'):
    # This finite local profile intentionally fixes connector/slot identities.
    # Repointing an existing slot to another schema would silently skip history.
    if schema != 'member3_delivery' or prefix != 'local-m3.':
        raise ValueError('this connector is bound to member3_delivery/local-m3')
    return {
        'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
        'tasks.max': '1', 'database.hostname': 'postgres', 'database.port': '5432',
        'database.user': 'member3_cdc', 'database.password': 'member3_cdc_disposable',
        'database.dbname': 'local_dev_control', 'topic.prefix': 'local-dev.member3.cdc',
        'plugin.name': 'pgoutput', 'slot.name': 'member3_outbox',
        'publication.name': 'member3_outbox', 'publication.autocreate.mode': 'disabled',
        'table.include.list': schema + '.action_outbox', 'snapshot.mode': 'initial',
        'tombstones.on.delete': 'false', 'include.schema.changes': 'false',
        'transforms': 'outbox', 'transforms.outbox.type': 'io.debezium.transforms.outbox.EventRouter',
        'transforms.outbox.table.field.event.id': 'id',
        'transforms.outbox.table.field.event.key': 'aggregateid',
        'transforms.outbox.route.by.field': 'aggregatetype',
        'transforms.outbox.route.topic.replacement': prefix + '${routedByValue}',
        'transforms.outbox.table.expand.json.payload': 'false',
        'transforms.outbox.table.op.invalid.behavior': 'fatal',
        'key.converter': 'org.apache.kafka.connect.storage.StringConverter',
        'value.converter': 'org.apache.kafka.connect.storage.StringConverter',
        'errors.tolerance': 'none', 'heartbeat.interval.ms': '1000',
    }


def request(path, method='GET', value=None):
    req = Request('http://127.0.0.1:18083/' + path, method=method,
                  data=json.dumps(value).encode() if value is not None else None,
                  headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=5) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['configure', 'status'])
    args = parser.parse_args()
    if args.command == 'configure':
        request('connectors/member3-outbox/config', 'PUT', configuration())
    print(json.dumps(request('connectors/member3-outbox/status'), indent=2))
