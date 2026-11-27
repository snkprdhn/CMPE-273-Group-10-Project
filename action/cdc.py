"""Pinned Debezium configuration and bounded local Connect REST operations."""
import json
from urllib.request import Request, urlopen


def configuration(schema='delivery', prefix='local-delivery.'):
    # This finite local profile intentionally fixes connector/slot identities.
    # Repointing an existing slot to another schema would silently skip history.
    if schema != 'delivery' or prefix != 'local-delivery.':
        raise ValueError('this connector is bound to delivery/local-delivery')
    return {
        'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
        'tasks.max': '1', 'database.hostname': 'postgres', 'database.port': '5432',
        'database.user': 'cdc', 'database.password': 'cdc_disposable',
        'database.dbname': 'local_dev_control', 'topic.prefix': 'local-dev.cdc',
        'plugin.name': 'pgoutput', 'slot.name': 'delivery_outbox',
        'publication.name': 'delivery_outbox', 'publication.autocreate.mode': 'disabled',
        'table.include.list': schema + '.action_outbox', 'snapshot.mode': 'initial',
        'tombstones.on.delete': 'false', 'include.schema.changes': 'false',
        'transforms': 'outbox', 'transforms.outbox.type': 'io.debezium.transforms.outbox.EventRouter',
        'transforms.outbox.table.field.event.id': 'id',
        'transforms.outbox.table.field.event.key': 'aggregateid',
        'transforms.outbox.route.by.field': 'aggregatetype',
        'transforms.outbox.route.topic.replacement': prefix + '${routedByValue}',
        'transforms.outbox.table.expand.json.payload': 'false',
        'transforms.outbox.table.fields.additional.placement': 'traceparent:header:traceparent,correlation_id:header:correlation_id,causation_id:header:causation_id',
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
        request('connectors/delivery-outbox/config', 'PUT', configuration())
    print(json.dumps(request('connectors/delivery-outbox/status'), indent=2))
