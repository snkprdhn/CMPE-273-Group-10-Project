"""Explicit live Registry context; only generated RPCs cross the ownership boundary.

Registry v1 cannot enumerate inventory or supply transformer policy/classification.
The integration owner supplies bounded device IDs and Transformer snapshots from
its topology/configuration and an actual load, never another service's tables.
"""
import argparse
from datetime import timedelta
import json
import math
import os
import sys
import time
from uuid import uuid4

from observability import headers, traced

from psycopg.types.json import Jsonb

from member4.contracts import ApiError, ROOT, instant, stamp, utc, validate


class RegistryContext:
    def __init__(self, address, token):
        # Optional dependencies keep the standalone projection environment independent.
        import grpc
        from google.protobuf import json_format

        sys.path.insert(0, str(ROOT / '.tools/generated'))
        import registry_pb2 as pb
        import registry_pb2_grpc as rpc

        if not address or not token or not token.isascii() or any(c.isspace() for c in token):
            raise ValueError('Registry address and ASCII service token required')
        self.grpc, self.pb, self.json_format = grpc, pb, json_format
        self.channel = grpc.insecure_channel(address, options=[('grpc.max_receive_message_length', 65536)])
        self.stub = rpc.DeviceRegistryStub(self.channel)
        self.metadata = (('x-service-identity', 'api'), ('authorization', 'Bearer ' + token))

    def close(self):
        self.channel.close()

    @traced('grpc.client')
    def devices(self, device_ids, timeout=2.0):
        """Fetch a bounded inventory atomically within one shared RPC deadline.

        No automatic retry: the caller owns refresh/retry scheduling. A failed
        batch leaves the previous projection context and cursor untouched.
        """
        if not math.isfinite(timeout) or not 0 < timeout <= 2:
            raise ValueError('Registry deadline must be in (0,2] seconds')
        if not 1 <= len(device_ids) <= 200 or len(set(device_ids)) != len(device_ids):
            raise ValueError('Provide 1..200 unique device IDs')
        for device_id in device_ids:
            validate('Id', device_id)
        end = time.monotonic() + timeout
        metadata = self.metadata + (('x-request-id', uuid4().hex),) + tuple(headers().items())
        result = []
        for device_id in device_ids:
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise ApiError(504, 'Registry context deadline exhausted')
            try:
                response = self.stub.GetDevice(self.pb.GetDeviceRequest(device_id=device_id),
                                               timeout=remaining, metadata=metadata)
            except self.grpc.RpcError as exc:
                status = {self.grpc.StatusCode.DEADLINE_EXCEEDED: 504,
                          self.grpc.StatusCode.NOT_FOUND: 404}.get(exc.code(), 503)
                raise ApiError(status, 'Registry context read failed') from exc
            device = self.json_format.MessageToDict(response, preserving_proto_field_name=True,
                                                   always_print_fields_with_no_presence=True)
            device['capability_version'] = int(device['capability_version'])
            validate('Device', device)
            if device['device_id'] != device_id:
                raise ValueError('Registry returned a different device ID')
            if (not device['capability_version'] or not 0 <= device['priority'] <= 100 or
                    not 0 <= device['minimum_rate_kw'] <= device['maximum_rate_kw'] or
                    device['controllable'] and device['device_type'] != 'EV_CHARGER' or
                    instant(device['updated_at']) > utc() + timedelta(seconds=5)):
                raise ValueError('Invalid Registry capability')
            result.append(device)
        return result

    def refresh(self, store, transformers, device_ids, timeout=2.0):
        """Publish a complete validated RPC batch and notifications in one SQL tx.

        Transformer baselines retain standalone immutability; device capability
        versions advance and heartbeats may only advance updated_at. No deletion
        is inferred from this explicit, potentially partial inventory scope.
        """
        for transformer in transformers:
            validate('Transformer', transformer)
            if transformer['load']['transformer_id'] != transformer['transformer_id']:
                raise ValueError('Context transformer/load mismatch')
        ids = [t['transformer_id'] for t in transformers]
        if len(ids) != len(set(ids)):
            raise ValueError('Duplicate transformer context')
        devices = self.devices(device_ids, timeout)
        with store.transaction() as db:
            changed = []
            known = {t['transformer_id'] for t in store._context(db, 'transformers')} | set(ids)
            if any(d['transformer_id'] not in known for d in devices):
                raise ValueError('Registry assignment is outside transformer context')
            for kind, values, field in [('transformers', transformers, 'transformer_id'),
                                         ('devices', devices, 'device_id')]:
                for value in values:
                    entity = value[field]
                    old = db.execute('SELECT value FROM projection_context WHERE kind=%s AND id=%s',
                                     (kind, entity)).fetchone()
                    if old and old[0] == value:
                        continue
                    if kind == 'transformers':
                        if old:
                            raise ApiError(409, 'Transformer baseline is immutable')
                        load = db.execute("SELECT event->'payload' FROM projection_current "
                                          "WHERE kind='Load' AND id=%s",
                                          (value['load']['window_id'],)).fetchone()
                        if load:
                            store._check_context_load(value['load'], load[0])
                    elif old:
                        previous = old[0]
                        if (value['capability_version'] < previous['capability_version'] or
                                instant(value['updated_at']) < instant(previous['updated_at'])):
                            raise ApiError(409, 'Registry context version or heartbeat regressed')
                        if value['capability_version'] == previous['capability_version'] and (
                                {k: v for k, v in value.items() if k != 'updated_at'} !=
                                {k: v for k, v in previous.items() if k != 'updated_at'}):
                            raise ApiError(409, 'Registry capability version conflict')
                    db.execute('INSERT INTO projection_context VALUES(%s,%s,%s) ON CONFLICT(kind,id) '
                               'DO UPDATE SET value=excluded.value', (kind, entity, Jsonb(value)))
                    changed.append((kind, entity))
            if changed:
                now = utc()
                views = store.views(db, now)
                for kind, entity in changed:
                    payload = next(v for v in views[kind]
                                   if (v['device'] if kind == 'devices' else v)[
                                       'device_id' if kind == 'devices' else 'transformer_id'] == entity)
                    store._emit(db, 'device.updated' if kind == 'devices' else 'transformer.updated',
                                entity, payload, now)
                store._emit(db, 'system.health', 'projection',
                            dict(status='degraded' if store.incomplete(db, views) else 'ok',
                                 observed_at=stamp(now)), now)
            return len(changed)


def main():
    parser = argparse.ArgumentParser(description='Member_4 explicit live Registry context refresh')
    parser.add_argument('--registry', default='127.0.0.1:15051')
    parser.add_argument('--context', required=True,
                        help='JSON containing transformers and device_ids (no fixture devices/events)')
    args = parser.parse_args()
    from member4.store import Store
    with open(args.context) as source:
        context = json.load(source)
    if set(context) != {'transformers', 'device_ids'}:
        raise ValueError('Expected only transformers and device_ids')
    client = RegistryContext(args.registry, os.environ['REGISTRY_API_TOKEN'])
    try:
        count = client.refresh(Store(os.environ['PROJECTION_DSN']), **context)
        print(f'Published {count} live context changes')
    finally:
        client.close()


if __name__ == '__main__':
    main()
