"""Member_1 Registry: PostgreSQL authoritative state, generated gRPC interface."""
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import wraps
import hmac
import json
import sys
import random
import time

import grpc
from google.protobuf import json_format
from observability import boundary, headers, traced

import psycopg
from psycopg.types.json import Jsonb

from member1.common import ROOT, instant, log, METRICS, stamp, utc, validate

# Only the canonical generator writes these files (make bindings).
sys.path.insert(0, str(ROOT / '.tools/generated'))
import registry_pb2 as pb
import registry_pb2_grpc as rpc


class Rejected(Exception):
    def __init__(self, code, reason):
        self.code, self.reason = code, reason


def reject(code, reason):
    raise Rejected(getattr(grpc.StatusCode, code), reason)


def device_dict(message):
    result = json_format.MessageToDict(message, preserving_proto_field_name=True,
                                     always_print_fields_with_no_presence=True)
    result['capability_version'] = int(result['capability_version'])
    validate('Device', result)
    return result


def device_message(value):
    return json_format.ParseDict(value, pb.Device())


def guarded(write=False):
    def decorate(method):
        @wraps(method)
        def call(self, request, context):
            with boundary('grpc.server', context.invocation_metadata()) as span:
                if span:
                    span.set_attribute('rpc.method', method.__name__)
                return invoke(self, request, context)
        def invoke(self, request, context):
            started = time.monotonic()
            try:
                self.authorize(context, write)
                remaining = context.time_remaining()
                if remaining is None or remaining <= 0:
                    reject('DEADLINE_EXCEEDED', 'explicit deadline required')
                budget = min(2.0, remaining)
                # Connection and statement/lock waits are bounded by the RPC budget.
                with boundary('db.registry'), psycopg.connect(self.dsn, connect_timeout=1,
                                     options=f'-c statement_timeout={max(1, int(budget * 1000))}') as db:
                    # Serialize small Registry operations, across replicas. This also
                    # makes op-ID conflict checks atomic with registration/heartbeat.
                    # ponytail: one DB lock; per-device locks if Registry load warrants it.
                    db.execute('SELECT pg_advisory_xact_lock(273001)')
                    if not context.is_active():
                        reject('DEADLINE_EXCEEDED', 'deadline exhausted')
                    result = method(self, request, db)
                    if not context.is_active() or time.monotonic() - started >= budget:
                        reject('DEADLINE_EXCEEDED', 'deadline exhausted')
                METRICS['registry_ok_total'] += 1
                metadata = dict(context.invocation_metadata())
                log('OK', rpc=method.__name__, operation_id=getattr(request, 'operation_id', None),
                    caller_identity=metadata.get('x-service-identity'),
                    traceparent=metadata.get('traceparent'),
                    remaining_deadline_ms=int(context.time_remaining() * 1000))
                return result
            except Rejected as exc:
                code, reason = exc.code, exc.reason
            except (ValueError, TypeError, KeyError, OverflowError):
                code, reason = grpc.StatusCode.INVALID_ARGUMENT, 'invalid request'
            except psycopg.errors.QueryCanceled:
                code, reason = grpc.StatusCode.DEADLINE_EXCEEDED, 'database deadline'
            except psycopg.Error:
                code, reason = grpc.StatusCode.UNAVAILABLE, 'registry store unavailable'
            except Exception as exc:
                # jsonschema validation errors do not expose input/backend details.
                from jsonschema.exceptions import ValidationError
                if not isinstance(exc, ValidationError):
                    raise
                code, reason = grpc.StatusCode.INVALID_ARGUMENT, 'invalid request'
            METRICS['registry_errors_total'] += 1
            log(code.name, rpc=method.__name__, error_category=reason)
            context.abort(code, reason)
        return call
    return decorate


class Registry(rpc.DeviceRegistryServicer):
    def __init__(self, dsn, tokens, cursor_key):
        if (len(cursor_key) < 16 or not tokens or any(not v for v in tokens.values())
                or len(set(tokens.values())) != len(tokens)):
            raise ValueError('nonempty identity tokens and >=16-byte cursor key required')
        self.dsn, self.tokens, self.cursor_key = dsn, tokens, cursor_key.encode()

    def migrate(self):
        with psycopg.connect(self.dsn, connect_timeout=2) as db:
            db.execute('SELECT pg_advisory_xact_lock(273001)')
            db.execute('''CREATE TABLE IF NOT EXISTS member1_devices (
                id TEXT PRIMARY KEY, registered JSONB NOT NULL,
                device JSONB NOT NULL, seen TIMESTAMPTZ NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS member1_operations (
                id TEXT PRIMARY KEY, request BYTEA NOT NULL, response BYTEA NOT NULL)''')

    def ready(self):
        with psycopg.connect(self.dsn, connect_timeout=1, options='-c statement_timeout=500') as db:
            return db.execute('SELECT 1 FROM member1_devices LIMIT 1').statusmessage is not None

    def authorize(self, context, write):
        metadata = dict(context.invocation_metadata())
        identity = metadata.get('x-service-identity', '')
        token = self.tokens.get(identity)
        if not token or not hmac.compare_digest(metadata.get('authorization', ''), 'Bearer ' + token):
            reject('UNAUTHENTICATED', 'service credentials required')
        if write and identity != 'simulator':
            reject('PERMISSION_DENIED', 'simulator identity required')

    def operation(self, db, request):
        validate('Id', request.operation_id)
        encoded = request.DESCRIPTOR.full_name.encode() + b'\0' + request.SerializeToString(deterministic=True)
        old = db.execute('SELECT request,response FROM member1_operations WHERE id=%s',
                         (request.operation_id,)).fetchone()
        if old and bytes(old[0]) != encoded:
            reject('ALREADY_EXISTS', 'operation ID conflict')
        return encoded, pb.Device.FromString(bytes(old[1])) if old else None

    def record(self, db, request, encoded, result):
        db.execute('INSERT INTO member1_operations VALUES (%s,%s,%s)',
                   (request.operation_id, encoded, result.SerializeToString()))
        return result

    @guarded(write=True)
    def RegisterDevice(self, request, db):
        d = device_dict(request.device)
        encoded, replay = self.operation(db, request)
        if replay is not None:
            return replay
        old = db.execute('SELECT registered,device FROM member1_devices WHERE id=%s', (d['device_id'],)).fetchone()
        if old:
            version = old[0]['capability_version']
            if d['capability_version'] < version:
                reject('FAILED_PRECONDITION', 'lower capability version')
            if d['capability_version'] == version:
                if d != old[0]:
                    reject('ALREADY_EXISTS', 'capability version conflict')
                return self.record(db, request, encoded, device_message(old[1]))
        db.execute('''INSERT INTO member1_devices VALUES (%s,%s,%s,%s)
            ON CONFLICT(id) DO UPDATE SET registered=excluded.registered,
            device=excluded.device, seen=excluded.seen''',
                   (d['device_id'], Jsonb(d), Jsonb(d), instant(d['updated_at'])))
        return self.record(db, request, encoded, device_message(d))

    @guarded(write=True)
    def Heartbeat(self, request, db):
        validate('Id', request.device_id)
        if not request.capability_version or not request.HasField('observed_at'):
            raise ValueError('heartbeat required fields')
        observed = request.observed_at.ToDatetime(tzinfo=utc().tzinfo)
        encoded, replay = self.operation(db, request)
        if replay is not None:
            return replay
        old = db.execute('SELECT device,seen FROM member1_devices WHERE id=%s', (request.device_id,)).fetchone()
        if not old:
            reject('NOT_FOUND', 'device absent')
        d, seen = old
        if request.capability_version != d['capability_version']:
            reject('FAILED_PRECONDITION', 'capability version mismatch')
        if observed > seen:
            d['updated_at'] = stamp(observed)
            db.execute('UPDATE member1_devices SET device=%s,seen=%s WHERE id=%s',
                       (Jsonb(d), observed, request.device_id))
        return self.record(db, request, encoded, device_message(d))

    @guarded()
    def GetDevice(self, request, db):
        validate('Id', request.device_id)
        row = db.execute('SELECT device FROM member1_devices WHERE id=%s', (request.device_id,)).fetchone()
        if not row:
            reject('NOT_FOUND', 'device absent')
        return device_message(row[0])

    def cursor(self, scope, last):
        raw = json.dumps([scope, last], sort_keys=True, separators=(',', ':')).encode()
        return base64.urlsafe_b64encode(hmac.digest(self.cursor_key, raw, 'sha256') + raw).decode()

    @guarded()
    def ListEligibleChargers(self, request, db):
        validate('Id', request.transformer_id)
        if not 1 <= request.limit <= 200 or not request.policy_version or not request.HasField('evaluated_at'):
            raise ValueError('pagination required fields')
        evaluated = request.evaluated_at.ToDatetime(tzinfo=utc().tzinfo)
        scope = [request.transformer_id, request.policy_version, request.limit, stamp(evaluated)]
        last = ''
        if request.cursor:
            try:
                token = base64.b64decode(request.cursor, altchars=b'-_', validate=True)
                signature, raw = token[:32], token[32:]
                if not hmac.compare_digest(signature, hmac.digest(self.cursor_key, raw, 'sha256')):
                    raise ValueError('cursor signature')
                previous, last = json.loads(raw)
                if previous != scope:
                    raise ValueError('cursor scope')
                validate('Id', last)
            except Exception as exc:
                raise ValueError('invalid cursor') from exc
        rows = db.execute('''SELECT device FROM member1_devices
            WHERE id COLLATE "C" > %s AND device->>'transformer_id'=%s
            AND device->>'device_type'='EV_CHARGER'
            AND (device->>'controllable')::boolean
            AND seen >= %s AND seen <= %s
            AND (NOT device ? 'departure_time' OR (device->>'departure_time')::timestamptz > %s)
            ORDER BY id COLLATE "C" LIMIT %s''',
            (last, request.transformer_id, evaluated - timedelta(seconds=15), evaluated,
             evaluated + timedelta(minutes=5), request.limit + 1)).fetchall()
        selected = rows[:request.limit]
        result = pb.ChargerPage(devices=[device_message(row[0]) for row in selected],
            next_cursor=self.cursor(scope, selected[-1][0]['device_id']) if len(rows) > request.limit else '')
        result.observed_at.CopyFrom(request.evaluated_at)
        return result


def serve(registry, address='127.0.0.1:50051'):
    server = grpc.server(ThreadPoolExecutor(max_workers=8), maximum_concurrent_rpcs=16,
                         options=[('grpc.max_receive_message_length', 65536)])
    rpc.add_DeviceRegistryServicer_to_server(registry, server)
    port = server.add_insecure_port(address)
    if not port:
        raise RuntimeError('Registry bind failed')
    server.start()
    return server, port


class Client:
    """One retry owner; three total attempts within the same 2s budget."""
    def __init__(self, address, identity, token):
        self.channel = grpc.insecure_channel(address, options=[('grpc.max_receive_message_length', 1048576)])
        self.stub = rpc.DeviceRegistryStub(self.channel)
        self.metadata = (('x-service-identity', identity), ('authorization', 'Bearer ' + token))

    @traced('grpc.client')
    def call(self, name, request, timeout=2.0):
        if not 0 < timeout <= 2:
            raise ValueError('deadline must be in (0,2]')
        if name == 'ListEligibleChargers' and request.limit == 0:
            copied = pb.ListEligibleChargersRequest()
            copied.CopyFrom(request)
            copied.limit = 50
            request = copied
        end = time.monotonic() + timeout
        for attempt in range(3):
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Registry deadline exhausted')
            try:
                return getattr(self.stub, name)(request, timeout=remaining, metadata=self.metadata + tuple(headers().items()))
            except grpc.RpcError as exc:
                if exc.code() != grpc.StatusCode.UNAVAILABLE or attempt == 2:
                    raise
                METRICS['registry_client_retries_total'] += 1
                time.sleep(min(random.uniform(0, 0.025 * 2**attempt), max(0, end - time.monotonic())))
