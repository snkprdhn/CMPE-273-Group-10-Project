"""Member_3 read-only Registry boundary; v1 supplies no charger observations."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import time
import threading

import grpc
from observability import traced, headers

import psycopg
from psycopg.types.json import Jsonb
from google.protobuf import json_format

from member3.domain import close, instant, stamp, validate

# Generated exclusively by make bindings; no Member_1 application imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / '.tools/generated'))
import registry_pb2 as pb
import registry_pb2_grpc as rpc


class Registry:
    def __init__(self, address, token, dsn):
        if not address or not token or not token.isascii() or not token.isprintable():
            raise ValueError('Registry address and printable ASCII action token required')
        self.dsn = dsn
        self.observation_db = None
        self.observation_lock = threading.Lock()
        with psycopg.connect(dsn, connect_timeout=2) as db:
            db.execute("SET LOCAL statement_timeout = '2s'")
            db.execute('CREATE TABLE IF NOT EXISTS action_registry_observations (id TEXT PRIMARY KEY, event JSONB NOT NULL)')
        self.channel = grpc.insecure_channel(address, options=[
            ('grpc.max_receive_message_length', 1048576), ('grpc.enable_retries', 0)])
        self.stub = rpc.DeviceRegistryStub(self.channel)
        self.metadata = (('x-service-identity', 'action'), ('authorization', 'Bearer ' + token))

    def close(self):
        self.channel.close()
        if self.observation_db is not None:
            self.observation_db.close()

    @traced('grpc.client')
    def snapshots(self, transformer_id, evaluated_at, policy_version):
        validate('Id', transformer_id)
        stamp(evaluated_at)
        if (isinstance(policy_version, bool) or not isinstance(policy_version, int)
                or not 1 <= policy_version < 2**64):
            raise ValueError('positive uint64 policy version required')
        request = pb.ListEligibleChargersRequest(transformer_id=transformer_id,
                                                 policy_version=policy_version, limit=50)
        request.evaluated_at.FromDatetime(evaluated_at)
        end = time.monotonic() + 2.0
        retries = 0
        snapshots, devices, cursors = [], set(), set()
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Registry deadline exhausted')
            try:
                page = self.stub.ListEligibleChargers(request, timeout=remaining, metadata=self.metadata + tuple(headers().items()))
            except grpc.RpcError as exc:
                if exc.code() != grpc.StatusCode.UNAVAILABLE or retries >= 2:
                    raise
                retries += 1
                time.sleep(min(.025 * 2**(retries - 1), max(0, end - time.monotonic())))
                continue
            if time.monotonic() >= end:
                raise TimeoutError('Registry deadline exhausted')
            if (len(page.devices) > request.limit or not page.HasField('observed_at')
                    or page.observed_at != request.evaluated_at):
                raise ValueError('invalid Registry page')
            for message in page.devices:
                device = json_format.MessageToDict(message, preserving_proto_field_name=True,
                                                    always_print_fields_with_no_presence=True)
                device['capability_version'] = int(device['capability_version'])
                validate('Device', device)
                if device['transformer_id'] != transformer_id or device['device_id'] in devices:
                    raise ValueError('Registry assignment or duplicate device')
                devices.add(device['device_id'])
                # Heartbeat freshness is not proof of online/charging/current rate.
                snapshots.append({'device': device})
            if not page.next_cursor:
                with psycopg.connect(self.dsn, connect_timeout=2) as db:
                    db.execute("SET LOCAL statement_timeout = '2s'")
                    observations = dict(db.execute(
                        'SELECT id,event FROM action_registry_observations WHERE id=ANY(%s)', (list(devices),)))
                for snapshot in snapshots:
                    d = snapshot['device']
                    event = observations.get(d['device_id'])
                    if event is None:
                        continue
                    p = event['payload']
                    rate = p.get('current_rate_kw')
                    if (p['transformer_id'] == transformer_id and p['quality'] == 'GOOD'
                            and rate is not None and close(rate, p['power_kw'])
                            and 0 <= (evaluated_at - instant(event['event_time'])).total_seconds() <= 15):
                        # In the simulated profile a fresh GOOD report proves
                        # reporting liveness; positive measured rate is charging.
                        snapshot['telemetry'] = dict(observed_at=event['event_time'],
                            current_rate_kw=rate, online=True, charging=rate > 0)
                return snapshots
            if not page.devices or page.next_cursor in cursors:
                raise ValueError('Registry pagination did not advance')
            cursors.add(page.next_cursor)
            request.cursor = page.next_cursor

    def observe(self, event, key, now=None):
        """Persist latest validated charger report before Kafka offset commit."""
        now = now or datetime.now(timezone.utc)
        stamp(now)
        validate('Telemetry', event)
        p = event['payload']
        if key != p['transformer_id']:
            raise ValueError('PARTITION_KEY')
        observed = instant(event['event_time'])
        if observed > now or observed > instant(event['ingested_at']) + timedelta(seconds=5):
            raise ValueError('FUTURE_TELEMETRY')
        if p['device_type'] != 'EV_CHARGER':
            return
        # Retain invalid/estimated/missing-rate latest reports to invalidate old
        # GOOD observations rather than silently carrying them forward.
        with self.observation_lock:
            if self.observation_db is None or self.observation_db.closed:
                self.observation_db = psycopg.connect(self.dsn, connect_timeout=2, autocommit=True)
                self.observation_db.execute('SET statement_timeout = 2000')
            self._observe(self.observation_db, event, observed)

    def _observe(self, db, event, observed):
        p = event['payload']
        # One atomic row-locked UPSERT replaces four round trips per charger.
        # Equal/older reports retain the original payload; conflicting equal
        # ranks still fail before the Kafka offset becomes eligible for commit.
        saved = db.execute(r"""INSERT INTO action_registry_observations AS prior VALUES (%s,%s)
            ON CONFLICT(id) DO UPDATE SET event=CASE WHEN
                (regexp_replace(excluded.event->>'event_time', '([.][0-9]{6})[0-9]+', '\1')::timestamptz,
                 (excluded.event->'payload'->>'sequence_number')::numeric) >
                (regexp_replace(prior.event->>'event_time', '([.][0-9]{6})[0-9]+', '\1')::timestamptz,
                 (prior.event->'payload'->>'sequence_number')::numeric)
                THEN excluded.event ELSE prior.event END
            RETURNING event""", (p['device_id'], Jsonb(event))).fetchone()[0]
        rank = observed, p['sequence_number']
        prior = instant(saved['event_time']), saved['payload']['sequence_number']
        if rank == prior and saved != event:
            raise ValueError('CONFLICTING_TELEMETRY')
