"""Simulated gateway: PostgreSQL effect and fencing commit atomically.

Own tables/role/schema; the only Control call is authorize_command(). SQLite
standalone state is deliberately not reused across a distributed transaction.
"""
from datetime import timedelta
from observability import boundary, headers as trace_headers

import json
import time
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from devices.common import event, instant, stamp, validate
from devices.simulator import EdgeState


class FencedEdgeState:
    # Reading generation is pure apart from current(); preserve telemetry semantics.
    reading = EdgeState.reading

    def __init__(self, dsn, capability, pending_limit=1000, db=None):
        validate('Device', capability)
        if capability['device_type'] != 'EV_CHARGER' or not capability['controllable']:
            raise ValueError('controllable EV required')
        if pending_limit < 2:
            raise ValueError('pending_limit must be at least 2')
        self.device, self.limit = capability, pending_limit
        self.boot_id, self.sequence = str(uuid4()), 0
        # A fleet shares one autocommit connection; a single device owns its own.
        self.db = db if db is not None else psycopg.connect(dsn, connect_timeout=2)
        with self.db.transaction():
            self.db.execute('''CREATE TABLE IF NOT EXISTS edge_state (
                device TEXT PRIMARY KEY, rate DOUBLE PRECISION NOT NULL,
                epoch BIGINT NOT NULL, effects BIGINT NOT NULL)''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS edge_commands (
                id TEXT PRIMARY KEY, payload JSONB NOT NULL, result JSONB NOT NULL)''')
            self.db.execute('''CREATE TABLE IF NOT EXISTS edge_outbox (
                sequence BIGINT GENERATED ALWAYS AS IDENTITY, id TEXT PRIMARY KEY,
                event JSONB NOT NULL, headers JSONB NOT NULL)''')
            self.db.execute('INSERT INTO edge_state VALUES (%s,%s,0,0) ON CONFLICT DO NOTHING',
                            (capability['device_id'], capability['maximum_rate_kw']))
        if db is None:
            self.db.autocommit = True

    def current(self):
        rate, epoch, effects = self.db.execute('SELECT rate,epoch,effects FROM edge_state WHERE device=%s',
                                               (self.device['device_id'],)).fetchone()
        return rate, str(epoch), effects

    def pending(self):
        return [(i, json.dumps(e), json.dumps(h)) for i, e, h in self.db.execute(
            'SELECT id,event,headers FROM edge_outbox ORDER BY sequence')]

    def acknowledge(self, identity):
        self.db.execute('DELETE FROM edge_outbox WHERE id=%s', (identity,))

    def apply(self, command, key, now, headers=None):
        started = time.monotonic()
        validate('Command', command, key)
        p, d = command['payload'], self.device
        if key != d['device_id']:
            raise ValueError('WRONG_SHARD')
        carried = [(k, v.decode() if isinstance(v, bytes) else v)
                   for k, v in trace_headers(headers).items() if k == 'traceparent' and v is not None]
        with boundary('db.gateway', event=command), self.db.transaction():
            self.db.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
            self.db.execute("SET LOCAL statement_timeout = '4s'")
            # Typed, least-privilege boundary. The shared xact lock acquired by
            # this function orders authorization + effect against epoch changes.
            allowed = self.db.execute('SELECT authorize_command(%s,%s,%s)',
                (p['command_id'], command['leader_epoch'], Jsonb(command))).fetchone()[0]
            self.db.execute('SELECT device FROM edge_state WHERE device=%s FOR UPDATE', (key,))
            if self.db.execute('SELECT count(*) FROM edge_outbox').fetchone()[0] + 2 > self.limit:
                raise BufferError('result outbox full; do not commit Kafka offset')
            # The supplied instant is arrival time, not time after DB lock waits.
            evaluated_at = now + timedelta(seconds=time.monotonic() - started)
            previous, epoch, effects = self.current()
            old = self.db.execute('SELECT payload,result FROM edge_commands WHERE id=%s', (p['command_id'],)).fetchone()
            outcome, reason, applied = 'REJECTED', 'CAPABILITY', previous
            if old:
                if old[0] != p:
                    reason = 'COMMAND_ID_CONFLICT'
                else:
                    saved = old[1]
                    outcome = 'ALREADY_APPLIED' if saved['outcome'] == 'APPLIED' else saved['outcome']
                    previous, applied = saved['previous_rate_kw'], saved['applied_rate_kw']
                    reason = saved.get('reason', 'DUPLICATE')
            elif not allowed or p['leader_epoch'] < int(epoch):
                reason = 'STALE_EPOCH'
            elif evaluated_at >= instant(p['expires_at']):
                outcome, reason = 'EXPIRED', 'EXPIRED'
            elif instant(p['issued_at']) > evaluated_at:
                reason = 'FUTURE_COMMAND'
            elif (p['transformer_id'] == d['transformer_id'] and
                  d['minimum_rate_kw'] <= p['target_rate_kw'] <= d['maximum_rate_kw']):
                outcome, reason, applied = 'APPLIED', 'APPLIED', p['target_rate_kw']
                self.db.execute('UPDATE edge_state SET rate=%s,epoch=%s,effects=effects+1 WHERE device=%s',
                                (applied, p['leader_epoch'], key))
            result = dict(command_id=p['command_id'], action_id=p['action_id'], device_id=key,
                          outcome=outcome, previous_rate_kw=previous, applied_rate_kw=applied,
                          result_time=stamp(evaluated_at), reason=reason)
            if not old:
                self.db.execute('INSERT INTO edge_commands VALUES (%s,%s,%s)',
                                (p['command_id'], Jsonb(p), Jsonb(result)))
            output = event('command.result', result, evaluated_at, str(uuid4()), command['correlation_id'], command['event_id'])
            validate('Result', output, key)
            events = [output]
            if outcome == 'APPLIED':
                events.append(self.reading(evaluated_at, command['correlation_id'], p['command_id']))
            for value in events:
                self.db.execute('INSERT INTO edge_outbox(id,event,headers) VALUES (%s,%s,%s)',
                                (value['event_id'], Jsonb(value), Jsonb(carried)))
            if outcome == 'APPLIED':
                # Authority can expire while this shared lock blocks renewal.
                # A failed final guard rolls back rate, dedup and outputs together.
                if not self.db.execute('SELECT authorize_command(%s,%s,%s)',
                        (p['command_id'], command['leader_epoch'], Jsonb(command))).fetchone()[0]:
                    raise PermissionError('fencing authority expired during effect transaction')
                if now + timedelta(seconds=time.monotonic() - started) >= instant(p['expires_at']):
                    raise TimeoutError('command expired during effect transaction')
        return output
