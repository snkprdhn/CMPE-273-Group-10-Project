"""Seeded waveforms and durable simulated charger effects/inbox/outbox."""
from datetime import timedelta
import json
import math
import random
import sqlite3
from uuid import uuid4

from member1.common import event, instant, log, METRICS, stamp, validate


def devices(now):
    return [dict(device_id=f'{prefix}-001', transformer_id='tx-001', device_type=kind,
                 controllable=kind == 'EV_CHARGER', minimum_rate_kw=1 if prefix == 'ev' else 0,
                 maximum_rate_kw=7.2 if prefix == 'ev' else 100, priority=1,
                 capability_version=1, updated_at=stamp(now))
            for prefix, kind in [('ev', 'EV_CHARGER'), ('meter', 'SMART_METER'), ('solar', 'SOLAR')]]


def topology(now):
    return {'topology_version': 'member1-v1', 'transformers': [
        {'transformer_id': 'tx-001', 'capacity_kw': 20, 'devices': [
            {k: d[k] for k in ('device_id', 'device_type')} for d in devices(now)]}]}


def scenario(seed, start, steps=30, run_id='scenario-001', boot_id='fixture-boot', interval=1):
    """One-second default samples: ten normal, ten overload, ten recovery by default.

    A fixed boot is for fixture replay only. Live processes supply a new boot UUID.
    """
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError('interval must be positive')
    if steps < 3:
        raise ValueError('steps must be >= 3')
    rng = random.Random(seed)
    for tick in range(steps):
        phase = min(2, tick * 3 // steps)
        for d in devices(start):
            kind = d['device_type']
            kw = ({'SMART_METER': (8, 18, 8)[phase], 'SOLAR': -2,
                   'EV_CHARGER': 7.2 if phase == 1 else 4}[kind])
            if kind != 'EV_CHARGER':
                kw = round(kw + rng.uniform(-0.1, 0.1), 6)
            now = start + timedelta(seconds=interval * tick)
            p = dict(device_id=d['device_id'], transformer_id=d['transformer_id'],
                     device_type=kind, boot_id=boot_id, power_kw=kw,
                     quality='GOOD', sequence_number=tick)
            if kind == 'EV_CHARGER':
                p.update(current_rate_kw=kw, minimum_rate_kw=1, priority=1)
            e = event('telemetry.reading', p, now,
                      f'{run_id}/{seed}/{stamp(start)}/{steps}/{float(interval)}/{boot_id}/{tick}/{d["device_id"]}', run_id)
            validate('Telemetry', e, 'tx-001')
            yield e


class EdgeState:
    """One SQLite file per shard, on a retained volume. Transactions own effects.

    This is a simulated rate, not an atomic transaction with physical hardware.
    No pruning: command IDs remain valid for the lifetime of this state volume.
    """
    def __init__(self, path, capability, pending_limit=1000):
        validate('Device', capability)
        if capability['device_type'] != 'EV_CHARGER' or not capability['controllable']:
            raise ValueError('controllable EV required')
        if pending_limit < 2:
            raise ValueError('pending_limit must be at least 2')
        self.device = capability
        self.limit = pending_limit
        self.boot_id, self.sequence = str(uuid4()), 0
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS state (device TEXT PRIMARY KEY, rate REAL, epoch TEXT, effects INTEGER);
            CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, payload TEXT NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, event TEXT NOT NULL, headers TEXT NOT NULL);
        ''')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO state VALUES (?, ?, ?, 0)',
                            (capability['device_id'], capability['maximum_rate_kw'], '0'))

    def current(self):
        return self.db.execute('SELECT rate, epoch, effects FROM state WHERE device=?',
                               (self.device['device_id'],)).fetchone()

    def pending(self):
        return self.db.execute('SELECT id,event,headers FROM outbox ORDER BY rowid').fetchall()

    def acknowledge(self, identity):
        with self.db:
            self.db.execute('DELETE FROM outbox WHERE id=?', (identity,))

    def apply(self, command, key, now, headers=None):
        validate('Command', command, key)
        p, d = command['payload'], self.device
        if key != d['device_id']:
            raise ValueError('WRONG_SHARD')
        encoded = json.dumps(p, sort_keys=True, allow_nan=False)
        carried = json.dumps([(k, v.decode() if isinstance(v, bytes) else v)
                              for k, v in (headers or []) if k == 'traceparent' and v is not None])
        with self.db:
            # Serialize concurrent processes too, before any dedup/fence read.
            self.db.execute('BEGIN IMMEDIATE')
            if self.db.execute('SELECT count(*) FROM outbox').fetchone()[0] + 2 > self.limit:
                raise BufferError('result outbox full; do not commit Kafka offset')
            previous, epoch, effects = self.current()
            old = self.db.execute('SELECT payload,result FROM commands WHERE id=?', (p['command_id'],)).fetchone()
            outcome, reason, applied = 'REJECTED', 'CAPABILITY', previous
            if old:
                if old[0] != encoded:
                    reason = 'COMMAND_ID_CONFLICT'
                else:
                    saved = json.loads(old[1])
                    outcome = 'ALREADY_APPLIED' if saved['outcome'] == 'APPLIED' else saved['outcome']
                    previous, applied = saved['previous_rate_kw'], saved['applied_rate_kw']
                    reason = saved.get('reason', 'DUPLICATE')
            elif now >= instant(p['expires_at']):
                outcome, reason = 'EXPIRED', 'EXPIRED'
            elif p['leader_epoch'] < int(epoch):
                reason = 'STALE_EPOCH'
            elif instant(p['issued_at']) > now:
                reason = 'FUTURE_COMMAND'
            elif (p['transformer_id'] == d['transformer_id'] and
                  d['minimum_rate_kw'] <= p['target_rate_kw'] <= d['maximum_rate_kw']):
                outcome, reason, applied = 'APPLIED', 'APPLIED', p['target_rate_kw']
                self.db.execute('UPDATE state SET rate=?,epoch=?,effects=? WHERE device=?',
                                (applied, str(p['leader_epoch']), effects + 1, key))
            result = dict(command_id=p['command_id'], action_id=p['action_id'], device_id=key,
                          outcome=outcome, previous_rate_kw=previous, applied_rate_kw=applied,
                          result_time=stamp(now), reason=reason)
            if not old:
                self.db.execute('INSERT INTO commands VALUES (?,?,?)',
                                (p['command_id'], encoded, json.dumps(result)))
            result_event = event('command.result', result, now, str(uuid4()),
                                 command['correlation_id'], command['event_id'])
            validate('Result', result_event, key)
            self.db.execute('INSERT INTO outbox VALUES (?,?,?)',
                            (result_event['event_id'], json.dumps(result_event), carried))
            if outcome == 'APPLIED':
                reading = self.reading(now, command['correlation_id'], p['command_id'])
                self.db.execute('INSERT INTO outbox VALUES (?,?,?)',
                                (reading['event_id'], json.dumps(reading), carried))
        METRICS['commands_' + outcome.lower() + '_total'] += 1
        log(outcome, command_id=p['command_id'], event_id=command['event_id'],
            correlation_id=command['correlation_id'])
        return result_event

    def reading(self, now, correlation, cause=None):
        d = self.device
        self.sequence += 1
        rate = self.current()[0]
        payload = dict(device_id=d['device_id'], transformer_id=d['transformer_id'],
                       device_type='EV_CHARGER', boot_id=self.boot_id, power_kw=rate,
                       quality='GOOD', sequence_number=self.sequence, current_rate_kw=rate,
                       minimum_rate_kw=d['minimum_rate_kw'], priority=d['priority'])
        return event('telemetry.reading', payload, now,
                     f"{self.boot_id}/{self.sequence}", correlation, cause)


def command_fixture(now, run_id='fixture-001', case='throttle'):
    """A Member_3 boundary test client, not Action/control policy logic."""
    if case not in ('throttle', 'restore', 'expired', 'stale'):
        raise ValueError('unknown command case')
    issued = now - timedelta(seconds=10) if case == 'expired' else now
    payload = dict(command_id=f'{run_id}-{case}', action_id=f'{run_id}-action',
                   device_id='ev-001', transformer_id='tx-001',
                   command_type='RESTORE' if case == 'restore' else 'THROTTLE',
                   target_rate_kw=7.2 if case == 'restore' else 3,
                   issued_at=stamp(issued), expires_at=stamp(issued + timedelta(seconds=10)),
                   reason='Member_1 standalone fixture', policy_version=1, saga_version=1,
                   leader_epoch=41 if case == 'stale' else 42)
    result = event('charger.command', payload, now, f'{run_id}/{case}', run_id)
    result.update(leader_epoch=payload['leader_epoch'], saga_version=1)
    validate('Command', result, 'ev-001')
    return result
