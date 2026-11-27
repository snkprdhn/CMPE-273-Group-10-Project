"""Transactional journal, materialized views and cross-replica durable SSE log."""
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from observability import boundary, headers

import os

import psycopg
from psycopg.types.json import Jsonb

from projection.contracts import ApiError, BUNDLE, TOPICS, instant, stamp, utc, validate


# A deployment whose windows close late (a ten second watermark plus transport) may widen these; defaults keep v1 behavior.
FRESH_SECONDS = float(os.getenv('PROJECTION_FRESH_SECONDS', '15'))
ALERT_GRACE_SECONDS = float(os.getenv('PROJECTION_ALERT_GRACE_SECONDS', '0'))


def freshness(values):
    for state in ('UNKNOWN', 'STALE', 'PARTIAL'):
        if not values or state in values:
            return state
    return 'FRESH'


class Store:
    def __init__(self, dsn, connection=None):
        self.dsn = dsn
        # The single-thread consumer may own one connection; HTTP stores do not.
        self.connection = connection

    @contextmanager
    def transaction(self):
        resource = nullcontext(self.connection) if self.connection is not None else psycopg.connect(self.dsn, connect_timeout=3)
        with boundary('db.projection'), resource as db:
            with db.transaction():
                # Serial writes retain the existing replay and notification ordering.
                db.execute('SET LOCAL statement_timeout = 3000')
                db.execute('SELECT pg_advisory_xact_lock(427304)')
                yield db

    def migrate(self):
        with self.transaction() as db:
            db.execute(Path(__file__).with_name('001_projection.sql').read_text())
            db.execute('INSERT INTO projection_meta(singleton,generation) VALUES(true,%s) ON CONFLICT DO NOTHING',
                       (uuid4().hex,))

    def context(self, fixtures):
        for name, schema in [('transformers', 'Transformer'), ('devices', 'Device')]:
            for value in fixtures.get(name, []):
                validate(schema, value)
        with self.transaction() as db:
            for name in ('transformers', 'devices'):
                field = 'transformer_id' if name == 'transformers' else 'device_id'
                for value in fixtures.get(name, []):
                    old = db.execute('SELECT value FROM projection_context WHERE kind=%s AND id=%s',
                                     (name, value[field])).fetchone()
                    if old and old[0] != value:
                        raise ApiError(409, 'Context is immutable; use a new rebuild store')
                    if name == 'transformers':
                        if value['load']['transformer_id'] != value[field]:
                            raise ValueError('context transformer/load mismatch')
                        load = db.execute(
                            "SELECT event->'payload' FROM projection_current WHERE kind='Load' AND id=%s",
                            (value['load']['window_id'],)).fetchone()
                        if load:
                            self._check_context_load(value['load'], load[0])
                    db.execute('INSERT INTO projection_context VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                               (name, value[field], Jsonb(value)))

    @staticmethod
    def _check_context_load(baseline, payload):
        if (baseline['window_id'], baseline['revision']) == (payload['window_id'], payload['revision']) and baseline != payload:
            raise ValueError('context load version conflicts with event')

    def cursor(self, db):
        generation, seq = db.execute('SELECT generation,sequence FROM projection_meta').fetchone()
        return f'{generation}:{seq}'

    def _emit(self, db, event_type, entity_id, payload, now):
        seq = db.execute('UPDATE projection_meta SET sequence=sequence+1 RETURNING sequence').fetchone()[0]
        event = dict(cursor=self.cursor(db), event_type=event_type, entity_id=entity_id,
                     version=seq, event_time=stamp(now), payload=payload)
        validate('SseEvent', event)
        db.execute('INSERT INTO projection_stream VALUES(%s,%s,%s)', (seq, now, Jsonb(event)))
        db.execute('INSERT INTO projection_trace VALUES(%s,%s)', (seq, Jsonb(headers())))
        self._prune(db, now)

    def _prune(self, db, now):
        db.execute('DELETE FROM projection_stream WHERE created_at <= %s OR sequence <= '
                   '(SELECT sequence-1000 FROM projection_meta)', (now - timedelta(minutes=5),))
        db.execute('DELETE FROM projection_pages WHERE created_at <= %s', (now - timedelta(minutes=5),))

    def _identity(self, name, event):
        p = event['payload']
        if name == 'Load':
            return p['window_id'], [instant(p['window_end']).timestamp(), p['revision']]
        if name == 'Action':
            if event['leader_epoch'] != p['leader_epoch']:
                raise ValueError('epoch mismatch')
            return p['action_id'], [event['saga_version']]
        if name == 'Alert':
            # No alert version in v1: owner event time orders transitions, ID breaks equal-time ties.
            return p['alert_id'], [instant(event['event_time']).timestamp(), p['load_revision'],
                                   {'OPEN': 0, 'ACKNOWLEDGED': 1, 'RESOLVED': 2}[p['status']], event['event_id']]
        if name == 'Telemetry':
            return p['device_id'], [instant(event['event_time']).timestamp(), p['sequence_number'],
                                    p['boot_id'], event['event_id']]
        if name == 'Command':
            if any(event[k] != p[k] for k in ('leader_epoch', 'saga_version')):
                raise ValueError('command envelope mismatch')
            return p['command_id'], [p['leader_epoch'], p['saga_version']]
        return event['event_id'], [instant(p['result_time']).timestamp(), event['event_id']]

    def ingest(self, topic, key, event, partition=None, offset=None, now=None):
        name = TOPICS[topic]
        validate(name, event)
        if key != event['payload'][BUNDLE['$defs'][name]['x-partition-key']]:
            raise ValueError('partition key mismatch')
        identity, ordering = self._identity(name, event)
        with self.transaction() as db:
            now = now or utc()
            old = db.execute('SELECT topic,event FROM projection_events WHERE event_id=%s',
                             (event['event_id'],)).fetchone()
            if old and old != (topic, event):
                raise ValueError('event identity reused with different contents')
            if instant(event['event_time']) > now + timedelta(seconds=5):
                raise ValueError('event time is in the future')
            if name == 'Load' and instant(event['payload']['window_end']) > now + timedelta(seconds=5):
                raise ValueError('load window is in the future')
            if name == 'Load':
                baseline = db.execute(
                    "SELECT value->'load' FROM projection_context WHERE kind='transformers' AND id=%s",
                    (event['payload']['transformer_id'],)).fetchone()
                if baseline:
                    self._check_context_load(baseline[0], event['payload'])
            changed = False
            if not old:
                db.execute('INSERT INTO projection_events VALUES(%s,%s,%s)',
                           (event['event_id'], topic, Jsonb(event)))
                prior = db.execute('SELECT ordering,event FROM projection_current WHERE kind=%s AND id=%s',
                                   (name, identity)).fetchone()
                if prior and prior[0] == ordering and prior[1]['payload'] != event['payload']:
                    raise ValueError('entity version reused with different payload')
                if not prior or ordering > prior[0]:
                    db.execute('INSERT INTO projection_current VALUES(%s,%s,%s,%s) ON CONFLICT(kind,id) '
                               'DO UPDATE SET ordering=excluded.ordering,event=excluded.event',
                               (name, identity, Jsonb(ordering), Jsonb(event)))
                    changed = True
                    self._updates(db, name, event, now)
            if partition is not None:
                if offset is None or partition < 0 or offset < 0:
                    raise ValueError('invalid source position')
                db.execute('INSERT INTO projection_offsets VALUES(%s,%s,%s) ON CONFLICT(topic,partition) '
                           'DO UPDATE SET offset_value=greatest(projection_offsets.offset_value,excluded.offset_value)',
                           (topic, partition, offset))
            return changed

    def _rows(self, db, kind):
        return [r[0] for r in db.execute('SELECT event FROM projection_current WHERE kind=%s ORDER BY id', (kind,))]

    def _context(self, db, kind):
        return [r[0] for r in db.execute('SELECT value FROM projection_context WHERE kind=%s ORDER BY id', (kind,))]

    def views(self, db, now):
        loads = [e['payload'] for e in self._rows(db, 'Load')]
        alerts = [e['payload'] for e in self._rows(db, 'Alert')]
        acknowledgements = {r[0] for r in db.execute("SELECT response->>'alert_id' FROM projection_operations "
                                                     "WHERE fingerprint->>0='acknowledge'")}
        for a in alerts:
            if a['alert_id'] in acknowledgements and a['status'] == 'OPEN':
                a['status'] = 'ACKNOWLEDGED'
        transformers = []
        policies = {p[0]: p[1] for p in db.execute(
            "SELECT id,value FROM projection_context WHERE kind='mock_policy'")}
        for base in self._context(db, 'transformers'):
            t = deepcopy(base)
            candidates = [base['load']] + [p for p in loads if p['transformer_id'] == t['transformer_id']]
            if candidates:
                t['load'] = max(candidates, key=lambda p: (instant(p['window_end']), p['revision']))
            if t['transformer_id'] in policies:
                t['policy'] = policies[t['transformer_id']]
            p = t['load']
            age = max(0, (now - instant(p['window_end'])).total_seconds())
            t['data_age_seconds'] = age
            t['freshness'] = 'STALE' if age > FRESH_SECONDS or p['completeness'] == 'STALE' else (
                'PARTIAL' if p['completeness'] == 'PARTIAL' else 'FRESH')
            matching = [a for a in alerts if a['transformer_id'] == t['transformer_id'] and
                        a['load_window_id'] == p['window_id'] and a['load_revision'] == p['revision'] and
                        a['status'] != 'RESOLVED']
            if matching:
                t['state'] = max(matching, key=lambda a: a['severity'] == 'CRITICAL')['severity']
            elif any(a['transformer_id'] == t['transformer_id'] and
                     a['load_window_id'] == p['window_id'] and a['load_revision'] == p['revision'] and
                     a['status'] == 'RESOLVED' and
                     a['message'] == 'Authoritative load classification: NORMAL' for a in alerts):
                t['state'] = 'NORMAL'
            elif p != base['load'] and (carried := self._carried(t['transformer_id'], p, loads, alerts)):
                # The classification of the previous window is still the newest authoritative evidence for a
                # moment: the alert for this window trails its load by a second or two.
                t['state'] = carried
            elif p != base['load']:
                # Classification belongs to Control; missing evidence is never NORMAL.
                t['state'] = 'STALE'
                if t['freshness'] == 'FRESH':
                    t['freshness'] = 'PARTIAL'
            if t['freshness'] == 'STALE':
                t['state'] = 'STALE'
            transformers.append(t)
        devices = []
        telemetry = {e['payload']['device_id']: e for e in self._rows(db, 'Telemetry')}
        results = self._rows(db, 'Result')
        for device in self._context(db, 'devices'):
            e = telemetry.get(device['device_id'])
            state = 'UNKNOWN' if not e else ('STALE' if (now-instant(e['event_time'])).total_seconds() > 15
                    or e['payload']['quality'] == 'INVALID' else (
                        'PARTIAL' if e['payload']['quality'] == 'ESTIMATED' else 'FRESH'))
            history = [r['payload'] for r in results if r['payload']['device_id'] == device['device_id']]
            history.sort(key=lambda r: (instant(r['result_time']), r['command_id'], r['outcome']))
            devices.append(dict(device=device, freshness=state,
                                latest_telemetry=e['payload'] if e else None, command_history=history[-200:]))
        return dict(transformers=transformers, devices=devices, alerts=alerts,
                    actions=[e['payload'] for e in self._rows(db, 'Action')], loads=loads)

    @staticmethod
    def _carried(transformer, latest, loads, alerts):
        """Severity of the newest classified window within the grace period, or None. Off unless configured."""
        if ALERT_GRACE_SECONDS <= 0:
            return None
        ends = {p['window_id']: instant(p['window_end']) for p in loads if p['transformer_id'] == transformer}
        floor = instant(latest['window_end']) - timedelta(seconds=ALERT_GRACE_SECONDS)
        best = None
        for a in alerts:
            end = ends.get(a['load_window_id'])
            if a['transformer_id'] == transformer and end is not None and end >= floor and (best is None or end > best[0]):
                best = end, a
        if best is None:
            return None
        a = best[1]
        if a['status'] != 'RESOLVED':
            return a['severity']
        return 'NORMAL' if a['message'] == 'Authoritative load classification: NORMAL' else None

    def incomplete(self, db, views):
        transformers = {t['transformer_id'] for t in views['transformers']}
        devices = {d['device']['device_id'] for d in views['devices']}
        if any(p['transformer_id'] not in transformers for kind in ('loads', 'alerts', 'actions') for p in views[kind]):
            return True
        return any(e['payload']['device_id'] not in devices or
                   ('transformer_id' in e['payload'] and e['payload']['transformer_id'] not in transformers)
                   for kind in ('Telemetry', 'Command', 'Result') for e in self._rows(db, kind))

    def device_view(self, db, device_id, now):
        """Build one device notification without scanning the complete fleet."""
        base = db.execute("SELECT value FROM projection_context WHERE kind='devices' AND id=%s", (device_id,)).fetchone()
        if not base:
            return None
        reading = db.execute("SELECT event FROM projection_current WHERE kind='Telemetry' AND id=%s", (device_id,)).fetchone()
        e = reading[0] if reading else None
        state = 'UNKNOWN' if not e else ('STALE' if (now-instant(e['event_time'])).total_seconds() > 15
                or e['payload']['quality'] == 'INVALID' else (
                    'PARTIAL' if e['payload']['quality'] == 'ESTIMATED' else 'FRESH'))
        results = [r[0]['payload'] for r in db.execute(
            "SELECT event FROM projection_current WHERE kind='Result' AND event->'payload'->>'device_id'=%s",
            (device_id,))]
        results.sort(key=lambda r: (instant(r['result_time']), r['command_id'], r['outcome']))
        return dict(device=base[0], freshness=state, latest_telemetry=e['payload'] if e else None,
                    command_history=results[-200:])

    def _updates(self, db, name, event, now):
        p = event['payload']
        if name in ('Telemetry', 'Result'):
            device = self.device_view(db, p['device_id'], now)
            if device is not None:
                self._emit(db, 'device.updated', p['device_id'], device, now)
            known = device is not None and db.execute(
                "SELECT 1 FROM projection_context WHERE kind='transformers' AND id=%s",
                (device['device']['transformer_id'],)).fetchone()
            if not known:
                self._emit(db, 'system.health', 'projection',
                           {'status': 'degraded', 'observed_at': stamp(now)}, now)
            return
        views = self.views(db, now)
        if name in ('Load', 'Alert'):
            for t in views['transformers']:
                if t['transformer_id'] == p['transformer_id']:
                    self._emit(db, 'transformer.updated', t['transformer_id'], t, now)
        if name == 'Alert':
            self._emit(db, 'alert.updated', p['alert_id'],
                       next(a for a in views['alerts'] if a['alert_id'] == p['alert_id']), now)
        elif name == 'Action':
            self._emit(db, 'action.updated', p['action_id'], p, now)
        elif name in ('Telemetry', 'Result'):
            for d in views['devices']:
                if d['device']['device_id'] == p['device_id']:
                    self._emit(db, 'device.updated', p['device_id'], d, now)
        if self.incomplete(db, views):
            self._emit(db, 'system.health', 'projection',
                       {'status': 'degraded', 'observed_at': stamp(now)}, now)

    def events(self, cursor, now=None):
        return [event for event, _ in self.events_with_context(cursor, now)]

    def events_with_context(self, cursor, now=None):
        with self.transaction() as db:
            now = now or utc()
            self._prune(db, now)
            current = self.cursor(db)
            rows = db.execute('SELECT sequence,event,context FROM projection_stream '
                              'LEFT JOIN projection_trace USING(sequence) ORDER BY sequence').fetchall()
            try:
                generation, seq = cursor.split(':')
                seq = int(seq)
                if generation != current.split(':')[0] or seq < 0 or seq > int(current.split(':')[1]):
                    raise ValueError()
                pending = [(n, event, context) for n, event, context in rows if n > seq]
                if len(pending) != int(current.split(':')[1])-seq or any(
                        n != seq+i for i, (n, _, _) in enumerate(pending, 1)):
                    raise ValueError()
            except (ValueError, AttributeError):
                reset = dict(cursor=current, event_type='snapshot.required', entity_id='projection',
                             version=int(current.split(':')[1]), event_time=stamp(now),
                             payload={'status': 'degraded', 'observed_at': stamp(now)})
                return [(reset, {})]
            return [(event, context or {}) for _, event, context in pending]

    def inventory(self):
        """Transformer and device IDs this projection manages. Everything else is out of scope."""
        with self.transaction() as db:
            transformers = {r[0] for r in db.execute("SELECT id FROM projection_context WHERE kind='transformers'")}
            devices = {r[0] for r in db.execute("SELECT id FROM projection_context WHERE kind='devices'")}
        return transformers, devices

    def advance(self, positions):
        """Record source offsets of events that were deliberately not stored (out of scope)."""
        if not positions:
            return
        with self.transaction() as db:
            for (topic, partition), offset in positions.items():
                db.execute('INSERT INTO projection_offsets VALUES(%s,%s,%s) ON CONFLICT(topic,partition) '
                           'DO UPDATE SET offset_value=greatest(projection_offsets.offset_value,excluded.offset_value)',
                           (topic, partition, offset))

    def prune(self, now=None, minutes=30, batch=20000, alert_minutes=None, command_minutes=None, action_minutes=None):
        """Rolling windows per kind. Loads and device telemetry arrive continuously; alerts are written for every
        classified window and commands for every action. Without limits the views, the journal and the volume grow
        with run time. Defaults keep loads `minutes`, alerts 60 minutes, commands and results 3 hours, actions 24 hours.
        The per-device telemetry row (one per device) is not removed."""
        now = now or utc()
        windows = {'Load': minutes, 'Telemetry': minutes, 'Alert': alert_minutes or max(minutes, 60),
                   'Command': command_minutes or 180, 'Result': command_minutes or 180, 'Action': action_minutes or 1440}
        removed = {'events': 0, 'loads': 0}
        with self.transaction() as db:
            for kind, keep in windows.items():
                cutoff = now - timedelta(minutes=keep)
                topics = [t for t, name in TOPICS.items() if name == kind]
                removed['events'] += db.execute(
                    'DELETE FROM projection_events WHERE event_id IN (SELECT event_id FROM projection_events '
                    "WHERE topic=ANY(%s) AND (event->>'event_time')::timestamptz < %s LIMIT %s)",
                    (topics, cutoff, batch)).rowcount
                if kind == 'Telemetry':
                    continue
                count = db.execute("DELETE FROM projection_current WHERE kind=%s AND (event->>'event_time')::timestamptz < %s",
                                   (kind, cutoff)).rowcount
                if kind == 'Load':
                    removed['loads'] = count
                elif count:
                    removed[kind.lower()] = count
        return removed

    def rebuild(self):
        with self.transaction() as db:
            self._rebuild(db)

    def _rebuild(self, db):
        # Existing consumers must reload snapshots after the generation rotates.
        rows = db.execute('SELECT topic,event FROM projection_events ORDER BY event_id').fetchall()
        db.execute('DELETE FROM projection_current')
        db.execute('DELETE FROM projection_stream')
        db.execute('DELETE FROM projection_pages')
        db.execute('UPDATE projection_meta SET generation=%s,sequence=0', (uuid4().hex,))
        for topic, event in rows:
            name = TOPICS[topic]
            identity, ordering = self._identity(name, event)
            prior = db.execute('SELECT ordering FROM projection_current WHERE kind=%s AND id=%s',
                               (name, identity)).fetchone()
            if not prior or ordering > prior[0]:
                db.execute('INSERT INTO projection_current VALUES(%s,%s,%s,%s) ON CONFLICT(kind,id) '
                           'DO UPDATE SET ordering=excluded.ordering,event=excluded.event',
                           (name, identity, Jsonb(ordering), Jsonb(event)))
