"""PostgreSQL-backed Action saga. No broker, simulator, Flink or model dependency."""
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from member3.domain import (allocate, candidates, close, default_policy, fresh,
                            identity, instant, stamp, validate)


class Conflict(ValueError):
    """Optimistic version or logical identity conflict; transaction is rolled back."""


class Control:
    def __init__(self, dsn, registry, leader_epoch=1, leadership=None):
        if isinstance(leader_epoch, bool) or not isinstance(leader_epoch, int) or leader_epoch < 1:
            raise ValueError('positive leader epoch required')
        self.dsn, self.registry, self.epoch = dsn, registry, leader_epoch
        self.leadership = leadership

    @contextmanager
    def transaction(self, migration=False):
        with psycopg.connect(self.dsn, connect_timeout=2) as db:
            db.execute("SET LOCAL statement_timeout = '5s'")
            db.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
            db.execute("SET LOCAL lock_timeout = '4s'")
            # ponytail: serialize the small standalone control store; shard this
            # lock by transformer when measured throughput requires it.
            db.execute('SELECT pg_advisory_xact_lock(273003)')
            if not migration:
                if self.leadership:
                    if self.epoch != self.leadership.epoch:
                        raise Conflict('STALE_EPOCH')
                    self.leadership.check(db)
                elif db.execute('SELECT EXISTS(SELECT 1 FROM action_authority)').fetchone()[0]:
                    raise Conflict('LEADERSHIP_REQUIRED')
            yield db
            if not migration and self.leadership:
                self.leadership.check(db)

    def migrate(self):
        with self.transaction(migration=True) as db:
            # SECURITY DEFINER functions must search the owned schema before temp.
            db.execute("SELECT set_config('search_path', current_setting('search_path') || ',pg_temp', true)")
            db.execute((Path(__file__).with_name('state.sql')).read_text())

    def transformer(self, db, tx):
        validate('Id', tx)
        state = dict(policy=default_policy(), latest=None, recovery=0, recovery_end=None,
                     last_plan=None, health='UNKNOWN', epoch=self.epoch)
        db.execute('INSERT INTO action_transformers VALUES (%s,0,%s) ON CONFLICT DO NOTHING', (tx, Jsonb(state)))
        version, state = db.execute('SELECT version,state FROM action_transformers WHERE id=%s FOR UPDATE', (tx,)).fetchone()
        if self.epoch < state['epoch']:
            raise Conflict('STALE_EPOCH')
        state['epoch'] = self.epoch
        return version, state

    def save_transformer(self, db, tx, version, state):
        if db.execute('UPDATE action_transformers SET version=version+1,state=%s WHERE id=%s AND version=%s',
                      (Jsonb(state), tx, version)).rowcount != 1:
            raise Conflict('TRANSFORMER_VERSION')

    def policy(self, tx, policy, expected_version):
        validate('Policy', policy)
        with self.transaction() as db:
            version, state = self.transformer(db, tx)
            if state['policy']['version'] != expected_version or policy['version'] != expected_version + 1:
                raise Conflict('POLICY_VERSION')
            state['policy'] = policy
            state['recovery'], state['recovery_end'] = 0, None
            self.save_transformer(db, tx, version, state)

    def inbox(self, db, consumer, event):
        old = db.execute('SELECT event FROM action_inbox WHERE consumer=%s AND id=%s',
                         (consumer, event['event_id'])).fetchone()
        if old:
            if old[0] != event:
                raise Conflict('EVENT_ID_REUSED')
            return False
        db.execute('INSERT INTO action_inbox VALUES (%s,%s,%s)', (consumer, event['event_id'], Jsonb(event)))
        return True

    def outbox(self, db, event, schema, key, delivery=None):
        validate(schema, event)
        db.execute('INSERT INTO action_outbox(id,aggregatetype,aggregateid,type,payload) VALUES (%s,%s,%s,%s,%s)',
                   (delivery or event['event_id'], 'grid.action.v1' if schema == 'Action' else 'grid.charger-command.v1',
                    key, event['event_type'], Jsonb(event)))

    def envelope(self, kind, payload, now, eid, action, cause):
        return dict(event_id=eid, event_type=kind, schema_version=1, source='member3.action',
                    event_time=stamp(now), ingested_at=stamp(now), correlation_id=action['id'],
                    causation_id=cause, leader_epoch=self.epoch, saga_version=action['version'], payload=payload)

    def save_action(self, db, a):
        a['epoch'] = self.epoch
        if db.execute('UPDATE action_plans SET version=%s,state=%s,active=%s WHERE id=%s AND version=%s',
                   (a['version'], Jsonb(a), a['status'] != 'COMPLETED', a['id'], a['version'] - 1)).rowcount != 1:
            raise Conflict('SAGA_VERSION')

    def transition(self, db, a, status, now, reason, cause=None):
        a.update(status=status, version=a['version'] + 1, reason=reason, transition_time=stamp(now))
        payload = dict(action_id=a['id'], transformer_id=a['tx'], status=status,
                       trigger_window_id=a['window_id'], trigger_revision=a['revision'],
                       policy_version=a['policy']['version'], leader_epoch=self.epoch,
                       target_reduction_kw=a['target'], expected_reduction_kw=a['expected'],
                       confirmed_reduction_kw=a['confirmed'], selected_device_count=len(a['allocations']),
                       acknowledged_device_count=a['acknowledged'], reason=reason, transition_time=stamp(now))
        event = self.envelope('action.transition', payload, now,
                              identity(f"action-event/{a['id']}/{a['version']}"), a, cause or a['cause'])
        self.save_action(db, a)
        self.outbox(db, event, 'Action', a['tx'])

    def commands(self, db, aid, kind):
        return [r[0] for r in db.execute('SELECT state FROM action_commands WHERE action_id=%s AND command_type=%s ORDER BY device_id', (aid, kind))]

    def save_command(self, db, c):
        db.execute('UPDATE action_commands SET state=%s WHERE id=%s', (Jsonb(c), c['id']))

    def issue(self, db, a, kind, now):
        for allocation in a['allocations']:
            cid = identity(f"command/{a['id']}/{allocation['device_id']}/{kind}")
            payload = dict(command_id=cid, action_id=a['id'], device_id=allocation['device_id'],
                           transformer_id=a['tx'], command_type=kind,
                           target_rate_kw=allocation['target_rate_kw'] if kind == 'THROTTLE' else allocation['original_rate_kw'],
                           issued_at=stamp(now), expires_at=stamp(now + timedelta(seconds=a['policy']['command_ttl_seconds'])),
                           reason='critical load' if kind == 'THROTTLE' else 'fresh recovery and verified headroom',
                           policy_version=a['policy']['version'], saga_version=a['version'], leader_epoch=self.epoch)
            event = self.envelope('charger.command', payload, now, identity(f'command-event/{cid}'), a, a['cause'])
            c = dict(id=cid, event=event, status='PENDING', attempts=1,
                     deadline=stamp(now + timedelta(seconds=a['policy']['ack_timeout_seconds'])), result=None)
            db.execute('INSERT INTO action_commands VALUES (%s,%s,%s,%s,%s)',
                       (cid, a['id'], allocation['device_id'], kind, Jsonb(c)))
            self.outbox(db, event, 'Command', allocation['device_id'])

    def active(self, db, tx):
        row = db.execute('SELECT state FROM action_plans WHERE transformer_id=%s AND active', (tx,)).fetchone()
        return row[0] if row else None

    def load(self, event, key, now):
        stamp(now)
        validate('Load', event)
        p = event['payload']
        if key != p['transformer_id']:
            raise ValueError('PARTITION_KEY')
        with self.transaction() as db:
            version, state = self.transformer(db, key)
            if not self.inbox(db, 'load', event):
                return 'DUPLICATE'
            old = db.execute('SELECT revision FROM action_windows WHERE transformer_id=%s AND window_id=%s',
                             (key, p['window_id'])).fetchone()
            if old and old[0] >= p['revision']:
                return 'OLD_REVISION'
            db.execute('INSERT INTO action_windows VALUES (%s,%s,%s) ON CONFLICT(transformer_id,window_id) DO UPDATE SET revision=excluded.revision',
                       (key, p['window_id'], p['revision']))
            previous = state['latest']
            if previous and instant(p['window_end']) < instant(previous['window_end']):
                state['recovery'], state['recovery_end'] = 0, None
                self.save_transformer(db, key, version, state)
                return 'HISTORICAL'
            state['latest'] = p
            valid = fresh(p, now)
            policy = state['policy']
            state['health'] = ('SAFE_HOLD' if not valid else 'CRITICAL' if p['utilization_pct'] >= policy['critical_pct']
                               else 'WARNING' if p['utilization_pct'] >= policy['warning_pct'] else 'NORMAL')
            a = self.active(db, key)
            if a:
                self.recovery(db, a, state, previous, now, event['event_id'])
            elif valid and p['utilization_pct'] >= policy['critical_pct']:
                exists = db.execute('SELECT 1 FROM action_plans WHERE transformer_id=%s AND window_id=%s', (key, p['window_id'])).fetchone()
                cooled = not state['last_plan'] or now >= instant(state['last_plan']) + timedelta(seconds=policy['cooldown_seconds'])
                if not exists and cooled:
                    self.plan(db, state, event, now)
            self.save_transformer(db, key, version, state)
            return state['health']

    def plan(self, db, state, event, now):
        p, policy = event['payload'], state['policy']
        # Fake boundary returns complete snapshots or raises. Failure rolls back
        # inbox and state, so the exact input can be retried after recovery.
        eligible = candidates(self.registry.snapshots(p['transformer_id'], now, policy['version']), p['transformer_id'], now)
        target = max(0, p['net_load_kw'] - p['capacity_kw'] * (policy['recovery_pct'] / 100))
        allocations = allocate(eligible, target)
        expected = sum(x['original_rate_kw'] - x['target_rate_kw'] for x in allocations)
        aid = identity(f"action/{p['transformer_id']}/{p['window_id']}/{policy['version']}")
        a = dict(id=aid, tx=p['transformer_id'], window_id=p['window_id'], revision=p['revision'],
                 policy=policy, epoch=self.epoch, version=0, status='PLAN_CREATED', allocations=allocations,
                 candidates=eligible, target=target, expected=expected, confirmed=0, acknowledged=0,
                 cause=event['event_id'], created_at=stamp(now), confirmed_at=None)
        db.execute('INSERT INTO action_plans VALUES (%s,%s,%s,true,0,%s)', (aid, a['tx'], a['window_id'], Jsonb(a)))
        self.transition(db, a, 'PLAN_CREATED', now, 'critical load')
        state['last_plan'], state['recovery'], state['recovery_end'] = stamp(now), 0, None
        if not allocations or expected + 1e-9 < target:
            self.transition(db, a, 'NEEDS_OPERATOR_REVIEW', now, 'insufficient flexible capacity; safe hold')
            state['health'] = 'SAFE_HOLD'
            return
        self.transition(db, a, 'COMMANDS_DISPATCHED', now, 'commands committed to outbox; delivery unconfirmed')
        self.issue(db, a, 'THROTTLE', now)
        self.transition(db, a, 'ACKS_PENDING', now, 'awaiting device results')

    def recovery(self, db, a, state, previous, now, cause):
        if a['status'] == 'NEEDS_OPERATOR_REVIEW':
            state['health'], state['recovery'], state['recovery_end'] = 'SAFE_HOLD', 0, None
            return
        p, policy = state['latest'], a['policy']
        safe = (fresh(p, now) and a['confirmed_at'] is not None
                and instant(p['window_start']) >= instant(a['confirmed_at'])
                and p['utilization_pct'] <= policy['recovery_pct'])
        new_window = previous is None or p['window_id'] != previous['window_id']
        if not safe:
            state['recovery'], state['recovery_end'] = 0, None
        elif new_window and a['status'] == 'RECOVERY_MONITORING':
            consecutive = state['recovery_end'] == p['window_start']
            state['recovery'] = state['recovery'] + 1 if consecutive else 1
            state['recovery_end'] = p['window_end']
        elif not new_window:
            # Corrections are history; cannot act as a second recovery window.
            return
        if a['status'] != 'RECOVERY_MONITORING' or state['recovery'] < policy['recovery_windows']:
            return
        # Restoring the full previous rates must not re-enter CRITICAL.
        if p['net_load_kw'] + a['confirmed'] >= p['capacity_kw'] * (policy['critical_pct'] / 100):
            state['health'] = 'SAFE_HOLD'
            return
        current = {d['device_id']: d for d in candidates(
            self.registry.snapshots(a['tx'], now, policy['version']), a['tx'], now, restoring=True)}
        for allocation in a['allocations']:
            d = current.get(allocation['device_id'])
            if (not d or d['capability_version'] != allocation['capability_version']
                    or not close(d['current_rate_kw'], allocation['target_rate_kw'])
                    or allocation['original_rate_kw'] > d['maximum_rate_kw']):
                state['health'] = 'SAFE_HOLD'
                return
        self.transition(db, a, 'RESTORE_DISPATCHED', now, 'two fresh recovery windows; headroom and rates verified', cause)
        self.issue(db, a, 'RESTORE', now)
        state['recovery'], state['recovery_end'] = 0, None

    def result(self, event, key, now, expected_version=None):
        stamp(now)
        validate('Result', event)
        p = event['payload']
        if key != p['device_id']:
            raise ValueError('PARTITION_KEY')
        if instant(p['result_time']) > now or instant(event['event_time']) > now:
            raise ValueError('FUTURE_RESULT')
        with self.transaction() as db:
            row = db.execute('SELECT state FROM action_commands WHERE id=%s', (p['command_id'],)).fetchone()
            if not row:
                raise ValueError('UNKNOWN_COMMAND')
            c = row[0]
            cp = c['event']['payload']
            if p['action_id'] != cp['action_id'] or p['device_id'] != cp['device_id']:
                raise ValueError('RESULT_IDENTITY')
            if instant(p['result_time']) < instant(cp['issued_at']):
                raise ValueError('RESULT_BEFORE_COMMAND')
            a = db.execute('SELECT state FROM action_plans WHERE id=%s', (p['action_id'],)).fetchone()[0]
            version, state = self.transformer(db, a['tx'])
            if not self.inbox(db, 'result', event):
                return 'DUPLICATE'
            if expected_version is not None and a['version'] != expected_version:
                raise Conflict('SAGA_VERSION')
            if c['result']:
                # Logical result replay with a different event ID cannot overwrite
                # an authoritative outcome or resurrect a reviewed plan.
                old = c['result']
                if (p['outcome'] in ('APPLIED', 'ALREADY_APPLIED') and old['outcome'] in ('APPLIED', 'ALREADY_APPLIED')
                        and close(p['applied_rate_kw'], old['applied_rate_kw'])) or p == old:
                    return 'KNOWN_RESULT'
                raise Conflict('CONFLICTING_RESULT')
            kind = cp['command_type']
            phase_commands = self.commands(db, a['id'], kind)
            # The process may have been down across the final deadline. Apply
            # the same deadline rule as tick before allowing late results to
            # advance the saga. Physical effects remain recorded for review.
            exhausted = [cmd for cmd in phase_commands if self.exhausted(cmd, now)]
            if a['status'] in ('ACKS_PENDING', 'RESTORE_DISPATCHED') and exhausted:
                for cmd in exhausted:
                    cmd['status'] = 'TIMED_OUT'
                    self.save_command(db, cmd)
                status = 'PARTIALLY_APPLIED' if any(cmd['status'] == 'APPLIED' for cmd in phase_commands) else 'TIMED_OUT'
                self.review(db, a, now, status, 'deadline elapsed during downtime; outcome requires reconciliation', event['event_id'])
                state['health'] = 'SAFE_HOLD'
            positive = p['outcome'] in ('APPLIED', 'ALREADY_APPLIED') and close(p['applied_rate_kw'], cp['target_rate_kw'])
            c['result'], c['status'] = p, 'APPLIED' if positive else 'FAILED'
            self.save_command(db, c)
            commands = self.commands(db, a['id'], kind)
            if kind == 'THROTTLE':
                a['acknowledged'] = sum(cmd['status'] == 'APPLIED' for cmd in commands)
                a['confirmed'] = sum(max(0, allocation['original_rate_kw'] - cmd['result']['applied_rate_kw'])
                    for allocation in a['allocations'] for cmd in commands
                    if cmd['event']['payload']['device_id'] == allocation['device_id'] and cmd['status'] == 'APPLIED')
            if a['status'] == 'NEEDS_OPERATOR_REVIEW':
                state['health'] = 'SAFE_HOLD'
                self.transition(db, a, 'NEEDS_OPERATOR_REVIEW', now, 'late result recorded; operator reconciliation required', event['event_id'])
            elif any(cmd['status'] == 'PENDING' for cmd in commands):
                self.transition(db, a, a['status'], now, 'command result recorded; other acknowledgements pending', event['event_id'])
            elif all(cmd['status'] == 'APPLIED' for cmd in commands):
                if kind == 'RESTORE':
                    self.transition(db, a, 'COMPLETED', now, 'all restores confirmed', event['event_id'])
                else:
                    a['confirmed_at'] = stamp(now)
                    self.transition(db, a, 'REDUCTION_CONFIRMED', now, 'all reductions confirmed', event['event_id'])
                    self.transition(db, a, 'RECOVERY_MONITORING', now, 'waiting for fresh post-effect recovery windows', event['event_id'])
                    state['recovery'], state['recovery_end'] = 0, None
            else:
                self.review(db, a, now, 'PARTIALLY_APPLIED', 'one or more commands failed', event['event_id'])
                state['health'] = 'SAFE_HOLD'
            self.save_transformer(db, a['tx'], version, state)
            return a['status']

    def review(self, db, a, now, status, reason, cause=None):
        self.transition(db, a, status, now, reason, cause)
        self.transition(db, a, 'NEEDS_OPERATOR_REVIEW', now, 'safe hold; reconciliation/operator review required', cause)

    def exhausted(self, command, now):
        return (command['status'] == 'PENDING' and now >= instant(command['deadline'])
                and (command['attempts'] >= 2
                     or now >= instant(command['event']['payload']['expires_at'])
                     or command['event']['leader_epoch'] != self.epoch))

    def tick(self, now):
        """Restart-safe explicit clock: at most one retransmission before expiry.

        Retry preserves the complete command/event payload including expiry and
        epoch. No renewed TTL, new logical command, or assumed physical outcome.
        """
        stamp(now)
        with self.transaction() as db:
            for (a,) in db.execute('SELECT state FROM action_plans WHERE active ORDER BY id').fetchall():
                version, state = self.transformer(db, a['tx'])
                if state['latest'] and not fresh(state['latest'], now):
                    state['health'], state['recovery'], state['recovery_end'] = 'SAFE_HOLD', 0, None
                kind = 'RESTORE' if a['status'] == 'RESTORE_DISPATCHED' else 'THROTTLE'
                if a['status'] in ('ACKS_PENDING', 'RESTORE_DISPATCHED'):
                    timed_out = False
                    for c in self.commands(db, a['id'], kind):
                        if c['status'] != 'PENDING' or now < instant(c['deadline']):
                            continue
                        expires = instant(c['event']['payload']['expires_at'])
                        if not self.exhausted(c, now):
                            c['attempts'] += 1
                            c['deadline'] = stamp(min(expires, now + timedelta(seconds=a['policy']['ack_timeout_seconds'])))
                            self.outbox(db, c['event'], 'Command', c['event']['payload']['device_id'],
                                        identity(f"delivery/{c['id']}/{c['attempts']}"))
                        else:
                            c['status'], timed_out = 'TIMED_OUT', True
                        self.save_command(db, c)
                    if timed_out:
                        status = 'PARTIALLY_APPLIED' if any(c['status'] == 'APPLIED' for c in self.commands(db, a['id'], kind)) else 'TIMED_OUT'
                        self.review(db, a, now, status, 'acknowledgement deadline exhausted; outcome unknown')
                        state['health'] = 'SAFE_HOLD'
                self.save_transformer(db, a['tx'], version, state)

    def reconcile(self, now):
        """New ownership holds all unfinished old-epoch work, retaining evidence."""
        with self.transaction() as db:
            for (a,) in db.execute('SELECT state FROM action_plans WHERE active ORDER BY id').fetchall():
                if a['epoch'] == self.epoch:
                    continue
                version, state = self.transformer(db, a['tx'])
                for kind in ('THROTTLE', 'RESTORE'):
                    for c in self.commands(db, a['id'], kind):
                        if c['status'] == 'PENDING':
                            c['status'] = 'TIMED_OUT'
                            self.save_command(db, c)
                self.transition(db, a, 'NEEDS_OPERATOR_REVIEW', now,
                                'leadership changed; reconcile effects before further commands')
                state['health'], state['recovery'], state['recovery_end'] = 'SAFE_HOLD', 0, None
                self.save_transformer(db, a['tx'], version, state)

    def inspect(self):
        """Read-only durable evidence, ordered for fixture assertions and local review."""
        with psycopg.connect(self.dsn, connect_timeout=2) as db:
            db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            return dict(transformers={r[0]: dict(r[2], version=r[1]) for r in db.execute('SELECT id,version,state FROM action_transformers ORDER BY id')},
                        actions=[r[0] for r in db.execute('SELECT state FROM action_plans ORDER BY id')],
                        commands=[r[0] for r in db.execute('SELECT state FROM action_commands ORDER BY id')],
                        outbox=[dict(id=str(r[0]), topic=r[1], key=r[2], event=r[3]) for r in db.execute('SELECT id,aggregatetype,aggregateid,payload FROM action_outbox ORDER BY sequence')],
                        inbox=db.execute('SELECT count(*) FROM action_inbox').fetchone()[0])
