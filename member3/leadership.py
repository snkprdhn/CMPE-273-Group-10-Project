"""Local kind Lease client and PostgreSQL fencing authority; no Kubernetes SDK."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import ssl
import subprocess
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4

from observability import traced

import psycopg

from member3.control import Conflict


class Lease:
    def __init__(self, namespace='smart-grid-control', name='action', duration=15,
                 kubectl='.venv/bin/kubectl', context='kind-smart-grid-dev'):
        if not 5 <= duration <= 60:
            raise ValueError('lease duration must be 5..60 seconds')
        self.command = [kubectl, '--context', context, '-n', namespace]
        self.namespace, self.name, self.duration = namespace, name, duration
        self.holder = str(uuid4())  # process incarnation, never a reusable pod name
        self.deadline = 0
        self.observed = None
        self.changed = time.monotonic()
        self.service_account = Path('/var/run/secrets/kubernetes.io/serviceaccount')
        self.api_host = os.getenv('KUBERNETES_SERVICE_HOST')

    def request(self, verb, body=None):
        if self.api_host:
            address = self.api_host
            if ':' in address:
                address = f'[{address}]'
            path = '/apis/coordination.k8s.io/v1/namespaces/' + quote(self.namespace, safe='') + '/leases'
            if verb != 'create':
                path += '/' + quote(self.name, safe='')
            request = Request(f'https://{address}:{os.environ["KUBERNETES_SERVICE_PORT"]}{path}',
                              method={'get': 'GET', 'replace': 'PUT', 'create': 'POST'}[verb],
                              data=json.dumps(body).encode() if body else None,
                              headers={'Authorization': 'Bearer ' + (self.service_account / 'token').read_text().strip(),
                                       'Content-Type': 'application/json'})
            context = ssl.create_default_context(cafile=self.service_account / 'ca.crt')
            try:
                with urlopen(request, context=context, timeout=3) as response:
                    return json.load(response)
            except HTTPError as exc:
                if verb == 'get' and exc.code == 404:
                    return None
                raise RuntimeError(f'Lease API {verb} failed with HTTP {exc.code}') from None
        args = ['get', 'lease', self.name] if verb == 'get' else [verb, '-f', '-']
        result = subprocess.run(self.command + args + ['-o', 'json'],
                                input=json.dumps(body) if body else None,
                                capture_output=True, text=True, timeout=3)
        if result.returncode:
            if verb == 'get' and 'NotFound' in result.stderr:
                return None
            raise RuntimeError('Lease API request failed: ' + result.stderr[-500:])
        return json.loads(result.stdout)

    @traced('leadership.step')
    def step(self):
        """CAS acquisition/renewal. Any uncertain renewal immediately stops work.

        Expiry uses elapsed local observation of an unchanged record, avoiding
        trust in another machine's wall clock. Acquisition may be conservative.
        """
        started = time.monotonic()
        try:
            record = self.request('get')
            if record:
                spec = record['spec']
                observed = (record['metadata']['uid'], spec.get('holderIdentity'), spec.get('renewTime'))
                if observed != self.observed:
                    self.observed, self.changed = observed, started
                own = spec.get('holderIdentity') == self.holder
                previous_deadline = self.deadline
                if own and time.monotonic() >= previous_deadline:
                    self.deadline = 0
                    return False  # expired incarnation must not renew itself
                if not own and spec.get('holderIdentity') and started - self.changed < spec['leaseDurationSeconds']:
                    return False
            else:
                record = dict(apiVersion='coordination.k8s.io/v1', kind='Lease',
                              metadata=dict(name=self.name, namespace=self.namespace), spec={})
                own = False
            spec = record['spec']
            now = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
            spec.update(holderIdentity=self.holder, leaseDurationSeconds=self.duration, renewTime=now,
                        leaseTransitions=spec.get('leaseTransitions', 0) + (not own))
            if not own:
                spec['acquireTime'] = now
            record = self.request('replace' if 'resourceVersion' in record['metadata'] else 'create', record)
            # A response arriving after our prior budget cannot revive this
            # incarnation, even if Kubernetes accepted the renewal.
            if own and time.monotonic() >= previous_deadline:
                self.deadline = 0
                return False
            self.token = (record['metadata']['uid'], record['spec']['leaseTransitions'])
            self.deadline = started + self.duration - 1
            return time.monotonic() < self.deadline
        except Exception:
            self.deadline = 0
            raise

    def release(self):
        self.deadline = 0
        if not hasattr(self, 'token'):
            return
        record = self.request('get')
        if record and record['spec'].get('holderIdentity') == self.holder:
            record['spec']['holderIdentity'] = ''
            self.request('replace', record)  # CAS: cannot release somebody else's Lease

    def verify(self):
        if time.monotonic() >= self.deadline:
            raise Conflict('LEASE_LOST')
        record = self.request('get')
        if (not record or record['spec'].get('holderIdentity') != self.holder
                or (record['metadata']['uid'], record['spec']['leaseTransitions']) != self.token):
            self.deadline = 0
            raise Conflict('LEASE_LOST')
        if time.monotonic() >= self.deadline:
            raise Conflict('LEASE_LOST')


class Leadership:
    def __init__(self, dsn, lease):
        self.dsn, self.lease, self.epoch = dsn, lease, None

    def activate(self):
        """Called after each successful Lease step, before serving any work."""
        with psycopg.connect(self.dsn, connect_timeout=2) as db:
            db.execute("SET LOCAL statement_timeout = '5s'")
            db.execute("SET LOCAL lock_timeout = '4s'")
            db.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
            db.execute('SELECT pg_advisory_xact_lock(273003)')
            self.lease.verify()
            uid, generation = self.lease.token
            row = db.execute('SELECT epoch,lease_uid,generation,holder FROM action_authority WHERE shard=1 FOR UPDATE').fetchone()
            if row and (row[1] != uid or generation < row[2]):
                # Lease deletion/recreation requires explicit operator recovery;
                # an old delayed acquisition must never resurrect authority.
                raise Conflict('LEASE_GENERATION')
            same = row and row[1:] == (uid, generation, self.lease.holder)
            if row:
                epoch = row[0] if same else row[0] + 1
            else:
                # Migration from the supplied-epoch standalone domain must fence
                # every historical command, even before a second acquisition.
                highest = db.execute('''SELECT max(epoch) FROM (
                    SELECT (state->>'epoch')::bigint AS epoch FROM action_transformers
                    UNION ALL SELECT (state->>'epoch')::bigint FROM action_plans
                    UNION ALL SELECT (state->'event'->>'leader_epoch')::bigint FROM action_commands
                ) AS history''').fetchone()[0]
                epoch = (highest or 0) + 1
            remaining = self.lease.deadline - time.monotonic()
            if remaining <= 0:
                raise Conflict('LEASE_LOST')
            db.execute('''INSERT INTO action_authority VALUES (1,%s,%s,%s,%s,clock_timestamp() + %s * interval '1 second')
                ON CONFLICT(shard) DO UPDATE SET epoch=excluded.epoch,lease_uid=excluded.lease_uid,
                generation=excluded.generation,holder=excluded.holder,valid_until=excluded.valid_until''',
                       (epoch, uid, generation, self.lease.holder, remaining))
            self.lease.verify()
        self.epoch = epoch
        return epoch

    def check(self, db):
        self.lease.verify()
        row = db.execute('SELECT epoch,holder,valid_until > clock_timestamp() FROM action_authority WHERE shard=1 FOR UPDATE').fetchone()
        if not row or row != (self.epoch, self.lease.holder, True):
            raise Conflict('STALE_EPOCH')

    def stop(self):
        self.lease.deadline = 0
        with psycopg.connect(self.dsn, connect_timeout=2) as db:
            db.execute("SET LOCAL statement_timeout = '5s'")
            db.execute("SET LOCAL lock_timeout = '4s'")
            db.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
            db.execute('SELECT pg_advisory_xact_lock(273003)')
            db.execute('UPDATE action_authority SET valid_until=clock_timestamp() WHERE shard=1 AND epoch=%s AND holder=%s',
                       (self.epoch, self.lease.holder))
        self.lease.release()
