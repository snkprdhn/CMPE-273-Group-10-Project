"""Real local acceptance; run with .tools/member3-venv/bin/python -m tools.integration.

Never resets a store, stops an existing job, or substitutes a fixture service.
Parent-owned setup supplies a fresh CDC-bound control schema and gateway DSN.
Evidence is synthetic and private under .tools/integration/<run>/report.json.
Host/Compose acceptance never satisfies the separately reported kind gate.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from uuid import NAMESPACE_URL, uuid5
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SEED = 273
GATES = ('member1-member2', 'member2-member3', 'member3-member1', 'members23-member4')
COMPOSE = ['docker', 'compose', '-p', 'smart-grid-local-dev', '-f',
           'infra/compose/compose.yaml', '-f', 'infra/compose/member2.yaml']


class Blocked(RuntimeError):
    """A real prerequisite is unavailable; this is never a successful check."""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def stamp(now):
    return now.isoformat().replace('+00:00', 'Z')


def http(url, method='GET', body=None, headers=None, timeout=5):
    request = Request(url, method=method, headers=headers or {},
                      data=None if body is None else json.dumps(body).encode())
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def wait(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.2)
    raise Blocked('deadline waiting for ' + description)


def stable_id(path):
    return str(uuid5(NAMESPACE_URL, 'smart-grid/v1/' + path))


def from_beginning(reader, names):
    """Read every partition of the pre-provisioned scenario topics."""
    from confluent_kafka import TopicPartition
    topics = reader.list_topics(timeout=10).topics
    require(all(name in topics and not topics[name].error and topics[name].partitions for name in names),
            'scenario topics are missing or unavailable')
    reader.assign([TopicPartition(name, partition, 0) for name in names for partition in topics[name].partitions])


def evaluate(name, evidence):
    """Pure evidence checks; missing evidence fails closed, never skips to PASS."""
    loads = [e['payload'] for e in evidence.get('loads', [])]
    actions = [e['payload'] for e in evidence.get('actions', [])]
    commands = [e['payload'] for e in evidence.get('commands', [])]
    results = [e['payload'] for e in evidence.get('results', [])]
    if name == GATES[0]:
        require(loads, 'no read_committed PyFlink output')
        for event in evidence['loads']:
            p = event['payload']
            window = stable_id(f"window/{p['transformer_id']}/{p['window_start']}/5")
            require(p['window_id'] == window and event['event_id'] == stable_id(f"load/{window}/{p['revision']}"),
                    'unstable load identity')
            require(p['capacity_kw'] == 20 and p['expected_device_count'] == 3, 'topology mismatch')
            require(math.isclose(p['meter_demand_kw'] + p['ev_demand_kw'] - p['solar_generation_kw'],
                                 p['net_load_kw'], abs_tol=1e-9), 'unbalanced load')
        require(any(p['completeness'] == 'COMPLETE' and math.isclose(p['net_load_kw'], 13.2, abs_tol=1e-9)
                    for p in loads), 'normal window missing')
        require(any(p['completeness'] == 'COMPLETE' and math.isclose(p['net_load_kw'], 20.2, abs_tol=1e-9)
                    for p in loads), 'critical window missing')
        require(evidence.get('checkpoint_completed', 0) > 0, 'no completed Flink checkpoint')
    elif name == GATES[1]:
        require(evidence.get('live_registry') is True, 'live Registry/telemetry adapter not selected')
        require(evidence.get('lease_epoch', 0) > 0, 'no durable Lease authority')
        ids = {a['action_id'] for a in actions}
        require(len(ids) == 1, 'expected exactly one logical action')
        for event in evidence['actions']:
            a = event['payload']
            aid = stable_id(f"action/{a['transformer_id']}/{a['trigger_window_id']}/{a['policy_version']}")
            require(a['action_id'] == aid and event['event_id'] == stable_id(f"action-event/{aid}/{event['saga_version']}"),
                    'unstable action identity')
        statuses = {a['status'] for a in actions}
        require({'PLAN_CREATED', 'COMMANDS_DISPATCHED', 'REDUCTION_CONFIRMED',
                 'RECOVERY_MONITORING', 'RESTORE_DISPATCHED', 'COMPLETED'} <= statuses,
                'missing allowed action transitions')
        require('NEEDS_OPERATOR_REVIEW' not in statuses, 'action entered safe hold')
        require(len({(c['action_id'], c['device_id'], c['command_type']) for c in commands}) == 2,
                'expected one throttle and one restore')
        require(all(c['action_id'] in ids for c in commands), 'command action mismatch')
        require(all(a['leader_epoch'] == evidence['lease_epoch'] for a in actions), 'action fencing mismatch')
    elif name == GATES[2]:
        require(evidence.get('connector_running') is True, 'real CDC connector not running')
        by_id = {c['command_id']: c for c in commands}
        require(len(by_id) == 2, 'expected two CDC commands')
        for event in evidence['commands']:
            c = event['payload']
            cid = stable_id(f"command/{c['action_id']}/{c['device_id']}/{c['command_type']}")
            rate = {'THROTTLE': 2, 'RESTORE': 7.2}[c['command_type']]
            require(c['command_id'] == cid and event['event_id'] == stable_id(f'command-event/{cid}'),
                    'unstable command identity')
            require(math.isclose(c['target_rate_kw'], rate), 'unexpected command target')
        applied = {r['command_id'] for r in results if r['outcome'] == 'APPLIED'}
        require(applied == set(by_id), 'missing applied device results')
        require(any(r['outcome'] == 'ALREADY_APPLIED' for r in results), 'duplicate command not observed')
        for r in results:
            require(r['command_id'] in by_id, 'unrelated result')
            require(r['action_id'] == by_id[r['command_id']]['action_id'], 'result action mismatch')
        require(evidence.get('edge_effects') == 2, 'duplicate command changed physical effect count')
        require(math.isclose(evidence.get('edge_rate', -1), 7.2, abs_tol=1e-9), 'restore rate mismatch')
        for meter, ev, net in [(15,2,15), (8,2,8), (8,7.2,13.2)]:
            require(any(p['completeness'] == 'COMPLETE' and math.isclose(p['meter_demand_kw'], meter)
                        and math.isclose(p['ev_demand_kw'], ev) and math.isclose(p['net_load_kw'], net)
                        for p in loads), f'missing measured {meter}/{ev}/{net} kW window')
        final = loads[-1]
        require(math.isclose(final['ev_demand_kw'], 7.2) and math.isclose(final['net_load_kw'], 13.2),
                'final load did not return to safe restored demand')
    elif name == GATES[3]:
        require(evidence.get('projection_equal') is True, 'recorded/live projection mismatch')
        require(evidence.get('api_action_equal') is True, 'API action differs from CDC')
        require(evidence.get('sse_action_equal') is True, 'SSE action differs from CDC')
        require(evidence.get('ui_action_equal') is True, 'real browser UI action not verified')
    else:
        raise ValueError('unknown gate')


def verdicts(evidence, blockers=None):
    result = {}
    for name in GATES:
        try:
            evaluate(name, evidence)
            result[name] = {'status': 'PASS'}
        except AssertionError as exc:
            reason = (blockers or {}).get(name)
            result[name] = {'status': 'BLOCKED' if reason else 'FAIL', 'reason': reason or str(exc)}
    return result


def verify_http(api_url, evidence, token, generation):
    headers = {'Authorization': 'Bearer ' + token}
    final = next((e['payload'] for e in reversed(evidence['actions']) if e['payload']['status'] == 'COMPLETED'), None)
    require(final is not None, 'no completed action to verify over HTTP')
    actual = http(api_url + '/actions/' + final['action_id'], headers=headers)
    evidence['api_action'] = actual
    evidence['api_action_equal'] = actual == final
    # Start from the durable pre-action cursor for this projection generation.
    request = Request(api_url + '/events', headers=dict(headers, **{'Last-Event-ID': generation + ':0'}))
    with urlopen(request, timeout=20) as stream:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            line = stream.readline().decode().strip()
            if line.startswith('data: '):
                event = json.loads(line[6:])
                if event['event_type'] == 'action.updated' and event['payload'] == final:
                    evidence['sse_action'] = event
                    evidence['sse_action_equal'] = True
                    break
    return final

class Runner:
    def __init__(self, args):
        self.args = args
        self.directory = ROOT / '.tools/integration' / args.run
        self.directory.mkdir(parents=True, exist_ok=False)
        self.directory.chmod(0o700)
        self.evidence = {'loads': [], 'actions': [], 'commands': [], 'results': [], 'telemetry': []}
        self.blockers = {}
        self.children = []
        self.commands = []
        self.job = None
        self.manager = None
        self.initial_jobs = []

    def command(self, args, timeout=90):
        # Commands contain no DSNs/tokens; credentials travel through environment.
        self.commands.append(args)
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, cwd=ROOT)
        if result.returncode:
            raise Blocked('command failed: ' + ' '.join(args[:6]) + '; ' + result.stderr[-500:])
        return result.stdout

    def spawn(self, name, command, env):
        self.commands.append(command)
        log = (self.directory / (name + '.log')).open('w')
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        self.children.append((name, process, log))
        return process

    def healthy_children(self):
        for name, process, _ in self.children:
            if process.poll() is not None:
                raise Blocked(name + ' exited; inspect private run log')

    def preflight(self):
        from confluent_kafka.admin import AdminClient, NewTopic
        import psycopg
        from member1.common import BUNDLE
        for name in ('control_dsn', 'edge_dsn', 'connector'):
            if not getattr(self.args, name):
                raise Blocked('missing INTEGRATION_' + name.upper())
        # A fresh, explicitly provisioned schema is required; never clear history.
        with psycopg.connect(self.args.control_dsn, connect_timeout=3) as db:
            schema = db.execute('SELECT current_schema()').fetchone()[0]
            require(schema.startswith('integration_'), 'control schema must be run-owned integration_*')
            for table in ('action_inbox', 'action_outbox', 'action_plans'):
                require(db.execute('SELECT count(*) FROM ' + table).fetchone()[0] == 0,
                        'control schema contains history; provision a fresh run')
        config = http(self.args.connect_url + '/connectors/' + self.args.connector + '/config')
        require(config['table.include.list'] == schema + '.action_outbox', 'CDC schema mismatch')
        require(config['transforms.outbox.route.topic.replacement'] == self.args.prefix + '${routedByValue}',
                'CDC prefix mismatch')
        status = http(self.args.connect_url + '/connectors/' + self.args.connector + '/status')
        require(status['connector']['state'] == 'RUNNING' and status['tasks'] and
                all(t['state'] == 'RUNNING' for t in status['tasks']), 'CDC not RUNNING')
        self.evidence['connector_running'] = True
        source = (ROOT / 'member3/worker.py').read_text()
        require('CONTROL_SNAPSHOTS' in source and 'REGISTRY_ACTION_TOKEN' in source,
                'live Member3 worker adapter unavailable')
        self.evidence['live_registry'] = True
        admin = AdminClient({'bootstrap.servers': self.args.bootstrap})
        existing = admin.list_topics(timeout=5).topics
        names = [self.args.prefix + topic for topic in BUNDLE['x-events']]
        names.append('__debezium-heartbeat.' + config['topic.prefix'])
        missing = [NewTopic(t, 1, 1) for t in names if t not in existing]
        if missing:
            for future in admin.create_topics(missing).values():
                future.result(10)
        self.initial_jobs = http(self.args.flink_url + '/jobs/overview')['jobs']

    def flink(self):
        if not self.args.start_flink:
            self.job = next((j['jid'] for j in self.initial_jobs
                             if j['name'] == 'member2-' + self.args.run and j['state'] == 'RUNNING'), None)
            if not self.job:
                raise Blocked('no run-specific Flink job; use --start-flink to provision own capacity')
            return
        self.manager = 'smart-grid-integration-' + self.args.run
        self.command(COMPOSE + ['run', '-d', '--no-deps', '--name', self.manager, 'taskmanager'])
        wait(lambda: http(self.args.flink_url + '/overview')['slots-available'] > 0, 'own Flink slot')
        jm = self.command(COMPOSE + ['ps', '-q', 'jobmanager']).strip()
        target = '/tmp/integration-' + self.args.run
        self.command(['docker', 'cp', 'member1/fixtures/topology.json', jm + ':' + target + '.json'])
        self.command(['docker', 'cp', 'member2/job.py', jm + ':' + target + '.py'])
        # Separate job name, transactional IDs, source group and checkpoint path.
        self.command(COMPOSE + ['exec', '-T', 'jobmanager', 'flink', 'run', '-d', '-py', target + '.py',
                     '--run', self.args.run, '--prefix', self.args.prefix, '--topology', target + '.json',
                     '--checkpoint', 'file:///checkpoints/integration-' + self.args.run,
                     '--checkpoint-ms', '1000'], timeout=180)
        self.job = wait(lambda: next((j['jid'] for j in http(self.args.flink_url + '/jobs/overview')['jobs']
                                     if j['name'] == 'member2-' + self.args.run and j['state'] == 'RUNNING'), None),
                        'run-specific Flink job', 90)

    def flow(self):
        from confluent_kafka import Consumer
        import grpc
        import psycopg
        from member1.common import utc, event
        from member1.fencing import FencedEdgeState
        from member1.kafka import Publisher, flush_results, handle_message
        from member1.registry import Client, pb, device_dict, device_message
        from member1.simulator import devices

        prefix, run = self.args.prefix, self.args.run
        client = Client(self.args.registry, 'simulator', os.environ.get('REGISTRY_SIMULATOR_TOKEN', 'simulator_disposable'))
        fleet = []
        try:
            for wanted in devices(utc()):
                try:
                    actual = device_dict(client.call('GetDevice', pb.GetDeviceRequest(device_id=wanted['device_id'])))
                    require(all(actual[k] == wanted[k] for k in wanted if k != 'updated_at'),
                            'existing Registry capability mismatch; refusing to overwrite')
                except grpc.RpcError as exc:
                    if exc.code() != grpc.StatusCode.NOT_FOUND:
                        raise
                    actual = device_dict(client.call('RegisterDevice', pb.RegisterDeviceRequest(
                        operation_id=run + '-' + wanted['device_id'], device=device_message(wanted))))
                fleet.append(actual)
            edge = FencedEdgeState(self.args.edge_dsn, fleet[0])
            require(edge.current()[2] == 0, 'edge store contains existing effects')
            self.edge = edge
            publisher = Publisher(self.args.bootstrap, prefix)
            reader = Consumer({'bootstrap.servers': self.args.bootstrap, 'group.id': prefix + 'acceptance',
                               'enable.auto.commit': False, 'isolation.level': 'read_committed'})
            topics = ['grid.transformer-load.v1', 'grid.action.v1', 'grid.charger-command.v1', 'grid.command-result.v1']
            edge_reader = Consumer({'bootstrap.servers': self.args.bootstrap, 'group.id': prefix + 'edge',
                                    'enable.auto.commit': False, 'enable.auto.offset.store': False,
                                    'isolation.level': 'read_committed'})
            from_beginning(reader, [prefix + topic for topic in topics])
            from_beginning(edge_reader, [prefix + 'grid.charger-command.v1'])
            self.start_workers()
            start, next_sample, tick = time.monotonic(), 0, 0
            recovery = False
            duplicated = False
            end_completed = None
            try:
                while time.monotonic() - start < self.args.duration:
                    self.healthy_children()
                    elapsed = time.monotonic() - start
                    now = utc()
                    if elapsed >= next_sample:
                        # Start normal; hold feasible overload until real confirmation,
                        # then hold recovery long enough for two post-effect windows.
                        statuses = {e['payload']['status'] for e in self.evidence['actions']}
                        recovery = recovery or ('REDUCTION_CONFIRMED' in statuses and any(
                            math.isclose(e['payload']['ev_demand_kw'], 2) and math.isclose(e['payload']['net_load_kw'], 15)
                            for e in self.evidence['loads']))
                        meter = 8 if elapsed < 20 or recovery else 15
                        for device in fleet:
                            heartbeat = pb.HeartbeatRequest(operation_id=f'{run}-hb-{tick}-{device["device_id"]}',
                                                            device_id=device['device_id'], capability_version=device['capability_version'])
                            # Report a completed sample interval. The Registry stores only
                            # the newest heartbeat and rejects one newer than evaluation time.
                            heartbeat.observed_at.FromDatetime(now - timedelta(seconds=2))
                            client.call('Heartbeat', heartbeat)
                            kind = device['device_type']
                            power = {'SMART_METER': meter, 'SOLAR': -2, 'EV_CHARGER': edge.current()[0]}[kind]
                            if kind == 'EV_CHARGER':
                                reading = edge.reading(now, run)
                            else:
                                reading = event('telemetry.reading', dict(device_id=device['device_id'], transformer_id='tx-001',
                                    device_type=kind, boot_id=run, power_kw=power, quality='GOOD', sequence_number=tick),
                                    now, f'{run}/{SEED}/{tick}/{device["device_id"]}', run)
                            self.evidence['telemetry'].append(reading)
                            publisher.send('grid.telemetry.v1', 'tx-001', reading)
                        publisher.drain()
                        tick += 1
                        next_sample += 1
                    message = edge_reader.poll(.05)
                    if message is not None:
                        handle_message(message, edge, publisher, edge_reader, now)
                    message = reader.poll(.05)
                    if message is not None:
                        require(not message.error(), 'Kafka evidence consumer failed')
                        value = json.loads(message.value())
                        key = {'grid.transformer-load.v1': 'loads', 'grid.action.v1': 'actions',
                               'grid.charger-command.v1': 'commands', 'grid.command-result.v1': 'results'}[
                                   message.topic().removeprefix(prefix)]
                        self.evidence[key].append(value)
                        if key == 'commands' and not duplicated:
                            publisher.send('grid.charger-command.v1', value['payload']['device_id'], value)
                            publisher.drain()
                            duplicated = True
                        if key == 'actions' and value['payload']['status'] == 'COMPLETED':
                            end_completed = time.monotonic() + 18
                    if end_completed and time.monotonic() >= end_completed:
                        break
                flush_results(edge, publisher)
                rate, epoch, effects = edge.current()
                self.evidence.update(edge_rate=rate, edge_effects=effects)
                with psycopg.connect(self.args.control_dsn) as db:
                    row = db.execute('SELECT epoch FROM action_authority').fetchone()
                    self.evidence['lease_epoch'] = row[0] if row else 0
                checks = http(self.args.flink_url + '/jobs/' + self.job + '/checkpoints')
                self.evidence['checkpoint_completed'] = checks['counts']['completed']
                self.finish_projection()
            finally:
                reader.close()
                edge_reader.close()
                edge.db.close()
        finally:
            client.channel.close()

    def start_workers(self):
        env = dict(os.environ, CONTROL_DSN=self.args.control_dsn, KAFKA_PREFIX=self.args.prefix,
                   KAFKA_BOOTSTRAP=self.args.bootstrap, LEASE_NAME='integration-' + self.args.run,
                   REGISTRY_ADDRESS=self.args.registry,
                   REGISTRY_ACTION_TOKEN=os.environ.get('REGISTRY_ACTION_TOKEN', 'action_disposable'))
        env.pop('CONTROL_SNAPSHOTS', None)
        self.spawn('control', [str(ROOT / '.tools/member3-venv/bin/python'), '-m', 'member3.worker'], env)
        self.start_projection(env)

    def start_projection(self, env):
        if not self.args.projection_dsn:
            self.blockers[GATES[3]] = 'missing INTEGRATION_PROJECTION_DSN'
            return
        from member4.store import Store
        self.store = Store(self.args.projection_dsn)
        self.store.migrate()
        with self.store.transaction() as db:
            require(db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 0,
                    'projection contains prior history')
        env = dict(env, PROJECTION_DSN=self.args.projection_dsn, PROJECTION_GROUP=self.args.prefix + 'projection')
        self.token = 'integration-local-viewer-token'
        env.update(MEMBER4_AUTH_MODE='mock', MEMBER4_MOCK_IDENTITIES=json.dumps({self.token: {'actor':'integration-viewer','role':'Viewer'}}))
        self.spawn('api', [str(ROOT / '.tools/member4-venv/bin/python'), '-m', 'member4', 'serve', '--port', '18094'], env)
        ui_env = dict(env, DASHBOARD_API_TOKEN=self.token)
        self.args.ui_url = self.args.ui_url or 'http://127.0.0.1:18096'
        self.spawn('ui', [str(ROOT / '.tools/dashboard-venv/bin/python'), '-m', 'dashboard', '--port', '18096', '--api-url', self.args.api_url], ui_env)
        self.spawn('projection', [str(ROOT / '.tools/member4-venv/bin/python'), '-m', 'member4', 'consume'], env)

    def projected_journal(self):
        expected = {e['event_id']: e for key in ('loads', 'actions', 'commands', 'results', 'telemetry') for e in self.evidence[key]}
        def projected():
            with self.store.transaction() as db:
                actual = {r[0]: r[1] for r in db.execute('SELECT event_id,event FROM projection_events')}
            return all(actual.get(k) == v for k, v in expected.items())
        wait(projected, 'projection journal convergence', 20)
        from member4.live_context import RegistryContext
        from member3.domain import default_policy
        client = RegistryContext(self.args.registry, os.environ.get('REGISTRY_API_TOKEN','api_disposable'))
        try:
            p = self.evidence['loads'][0]['payload']
            baseline = dict(transformer_id='tx-001', state='STALE', load=p, policy=default_policy(), freshness='FRESH', data_age_seconds=0)
            client.refresh(self.store, [baseline], ['ev-001','meter-001','solar-001'])
        finally:
            client.close()
        return expected

    def finish_projection(self):
        if not hasattr(self, 'store'):
            return
        expected = self.projected_journal()
        with self.store.transaction() as db:
            generation = db.execute('SELECT generation FROM projection_meta').fetchone()[0]
        token = Path(self.args.bearer_file).read_text().strip() if self.args.bearer_file else self.token
        final = verify_http(self.args.api_url, self.evidence, token, generation)
        if self.args.ui_url:
            self.command([str(ROOT / '.tools/dashboard-test-venv/bin/python'), '-m',
                'tools.integration_browser', '--url', self.args.ui_url, '--action', final['action_id'],
                '--output', str(self.directory / 'ui.json')], timeout=60)
            self.evidence['ui_action'] = json.loads((self.directory / 'ui.json').read_text())
            self.evidence['ui_action_equal'] = self.evidence['ui_action']['matched']
        projection = next(process for name, process, _ in self.children if name == 'projection')
        projection.terminate()
        projection.wait(10)
        with self.store.transaction() as db:
            before = list(db.execute('SELECT kind,id,event FROM projection_current ORDER BY kind,id'))
        self.store.rebuild()
        with self.store.transaction() as db:
            after = list(db.execute('SELECT kind,id,event FROM projection_current ORDER BY kind,id'))
        self.evidence['projection_equal'] = bool(expected) and before == after

    def cleanup(self):
        errors = []
        for name, process, log in reversed(self.children):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)
            log.close()
        if self.job and self.args.start_flink:
            job_url = self.args.flink_url + '/jobs/' + self.job
            try:
                with urlopen(Request(job_url, method='PATCH'), timeout=5) as response:
                    response.read()
                wait(lambda: http(job_url)['state'] in ('CANCELED', 'FINISHED', 'FAILED'),
                     'own Flink job to stop', 25)
            except (OSError, Blocked) as exc:
                errors.append('own Flink cancellation: ' + str(exc))
        if self.manager:
            try:
                self.command(['docker', 'rm', '-f', self.manager], timeout=30)
            except Exception as exc:
                errors.append('own TaskManager cleanup: ' + type(exc).__name__)
        try:
            current = {j['jid']: j['state'] for j in http(self.args.flink_url + '/jobs/overview')['jobs']}
            require(all(current.get(j['jid']) == 'RUNNING' for j in self.initial_jobs if j['state'] == 'RUNNING'),
                    'pre-existing Flink job no longer RUNNING')
        except Exception as exc:
            errors.append('preservation check: ' + str(exc))
        return errors

    def run(self):
        started = stamp(datetime.now(timezone.utc))
        failure = None
        try:
            self.preflight()
            self.flink()
            self.flow()
        except Exception as exc:
            failure = {'type': type(exc).__name__, 'reason': str(exc)}
            if isinstance(exc, (Blocked, OSError, ImportError)):
                for gate in GATES:
                    self.blockers.setdefault(gate, type(exc).__name__ + ': ' + str(exc))
        finally:
            cleanup = self.cleanup()
        gates = verdicts(self.evidence, self.blockers)
        passed = all(g['status'] == 'PASS' for g in gates.values()) and not failure and not cleanup
        report = dict(run=self.args.run, seed=SEED, contract_version=1, runtime='host-compose', started_at=started,
                      finished_at=stamp(datetime.now(timezone.utc)), prefix=self.args.prefix,
                      git_commit=self.command(['git', 'rev-parse', 'HEAD']).strip(),
                      contract_sha256=hashlib.sha256((ROOT / 'schemas/v1/bundle.json').read_bytes()).hexdigest(),
                      gates=gates, full_loop={'status': 'PASS' if passed else 'FAIL', 'runtime': 'host-compose'},
                      required_kind={'status': 'BLOCKED', 'reason': 'host/Compose evidence does not establish the kind workload gate'},
                      expected_kw=dict(normal=13.2, overload=20.2, throttled=15, recovery=8, restored=13.2),
                      failure=failure, cleanup_errors=cleanup, commands=self.commands, evidence=self.evidence,
                      owner_signoff='pending human review',
                      coverage_limits=['No checkpoint crash/failover injection', 'No stale/expired command matrix',
                                       'No security or NetworkPolicy acceptance', 'No kind application deployment'])
        path = self.directory / 'report.json'
        path.write_text(json.dumps(report, indent=2) + '\n')
        path.chmod(0o600)
        print(json.dumps({'report': str(path), 'gates': gates, 'full_loop': report['full_loop'],
                          'required_kind': report['required_kind']}))
        return 0 if passed else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run')
    parser.add_argument('--check-gate', choices=GATES)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--prefix')
    parser.add_argument('--control-dsn', default=os.getenv('INTEGRATION_CONTROL_DSN'))
    parser.add_argument('--edge-dsn', default=os.getenv('INTEGRATION_EDGE_DSN'))
    parser.add_argument('--projection-dsn', default=os.getenv('INTEGRATION_PROJECTION_DSN'))
    parser.add_argument('--connector', default=os.getenv('INTEGRATION_CONNECTOR'))
    parser.add_argument('--bootstrap', default='127.0.0.1:19092')
    parser.add_argument('--registry', default='127.0.0.1:15051')
    parser.add_argument('--connect-url', default='http://127.0.0.1:18083')
    parser.add_argument('--flink-url', default='http://127.0.0.1:18081')
    parser.add_argument('--api-url', default='http://127.0.0.1:18094/api/v1')
    parser.add_argument('--ui-url')
    parser.add_argument('--bearer-file')
    parser.add_argument('--start-flink', action='store_true')
    parser.add_argument('--duration', type=int, default=130)
    parser.add_argument('--config', help='private JSON environment from integration_setup')
    args = parser.parse_args(argv)
    if args.check_gate:
        if args.report is None:
            parser.error('--check-gate requires --report')
        report = json.loads(args.report.read_text())
        try:
            evaluate(args.check_gate, report['evidence'])
        except AssertionError as exc:
            print(f'{args.check_gate}: FAIL ({exc})')
            return 1
        print(f'{args.check_gate}: PASS')
        return 0
    if not args.run:
        parser.error('--run is required')
    if args.config:
        values = json.loads(Path(args.config).read_text())
        for key, value in values.items():
            setattr(args, key.removeprefix('INTEGRATION_').lower(), value)
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,31}', args.run):
        parser.error('--run must contain 1..32 lowercase ASCII letters/digits/underscore/hyphen')
    args.prefix = args.prefix or 'integration-' + args.run + '.'
    if not re.fullmatch(r'integration-[a-z0-9_.-]{1,64}\.', args.prefix):
        parser.error('--prefix must be a run-owned integration-* namespace ending in period')
    if not 60 <= args.duration <= 600:
        parser.error('--duration must be 60..600 seconds')
    return Runner(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
