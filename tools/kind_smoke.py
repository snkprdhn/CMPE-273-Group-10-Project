"""Fail-closed acceptance for NetworkPolicy, Action RBAC and the in-kind closed loop."""
import base64
import gzip
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
import subprocess
import time

from tools.integration import GATES, verdicts
from tools.kind_local import PYTHON_IMAGE, ROOT, run, write_private

SOCKET_CHECK = """
import socket, sys
arguments = sys.argv[1:]
for host, port, expected in zip(arguments[::3], arguments[1::3], arguments[2::3], strict=True):
    ip = socket.gethostbyname(host)
    try:
        with socket.create_connection((ip, int(port)), timeout=3):
            pass
    except TimeoutError:
        assert expected == 'denied', (host, port, 'unexpected timeout')
    else:
        assert expected == 'allowed', (host, port, 'unexpected connection')
    print(host, ip, port, expected, flush=True)
"""


def require_rbac(cluster):
    actor = 'system:serviceaccount:platform:action'
    cluster.k('auth', 'can-i', 'update', 'lease/action', '--as', actor)
    for verb, resource in (('get', 'secrets'), ('create', 'leases'), ('update', 'lease/other')):
        try:
            cluster.k('auth', 'can-i', verb, resource, '--as', actor, capture=True)
        except subprocess.CalledProcessError as exc:
            if exc.returncode != 1 or exc.stdout.strip() != 'no':
                raise
        else:
            raise RuntimeError(f'Action unexpectedly can {verb} {resource}')


@contextmanager
def policy_pod(cluster, namespace, name, labels, port=None):
    command = ['/bin/sleep', '300']
    if port:
        command = ['/opt/venv/bin/python', '-m', 'http.server', str(port), '--bind', '0.0.0.0']
    spec = {
        'apiVersion': 'v1', 'kind': 'Pod',
        'metadata': {'name': name, 'namespace': namespace, 'labels': labels},
        'spec': {
            'restartPolicy': 'Never', 'automountServiceAccountToken': False,
            'containers': [{
                'name': 'probe', 'image': PYTHON_IMAGE, 'imagePullPolicy': 'Never',
                'command': command,
                'resources': {'requests': {'cpu': '10m', 'memory': '16Mi'},
                              'limits': {'cpu': '100m', 'memory': '48Mi'}},
                'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
            }],
        },
    }
    if port:
        spec['spec']['containers'][0]['readinessProbe'] = {'tcpSocket': {'port': port}}
    cluster.k('-n', namespace, 'create', '-f', '-', data=json.dumps(spec))
    try:
        cluster.k('-n', namespace, 'wait', '--for=condition=Ready', 'pod/' + name, '--timeout=150s')
        yield
    finally:
        cluster.k('-n', namespace, 'delete', 'pod/' + name, '--wait=false')


def enforce_network(cluster):
    command = ['/opt/venv/bin/python', '-c', SOCKET_CHECK]
    outside, inside, target = 'grid-policy-outside', 'grid-policy-inside', 'grid-policy-target'
    labels = {'grid-owner': 'Member_4'}
    # Match the actual PostgreSQL policies without serving credentials or
    # conflating a database restart with a policy denial.
    with policy_pod(cluster, 'default', outside, labels, 8080), policy_pod(
            cluster, 'platform', inside, dict(labels, **{'grid-app': 'database-bootstrap'})), policy_pod(
            cluster, 'platform', target, dict(labels, **{'grid-app': 'postgres'}), 5432):
        outside_ip = cluster.k('-n', 'default', 'get', 'pod/' + outside, '-o', 'jsonpath={.status.podIP}', capture=True)
        target_ip = cluster.k('get', 'pod/' + target, '-o', 'jsonpath={.status.podIP}', capture=True)
        cluster.k('exec', inside, '--', *command, target_ip, '5432', 'allowed', outside_ip, '8080', 'denied')
        cluster.k('-n', 'default', 'exec', outside, '--', *command,
                  outside_ip, '8080', 'allowed', target_ip, '5432', 'denied')
    print('Calico NetworkPolicy ingress and egress checks passed')


def smoke(cluster):
    started = datetime.now(timezone.utc).isoformat()
    evidence, network, rbac, failure = {}, None, None, None
    try:
        # Preserve the one-shot scenario until the full dependency chain can serve it.
        cluster.ready_connector(timeout=15)
        cluster.k('wait', '--for=jsonpath={.status.jobStatus.state}=RUNNING',
                  'flinkdeployment/aggregation', '--timeout=180s')
        cluster.k('rollout', 'status', 'deployment/ui', '--timeout=15s')
        cluster.k('rollout', 'status', 'deployment/action', '--timeout=15s')
        cluster.k('rollout', 'status', 'deployment/projection', '--timeout=15s')
        rbac = False
        require_rbac(cluster)
        rbac = True
        network = False
        enforce_network(cluster)
        network = True
        cluster.stage('smoke')
        deadline = time.monotonic() + 280
        next_sample = 0
        while time.monotonic() < deadline:
            state = json.loads(cluster.k('get', 'job/simulator', '-o', 'json', capture=True))['status']
            if any(c['type'] in ('Complete', 'Failed') and c['status'] == 'True' for c in state.get('conditions', [])):
                break
            if time.monotonic() >= next_sample:
                cluster.measure(quiet=True)
                next_sample = time.monotonic() + 20
            time.sleep(5)
        logs = cluster.k('logs', 'job/simulator', capture=True)
        write_private(cluster.directory / 'simulator.log', logs)
        prefix = 'KIND_EVIDENCE_CHUNK '
        chunks = [line.removeprefix(prefix) for line in logs.splitlines() if line.startswith(prefix)]
        if chunks:
            evidence = json.loads(gzip.decompress(base64.b64decode(''.join(chunks), validate=True)))
        if not state.get('succeeded') or not evidence:
            raise RuntimeError('Simulator did not pass; inspect the private simulator log')
        write_private(cluster.directory / 'evidence.json', json.dumps(evidence))
        # Temporary operator-owned loopback API connection for REST/SSE assertions.
        # Ordinary dashboard users only need the UI route.
        with cluster.forwards(('ui', 18086, 18086), ('api', 18094, 18084)):
            env = dict(os.environ, PLAYWRIGHT_BROWSERS_PATH=str(ROOT / '.tools/playwright'))
            subprocess.run([ROOT / '.tools/dashboard-test-venv/bin/python', '-m', 'tools.kind_browser',
                            cluster.directory], cwd=ROOT, env=env, check=True, timeout=125)
        evidence = json.loads((cluster.directory / 'evidence.json').read_text())
    except Exception as exc:
        failure = str(exc)
    gates = verdicts(evidence, {name: failure for name in GATES} if not evidence and failure else None)
    passed = network and rbac and not failure and all(g['status'] == 'PASS' for g in gates.values())
    statuses = {True: 'PASS', False: 'FAIL', None: 'NOT_RUN'}
    report = dict(profile=cluster.profile, started_at=started, finished_at=datetime.now(timezone.utc).isoformat(),
                  git_base=run('git', 'rev-parse', 'HEAD', capture=True).strip(), runtime='kind',
                  network_policy=statuses[network], action_rbac=statuses[rbac],
                  gates=gates, full_loop='PASS' if passed else 'FAIL', failure=failure, evidence=evidence)
    write_private(cluster.directory / 'report.json', json.dumps(report, indent=2) + '\n')
    print(f'Full closed loop {report["full_loop"]}. Report {cluster.directory / "report.json"}')
    if not passed:
        raise RuntimeError(failure or 'Closed-loop assertions failed; inspect report')
