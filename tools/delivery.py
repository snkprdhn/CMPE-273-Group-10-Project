"""Run the explicitly invoked local acceptance gates and retain each outcome."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import subprocess
import time
from urllib.request import urlopen

from tools.kind_local import BIN, ROOT, Cluster, run, write_private
from tools.kind_smoke import smoke
from tools.delivery_backup import export, restore_drill

RUNTIME_ERRORS = (OSError, RuntimeError, ValueError, KeyError, AssertionError, subprocess.SubprocessError)


def capture_inventory(cluster):
    cluster.inspect()
    pods = json.loads(cluster.k('get', 'pods', '-A', '-o', 'json', capture=True))['items']
    rows = []
    for pod in pods:
        metadata = pod['metadata']
        for field in ('initContainerStatuses', 'containerStatuses'):
            for container in pod.get('status', {}).get(field, []):
                rows.append(dict(namespace=metadata['namespace'], pod=metadata['name'],
                                 image=container['image'], image_id=container.get('imageID', '')))
    write_private(cluster.directory / 'runtime-images.json', json.dumps(rows, indent=2) + '\n')


def flink_recovery(cluster):
    # Use the authenticated loopback tunnel. AKS NetworkPolicy intentionally
    # excludes control-plane proxy traffic from the private Flink REST service.
    with cluster.forwards(('aggregation-rest', 18098, 8081)):
        _flink_recovery(cluster)


def _flink_recovery(cluster):
    base = 'http://127.0.0.1:18098/jobs/'

    def query(path):
        with urlopen(base + path, timeout=8) as response:
            return json.load(response)

    job = next((item for item in query('overview')['jobs']
                if item['name'] == 'aggregation-kind' and item['state'] == 'RUNNING'), None)
    if job is None:
        raise RuntimeError('The operator-managed aggregation job is not running')
    job_id = job['jid']
    before = query(job_id + '/checkpoints')['latest']['completed']
    if not before or before['id'] < 1:
        raise RuntimeError('No completed checkpoint available before the TaskManager fault')
    pods = json.loads(cluster.k('get', 'pods', '-l', 'component=taskmanager,grid-app=aggregation',
                               '-o', 'json', capture=True))['items']
    if not pods:
        raise RuntimeError('No aggregation TaskManager to restart')
    old_pod = pods[0]['metadata']['name']
    cluster.k('delete', 'pod/' + old_pod, '--timeout=90s')
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            latest = query(job_id + '/checkpoints')['latest']
            details = query(job_id)
            recovered = latest.get('restored')
            completed = latest.get('completed')
            if (details['state'] == 'RUNNING' and recovered and completed
                    and recovered['id'] >= before['id'] and completed['id'] > recovered['id']):
                evidence = dict(job_id=job_id, removed_taskmanager=old_pod, before=before,
                                restored=recovered, after=completed, state=details['state'])
                write_private(cluster.directory / 'flink-recovery.json', json.dumps(evidence, indent=2) + '\n')
                print('PASS: Flink restored a checkpoint and committed a newer one')
                return
        except (subprocess.CalledProcessError, OSError):
            pass
        time.sleep(3)
    raise RuntimeError('Flink did not report checkpoint restore and newer completion within the deadline')


def investigator(cluster):
    with cluster.forwards(('ui', 18086, 18086), ('api', 18094, 18084), ('copilot', 18097, 18087)):
        subprocess.run([ROOT / '.tools/dashboard-test-venv/bin/python', '-m', 'tools.delivery_browser',
                        cluster.directory], cwd=ROOT, check=True, timeout=150,
                       env=dict(os.environ, PLAYWRIGHT_BROWSERS_PATH=str(ROOT / '.tools/playwright')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='dev', choices=('dev', 'review'))
    parser.add_argument('--name', required=True)
    args = parser.parse_args()
    cluster = Cluster(args.profile, args.name)
    if cluster.name == 'smart-grid-kind-' + cluster.profile:
        parser.error('Use a dedicated named acceptance cluster with a suffix')
    if not cluster.kubeconfig.is_file():
        parser.error('Create, install and deploy the named cluster first')
    if run('docker', 'info', '--format', '{{.OperatingSystem}}', capture=True).strip() != 'Docker Desktop':
        parser.error('Use Docker Desktop')
    directory = cluster.directory
    acceptance = directory / 'acceptance.json'
    if acceptance.exists() or cluster.k('get', 'job/simulator', '--ignore-not-found', '-o', 'name', capture=True):
        parser.error('Use a fresh named cluster so the deterministic scenario actually executes')
    report = dict(cluster=cluster.name, git_commit=run('git', 'rev-parse', 'HEAD', capture=True).strip(),
                  source_dirty=bool(run('git', 'status', '--porcelain', capture=True)),
                  started_at=datetime.now(timezone.utc).isoformat(), provider='MOCK', status='FAIL', gates={})
    sources = sorted(set(run('git', 'ls-files', '-c', '-o', '--exclude-standard', '-z', capture=True).split('\0')) - {''})
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources if (ROOT / name).is_file()}
    write_private(directory / 'source-hashes.json', json.dumps(hashes, indent=2) + '\n')

    def record():
        write_private(acceptance, json.dumps(report, indent=2) + '\n')

    backup = directory / 'authoritative-backup'
    gates = {
        'contracts': lambda: run('make', 'contracts'),
        'mock_provider_failure_and_denials': lambda: run('make', 'copilot-test'),
        'closed_loop_and_projection_replay': lambda: smoke(cluster),
        'mock_investigator': lambda: investigator(cluster),
        'flink_checkpoint_recovery': lambda: flink_recovery(cluster),
        'authoritative_export': lambda: export(cluster, backup),
        'authoritative_restore_drill': lambda: restore_drill(cluster, backup),
        'correlated_observability': lambda: run('env', '-u', 'PROJECTION_DSN',
                                                BIN / 'python', '-m', 'tools.observability', 'incident',
                                                '--context', 'kind-' + cluster.name, '--kubeconfig', cluster.kubeconfig,
                                                '--run', 'delivery_' + datetime.now(timezone.utc).strftime('%y%m%d_%H%M%S')),
        'resource_measurement': lambda: cluster.measure(quiet=True),
        'workload_inventory': lambda: capture_inventory(cluster),
    }
    report['gates'] = dict.fromkeys(gates, 'NOT_RUN')
    record()
    # Stop dependent gates after failure, but still preserve inspection and any
    # independent verification whose prerequisites passed.
    for gate, check in gates.items():
        if gate == 'mock_investigator' and report['gates']['closed_loop_and_projection_replay'] != 'PASS':
            continue
        if gate == 'authoritative_restore_drill' and report['gates']['authoritative_export'] != 'PASS':
            continue
        print(f'=== {gate} ===', flush=True)
        try:
            check()
        except RUNTIME_ERRORS as exc:
            print(f'FAIL {gate}: {exc}', flush=True)
            report['gates'][gate] = 'FAIL'
            report.setdefault('failures', {})[gate] = str(exc)
        else:
            report['gates'][gate] = 'PASS'
        record()
    report['status'] = 'PASS' if all(value == 'PASS' for value in report['gates'].values()) else 'FAIL'
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    record()
    print(f'{report["status"]}: {acceptance}')
    return report['status'] != 'PASS'


if __name__ == '__main__':
    raise SystemExit(main())
