"""Pre-demo and soak checklist for the hosted control room. Read-only.

    .venv/bin/python -m tools.demo_check            # prints PASS / WARN / FAIL per check, exit 1 on any FAIL
    .venv/bin/python -m tools.demo_check --json     # machine-readable

Public checks use only the HTTPS site. Cluster checks use the private review kubeconfig.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
KUBECONFIG, CONTEXT = ROOT / '.tools/aks-review/kubeconfig', 'smart-grid-aks-review'
HOST = 'smart-grid-ev-review-9e9.denmarkeast.cloudapp.azure.com'
EXPECTED = ('action', 'aggregation', 'api', 'fleet', 'grid-brokers-0', 'grid-connect-connect-0', 'postgres-0', 'projection', 'registry', 'review-edge', 'ui')
results = []


def check(name, status, detail=''):
    results.append(dict(check=name, status=status, detail=detail))


def kubectl(*args):
    return subprocess.run([str(ROOT / '.venv/bin/kubectl'), '--kubeconfig', str(KUBECONFIG), '--context', CONTEXT, '-n', 'platform', *args],
                          capture_output=True, text=True, timeout=60, check=True).stdout


def get(path, timeout=15):
    with urlopen(Request(f'https://{HOST}{path}'), timeout=timeout) as response:
        return response.status, response.read()


def prom(expr):
    body = json.dumps({'queries': [{'refId': 'A', 'datasource': {'type': 'prometheus', 'uid': 'prometheus'}, 'expr': expr, 'instant': True}],
                       'from': 'now-5m', 'to': 'now'}).encode()
    request = Request(f'https://{HOST}/grafana/api/ds/query', body, {'Content-Type': 'application/json'})
    with urlopen(request, timeout=20) as response:
        frames = json.load(response)['results']['A'].get('frames', [])
    values = [f['data']['values'][1][-1] for f in frames if len(f['data']['values']) > 1 and f['data']['values'][1]]
    return values[0] if values else None


def public():
    for path, needle in (('/readyz', None), ('/live', b'GridBalance'), ('/operator', None), ('/grafana/api/health', b'"ok"')):
        try:
            status, body = get(path)
            check('public ' + path, 'PASS' if status == 200 and (needle is None or needle in body) else 'FAIL', f'HTTP {status}')
        except Exception as exc:
            check('public ' + path, 'FAIL', type(exc).__name__)
    try:
        net, capacity = prom('sum(max by (transformer) (grid_business_kw{signal="net_load_kw"}))'), prom('sum(max by (transformer) (grid_business_kw{signal="capacity_kw"}))')
        check('fleet data flowing', 'PASS' if net and capacity and capacity > 1000 else 'FAIL', f'net {net} kW of {capacity} kW')
        age = prom('time() - max(flink_taskmanager_job_task_operator_latest_output_ms) / 1000')
        check('output freshness', 'PASS' if age is not None and age < 20 else 'FAIL', f'{age:.1f} s' if age is not None else 'no data')
        lag = prom('sum(kafka_consumergroup_lag{consumergroup=~"agg-fleet.*",topic=~".*telemetry.v1"})')
        check('flink consumer lag', 'PASS' if lag is not None and lag < 2000 else 'WARN', f'{lag}')
        busy = prom('max(flink_taskmanager_job_task_busyTimeMsPerSecond) / 10')
        check('flink busy', 'PASS' if busy is not None and busy < 70 else 'WARN', f'{busy}%')
        incidents = prom('sum(increase(grid_domain_outcomes_total{outcome="COMPLETED"}[10m]))')
        check('actions completing', 'PASS' if incidents and incidents >= 1 else 'WARN', f'{incidents} completed in 10 min')
    except Exception as exc:
        check('prometheus via grafana', 'FAIL', type(exc).__name__)


def cluster():
    try:
        pods = json.loads(kubectl('get', 'pods', '-o', 'json'))['items']
    except Exception as exc:
        check('cluster access', 'FAIL', type(exc).__name__)
        return
    for expected in EXPECTED:
        match = [p for p in pods if p['metadata']['name'].startswith(expected) and p['status']['phase'] == 'Running']
        ready = [p for p in match if all(c.get('ready') for c in p['status'].get('containerStatuses', []))]
        restarts = sum(c['restartCount'] for p in match for c in p['status'].get('containerStatuses', []))
        check('pod ' + expected, 'PASS' if ready else 'FAIL', f'{len(ready)} ready, {restarts} restarts')
    flink = kubectl('get', 'flinkdeployment', 'aggregation', '-o', 'jsonpath={.status.lifecycleState}/{.status.jobStatus.state}')
    check('flink job', 'PASS' if flink == 'STABLE/RUNNING' else 'FAIL', flink)
    connector = json.loads(kubectl('exec', 'grid-connect-connect-0', '--', 'curl', '-fsS', '--max-time', '5',
                                   'http://127.0.0.1:8083/connectors/integration-kind/status'))
    states = [connector['connector']['state']] + [t['state'] for t in connector['tasks']]
    check('debezium connector and task', 'PASS' if set(states) == {'RUNNING'} else 'FAIL', ' '.join(states))
    sql = ("select 'slot_lag_mb', coalesce(round(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))/1048576),0) from pg_replication_slots "
           "union all select 'wal_mb', round(sum(size)/1048576) from pg_ls_waldir()")
    out = kubectl('exec', 'postgres-0', '--', 'sh', '-c', f'PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d postgres -At -F, -c "{sql}"')
    values = dict(line.split(',') for line in out.strip().splitlines())
    check('postgres replication slot lag', 'PASS' if float(values['slot_lag_mb']) < 100 else 'FAIL', values['slot_lag_mb'] + ' MB')
    check('postgres wal size', 'PASS' if float(values['wal_mb']) < 700 else 'WARN', values['wal_mb'] + ' MB')
    for claim, limit in (('data-0-grid-brokers-0', 75), ('data-postgres-0', 70), ('checkpoints', 70)):
        used = prom(f'100 * kubelet_volume_stats_used_bytes{{persistentvolumeclaim="{claim}"}} / kubelet_volume_stats_capacity_bytes{{persistentvolumeclaim="{claim}"}}')
        check('volume ' + claim, 'PASS' if used is not None and used < limit else 'WARN', f'{used:.0f}%' if used is not None else 'no data')
    held = kubectl('exec', 'postgres-0', '--', 'sh', '-c',
                   'PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d local_dev_control -At -c '
                   '"select count(*) from integration_kind.action_plans where active and state->>\'status\'=\'NEEDS_OPERATOR_REVIEW\'"').strip()
    check('plans held for review', 'PASS' if held == '0' else 'WARN', held + ' (compensation retries them)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--public-only', action='store_true')
    args = parser.parse_args()
    public()
    if not args.public_only:
        cluster()
    if args.json:
        print(json.dumps(results, indent=1))
    else:
        for row in results:
            print(f"{row['status']:5} {row['check']:34} {row['detail']}")
        print(time.strftime('checked %Y-%m-%d %H:%M:%S %Z'))
    sys.exit(1 if any(r['status'] == 'FAIL' for r in results) else 0)


if __name__ == '__main__':
    main()
