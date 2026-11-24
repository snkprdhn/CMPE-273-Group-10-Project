"""Run the existing kind business scenario with gradual reporting-rate load.

Requires a freshly deployed dedicated cluster with no simulator/control history.
Raw evidence stays private under .tools/load-test/<run>. No history is cleared.
"""
import argparse
from contextlib import ExitStack
import os
import math
import base64
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import time
from urllib.parse import quote

from tools.integration import GATES, evaluate
from tools.kind_local import Cluster, write_private


def resources(raw):
    """kubectl dry-run may print adjacent objects for a multi-document file."""
    result=[]
    decoder=json.JSONDecoder()
    while raw.strip():
        value,end=decoder.raw_decode(raw.lstrip())
        result.extend(value['items'] if value.get('kind')=='List' else [value])
        raw=raw.lstrip()[end:]
    return result


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    import math
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize(evidence):
    samples = evidence.get('load_samples', [])
    stages = []
    for low, high, target in [(0,60,3),(60,120,9),(120,180,30)]:
        selected = [s for s in samples if low <= s['elapsed'] < high]
        stages.append(dict(start=low, end=high, target_eps=target,
                           offered_count=sum(s['count'] for s in selected),
                           offered_eps=sum(s['count'] for s in selected)/60,
                           payload_bytes=[s['bytes']/s['count'] for s in selected]))
    def instant(value):
        return datetime.fromisoformat(value.replace('Z','+00:00'))
    arrivals = {v['event_id']: instant(v['observed_at']) for v in evidence.get('received', [])}
    delays = [(arrivals[e['event_id']]-instant(e['payload']['window_end'])).total_seconds()
              for e in evidence.get('loads', []) if e['event_id'] in arrivals]
    commands = {e['payload']['command_id']:e for e in evidence.get('commands', [])}
    ack = [(instant(e['payload']['result_time'])-instant(commands[e['payload']['command_id']]['payload']['issued_at'])).total_seconds()
           for e in evidence.get('results', []) if e['payload']['outcome']=='APPLIED' and e['payload']['command_id'] in commands]
    loads = evidence.get('loads', [])
    critical = [e for e in loads if e['payload']['net_load_kw'] > 20 and e['event_id'] in arrivals]
    safe = [e for e in loads if math.isclose(e['payload']['ev_demand_kw'],2,abs_tol=1e-9) and e['payload']['net_load_kw'] <= 20 and e['event_id'] in arrivals]
    first_critical = min((arrivals[e['event_id']] for e in critical), default=None)
    first_safe = min((arrivals[e['event_id']] for e in safe), default=None)
    actions = evidence.get('actions', [])
    triggered = [e for e in actions if e['payload']['status']=='PLAN_CREATED' and e['event_id'] in arrivals]
    first_telemetry = min((instant(e['event_time']) for e in evidence.get('telemetry', []) if e['payload']['device_type']=='SMART_METER' and e['payload']['power_kw']==15), default=None)
    first_action = min((arrivals[e['event_id']] for e in triggered), default=None)
    business = dict(overload_kw=max((e['payload']['net_load_kw'] for e in critical),default=None),
        observed_ev_reduction_kw=critical[0]['payload']['ev_demand_kw']-safe[0]['payload']['ev_demand_kw'] if safe and critical else None,
        critical_to_safe_observed_seconds=(first_safe-first_critical).total_seconds() if first_safe and first_critical else None,
        telemetry_to_action_observed_seconds=(first_action-first_telemetry).total_seconds() if first_action and first_telemetry else None,
        completed=any(e['payload']['status']=='COMPLETED' for e in actions))
    return dict(business=business, stages=stages, window_delay_seconds=dict(count=len(delays),p50=percentile(delays,.5),p95=percentile(delays,.95),p99=percentile(delays,.99)),
                acknowledgement_seconds=ack,
                loads=[e['payload'] for e in evidence.get('loads', [])],
                edge_effects=evidence.get('edge_effects'),edge_rate=evidence.get('edge_rate'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--run', required=True)
    parser.add_argument('--http-only', action='store_true', help='160-second populated read baseline; no telemetry/capacity claim')
    args=parser.parse_args()
    directory=Path('.tools/load-test')/args.run
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    cluster=Cluster('dev', args.cluster)
    report=dict(cluster=args.cluster,seed=273,planned_duration_seconds=180,status='FAIL',commands=[],samples=[],missing=['agent concurrency load', 'negative exporter lag is retained without clamping', 'SSE browser render acknowledgement'])
    def query(*command):
        report['commands'].append(list(command))
        return json.loads(cluster.k('--request-timeout=15s',*command,capture=True))
    report['git_commit']=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    report['source_sha256']={name:hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in ('tools/integration.py','tools/kind_workload.py','tools/load_test.py','tools/load_http.py','requirements-load.txt')}
    evidence={}
    stack=ExitStack()
    load_process=None
    token_path=directory/'token'
    try:
        for name, command in [('nodes',('get','nodes','-o','json')),('pods',('get','pods','-o','json')),('topics',('get','kafkatopics','-o','json')),('flink',('get','flinkdeployment/aggregation','-o','json'))]:
            data=query(*command)
            # Pod specs can reference secrets; capture image identities/resources only.
            if name=='pods':
                data=[dict(name=p['metadata']['name'],containers=[dict(name=c['name'],image=c['image'],resources=c.get('resources')) for c in p['spec']['containers']],status=p.get('status')) for p in data['items']]
            write_private(directory/(name+'.json'),json.dumps(data,indent=2))
        stack.enter_context(cluster.forwards(('ui',18086,18086),('api',18094,18084)))
        env=dict(os.environ, PLAYWRIGHT_BROWSERS_PATH=str(Path('.tools/playwright').resolve()))
        acquire_token = ['.tools/dashboard-test-venv/bin/python', '-m', 'tools.load_identity',
                         str(token_path), str(cluster.directory / 'local.json')]
        subprocess.run(acquire_token, env=env, check=True, timeout=60)
        env['GRID_LOAD_TOKEN_FILE']=str(token_path.resolve())
        logfile=stack.enter_context((directory/'locust.log').open('w'))
        load_process=subprocess.Popen(['.tools/load-venv/bin/locust','-f','tools/load_http.py','--headless','--host','http://127.0.0.1:18094','--users','3','--spawn-rate','1','--run-time','200s','--csv',str(directory/'http'),'--csv-full-history'],env=env,stdout=logfile,stderr=subprocess.STDOUT)
        if args.http_only:
            start=time.monotonic()
            next_auth=start+60
            while time.monotonic()-start < 160:
                if time.monotonic() >= next_auth:
                    subprocess.run(acquire_token, env=env, check=True, timeout=60)
                    report.setdefault('reauth_elapsed',[]).append(time.monotonic()-start)
                    next_auth=time.monotonic()+60
                time.sleep(5)
            report['status']='HTTP_BASELINE_ONLY'
        else:
            # Create a new Job only. A previously run simulator is an error, never replayed.
            rendered=resources(cluster.k('create','--dry-run=client','-f','infra/kind/base/jobs.yaml','-o','json',capture=True))
            job=next(v for v in rendered if v['metadata']['name']=='simulator')
            job['metadata']['namespace']='platform'
            job['spec']['activeDeadlineSeconds']=300
            job['spec']['template']['spec']['containers'][0]['env']=[dict(name='GRID_LOAD_STAGES',value='1')]
            cluster.k('create','-f','-',data=json.dumps(job))
            start=time.monotonic()
            next_auth=start+60
            while time.monotonic()-start<320:
                if time.monotonic() >= next_auth:
                    subprocess.run(acquire_token, env=env, check=True, timeout=60)
                    next_auth=time.monotonic()+60
                state=query('get','job/simulator','-o','json')['status']
                try:
                    stats=query('get','--raw',f'/api/v1/nodes/{cluster.node}/proxy/stats/summary')
                    report['samples'].append(dict(elapsed=time.monotonic()-start,node=stats['node'],pods=stats['pods']))
                    flink=query('get','--raw','/api/v1/namespaces/platform/services/aggregation-rest:8081/proxy/jobs/overview')
                    for job_state in flink['jobs']:
                        if job_state['state']=='RUNNING':
                            checkpoints=query('get','--raw','/api/v1/namespaces/platform/services/aggregation-rest:8081/proxy/jobs/'+job_state['jid']+'/checkpoints')
                            report['samples'][-1]['checkpoints']=checkpoints
                    expression='{__name__=~"kafka_consumergroup_lag|flink_taskmanager_job_task_operator_watermark_delay_ms|flink_taskmanager_job_task_backPressuredTimeMsPerSecond"}'
                    report['samples'][-1]['stream_metrics']=query('get','--raw','/api/v1/namespaces/observability/services/monitoring-prometheus:9090/proxy/api/v1/query?query='+quote(expression,safe=''))
                except subprocess.SubprocessError as exc:
                    report.setdefault('sampling_errors',[]).append(type(exc).__name__)
                if state.get('succeeded') or state.get('failed'):
                    break
                time.sleep(10)
            logs=cluster.k('logs','job/simulator',capture=True)
            write_private(directory/'simulator.log',logs)
            chunks=[line.removeprefix('KIND_EVIDENCE_CHUNK ') for line in logs.splitlines() if line.startswith('KIND_EVIDENCE_CHUNK ')]
            if chunks:
                evidence=json.loads(gzip.decompress(base64.b64decode(''.join(chunks),validate=True)))
            write_private(directory/'evidence.json',json.dumps(evidence))
            report['measurements']=summarize(evidence)
            for gate in GATES[:3]:
                evaluate(gate,evidence)
            if not state.get('succeeded'):
                raise RuntimeError('simulator job did not succeed')
            report['status']='BUSINESS_PASS_PARTIAL_MEASUREMENTS'
    except Exception as exc:
        report['failure']=dict(type=type(exc).__name__,reason=str(exc))
    finally:
        if load_process:
            if load_process.poll() is None:
                load_process.terminate()
            try:
                report['locust_exit_code']=load_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                load_process.kill()
                report['locust_exit_code']=load_process.wait(timeout=5)
        if load_process and report.get('locust_exit_code') not in (0,-15):
            report['status']='FAIL'
        csv_path=directory/'http_stats.csv'
        if csv_path.exists():
            with csv_path.open() as file:
                report['http_stats']=list(csv.DictReader(file))
            failures=[row for row in report['http_stats'] if int(row.get('Failure Count','0'))>0]
            if failures:
                report['status']='FAIL'
                report['http_failure']='Locust recorded request/SSE failures; inspect retained CSV and logs'
        elif load_process:
            report['status']='FAIL'
            report['http_failure']='Locust statistics missing'
        stack.close()
        token_path.with_suffix('.next').unlink(missing_ok=True)
        token_path.unlink(missing_ok=True)
        write_private(directory/'report.json',json.dumps(report,indent=2))
    print(json.dumps(dict(status=report['status'],report=str(directory/'report.json'))))
    return 0 if report['status'] in ('BUSINESS_PASS_PARTIAL_MEASUREMENTS','HTTP_BASELINE_ONLY') else 1


if __name__=='__main__':
    raise SystemExit(main())
