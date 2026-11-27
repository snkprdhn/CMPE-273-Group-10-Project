"""Seeded, bounded SQL-lock incident against the real projection HTTP boundary.

Use the projection venv plus observability/requirements.txt. The stack stays in
local kind; the independent service may run on loopback. No destructive reset.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from projection.store import Store
from observability.evidence import validate_packet

ROOT = Path(__file__).resolve().parents[1]


def request(url, token=None, parent=None):
    headers = {'Authorization':'Bearer '+token} if token else {}
    if parent:
        headers['traceparent'] = parent
        headers['correlation_id'] = 'incident-273'
    started = time.monotonic()
    try:
        response = urlopen(Request(url, headers=headers), timeout=6)
    except HTTPError as exc:
        response = exc
    with response:
        raw = response.read()
        return {'status':response.status, 'seconds':time.monotonic()-started,
            'traceparent':response.headers.get('traceparent'), 'body':json.loads(raw)}


def wait(fn, timeout=45):
    end = time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            result = fn()
            if result:
                return result
        except (HTTPError, URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(.25)
    raise RuntimeError('Collected evidence did not become available before deadline')



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    parser.add_argument('--prometheus', default='http://127.0.0.1:19090')
    parser.add_argument('--tempo', default='http://127.0.0.1:13200')
    parser.add_argument('--port', type=int, default=18094)
    args = parser.parse_args()
    if not re.fullmatch('[a-z][a-z0-9_]{0,24}',args.run):
        parser.error('run must be 1..25 lowercase alphanumerics/underscores')
    if not os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
        parser.error('Set OTEL_EXPORTER_OTLP_ENDPOINT to the local Collector')
    # Bind check before changing durable state; never steal another process's port.
    with socket.socket() as check:
        check.bind(('127.0.0.1', args.port))
    directory = ROOT/'.tools/observability-incidents'/args.run
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    schema = 'incident_'+args.run
    base = os.environ['PROJECTION_DSN']
    with psycopg.connect(base) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    dsn = make_conninfo(base, options='-csearch_path='+schema)
    store = Store(dsn)
    store.migrate()
    fixtures = json.loads((ROOT/'projection/fixtures.json').read_text())
    store.context(fixtures)
    for record in fixtures['events']:
        store.ingest(record['topic'], record['key'], record['event'])
    token = uuid4().hex
    env = dict(os.environ, PROJECTION_DSN=dsn, OTEL_SERVICE_NAME='grid-incident-api',
        AUTH_MODE='mock', MOCK_IDENTITIES=json.dumps({token:{'actor':'incident-viewer','role':'Viewer'}}))
    log_path = directory/'api.jsonl'
    started_at = datetime.now(timezone.utc).isoformat()
    with log_path.open('w') as log:
        process = subprocess.Popen([sys.executable,'-m','projection','serve','--port',str(args.port)],
            cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            url=f'http://127.0.0.1:{args.port}/api/v1/overview'
            wait(lambda: request(url,token)['status']==200)
            baseline = [request(url,token) for _ in range(5)]
            trace_id, parent = uuid4().hex, uuid4().hex[:16]
            traceparent = f'00-{trace_id}-{parent}-01'
            # Seed controls a bounded delay after one timed-out request. Only the
            # run-owned table is locked; all existing schemas/data are preserved.
            hold = random.Random(273).uniform(.2,.4)
            with psycopg.connect(dsn) as lock:
                lock.execute("SET LOCAL idle_in_transaction_session_timeout = '8s'")
                lock.execute('LOCK projection_meta IN ACCESS EXCLUSIVE MODE')
                fault_start = time.monotonic()
                fault = request(url,token,traceparent)
                time.sleep(hold)
                lock.rollback()
            mitigation_at = time.monotonic()
            recovery = [request(url,token) for _ in range(3)]
            recovered_at = time.monotonic()
            if fault['status']!=503:
                raise RuntimeError('Seeded lock did not produce the expected dependency failure')
            # Flush/export/scrape are asynchronous; query the real backends.
            query='sum by (outcome) (grid_api_requests_total{service="grid-incident-api",operation="query"})'
            def metric():
                value=request(args.prometheus+'/api/v1/query?'+urlencode({'query':query}))['body']
                return value if any(r['metric'].get('outcome')=='error' and float(r['value'][1])>=1 for r in value.get('data',{}).get('result',[])) else None
            observed_metric=wait(metric)
            observed_trace=wait(lambda: request(args.tempo+'/api/traces/'+trace_id)['body'])
            logs=[]
            for line in log_path.read_text().splitlines():
                try:
                    row=json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get('trace_id')==trace_id:
                    logs.append(row)
            packet=dict(seed=273, run=args.run, started_at=started_at,
                commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                source_dirty=bool(subprocess.check_output(['git','status','--porcelain'],text=True)),
                profile='local kind collection; independent loopback API + local PostgreSQL',
                hypothesis='A lock on the run-owned projection metadata blocks snapshot reads until the 3-second statement timeout.',
                injected_fault='ACCESS EXCLUSIVE lock on run-owned projection_meta',
                baseline=baseline, fault_response=fault, recovery=recovery,
                metric_query=query, metric=observed_metric, trace_id=trace_id, trace=observed_trace,
                correlated_logs=logs, recovery_seconds=recovered_at-fault_start,
                mitigation_to_recovery_seconds=recovered_at-mitigation_at,
                business_impact={'failed_operator_snapshots':1,'affected_transformer':'tx-001',
                    'snapshot_unavailable_seconds':mitigation_at-fault_start,
                    'control_effects':'No control operation was invoked by this read-only incident.',
                    'baseline_snapshot':baseline[-1]['body'],'recovered_snapshot':recovery[-1]['body']},
                observation='SQL wait, HTTP 503, collected error counter and same-trace database error agree.',
                alternative='This controlled table lock does not establish behavior under database crash or cluster resource exhaustion.',
                missing_signal='Lock-wait root blocker is established by the injector; the span alone cannot identify which session owns it.',
                slo='Proposed query SLO: successful AND <=1s, target 99.5%; small seeded window is not a capacity claim.')
            validate_packet(packet)
            (directory/'packet.json').write_text(json.dumps(packet,indent=2)+'\n')
            print(json.dumps({'packet':str(directory/'packet.json'),'trace_id':trace_id,
                'recovery_seconds':round(packet['recovery_seconds'],3),'result':'PASS'}))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    # Evidence schema is synthetic and retained for inspection. No reset/secret export.


if __name__=='__main__':
    main()
