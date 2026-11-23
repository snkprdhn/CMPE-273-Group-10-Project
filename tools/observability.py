"""Install/inspect the approved local Helm stack; never switches current context."""
import argparse
import base64
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import subprocess
import socket
import sys
import time
import os
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / '.venv/bin'
VALUES = ROOT / 'infra/observability'
CACHE = ROOT / '.tools/kind-downloads'
CHARTS = (
    ('monitoring', 'kube-prometheus-stack', '69.8.2', 'https://prometheus-community.github.io/helm-charts',
     'cc531ea8291ceb5fe332d5fa4c9b56a214face960a82fb7f7abae42b5d52ccf9', 'prometheus'),
    ('tempo', 'tempo', '1.18.2', 'https://grafana.github.io/helm-charts',
     'a4a3478378124140eae84b58e3b58fc148185577c697865b23bc20bf04b0b4fc', 'tempo'),
    ('otel', 'opentelemetry-collector', '0.117.3', 'https://open-telemetry.github.io/opentelemetry-helm-charts',
     '13696412fce5a06192191c43e6f7a91d87f1fcc51d159ca1b19c776ab435c093', 'collector'))


def run(*args, data=None, capture=False):
    return subprocess.run(list(map(str, args)), input=data, text=True, cwd=ROOT,
                          capture_output=capture, check=True).stdout


def charts():
    CACHE.mkdir(parents=True, exist_ok=True)
    for release, name, version, repo, expected, values in CHARTS:
        archive = CACHE / f'{name}-{version}.tgz'
        if not archive.exists():
            run(BIN/'helm', 'pull', name, '--repo', repo, '--version', version, '--destination', CACHE)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
            raise ValueError('chart checksum mismatch: '+name)
        yield release, archive, VALUES/(values+'.yaml')


@contextmanager
def forwards(k, postgres=False):
    processes = []
    try:
        services = [('observability','otel',14318,4318),('observability','monitoring-prometheus',19090,9090),('observability','tempo',13200,3100)]
        if postgres:
            services.append(('platform','postgres',15434,5432))
        for namespace, service, local, remote in services:
            with socket.socket() as check:
                check.bind(('127.0.0.1',local))
            process = subprocess.Popen([*map(str,k),'-n',namespace,'port-forward','--address','127.0.0.1',
                'svc/'+service,f'{local}:{remote}'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            processes.append(process)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError('Port forward failed: '+service)
                try:
                    with socket.create_connection(('127.0.0.1',local),.2):
                        break
                except OSError:
                    time.sleep(.1)
            else:
                raise RuntimeError('Port forward readiness timeout: '+service)
        yield
    finally:
        for process in reversed(processes):
            process.terminate()
            process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['render', 'install', 'inspect', 'incident', 'open-grafana'])
    parser.add_argument('--context', default='kind-smart-grid-kind-dev')
    parser.add_argument('--kubeconfig')
    parser.add_argument('--run', default='incident_'+time.strftime('%Y%m%d_%H%M%S'))
    args = parser.parse_args()
    if not args.context.startswith('kind-smart-grid-'):
        parser.error('Only named local smart-grid kind contexts are allowed')
    connection = ['--kubeconfig', args.kubeconfig] if args.kubeconfig else []
    k = [BIN/'kubectl', *connection, '--context', args.context]
    helm = [BIN/'helm', *connection, '--kube-context', args.context]
    if args.command == 'render':
        directory = ROOT/'.tools/observability-rendered'
        directory.mkdir(exist_ok=True)
        for release, archive, values in charts():
            (directory/(release+'.yaml')).write_text(run(*helm, 'template', release, archive,
                '--namespace', 'observability', '-f', values, capture=True))
        print(directory)
        return
    if run('docker', 'info', '--format', '{{.OperatingSystem}}', capture=True).strip() != 'Docker Desktop':
        raise ValueError('Use the selected Docker Desktop engine')
    if args.command == 'open-grafana':
        run(*k,'-n','observability','port-forward','--address','127.0.0.1','svc/monitoring-grafana','13000:80')
        return
    if args.command == 'incident':
        local_database = not os.getenv('PROJECTION_DSN')
        with forwards(k, postgres=local_database):
            env = dict(os.environ, OTEL_EXPORTER_OTLP_ENDPOINT='http://127.0.0.1:14318')
            if local_database:
                # Credentials remain in memory/environment, never command arguments or evidence.
                secret = json.loads(run(*k,'-n','platform','get','secret/projection-runtime','-o','json',capture=True))
                original = urlsplit(base64.b64decode(secret['data']['PROJECTION_DSN']).decode())
                env['PROJECTION_DSN'] = urlunsplit(original._replace(netloc=original.netloc.rsplit('@',1)[0]+'@127.0.0.1:15434'))
            subprocess.run([str(ROOT/'.tools/member4-venv/bin/python'),'-m','tools.observability_incident',
                '--run',args.run],cwd=ROOT,env=env,check=True)
            packet_path = ROOT/'.tools/observability-incidents'/args.run/'packet.json'
            packet = json.loads(packet_path.read_text())
            pods = json.loads(run(*k,'get','pods','-A','-o','json',capture=True))
            packet['collection_context'] = args.context
            packet['images'] = [{'namespace':p['metadata']['namespace'],'pod':p['metadata']['name'],
                'containers':[{'image':c['image'],'image_id':c.get('imageID')} for c in p['status'].get('containerStatuses',[])]}
                for p in pods['items'] if p['metadata']['namespace'] in ('platform','observability')]
            packet_path.write_text(json.dumps(packet,indent=2)+'\n')
        return
    if args.command == 'install':
        # Resolve namespace declaratively and keep generated Grafana credentials private.
        run(*k, 'apply', '-f', '-', data=json.dumps({'apiVersion':'v1','kind':'Namespace',
            'metadata':{'name':'observability','labels':{'grid-owner':'Member_1'}}}))
        for release, archive, values in charts():
            run(*helm, 'upgrade', '--install', release, archive, '-n', 'observability',
                '-f', values, '--wait', '--timeout', '5m')
        from observability.dashboards import configmaps
        run(*k, 'apply', '-f', '-', data=json.dumps(configmaps()))
        platform = run(*k, 'get', 'namespace/platform', '--ignore-not-found', '-o', 'name', capture=True)
        if platform:
            run(*k, 'apply', '-f', VALUES/'platform.yaml')
        # Strimzi owns the exporter image and lifecycle; no new Kafka client/exporter.
        kafka = json.loads(run(*k, '-n','platform','get','kafka/grid','--ignore-not-found','-o','json', capture=True) or '{}') if platform else {}
        if kafka:
            patch = {'spec':{'kafkaExporter':{'groupRegex':r'integration-kind\..*|m2-kind-aggregation-v1','topicRegex':r'integration-kind\.grid\..*\.v1',
                'resources':{'requests':{'cpu':'20m','memory':'32Mi'},'limits':{'cpu':'200m','memory':'128Mi'}},
                'template':{'pod':{'metadata':{'labels':{'grid-app':'kafka-exporter','grid-owner':'Member_2'}}}}}}}
            run(*k,'-n','platform','patch','kafka/grid','--type=merge','-p',json.dumps(patch))
    run(*k, '-n', 'observability', 'get', 'pods,svc,pvc', '-L', 'grid-owner')


if __name__ == '__main__':
    main()
