"""Create, install, deploy and inspect the dedicated local Docker Desktop kind platform."""
import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import time

from identity.realm import realm
from member3.cdc import configuration

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / 'infra/kind'
BIN = ROOT / '.venv/bin'
DOWNLOAD = ROOT / '.tools/kind-downloads'
NODE = 'kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a'
PYTHON_IMAGE = 'smart-grid-kind/acceptance:v1'
CHARTS = {
    'strimzi': ('strimzi-kafka-operator-helm-3-chart-0.45.2.tgz', '8845e9d8b6ec4e7b064cfd384c3d2099350714f3ffc7c02c595f52a1f86d6fa0',
                ['--repo', 'https://strimzi.io/charts/', 'strimzi-kafka-operator', '--version', '0.45.2']),
    'flink': ('flink-kubernetes-operator-1.15.0-helm.tgz', '73948033f014820b832dece5f9f3551b58de6227b8300c590a5db8a6ac7893dc',
              ['--repo', 'https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.15.0/', 'flink-kubernetes-operator', '--version', '1.15.0']),
    'postgres': ('postgres-0.22.0.tgz', 'aac2934c28a7b2ee9a1e3a837d24fe0fec737d797edc14a7b17a326b785da1bf',
                 ['oci://registry-1.docker.io/cloudpirates/postgres', '--version', '0.22.0']),
}


def run(*args, capture=False, data=None):
    argv = list(map(str, args))
    if not capture:
        print('+', ' '.join(argv), flush=True)
    return subprocess.run(argv, cwd=ROOT, check=True, input=data, text=True, capture_output=capture).stdout


def digest(path, expected):
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise RuntimeError(f'Checksum mismatch for {path}. Remove the cached artifact and retry.')
    return path


def download(path, url, expected):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temp = path.with_suffix('.part')
        run('curl', '--fail', '--location', '--retry', '3', '--silent', '--show-error', url, '-o', temp)
        digest(temp, expected).replace(path)
    return digest(path, expected)


def chart_archive(chart):
    filename, checksum, source = CHARTS[chart]
    DOWNLOAD.mkdir(parents=True, exist_ok=True)
    path = DOWNLOAD / filename
    if not path.exists():
        run(BIN / 'helm', 'pull', *source, '--destination', DOWNLOAD)
    return digest(path, checksum)


def calico_manifest(path):
    body = path.read_text()
    edits = {
        'calico_backend: "bird"': 'calico_backend: "vxlan"',
        'value: "k8s,bgp"': 'value: "k8s"',
        'name: CALICO_IPV4POOL_IPIP\n              value: "Always"':
            'name: CALICO_IPV4POOL_IPIP\n              value: "Never"',
        'name: CALICO_IPV4POOL_VXLAN\n              value: "Never"':
            'name: CALICO_IPV4POOL_VXLAN\n              value: "Always"',
        '# - name: CALICO_IPV4POOL_CIDR\n            #   value: "192.168.0.0/16"':
            '- name: CALICO_IPV4POOL_CIDR\n              value: "10.244.0.0/16"',
        '                - -bird-ready\n': '',
        '                - -bird-live\n': '',
    }
    for original, replacement in edits.items():
        if body.count(original) != 1:
            raise RuntimeError('Pinned Calico manifest does not contain a unique patch target')
        body = body.replace(original, replacement)
    return body


def write_private(path, contents):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'w') as file:
        file.write(contents)


def runtime_secrets(passwords, origin=None):
    def dsn(role, database, options=''):
        return f'postgresql://{role}:{passwords[role]}@postgres:5432/{database}{options}'

    schema = '?options=-csearch_path%3Dintegration_kind'
    control = dsn('control', 'local_dev_control', schema)
    projection = dsn('projection', 'local_dev_projection', schema)
    edge = dsn('member1_gateway', 'local_dev_control', '?options=-csearch_path%3Dintegration_edge%2Cintegration_kind')
    admin = dsn('local_admin', 'postgres')
    registry_tokens = {f'REGISTRY_{actor.upper()}_TOKEN': passwords[f'rpc_{actor}'] for actor in ('action', 'api', 'simulator')}
    roles = ('registry', 'control', 'projection', 'keycloak', 'member3_cdc', 'member1_gateway')
    return {
        'postgres-admin': {'postgres-password': passwords['local_admin']},
        'database-bootstrap': {**{role.upper() + '_PASSWORD': passwords[role] for role in roles},
                               'POSTGRES_ADMIN_DSN': admin, 'CONTROL_DSN': control, 'PROJECTION_DSN': projection},
        'registry-runtime': {'REGISTRY_DSN': dsn('registry', 'local_dev_registry'),
                             'REGISTRY_CURSOR_KEY': passwords['cursor'], **registry_tokens},
        'action-runtime': {'CONTROL_DSN': control, 'REGISTRY_ACTION_TOKEN': registry_tokens['REGISTRY_ACTION_TOKEN']},
        'projection-runtime': {'PROJECTION_DSN': projection},
        'simulator-runtime': {'CONTROL_DSN': control, 'EDGE_DSN': edge, 'PROJECTION_DSN': projection,
                              'REGISTRY_SIMULATOR_TOKEN': registry_tokens['REGISTRY_SIMULATOR_TOKEN'],
                              'REGISTRY_API_TOKEN': registry_tokens['REGISTRY_API_TOKEN']},
        'cdc-runtime': {'password': passwords['member3_cdc']},
        'keycloak-runtime': {'KC_DB_PASSWORD': passwords['keycloak']},
        'keycloak-realm': {'realm.json': json.dumps(realm(passwords, origin, review=True) if origin else realm(passwords))},
    }


def connector():
    config = configuration()
    config.update({
        'database.password': '${env:CDC_PASSWORD}',
        'slot.name': 'integration_kind', 'publication.name': 'integration_kind',
        'table.include.list': 'integration_kind.action_outbox', 'topic.prefix': 'integration_kind',
        'transforms.outbox.route.topic.replacement': 'integration-kind.${routedByValue}',
    })
    return {
        'apiVersion': 'kafka.strimzi.io/v1beta2', 'kind': 'KafkaConnector',
        'metadata': {'name': 'integration-kind', 'namespace': 'platform',
                     'labels': {'strimzi.io/cluster': 'grid-connect', 'grid-owner': 'Member_3'}},
        'spec': {'class': config.pop('connector.class'), 'tasksMax': int(config.pop('tasks.max')), 'config': config},
    }


class Cluster:
    origin = None
    probe_image = PYTHON_IMAGE
    probe_pull_policy = 'Never'
    postgres_values = ()
    runtime = 'kind'

    def __init__(self, profile, name=None):
        self.profile = profile
        prefix = 'smart-grid-kind-' + profile
        self.name = name or prefix
        if not re.fullmatch(prefix + r'(-[a-z0-9]{1,16})?', self.name):
            raise ValueError(f'Cluster name must be {prefix} or append a lowercase alphanumeric suffix.')
        self.node = self.name + '-control-plane'
        name = self.name.removeprefix('smart-grid-')
        self.directory = ROOT / '.tools' / name
        self.kubeconfig = self.directory.with_suffix('.kubeconfig')
        self.kubectl = [BIN / 'kubectl', '--kubeconfig', self.kubeconfig, '--context', 'kind-' + self.name]
        self.helm = [BIN / 'helm', '--kubeconfig', self.kubeconfig, '--kube-context', 'kind-' + self.name]

    def k(self, *args, **kwargs):
        return run(*self.kubectl, '-n', 'platform', *args, **kwargs)

    def apply(self, resource, server_side=False):
        flags = ('--server-side',) if server_side else ()
        self.k('apply', *flags, '-f', '-', data=json.dumps(resource))

    def stage(self, name, selector=''):
        self.k('apply', '-k', INFRA / self.profile, '-l', 'grid-stage=' + name + selector)

    def ready(self, *resources, timeout=360):
        self.k('wait', '--for=condition=Ready', *resources, f'--timeout={timeout}s')

    def ready_connector(self, timeout=120):
        self.ready('kafkaconnector/integration-kind', timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                status = json.loads(self.k(
                    '--request-timeout=8s', 'exec', 'grid-connect-connect-0', '--', 'curl',
                    '--fail', '--silent', '--show-error', '--max-time', '3',
                    'http://127.0.0.1:8083/connectors/integration-kind/status', capture=True))
            except subprocess.CalledProcessError:
                time.sleep(2)
                continue
            states = [status['connector']['state'], *(task['state'] for task in status['tasks'])]
            if 'FAILED' in states:
                raise RuntimeError('Debezium failed. Inspect the grid-connect-connect-0 logs.')
            if states == ['RUNNING', 'RUNNING']:
                print('Debezium connector and task are RUNNING')
                return
            time.sleep(2)
        raise RuntimeError('Debezium did not assign a running task before the deadline.')

    def create(self):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.profile == 'review':
            self.review_capacity()
        clusters = run(BIN / 'kind', 'get', 'clusters', capture=True).split()
        if self.name in clusters:
            if run('docker', 'inspect', '-f', '{{.State.Running}}', self.node, capture=True).strip() == 'false':
                run('docker', 'start', self.node)
            run(BIN / 'kind', 'export', 'kubeconfig', '--name', self.name, '--kubeconfig', self.kubeconfig)
        else:
            run(BIN / 'kind', 'create', 'cluster', '--name', self.name, '--image', NODE,
                '--kubeconfig', self.kubeconfig, '--config', INFRA / 'cluster.yaml')
        deadline = time.monotonic() + 180
        while True:
            try:
                self.k('--request-timeout=5s', 'get', '--raw', '/readyz', capture=True)
                break
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        path = download(DOWNLOAD / 'calico.yaml',
                        'https://raw.githubusercontent.com/projectcalico/calico/v3.31.0/manifests/calico.yaml',
                        '1990b1cec14002790718bb960b1b8445eb7b896cd16c32e952309f35a9026981')
        self.k('-n', 'kube-system', 'apply', '-f', '-', data=calico_manifest(path))
        self.k('-n', 'kube-system', 'rollout', 'status', 'daemonset/calico-node', '--timeout=300s')
        self.ready('node/' + self.node, timeout=180)
        self.k('uncordon', self.node)
        self.k('-n', 'kube-system', 'rollout', 'status', 'deployment/coredns', '--timeout=180s')
        # Docker Desktop can assign a different API endpoint after a VM restart.
        if self.k('get', 'networkpolicy/lease-and-topic-api', '--ignore-not-found', '-o', 'name', capture=True).strip():
            self.api_policy()

    def chart(self, chart, release, namespace, *values):
        run(*self.helm, 'upgrade', '--install', release, chart_archive(chart),
            '-n', namespace, '--create-namespace', '-f', INFRA / 'charts' / (chart + '.yaml'),
            *values, '--wait', '--timeout', '7m')

    def install(self):
        self.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'platform'}})
        self.chart('strimzi', 'strimzi', 'grid-operators')
        self.chart('flink', 'flink-operator', 'grid-operators')
        run(BIN / 'python', '-m', 'tools.observability', 'install',
            '--context', 'kind-' + self.name, '--kubeconfig', self.kubeconfig)

    def build(self):
        from tools.delivery_images import build

        repository = 'smart-grid-kind' if self.name == 'smart-grid-kind-' + self.profile else self.name
        images = build(repository, self.directory / 'images.json')
        # Export separately to keep peak memory and temporary disk usage bounded.
        for image, details in images.items():
            run(BIN / 'kind', 'load', 'docker-image', '--name', self.name, image)
            if image != details['manifest_image']:
                run('docker', 'exec', self.node, 'ctr', '-n', 'k8s.io', 'images', 'tag', '--force',
                    'docker.io/' + image, 'docker.io/' + details['manifest_image'])

    def credentials(self):
        path = self.directory / 'local.json'
        if path.exists():
            return json.loads(path.read_text())
        if self.k('get', 'secret/postgres-admin', '--ignore-not-found', '-o', 'name', capture=True):
            raise RuntimeError(f'Restore the original {path} before redeploying this persistent cluster.')
        names = ('local_admin', 'registry', 'control', 'projection', 'keycloak', 'member3_cdc',
                 'member1_gateway', 'rpc_api', 'rpc_action', 'rpc_simulator', 'cursor')
        names += ('professor',) if self.origin else ('viewer', 'operator', 'administrator')
        passwords = {name: secrets.token_urlsafe(24) for name in names}
        write_private(path, json.dumps(passwords, indent=2) + '\n')
        return passwords

    def api_policy(self):
        # Calico's translated destination can be the apiserver endpoint
        # instead of the Service IP. Allow only the actual local endpoints.
        svc = json.loads(self.k('-n', 'default', 'get', 'svc/kubernetes', '-o', 'json', capture=True))
        endpoints = json.loads(self.k('-n', 'default', 'get', 'endpointslices',
                                      '-l', 'kubernetes.io/service-name=kubernetes', '-o', 'json', capture=True))
        ips = set(svc['spec']['clusterIPs'])
        for item in endpoints['items']:
            ips.update(address for endpoint in item['endpoints'] for address in endpoint['addresses'])
        peers = [{'ipBlock': {'cidr': str(ipaddress.ip_network(ip))}} for ip in sorted(ips)]
        self.apply({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
                    'metadata': {'name': 'lease-and-topic-api', 'namespace': 'platform', 'labels': {'grid-owner': 'Member_3'}},
                    'spec': {'podSelector': {'matchExpressions': [{'key': 'grid-app', 'operator': 'In',
                                                                  'values': ['action', 'topic-operator']}]},
                             'policyTypes': ['Egress'], 'egress': [{'to': peers, 'ports': [
                                 {'port': 443, 'protocol': 'TCP'}, {'port': 6443, 'protocol': 'TCP'}]}]}})

    def provision(self):
        for name, values in runtime_secrets(self.credentials(), self.origin).items():
            self.apply({'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
                        'metadata': {'name': name, 'namespace': 'platform'},
                        'data': {k: base64.b64encode(v.encode()).decode() for k, v in values.items()}}, server_side=True)
        self.stage('policy')
        self.api_policy()
        self.chart('postgres', 'postgres', 'platform', *self.postgres_values)
        # The idempotent bootstrap also applies additive control/projection migrations.
        self.k('delete', 'job/database-bootstrap', '--ignore-not-found', '--timeout=60s')
        self.stage('bootstrap')
        self.k('wait', '--for=condition=Complete', 'job/database-bootstrap', '--timeout=160s')

    def deploy(self):
        self.build()
        self.provision()
        self.stage('kafka')
        self.ready('kafka/grid')
        self.stage('topics')
        self.k('wait', '--for=condition=Ready', 'kafkatopic', '--all', '--timeout=180s')
        self.stage('connect')
        self.ready('kafkaconnect/grid-connect')
        self.apply(connector())
        self.ready_connector()
        self.stage('flink')
        self.k('wait', '--for=jsonpath={.status.jobStatus.state}=RUNNING',
               'flinkdeployment/aggregation', '--timeout=360s')
        self.apps()

    def apps(self):
        # Bring up dependencies before starting the next pod on the local node.
        self.stage('apps', ',!grid-app')
        for name in ('registry', 'action', 'projection', 'api', 'keycloak', 'copilot', 'ui'):
            self.stage('apps', ',grid-app=' + name)
            self.k('rollout', 'status', 'deployment/' + name, '--timeout=360s')

    def inspect(self):
        self.k('get', 'pods,svc,pvc,netpol,kafka,kafkatopic,kafkaconnect,kafkaconnector,flinkdeployment', '-L', 'grid-owner')
        self.k('-n', 'grid-operators', 'get', 'deploy,pods', '-L', 'grid-owner')

    def stop(self):
        if run('docker', 'inspect', '-f', '{{.State.Running}}', self.node, capture=True).strip() != 'true':
            print(f'{self.name} is already stopped')
            return
        try:
            self.k('--request-timeout=10s', 'cordon', self.node)
            # Let workers flush and release authority before their Kafka and SQL dependencies.
            for group in ('simulator,ui,copilot,api,projection,action,registry,keycloak,aggregation,connect,topic-operator',
                          'kafka,postgres'):
                self.k('--request-timeout=15s', 'delete', 'pods', '-l', f'grid-app in ({group})',
                       '--ignore-not-found', '--timeout=75s')
        except subprocess.CalledProcessError:
            print('Kubernetes workload shutdown did not complete. Attempting bounded Docker node shutdown.',
                  file=sys.stderr)
        run('docker', 'stop', '--timeout', '60', self.node)

    def measure(self, quiet=False):
        stats = json.loads(self.k('get', '--raw', f'/api/v1/nodes/{self.node}/proxy/stats/summary', capture=True))
        def sample(entity):
            cpu = entity.get('cpu', {}).get('usageNanoCores')
            working_set = entity.get('memory', {}).get('workingSetBytes')
            return {'cpu_millicores': round(cpu / 1e6) if cpu is not None else None,
                    'memory_mib': round(working_set / 2**20) if working_set is not None else None}
        result = {'time': datetime.now(timezone.utc).isoformat(), 'profile': self.profile,
                  'node': sample(stats['node']),
                  'pods': {pod['podRef']['namespace'] + '/' + pod['podRef']['name']: sample(pod) for pod in stats['pods']}}
        if self.runtime == 'kind':
            cpus, memory = map(int, run('docker', 'info', '--format', '{{.NCPU}} {{.MemTotal}}', capture=True).split())
            result.update(docker_cpus=cpus, docker_memory_mib=round(memory / 2**20))
        with (self.directory / 'resources.jsonl').open('a') as output:
            output.write(json.dumps(result) + '\n')
        if not quiet:
            print(json.dumps(result, indent=2))

    def review_capacity(self):
        dev = ROOT / '.tools/kind-dev'
        if not (dev / 'resources.jsonl').is_file():
            raise RuntimeError('Measure the lightweight deployment with make kind-measure before creating review.')
        workload_prefixes = ('action-', 'aggregation-taskmanager-', 'api-', 'grid-connect-connect-', 'keycloak-', 'ui-')
        samples = (json.loads(line)['pods'] for line in (dev / 'resources.jsonl').read_text().splitlines())
        if not any(all(any(key.startswith('platform/' + prefix) and metrics['memory_mib']
                           for key, metrics in pods.items()) for prefix in workload_prefixes) for pods in samples):
            raise RuntimeError('Measure a running lightweight deployment with all platform workloads before review.')
        running = run('docker', 'ps', '--format', '{{.Names}}', capture=True).split()
        if any(node.startswith('smart-grid-kind-dev-') and node.endswith('-control-plane') for node in running):
            raise RuntimeError('Stop the lightweight cluster before running review to avoid competing workloads.')

    @contextmanager
    def forwards(self, *services):
        processes = []
        try:
            for service, local, remote in services:
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', local))
                log = (self.directory / (service + '-forward.log')).open('w')
                command = [*self.kubectl, '-n', 'platform', 'port-forward', '--address', '127.0.0.1',
                           'svc/' + service, f'{local}:{remote}']
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                processes.append((process, log))
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError(f'{service} port forward exited. See {log.name}')
                    try:
                        with socket.create_connection(('127.0.0.1', local), .3):
                            break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise RuntimeError(f'{service} port forward did not start. See {log.name}')
            yield
        finally:
            for process, log in reversed(processes):
                process.terminate()
                process.wait(timeout=10)
                log.close()

    def open_ui(self):
        self.k('port-forward', '--address', '127.0.0.1', 'svc/ui', '18086:18086')

    def reset(self):
        run(BIN / 'kind', 'delete', 'cluster', '--name', self.name, '--kubeconfig', self.kubeconfig)
        from tools.delivery_images import IMAGES

        prefix = self.name + '/'
        tags = {prefix + tag for tag, *_ in IMAGES}
        present = set(run('docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}}', capture=True).split())
        if tags & present:
            run('docker', 'image', 'rm', *sorted(tags & present))
        print(f'Kept private evidence in {self.directory}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['create', 'install', 'deploy', 'inspect', 'stop', 'reset', 'smoke', 'policy', 'measure', 'open-ui'])
    parser.add_argument('--profile', choices=['dev', 'review'], default='dev')
    parser.add_argument('--name', help='Optional profile-specific cluster name with isolated state')
    parser.add_argument('--confirm', help='Repeat the exact named cluster for destructive reset')
    args = parser.parse_args()
    try:
        cluster = Cluster(args.profile, args.name)
        if args.command == 'reset' and (args.confirm != cluster.name or cluster.name == 'smart-grid-kind-' + cluster.profile):
            parser.error('Reset requires a dedicated cluster name with a suffix and matching --confirm')
        if run('docker', 'info', '--format', '{{.OperatingSystem}}', capture=True).strip() != 'Docker Desktop':
            raise RuntimeError('Select the Docker Desktop engine for the local kind profile.')
        if args.command == 'smoke':
            from tools.kind_smoke import smoke
            smoke(cluster)
        elif args.command == 'policy':
            from tools.kind_smoke import enforce_network
            enforce_network(cluster)
        else:
            getattr(cluster, args.command.replace('-', '_'))()
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(exc, file=sys.stderr)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            print(exc.stderr.strip(), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
