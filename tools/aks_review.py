"""Publish and verify the one-broker professor review on a dedicated AKS cluster."""
import argparse
import json
import re
from urllib.request import urlopen

from observability.dashboards import configmaps
from tools.delivery import capture_inventory, flink_recovery
from tools.delivery_images import IMAGES
from tools.integration import wait
from tools.kind_local import BIN, Cluster, ROOT, run, write_private
from tools.kind_smoke import browser_check, smoke
from tools.observability import VALUES, charts

DIRECTORY = ROOT / '.tools/aks-review'
KUBECONFIG = DIRECTORY / 'kubeconfig'
CONTEXT = 'smart-grid-aks-review'
SOURCE_IMAGES = DIRECTORY / 'images.json'
PUBLISHED_IMAGES = DIRECTORY / 'published-images.json'
OVERLAY = ROOT / 'infra/aks'
WORKLOAD_IMAGES = {'smart-grid-kind/' + tag for tag, *_ in IMAGES}


def publish(registry, source=SOURCE_IMAGES, destination=PUBLISHED_IMAGES, architecture='amd64'):
    inventory = json.loads(source.read_text())
    if {item['manifest_image'] for item in inventory.values()} != WORKLOAD_IMAGES:
        raise ValueError('Build the complete review image inventory before publishing')
    run('az', 'acr', 'login', '-n', registry.removesuffix('.azurecr.io'))
    images = {}
    for local, item in inventory.items():
        check = run('docker', 'image', 'inspect', '--format', '{{.Id}} {{.Os}}/{{.Architecture}}', local, capture=True)
        if check.strip() != item['id'] + ' linux/' + architecture:
            raise ValueError(f'Rebuild the selected Linux {architecture} image: {local}')
        manifest_image = item['manifest_image']
        target = registry + '/' + manifest_image
        run('docker', 'tag', local, target)
        result = run('docker', 'push', target, capture=True)
        match = re.search(r'digest: (sha256:[a-f0-9]{64})', result)
        if match is None:
            raise RuntimeError('The registry did not report a content digest for ' + target)
        images[manifest_image] = target.split(':')[0] + '@' + match[1]
        print('Published', images[manifest_image], flush=True)
    write_private(destination, json.dumps(images, indent=2) + '\n')


class ReviewCluster(Cluster):
    name = 'smart-grid-aks-review'
    profile = 'aks'
    runtime = 'aks'
    probe_pull_policy = 'IfNotPresent'
    postgres_values = ('--set', 'persistence.storageClass=grid-ssd')

    def __init__(self, registry, host):
        self.directory, self.kubeconfig = DIRECTORY, KUBECONFIG
        self.kubectl = [BIN / 'kubectl', '--kubeconfig', self.kubeconfig, '--context', CONTEXT]
        self.helm = [BIN / 'helm', '--kubeconfig', self.kubeconfig, '--kube-context', CONTEXT]
        self.origin = 'https://' + host
        nodes = json.loads(self.k('get', 'nodes', '-o', 'json', capture=True))['items']
        if len(nodes) != 1 or nodes[0]['status']['nodeInfo']['architecture'] != 'amd64':
            raise ValueError('The review requires exactly one amd64 worker')
        self.node = nodes[0]['metadata']['name']
        self.images = json.loads(PUBLISHED_IMAGES.read_text())
        if self.images.keys() != WORKLOAD_IMAGES or any(
            not re.fullmatch(re.escape(registry + '/' + key.split(':')[0]) + '@sha256:[a-f0-9]{64}', value)
            for key, value in self.images.items()
        ):
            raise ValueError('The published inventory must reference only content digests in the review registry')
        self.probe_image = self.images['smart-grid-kind/acceptance:v1']
        body = run(BIN / 'kubectl', 'kustomize', OVERLAY, capture=True)
        self.manifest = re.sub(r'(?m)(^\s*image: )(smart-grid-kind/\S+)$',
                               lambda match: match[1] + self.images[match[2]], body)

    def stage(self, name, selector=''):
        self.k('apply', '-f', '-', '-l', 'grid-stage=' + name + selector, data=self.manifest)

    def api_policy(self):
        # Cilium tracks the API server identity from the policy in the overlay.
        self.k('get', 'ciliumnetworkpolicy/lease-and-topic-api', '-o', 'name')

    def build(self):
        # Publishing already verified the architecture and resolved immutable registry digests.
        print('Using the published review image inventory', flush=True)

    def install(self):
        self.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'platform'}})
        self.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'review-endpoint', 'namespace': 'platform'},
                    'data': {'origin': self.origin, 'host': self.origin.removeprefix('https://')}})
        self.chart('strimzi', 'strimzi', 'grid-operators')
        self.chart('flink', 'flink-operator', 'grid-operators')
        storage = {
            'monitoring': ['--set', 'grafana.persistence.storageClassName=grid-ssd',
                           '--set', 'prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.storageClassName=grid-ssd'],
            'tempo': ['--set', 'persistence.storageClassName=grid-ssd'],
        }
        self.stage('policy')
        for release, archive, values in charts():
            run(*self.helm, 'upgrade', '--install', release, archive,
                '-n', 'observability', '--create-namespace', '-f', values,
                *storage.get(release, []), '--wait', '--timeout', '7m')
        run(*self.kubectl, 'apply', '-f', '-', data=json.dumps(configmaps()))
        self.k('apply', '-f', VALUES / 'platform.yaml')

    def deploy(self):
        super().deploy()
        self.stage('edge')
        self.k('rollout', 'status', 'deployment/review-edge', '--timeout=240s')
        self.k('wait', '--for=jsonpath={.status.loadBalancer.ingress[0].ip}', 'service/review-edge', '--timeout=240s')

    def verify(self):
        def ready():
            try:
                with urlopen(self.origin + '/readyz', timeout=5) as response:
                    return response.status == 200
            except OSError:
                return False

        wait(ready, 'trusted HTTPS and dashboard readiness', timeout=300)
        smoke(self)
        self.recover()

    def recover(self):
        report = json.loads((self.directory / 'report.json').read_text())
        if report['full_loop'] != 'PASS':
            raise RuntimeError('Complete the seeded scenario and professor browser gates before recovery checks')
        flink_recovery(self)
        self.k('rollout', 'restart', 'statefulset/postgres')
        self.k('rollout', 'status', 'statefulset/postgres', '--timeout=240s')
        for name in ('keycloak', 'projection', 'api', 'copilot', 'ui', 'review-edge'):
            self.k('rollout', 'restart', 'deployment/' + name)
            self.k('rollout', 'status', 'deployment/' + name, '--timeout=360s')
        def ready():
            try:
                with urlopen(self.origin + '/readyz', timeout=5) as response:
                    return response.status == 200
            except OSError:
                return False

        wait(ready, 'HTTPS readiness after workload restarts', timeout=120)
        browser_check(self, after_restart=True)
        self.k('wait', '--for=jsonpath={.status.phase}=Bound', '--all', 'pvc', '--timeout=45s')
        volumes = self.k('get', 'pvc', '-o', 'json', capture=True)
        write_private(self.directory / 'persistent-volumes.json', volumes)
        pods = json.loads(self.k('get', 'pods', '-o', 'json', capture=True))['items']
        requested = {container['image'] for pod in pods for container in pod['spec']['containers']}
        if not set(self.images.values()) <= requested:
            raise RuntimeError('The runtime inventory does not confirm all eight registry pulls')
        capture_inventory(self)
        runtime = json.loads((self.directory / 'runtime-images.json').read_text())
        builds = {item['manifest_image']: item['id'] for item in json.loads(SOURCE_IMAGES.read_text()).values()}
        for name, image in self.images.items():
            expected = {image.split('@')[1], builds[name]}
            if not any(row['image_id'] == image or (row['image'] == image and row['image_id'].split('@')[-1] in expected) for row in runtime):
                raise RuntimeError('No matching runtime digest for ' + name)
        print('PASS: all eight application images match the published AMD64 inventory', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('publish', 'install', 'deploy', 'verify', 'recover'))
    parser.add_argument('--registry', required=True)
    parser.add_argument('--host')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9]{5,50}\.azurecr\.io', args.registry):
        parser.error('Use the login server of the dedicated Azure Container Registry')
    if args.command == 'publish':
        publish(args.registry)
    else:
        if not args.host or not re.fullmatch(r'[a-z0-9-]+\.[a-z0-9-]+\.cloudapp\.azure\.com', args.host):
            parser.error('Provide the reserved Azure public-IP hostname')
        if not KUBECONFIG.is_file():
            parser.error('Create the dedicated review kubeconfig first')
        cluster = ReviewCluster(args.registry, args.host)
        getattr(cluster, args.command)()


if __name__ == '__main__':
    main()
