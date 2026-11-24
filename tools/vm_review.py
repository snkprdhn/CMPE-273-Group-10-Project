"""Deploy the hosted review onto a dedicated kind cluster over a loopback SSH tunnel."""
import argparse
import json
import re
from urllib.request import urlopen

from tools.delivery import capture_inventory, flink_recovery
from tools.integration import wait
from tools.kind_local import BIN, DOWNLOAD, ROOT, Cluster, calico_manifest, download, write_private
from tools.kind_smoke import browser_check, smoke

NAME = 'smart-grid-kind-dev-professor2'
DIRECTORY = ROOT / '.tools/vm-review'
KUBECONFIG = DIRECTORY / 'kubeconfig'
OVERLAY = ROOT / 'infra/hosted'


class VMReview(Cluster):
    runtime = 'azure-vm'

    def __init__(self, host):
        super().__init__('dev', NAME)
        self.directory = DIRECTORY
        self.kubeconfig = KUBECONFIG
        self.kubectl = [BIN / 'kubectl', '--kubeconfig', KUBECONFIG, '--context', 'kind-' + NAME]
        self.helm = [BIN / 'helm', '--kubeconfig', KUBECONFIG, '--kube-context', 'kind-' + NAME]
        self.origin = 'https://' + host

    def network(self):
        path = download(DOWNLOAD / 'calico.yaml',
                        'https://raw.githubusercontent.com/projectcalico/calico/v3.31.0/manifests/calico.yaml',
                        '1990b1cec14002790718bb960b1b8445eb7b896cd16c32e952309f35a9026981')
        self.k('-n', 'kube-system', 'apply', '-f', '-', data=calico_manifest(path))
        self.k('-n', 'kube-system', 'rollout', 'status', 'daemonset/calico-node', '--timeout=300s')
        self.ready('node/' + self.node, timeout=180)
        self.k('uncordon', self.node)
        self.k('-n', 'kube-system', 'rollout', 'status', 'deployment/coredns', '--timeout=180s')

    def install(self):
        super().install()
        self.apply({'apiVersion': 'v1', 'kind': 'ConfigMap',
                    'metadata': {'name': 'review-endpoint', 'namespace': 'platform'},
                    'data': {'origin': self.origin}})

    def stage(self, name, selector=''):
        self.k('apply', '-k', OVERLAY, '-l', 'grid-stage=' + name + selector)

    def build(self):
        # The VM copies the eight registry digests into the dedicated kind node.
        print('Using the ARM64 images loaded from the review registry', flush=True)

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
        flink_recovery(self)
        for name in ('keycloak', 'projection', 'api', 'copilot', 'ui'):
            self.k('rollout', 'restart', 'deployment/' + name)
            self.k('rollout', 'status', 'deployment/' + name, '--timeout=420s')
        browser_check(self, after_restart=True)
        self.k('wait', '--for=jsonpath={.status.phase}=Bound', '--all', 'pvc', '--timeout=45s')
        volumes = self.k('get', 'pvc', '-o', 'json', capture=True)
        write_private(self.directory / 'persistent-volumes.json', volumes)
        capture_inventory(self)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('network', 'install', 'deploy', 'verify', 'recover'))
    parser.add_argument('--host', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9-]+\.canadacentral\.cloudapp\.azure\.com', args.host):
        parser.error('Supply the reserved Canada Central review hostname')
    if not KUBECONFIG.is_file():
        parser.error('Export the review kubeconfig through the loopback tunnel first')
    cluster = VMReview(args.host)
    getattr(cluster, args.command)()


if __name__ == '__main__':
    main()
