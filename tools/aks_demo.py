"""Scoped, digest-pinned rollouts of the continuous demo to the existing AKS review cluster.

Applies only named resources from the rendered AKS overlay, with local image tags replaced by the
digests in .tools/aks-demo/images.json. It never applies the whole overlay to the live cluster.

    .venv/bin/python -m tools.aks_demo render fleet FlinkDeployment/aggregation
    .venv/bin/python -m tools.aks_demo apply  fleet FlinkDeployment/aggregation
"""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / '.tools/aks-review'
KUBECONFIG, CONTEXT = DIRECTORY / 'kubeconfig', 'smart-grid-aks-review'
IMAGES = ROOT / '.tools/aks-demo/images.json'
KUBECTL = str(ROOT / '.venv/bin/kubectl')

GROUPS = {
    'fleet': ['Deployment/fleet', 'Service/fleet', 'PodMonitor/fleet', 'NetworkPolicy/fleet-ingress', 'NetworkPolicy/ui-fleet-egress'],
}


def sh(*args, data=None):
    return subprocess.run(args, input=data, text=True, check=True, capture_output=True).stdout


def rendered():
    body = sh(KUBECTL, 'kustomize', str(ROOT / 'infra/aks'))
    docs = {}
    for doc in re.split(r'(?m)^---\n', body):
        kind = re.search(r'(?m)^kind: (\S+)', doc)
        name = re.search(r'(?m)^  name: (\S+)', doc)
        if kind and name:
            docs[f'{kind[1]}/{name[1]}'] = doc.strip() + '\n'
    return docs


def pin(doc, images):
    return re.sub(r'(?m)^(\s*image: )(smart-grid-kind/\S+)$',
                  lambda m: m[1] + images[m[2].split(':')[0].removeprefix('smart-grid-kind/')], doc)


def select(names):
    chosen = []
    for name in names:
        chosen.extend(GROUPS.get(name, [name]))
    return chosen


def main(argv):
    command, *names = argv
    if command not in ('render', 'apply'):
        raise SystemExit(__doc__)
    images = json.loads(IMAGES.read_text())
    docs = rendered()
    chosen = select(names)
    missing = [n for n in chosen if n not in docs]
    if missing:
        raise SystemExit('Not in the AKS overlay: ' + ', '.join(missing))
    body = '---\n'.join(pin(docs[n], images) for n in chosen)
    if command == 'render':
        print(body)
        return
    print(sh(KUBECTL, '--kubeconfig', str(KUBECONFIG), '--context', CONTEXT, '-n', 'platform', 'apply', '-f', '-', data=body))


if __name__ == '__main__':
    main(sys.argv[1:])
