"""Build and inventory the same local images for kind and PR runners."""
import argparse
import json
from pathlib import Path

from tools.kind_local import ROOT, download, run, write_private


IMAGES = (
    ('registry:v1', 'member1/Dockerfile', 'registry'),
    ('action:v1', 'infra/kind/Dockerfile', 'action'),
    ('api:v1', 'infra/kind/Dockerfile', 'api'),
    ('copilot:v1', 'infra/kind/Dockerfile', 'copilot'),
    ('ui:v1', 'infra/kind/Dockerfile', 'ui'),
    ('acceptance:v1', 'infra/kind/Dockerfile', 'acceptance'),
    ('connect:3.0.8-v1', 'infra/member3/Dockerfile.connect', 'strimzi'),
    ('aggregation:2.2.1-v1', 'member2/Dockerfile', None),
)


def build(repository, inventory):
    archive = 'debezium-connector-postgres-3.0.8.Final-plugin.tar.gz'
    download(ROOT / '.tools/downloads' / archive,
             'https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/3.0.8.Final/' + archive,
             '600806875b810abb96b3774f789d9ed33dbe4ac687d3ed9454bc0caa76b7279c')
    run('make', 'bindings')
    inventory.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    images = {}
    for tag, file, target in IMAGES:
        image = repository + '/' + tag
        options = ['--target', target] if target else []
        run('docker', 'build', '--provenance=false', '-f', file, '-t', image, *options, '.')
        identity = run('docker', 'image', 'inspect', '--format', '{{.Id}} {{.Architecture}}', image, capture=True)
        image_id, architecture = identity.split()
        images[image] = dict(id=image_id, architecture=architecture, manifest_image='smart-grid-kind/' + tag)
        write_private(inventory, json.dumps(images, indent=2) + '\n')
    return images


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, default=ROOT / '.tools/delivery-images/images.json')
    parser.add_argument('--repository', default='smart-grid-kind')
    args = parser.parse_args()
    build(args.repository, args.inventory)
