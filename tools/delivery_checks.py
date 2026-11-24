"""Render pinned manifests and scan source locks and local application images."""
import argparse
import json
import platform
from pathlib import Path
import re
import tarfile

from tools.kind_local import BIN, CHARTS, INFRA, ROOT, chart_archive, download, run, write_private
from tools.observability import charts

VERSION = '0.69.3'
TRIVY = {
    ('Darwin', 'arm64'): ('macOS-ARM64', 'a2f2179afd4f8bb265ca3c7aefb56a666bc4a9a411663bc0f22c3549fbc643a5'),
    ('Linux', 'x86_64'): ('Linux-64bit', '1816b632dfe529869c740c0913e36bd1629cb7688bd5634f4a858c1d57c88b75'),
}
OUTPUT = ROOT / '.tools/delivery-checks'


def images(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == 'image' and isinstance(value, str):
                yield value
            else:
                yield from images(value)
    elif isinstance(node, list):
        for child in node:
            yield from images(child)


def verify_manifests(path):
    import yaml

    identities = set()
    for resource in filter(None, yaml.load_all(path.read_text(), Loader=yaml.BaseLoader)):
        metadata = resource['metadata']
        identity = (resource['apiVersion'], resource['kind'], metadata.get('namespace'), metadata['name'])
        if identity in identities:
            raise ValueError(f'Duplicate resource in {path.name}: {identity}')
        identities.add(identity)
        for image in images(resource):
            last = image.rsplit('/', 1)[-1]
            if not re.fullmatch(r'(?:[^:@]+:(?!latest$)[\w.-]+|[^@]+@sha256:[a-f0-9]{64})', last):
                raise ValueError(f'Floating image in {path.name}: {image}')
    if not identities:
        raise ValueError(f'No resources in {path.name}')


def render():
    directory = OUTPUT / 'manifests'
    directory.mkdir(exist_ok=True)
    overlays = {'dev': INFRA / 'dev', 'review': INFRA / 'review',
                'hosted': ROOT / 'infra/hosted', 'aks': ROOT / 'infra/aks'}
    for profile, overlay in overlays.items():
        path = directory / (profile + '.yaml')
        path.write_text(run(BIN / 'kubectl', 'kustomize', overlay, capture=True))
        verify_manifests(path)
    platform_charts = [('flink-operator' if name == 'flink' else name,
                        chart_archive(name), INFRA / 'charts' / (name + '.yaml')) for name in CHARTS]
    for name, archive, values in [*platform_charts, *charts()]:
        namespace = 'observability' if name in {'monitoring', 'tempo', 'otel'} else 'platform' if name == 'postgres' else 'grid-operators'
        run(BIN / 'helm', 'lint', archive, '-f', values)
        path = directory / (name + '.yaml')
        path.write_text(run(BIN / 'helm', 'template', name, archive, '-n', namespace,
                            '--include-crds', '-f', values, capture=True))
        verify_manifests(path)
    run('docker', 'compose', '-f', ROOT / 'infra/compose/compose.yaml', 'config', '--quiet')
    run(BIN / 'python', '-m', 'compileall', '-q', 'tools')
    print(f'PASS: manifests rendered and checked in {directory}')


def scanner():
    target = (platform.system(), platform.machine())
    if target not in TRIVY:
        raise ValueError(f'No checksum-pinned Trivy binary for {target}')
    suffix, checksum = TRIVY[target]
    filename = f'trivy_{VERSION}_{suffix}.tar.gz'
    archive = download(ROOT / '.tools/downloads' / filename,
                       f'https://github.com/aquasecurity/trivy/releases/download/v{VERSION}/{filename}', checksum)
    destination = BIN / 'trivy'
    with tarfile.open(archive, 'r:gz') as source:
        destination.write_bytes(source.extractfile('trivy').read())
    destination.chmod(0o755)
    return [destination, '--cache-dir', ROOT / '.tools/trivy-cache']


def scan(inventory):
    directory = OUTPUT / 'scans'
    directory.mkdir(exist_ok=True)
    command = scanner()
    run(*command, '--version')
    flags = ['--disable-telemetry', '--skip-version-check', '--no-progress', '--timeout', '15m']
    exclusions = [flag for name in ('.git', '.tools', '.venv') for flag in ('--skip-dirs', name)]
    # Report matched rule and path only. A secret finding can contain source text.
    private = directory / 'secrets-private.json'
    private.touch(mode=0o600)
    run(*command, 'fs', *flags, '--scanners', 'secret', '--format', 'json', '--output', private, *exclusions, ROOT)
    results = json.loads(private.read_text()).get('Results', [])
    findings = [{'path': result['Target'], 'rule': item['RuleID']}
                for result in results for item in result.get('Secrets', [])]
    private.unlink()
    write_private(directory / 'secrets.json', json.dumps(findings, indent=2))
    dependencies = directory / 'dependencies.json'
    run(*command, 'fs', *flags, '--scanners', 'vuln', '--format', 'json',
        '--file-patterns', r'pip:requirements[^/]*\.txt$',
        '--output', dependencies, *exclusions, ROOT)
    results = json.loads(dependencies.read_text()).get('Results', [])
    scanned = {row['Target'] for row in results if row['Type'] == 'pip' and row.get('Packages')}
    locks = set(run('git', 'ls-files', '*requirements*.txt', capture=True).splitlines())
    if locks - scanned:
        raise ValueError(f'Dependency report omitted lockfiles: {sorted(locks - scanned)}')
    entries = json.loads(inventory.read_text())
    from tools.delivery_images import IMAGES
    if {details['manifest_image'] for details in entries.values()} != {'smart-grid-kind/' + tag for tag, *_ in IMAGES}:
        raise ValueError('Build the complete image set before scanning')
    for image, details in entries.items():
        image_id = run('docker', 'image', 'inspect', '--format', '{{.Id}}', image, capture=True).strip()
        if image_id != details['id']:
            raise ValueError(f'Image changed since build: {image}')
        name = image.split('/')[-1].split(':')[0]
        vulnerabilities = directory / f'{name}.vulnerabilities.json'
        run(*command, 'image', *flags, '--image-src', 'docker', '--scanners', 'vuln',
            '--format', 'json', '--output', vulnerabilities, image)
        run(*command, 'convert', '--format', 'cyclonedx',
            '--output', directory / f'{name}.sbom.json', vulnerabilities)
    if findings:
        raise RuntimeError(f'Secret scan matched {len(findings)} entries. Review sanitized paths in secrets.json.')
    print(f'PASS: dependency and image scan reports in {directory}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('check', choices=('render', 'scan'))
    parser.add_argument('--inventory', type=Path, default=ROOT / '.tools/delivery-images/images.json')
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True, mode=0o700)
    render() if args.check == 'render' else scan(args.inventory)
