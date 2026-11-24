"""Capture a bounded, secret-free local load prerequisite packet (not a load result)."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess


def capture(argv):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=20)
        return {'command': argv, 'exit_code': result.returncode,
                'stdout': result.stdout.strip(), 'stderr': result.stderr.strip()}
    except subprocess.TimeoutExpired:
        return {'command': argv, 'exit_code': None, 'stdout': '', 'stderr': '20 second deadline exceeded'}


def inventory(cluster):
    if not re.fullmatch(r'smart-grid-kind-(dev|review)(-[a-z0-9]{1,16})?', cluster):
        raise ValueError('expected a dedicated smart-grid kind cluster')
    commands = {
        'git': ['git', 'rev-parse', 'HEAD'],
        'machine': ['sysctl', 'machdep.cpu.brand_string', 'hw.memsize', 'hw.logicalcpu'],
        'docker': ['docker', 'info', '--format', '{{.NCPU}} {{.MemTotal}} {{.ServerVersion}} {{.Architecture}}'],
        'containers': ['docker', 'ps', '--format', '{{.Names}} {{.Status}}'],
        'target': ['docker', 'inspect', '--format', '{{.State.Status}} {{.Image}}', cluster + '-control-plane'],
    }
    observations = {name: capture(command) for name, command in commands.items()}
    reasons = [name + ' inspection failed' for name, value in observations.items() if value['exit_code'] != 0]
    target = observations['target']['stdout'].split()
    if not target or target[0] != 'running':
        reasons.append('target kind node is not running; no workload or SLI measurement attempted')
    running = observations['containers']['stdout'].splitlines()
    if any(line.startswith('smart-grid-local-dev-') for line in running):
        reasons.append('standalone stack active; preserve its Flink state and stage separately before load')
    return dict(timestamp=datetime.now(timezone.utc).isoformat(), cluster=cluster,
                status='BLOCKED' if reasons else 'READY_FOR_WORKLOAD_CHECKS',
                blockers=reasons, observations=observations,
                load_executed=False, measurements=None,
                limitations=['This packet checks prerequisites only; it establishes no capacity or business outcome.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('refusing to overwrite existing evidence')
    report = inventory(args.cluster)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as file:
        args.output.chmod(0o600)
        json.dump(report, file, indent=2)
        file.write('\n')
    print(json.dumps({'status': report['status'], 'report': str(args.output)}))
    return 1 if report['blockers'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
