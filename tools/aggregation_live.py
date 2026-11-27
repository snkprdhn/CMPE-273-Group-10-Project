"""Local aggregation acceptance, using only stdlib on the host and component image.

Does not stop unrelated services, delete volumes, touch kind, or use simulators.
Leaves the job and Flink UI running for inspection. Use a new run ID each time.
"""
import argparse
import json
import re
import subprocess
import time
from urllib.request import urlopen

COMPOSE = ['docker', 'compose', '-p', 'smart-grid-local-dev', '-f',
           'infra/compose/compose.yaml', '-f', 'infra/compose/aggregation.yaml']


def run(*args, timeout=180):
    subprocess.run(COMPOSE + list(args), check=True, timeout=timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run', default='accept-' + time.strftime('%Y%m%d%H%M%S'))
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,48}', args.run):
        parser.error('invalid run ID')
    try:
        with urlopen('http://127.0.0.1:18081/jobs/overview', timeout=5) as response:
            active = [j for j in json.load(response)['jobs'] if j['state'] not in ('FINISHED', 'FAILED', 'CANCELED')]
    except OSError:
        active = []
    if active:
        raise RuntimeError('A Flink job is active. Inspect it first; make aggregation-down stops only this component before a fresh run.')
    run('up', '-d', 'kafka', 'jobmanager', 'taskmanager')
    deadline = time.monotonic() + 90
    while True:
        try:
            with urlopen('http://127.0.0.1:18081/taskmanagers', timeout=2) as response:
                if json.load(response)['taskmanagers']: break
        except OSError:
            pass
        if time.monotonic() > deadline: raise RuntimeError('Flink TaskManager did not register')
        time.sleep(1)
    probe = ['exec', '-T', 'jobmanager', '/opt/venv/bin/python', 'tests/aggregation/live.py']
    run(*probe, 'topics', args.run)
    run('exec', '-T', 'jobmanager', 'flink', 'run', '-d', '-py', '/app/aggregation/job.py', '--run', args.run, '--checkpoint-ms', '10000')
    run(*probe, 'before', args.run)
    run(*probe, 'pending', args.run)
    run('kill', '-s', 'SIGKILL', 'taskmanager')
    run('start', 'taskmanager')
    run(*probe, 'after', args.run)
    run(*probe, 'metrics', args.run)
    print('PASS: real Kafka/Flink checkpoint restore, replay, revision and audit. UI: http://127.0.0.1:18081')


if __name__ == '__main__': main()
