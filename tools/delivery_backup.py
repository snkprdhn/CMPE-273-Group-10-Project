"""Export kind PostgreSQL stores and verify restoration in disposable databases."""
import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from uuid import uuid4

from tools.kind_local import Cluster, ROOT, write_private

STORES = ('local_dev_registry', 'local_dev_control', 'local_dev_keycloak', 'local_dev_projection')
# Use the password already mounted in the PostgreSQL pod. No host credential
# arguments, TTY, or Kubernetes Secret export are needed.
PG = 'if [ -n "${POSTGRES_PASSWORD_FILE:-}" ]; then export PGPASSWORD="$(cat "$POSTGRES_PASSWORD_FILE")"; else export PGPASSWORD="$POSTGRES_PASSWORD"; fi; exec "$@"'
LOGIN = ('-h', '127.0.0.1', '-U', 'local_admin')


def pg(cluster, *args, stdin=None, stdout=subprocess.PIPE):
    return subprocess.run([*map(str, cluster.kubectl), '-n', 'platform', 'exec',
                           *(['-i'] if stdin is not None else []), 'statefulset/postgres', '--',
                           'sh', '-ec', PG, 'postgres-client', *args],
                          stdin=stdin, stdout=stdout, check=True, timeout=120).stdout


def file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def data_hash(cluster, tool, *args, stdin=None):
    # COPY escapes embedded newlines and gives each row one line. Sorting rows
    # per table tolerates physical row order and dump nonce changes.
    with tempfile.TemporaryFile() as output:
        pg(cluster, tool, *args, stdin=stdin, stdout=output)
        output.seek(0)
        tables, sequences = {}, []
        for line in output:
            if line.startswith(b'COPY ') and line.rstrip().endswith(b'FROM stdin;'):
                rows = []
                for row in output:
                    if row == b'\\.\n':
                        break
                    rows.append(row)
                else:
                    raise ValueError('Unterminated COPY data')
                tables[line] = sorted(rows)
            elif line.startswith(b'SELECT pg_catalog.setval('):
                sequences.append(line)
    contents = hashlib.sha256()
    for header, rows in sorted(tables.items()):
        contents.update(header)
        for row in rows:
            contents.update(row)
        contents.update(b'\\.\n')
    for sequence in sorted(sequences):
        contents.update(sequence)
    return {'tables': len(tables), 'rows': sum(map(len, tables.values())), 'sha256': contents.hexdigest()}


def export(cluster, directory):
    directory.mkdir(mode=0o700)
    archives = {}
    for database in STORES:
        path = directory / (database + '.dump')
        with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'wb') as stream:
            pg(cluster, 'pg_dump', *LOGIN, '--format=custom', database, stdout=stream)
        archives[database] = file_hash(path)
    write_private(directory / 'backup.json', json.dumps({'cluster': cluster.name, 'sha256': archives}, indent=2))
    print(f'Exported authoritative PostgreSQL databases to {directory}')


def restore_drill(cluster, directory):
    manifest = json.loads((directory / 'backup.json').read_text())
    if set(manifest['sha256']) != set(STORES):
        raise ValueError('Backup manifest does not contain all authoritative databases')
    for database in STORES:
        path = directory / (database + '.dump')
        if file_hash(path) != manifest['sha256'][database]:
            raise ValueError(f'Archive checksum mismatch for {database}')
    evidence = {}
    for database in STORES:
        path = directory / (database + '.dump')
        target = 'delivery_restore_' + uuid4().hex
        pg(cluster, 'psql', *LOGIN, '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'postgres',
           '-c', f'CREATE DATABASE {target} TEMPLATE template0 ALLOW_CONNECTIONS false')
        try:
            pg(cluster, 'psql', *LOGIN, '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'postgres',
               '-c', f'REVOKE ALL ON DATABASE {target} FROM PUBLIC',
               '-c', f'ALTER DATABASE {target} ALLOW_CONNECTIONS true')
            with path.open('rb') as stream:
                expected = data_hash(cluster, 'pg_restore', '--data-only', '--file=-', stdin=stream)
            with path.open('rb') as stream:
                pg(cluster, 'pg_restore', *LOGIN, '--exit-on-error', '--single-transaction', '--dbname=' + target,
                   stdin=stream)
            actual = data_hash(cluster, 'pg_dump', *LOGIN, '--data-only', target)
            if not expected['rows'] or expected != actual:
                raise RuntimeError(f'Empty or different restored data for {database}')
            evidence[database] = actual
        finally:
            pg(cluster, 'dropdb', *LOGIN, target)
    write_private(directory / 'restore.json', json.dumps(evidence, indent=2) + '\n')
    print(f'PASS: restored and compared all authoritative databases in {directory}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('export', 'restore-drill'))
    parser.add_argument('--profile', default='dev', choices=('dev', 'review'))
    parser.add_argument('--name', required=True)
    parser.add_argument('--run', required=True)
    args = parser.parse_args()
    if not args.run.isascii() or not args.run.isalnum() or len(args.run) > 24:
        parser.error('run must be 1–24 ASCII alphanumerics')
    cluster = Cluster(args.profile, args.name)
    base = ROOT / '.tools/delivery-backups'
    base.mkdir(exist_ok=True, mode=0o700)
    directory = base / (cluster.name + '-' + args.run)
    (export if args.command == 'export' else restore_drill)(cluster, directory)


if __name__ == '__main__':
    main()
