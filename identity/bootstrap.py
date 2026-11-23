"""Generate local credentials/import, create only Keycloak's isolated DB/login."""
import argparse
import json
import os
from pathlib import Path
import secrets

import psycopg
from psycopg import sql

from identity.realm import realm

ROOT = Path('.tools/identity')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reset', action='store_true', help='Drop only local_dev_keycloak; stop Keycloak first')
    args = parser.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(ROOT, 0o700)
    path = ROOT/'local.json'
    if path.exists():
        config = json.loads(path.read_text())
    else:
        config = {name: secrets.token_urlsafe(24) for name in
                  ('viewer', 'operator', 'administrator', 'database', 'bootstrap-admin')}
        path.write_text(json.dumps(config, indent=2)+'\n')
        os.chmod(path, 0o600)
    with psycopg.connect(os.environ.get('IDENTITY_ADMIN_DSN',
            'postgresql://local_admin:local_admin_disposable@127.0.0.1:15432/postgres'), autocommit=True) as db:
        if args.reset:
            db.execute('DROP DATABASE IF EXISTS local_dev_keycloak')
        if not db.execute("SELECT 1 FROM pg_roles WHERE rolname='keycloak'").fetchone():
            db.execute(sql.SQL('CREATE ROLE keycloak LOGIN PASSWORD {}').format(sql.Literal(config['database'])))
        elif args.reset:
            db.execute(sql.SQL('ALTER ROLE keycloak PASSWORD {}').format(sql.Literal(config['database'])))
        if not db.execute("SELECT 1 FROM pg_database WHERE datname='local_dev_keycloak'").fetchone():
            db.execute('CREATE DATABASE local_dev_keycloak OWNER keycloak')
        db.execute('REVOKE CONNECT ON DATABASE local_dev_keycloak FROM PUBLIC')
    (ROOT/'realm.json').write_text(json.dumps(realm(config), indent=2))
    (ROOT/'keycloak.env').write_text('\n'.join([
        'KC_DB=postgres', 'KC_DB_URL=jdbc:postgresql://postgres:5432/local_dev_keycloak',
        'KC_DB_USERNAME=keycloak', 'KC_DB_PASSWORD='+config['database'],
        'KC_BOOTSTRAP_ADMIN_USERNAME=local-bootstrap',
        'KC_BOOTSTRAP_ADMIN_PASSWORD='+config['bootstrap-admin']])+'\n')
    (ROOT/'Dockerfile').write_text('FROM quay.io/keycloak/keycloak:26.1.4@sha256:044a457e04987e1fff756be3d2fa325a4ef420fa356b7034ecc9f1b693c32761\nCOPY --chown=1000:0 realm.json /opt/keycloak/data/import/realm.json\n')
    (ROOT/'.dockerignore').write_text('*\n!Dockerfile\n!realm.json\n')
    for name in ('local.json', 'realm.json', 'keycloak.env', 'Dockerfile', '.dockerignore'):
        os.chmod(ROOT/name, 0o600)
    print('Generated ignored local identity files in .tools/identity; existing realm is never overwritten.')


if __name__ == '__main__':
    main()
