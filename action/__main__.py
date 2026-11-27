"""Finite fixture replay and durable-state inspection."""
import argparse
import json
import os
import re

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from action.control import Control
from action.domain import instant
from action.fixtures import FakeRegistry, replay, scenarios


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['cases', 'migrate', 'replay', 'inspect', 'tick'])
    parser.add_argument('--schema', help='optional isolated schema; created by migrate/replay only')
    parser.add_argument('--case', default='safe restore and restart')
    parser.add_argument('--at', help='explicit UTC Z evaluation time for tick')
    args = parser.parse_args()
    if args.command == 'cases':
        print('\n'.join(case['name'] for case in scenarios()))
        return
    dsn = os.environ['CONTROL_DSN']
    if args.schema:
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', args.schema):
            parser.error('schema must be a lowercase PostgreSQL identifier')
        if args.command in ('migrate', 'replay'):
            with psycopg.connect(dsn, connect_timeout=2) as db:
                db.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(args.schema)))
        dsn = make_conninfo(dsn, options=f'-c search_path={args.schema}')
    control = Control(dsn, FakeRegistry())
    if args.command in ('migrate', 'replay'):
        control.migrate()
    if args.command == 'replay':
        if control.inspect()['transformers']:
            parser.error('replay needs an empty schema; select a new --schema to preserve existing history')
        case = next((c for c in scenarios() if c['name'] == args.case), None)
        if case is None:
            parser.error('unknown fixture case; use cases to list names')
        replay(control, case)
    elif args.command == 'tick':
        if not args.at:
            parser.error('tick requires --at')
        control.tick(instant(args.at))
    print(json.dumps(control.inspect(), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
