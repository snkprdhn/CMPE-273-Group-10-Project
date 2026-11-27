"""Independent local operations runtime."""
import argparse
import json
import os
from pathlib import Path

from projection.store import Store


def main():
    from observability import setup
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['migrate', 'replay', 'rebuild', 'serve', 'consume'])
    parser.add_argument('--fixtures', type=Path, default=Path(__file__).with_name('fixtures.json'))
    parser.add_argument('--port', type=int, default=18084)
    parser.add_argument('--bind', default='127.0.0.1', choices=['127.0.0.1', '0.0.0.0'])
    args = parser.parse_args()
    setup('projection.projection' if args.command == 'consume' else 'projection.api')
    store = Store(os.environ['PROJECTION_DSN'])
    if args.command == 'migrate':
        store.migrate()
    elif args.command == 'replay':
        fixtures = json.loads(args.fixtures.read_text())
        store.context(fixtures)
        for record in fixtures['events']:
            store.ingest(record['topic'], record['key'], record['event'])
        print('Recorded fixtures replayed')
    elif args.command == 'rebuild':
        store.rebuild()
        print('Views rebuilt; old SSE cursors now require snapshots')
    elif args.command == 'consume':
        from projection.consumer import consume
        import psycopg
        with psycopg.connect(store.dsn, connect_timeout=3, autocommit=True) as connection:
            consume(Store(store.dsn, connection=connection))
    else:
        import uvicorn
        from projection.api import create_app
        identities = json.loads(os.environ['MOCK_IDENTITIES']) if os.environ.get('AUTH_MODE') == 'mock' else None
        app = create_app(store, identities, live=os.getenv('GRID_LOCAL_KIND') == '1')
        if identities is not None and args.bind != '127.0.0.1' and os.getenv('GRID_LOCAL_KIND') != '1':
            parser.error('Mock identities require a loopback bind outside the cluster')
        uvicorn.run(app, host=args.bind, port=args.port, timeout_keep_alive=5, timeout_graceful_shutdown=20)


if __name__ == '__main__':
    main()
