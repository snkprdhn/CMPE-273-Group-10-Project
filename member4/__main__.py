"""Independent local operations runtime."""
import argparse
import json
import os
from pathlib import Path

from member4.store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['migrate', 'replay', 'rebuild', 'serve', 'consume'])
    parser.add_argument('--fixtures', type=Path, default=Path(__file__).with_name('fixtures.json'))
    parser.add_argument('--port', type=int, default=18084)
    args = parser.parse_args()
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
        from member4.consumer import consume
        consume(store)
    else:
        import uvicorn
        from member4.api import create_app
        identities = json.loads(os.environ['MEMBER4_MOCK_IDENTITIES']) if os.environ.get('MEMBER4_AUTH_MODE') == 'mock' else None
        app = create_app(store, identities)
        # Explicit loopback-only standalone mock identity boundary.
        uvicorn.run(app, host='127.0.0.1', port=args.port, timeout_keep_alive=5)


if __name__ == '__main__':
    main()
