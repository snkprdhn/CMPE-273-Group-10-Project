import argparse
import os
from nicegui import ui
from dashboard.app import configure

parser = argparse.ArgumentParser(description='Member_4 local operator dashboard')
parser.add_argument('--fixture', action='store_true', help='Connect to disposable fixture API on port 18085')
parser.add_argument('--api-url', help='Override REST base URL (loopback only)')
parser.add_argument('--port', type=int, default=18086)
args = parser.parse_args()
if not args.fixture and os.environ.get('MEMBER4_AUTH_MODE') != 'mock' and args.port != 18086:
    parser.error('OIDC redirect is fixed to port 18086')
kind = os.getenv('GRID_LOCAL_KIND') == '1'
if kind and (args.fixture or args.api_url or os.environ.get('MEMBER4_AUTH_MODE') == 'mock'):
    parser.error('The kind dashboard requires the cluster API and OIDC')
configure(fixture=args.fixture, api_url=args.api_url or ('http://127.0.0.1:18085/api/v1' if args.fixture else None))
ui.run(host='0.0.0.0' if kind else '127.0.0.1', port=args.port, title='GridBalance · Operations',
       reload=False, show=False, access_log=False, timeout_graceful_shutdown=20)
