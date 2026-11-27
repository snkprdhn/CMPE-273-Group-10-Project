"""Contract validation and small local operational endpoints."""
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
from uuid import NAMESPACE_URL, uuid5

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = json.loads((ROOT / 'schemas/v1/bundle.json').read_text())
METRICS = Counter()


def utc():
    return datetime.now(timezone.utc)


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def validate(name, value, key=None):
    # JSON's NaN/Infinity extension is never a valid wire value.
    json.dumps(value, allow_nan=False)
    Draft202012Validator({'$ref': f'#/$defs/{name}', '$defs': BUNDLE['$defs']},
                          format_checker=FormatChecker()).validate(value)
    if name in BUNDLE['x-events'].values():
        field = BUNDLE['$defs'][name]['x-partition-key']
        if key != value['payload'][field]:
            raise ValueError('PARTITION_KEY')
    if name == 'Command':
        p = value['payload']
        if value['leader_epoch'] != p['leader_epoch'] or value['saga_version'] != p['saga_version']:
            raise ValueError('ENVELOPE_MISMATCH')
        if instant(p['expires_at']) <= instant(p['issued_at']):
            raise ValueError('COMMAND_TIME')
    if name == 'Device':
        d = value
        if not (0 <= d['minimum_rate_kw'] <= d['maximum_rate_kw'] and
                d['priority'] <= 100 and d['capability_version'] > 0):
            raise ValueError('CAPABILITY')
        if d['controllable'] and d['device_type'] != 'EV_CHARGER':
            raise ValueError('CONTROLLABLE')


def event(kind, payload, now, identity, correlation, cause=None):
    return dict(event_id=str(uuid5(NAMESPACE_URL, identity)), event_type=kind,
                schema_version=1, source='devices.simulator', event_time=stamp(now),
                ingested_at=stamp(now), correlation_id=correlation,
                causation_id=cause, payload=payload)


def log(outcome, **fields):
    from observability import log as structured_log
    structured_log(outcome, **fields)


def monitor(port, ready, host='127.0.0.1'):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                body = ''.join(f'devices_{k} {v}\n' for k, v in METRICS.copy().items())
                code, content = 200, 'text/plain; version=0.0.4'
            elif self.path in ('/healthz', '/readyz'):
                try:
                    healthy = self.path == '/healthz' or bool(ready())
                except Exception:
                    healthy = False
                code, content = (200 if healthy else 503), 'application/json'
                body = json.dumps({'healthy': healthy})
            else:
                code, content, body = 404, 'text/plain', 'not found'
            self.send_response(code)
            self.send_header('Content-Type', content)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
