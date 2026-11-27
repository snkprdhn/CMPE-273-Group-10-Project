"""Validate the frozen bundle without importing another service's internals."""
from datetime import datetime, timezone
import json
import re
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

FORMATS = FormatChecker()


@FORMATS.checks('date-time', raises=(ValueError, TypeError))
def valid_timestamp(value):
    if not isinstance(value, str):
        return True
    return bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z', value)) and (
        datetime.fromisoformat(value.replace('Z', '+00:00')).tzinfo is not None)


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = json.loads((ROOT / 'schemas/v1/bundle.json').read_text())
OPENAPI = json.loads((ROOT / 'schemas/v1/openapi.json').read_text())
TOPICS = {k: v for k, v in BUNDLE['x-events'].items()
          if v in ('Load', 'Alert', 'Action', 'Command', 'Result', 'Telemetry')}


def utc():
    return datetime.now(timezone.utc)


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def validate(name, value):
    json.dumps(value, allow_nan=False)
    Draft202012Validator({'$ref': '#/$defs/' + name, '$defs': BUNDLE['$defs']},
                        format_checker=FORMATS).validate(value)


class ApiError(Exception):
    CODES = {400: 'INVALID_ARGUMENT', 401: 'UNAUTHENTICATED', 403: 'FORBIDDEN',
             404: 'NOT_FOUND', 409: 'CONFLICT', 422: 'DOMAIN_INVALID',
             429: 'RATE_LIMITED', 503: 'DEPENDENCY_UNAVAILABLE', 504: 'DEADLINE_EXCEEDED'}

    def __init__(self, status, message):
        self.status, self.message = status, message
        super().__init__(message)

    def body(self, request_id):
        return {'error': {'code': self.CODES[self.status], 'message': self.message,
                          'request_id': request_id, 'details': []}}
