"""Independent service validation using the frozen contracts."""
import json
from datetime import datetime, timezone
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

DEFS = json.loads((Path(__file__).resolve().parents[1] / 'schemas/v1/bundle.json').read_text())['$defs']

def validate(name, value):
    json.dumps(value, allow_nan=False)
    Draft202012Validator({'$ref': '#/$defs/' + name, '$defs': DEFS}, format_checker=FormatChecker()).validate(value)

def now():
    return datetime.now(timezone.utc)

def stamp(value=None):
    return (value or now()).isoformat().replace('+00:00', 'Z')

def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))
