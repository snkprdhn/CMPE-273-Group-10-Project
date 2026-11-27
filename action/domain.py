"""Deterministic control policy; canonical wire shapes come from schemas/v1."""
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import re
from uuid import NAMESPACE_URL, uuid5

from jsonschema import Draft202012Validator, FormatChecker

BUNDLE = json.loads((Path(__file__).resolve().parents[1] / 'schemas/v1/bundle.json').read_text())
FORMATS = FormatChecker()


@FORMATS.checks('date-time', raises=ValueError)
def utc_timestamp(value):
    if not isinstance(value, str):
        return True
    return bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z', value)
                and instant(value).tzinfo)


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def stamp(value):
    if value.utcoffset() != timedelta(0):
        raise ValueError('UTC evaluation time required')
    return value.isoformat().replace('+00:00', 'Z')


def identity(value):
    return str(uuid5(NAMESPACE_URL, 'smart-grid/v1/' + value))


def close(a, b):
    return math.isclose(a, b, rel_tol=0, abs_tol=1e-9)


def validate(name, value):
    json.dumps(value, allow_nan=False)
    Draft202012Validator({'$ref': f'#/$defs/{name}', '$defs': BUNDLE['$defs']},
                          format_checker=FORMATS).validate(value)
    if name == 'Policy':
        if not value['recovery_pct'] < value['warning_pct'] < value['critical_pct']:
            raise ValueError('POLICY_THRESHOLDS')
        if value['ack_timeout_seconds'] > value['command_ttl_seconds']:
            raise ValueError('POLICY_TIMEOUT')
    if name == 'Device':
        if (value['minimum_rate_kw'] > value['maximum_rate_kw'] or value['priority'] > 100
                or value['capability_version'] < 1
                or (value['controllable'] and value['device_type'] != 'EV_CHARGER')):
            raise ValueError('CAPABILITY')
    if name == 'Load':
        p = value['payload']
        start, end = instant(p['window_start']), instant(p['window_end'])
        if end - start != timedelta(seconds=5) or start.timestamp() % 5:
            raise ValueError('WINDOW_BOUNDS')
        if (p['window_id'] != identity(f"window/{p['transformer_id']}/{stamp(start)}/5")
                or value['event_id'] != identity(f"load/{p['window_id']}/{p['revision']}")):
            raise ValueError('LOAD_ID')
        if p['good_device_count'] + p['estimated_device_count'] + p['missing_device_count'] != p['expected_device_count']:
            raise ValueError('DEVICE_COUNTS')
        if not close(p['meter_demand_kw'] + p['ev_demand_kw'] - p['solar_generation_kw'], p['net_load_kw']):
            raise ValueError('LOAD_TOTAL')
        capacity = p['capacity_kw'] is not None and p['capacity_kw'] > 0
        represented = (p['good_device_count'] + p['estimated_device_count']) / p['expected_device_count']
        quality = ('STALE' if not capacity or represented < .7 or 'OLD_RESULT' in p['quality_flags']
                   else 'COMPLETE' if p['good_device_count'] / p['expected_device_count'] >= .95
                   and not p['estimated_device_count'] else 'PARTIAL')
        if quality != p['completeness']:
            raise ValueError('COMPLETENESS')
        if capacity:
            if p['utilization_pct'] is None or not close(p['net_load_kw'] / p['capacity_kw'] * 100, p['utilization_pct']):
                raise ValueError('UTILIZATION')
        elif p['utilization_pct'] is not None or 'MISSING_CAPACITY' not in p['quality_flags']:
            raise ValueError('MISSING_CAPACITY')


def default_policy():
    return {key: spec['default'] for key, spec in BUNDLE['$defs']['Policy']['properties'].items()}


def fresh(p, now):
    return p['completeness'] == 'COMPLETE' and 0 <= (now - instant(p['window_end'])).total_seconds() <= 15


def candidates(snapshots, transformer, now, restoring=False):
    """Registry capabilities plus explicitly supplied, fresh charger observations.

    Registry v1 does NOT contain current rates or online/charging status. Never
    infer those from maximum_rate_kw. Live observation acquisition is a later gate.
    """
    result, seen = [], set()
    for snapshot in snapshots:
        d, t = snapshot['device'], snapshot.get('telemetry')
        validate('Device', d)
        if d['device_id'] in seen:
            raise ValueError('DUPLICATE_DEVICE')
        seen.add(d['device_id'])
        if (d['transformer_id'] != transformer or d['device_type'] != 'EV_CHARGER'
                or not d['controllable'] or not 0 <= (now - instant(d['updated_at'])).total_seconds() <= 15):
            continue
        if d.get('departure_time') and instant(d['departure_time']) <= now + timedelta(minutes=5):
            continue
        if not t:
            continue
        validate('Timestamp', t['observed_at'])
        rate = t['current_rate_kw']
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate):
            raise ValueError('CURRENT_RATE')
        if (t['online'] is not True or (not restoring and t['charging'] is not True)
                or not 0 <= (now - instant(t['observed_at'])).total_seconds() <= 15
                or not d['minimum_rate_kw'] <= rate <= d['maximum_rate_kw']
                or (not restoring and rate <= d['minimum_rate_kw'])):
            continue
        result.append(dict(d, current_rate_kw=rate))
    # No departure means least urgent; priority 0 is least important.
    return sorted(result, key=lambda d: (d['priority'],
        -instant(d['departure_time']).timestamp() if d.get('departure_time') else -math.inf, d['device_id']))


def allocate(devices, target):
    allocations = []
    remaining = target
    for d in devices:
        reduction = min(remaining, d['current_rate_kw'] - d['minimum_rate_kw'])
        if reduction > 1e-9:
            allocations.append(dict(device_id=d['device_id'], original_rate_kw=d['current_rate_kw'],
                                    target_rate_kw=d['current_rate_kw'] - reduction,
                                    capability_version=d['capability_version']))
            remaining -= reduction
    return allocations
