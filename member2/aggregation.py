"""Run-scoped deterministic aggregation state, stored by Flink checkpoints."""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
from uuid import NAMESPACE_URL, uuid5

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

FORMATS = FormatChecker()


@FORMATS.checks('date-time', raises=ValueError)
def utc_timestamp(value):
    # jsonschema's RFC3339 checker is optional and absent from this runtime lock.
    if not isinstance(value, str):
        return True  # The schema's type constraint handles non-strings.
    return (re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z', value) is not None
            and datetime.fromisoformat(value.replace('Z', '+00:00')).tzinfo is not None)


BUNDLE = json.loads((Path(__file__).resolve().parents[1] / 'schemas/v1/bundle.json').read_text())
VALIDATORS = {name: Draft202012Validator(
    {'$ref': f'#/$defs/{name}', '$defs': BUNDLE['$defs']}, format_checker=FORMATS)
    for name in ('Telemetry', 'Topology', 'Load', 'Dlq', 'Late')}
LOAD = 'grid.transformer-load.v1'
DLQ = 'grid.telemetry.dlq.v1'
LATE = 'grid.telemetry.late.v1'


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def identity(value):
    return str(uuid5(NAMESPACE_URL, 'smart-grid/v1/' + value))


def validate(name, value):
    json.dumps(value, allow_nan=False)
    VALIDATORS[name].validate(value)


def envelope(kind, payload, now, event_id, correlation='aggregation'):
    return dict(event_id=event_id, event_type=kind, schema_version=1,
                source='member2.aggregation', event_time=stamp(now), ingested_at=stamp(now),
                correlation_id=correlation, causation_id=None, payload=payload)


def load_numbers(readings, capacity):
    """Reject values whose category totals or utilization cannot be JSON numbers."""
    try:
        totals = {typ: math.fsum(p['power_kw'] for p in readings if p['device_type'] == typ)
                  for typ in ('SMART_METER', 'EV_CHARGER', 'SOLAR')}
        net = math.fsum(totals.values())
        utilization = net / capacity * 100 if capacity is not None and capacity > 0 else None
        if not all(math.isfinite(v) for v in [*totals.values(), net, utilization if utilization is not None else 0]):
            raise ValueError('POWER_RANGE')
    except OverflowError as exc:
        raise ValueError('POWER_RANGE') from exc
    return totals, net, round(utilization, 9) if utilization is not None else None


class Aggregator:
    def __init__(self, topology, max_events=100000):
        validate('Topology', topology)
        self.topology = deepcopy(topology)
        self.transformers = {t['transformer_id']: t for t in topology['transformers']}
        devices = [d['device_id'] for t in topology['transformers'] for d in t['devices']]
        if (not self.transformers or len(self.transformers) != len(topology['transformers'])
                or len(devices) != len(set(devices))
                or any(not t['devices'] for t in topology['transformers'])):
            raise ValueError('INVALID_TOPOLOGY_IDENTITIES')
        if max_events < 1:
            raise ValueError('INVALID_RUN_LIMIT')
        self.max_events = max_events
        self.seen = set()
        self.windows = {}
        self.greatest = None
        self.metrics = Counter(duplicates=0, dlq=0, very_late=0, corrections=0)

    def reading(self, raw, key, now=None, partition=None, offset=None):
        now = now or datetime.now(timezone.utc)
        event, stage = raw, 'DECODE'
        metadata = {f'original_{k}': v for k, v in [('partition', partition), ('offset', offset)] if v is not None}
        try:
            if isinstance(raw, str):
                event = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('NON_FINITE')))
            stage = 'SCHEMA'
            validate('Telemetry', event)
            stage = 'DOMAIN'
            p = event['payload']
            at, arrival = instant(event['event_time']), instant(event['ingested_at'])
            if key != p['transformer_id']:
                raise ValueError('PARTITION_KEY')
            if (at - arrival).total_seconds() > 5:
                raise ValueError('FUTURE_EVENT')
            if p['quality'] == 'INVALID':
                raise ValueError('INVALID_QUALITY')
            if (p['device_type'] == 'SOLAR' and p['power_kw'] > 0) or (p['device_type'] != 'SOLAR' and p['power_kw'] < 0):
                raise ValueError('POWER_SIGN')
            if p.get('minimum_rate_kw', 0) > p.get('current_rate_kw', math.inf):
                raise ValueError('RATE_RANGE')
            t = self.transformers.get(key)
            if t is None or {d['device_id']: d['device_type'] for d in t['devices']}.get(p['device_id']) != p['device_type']:
                raise ValueError('TOPOLOGY_ASSIGNMENT')
            start = at.replace(second=at.second // 5 * 5, microsecond=0)
            try:
                end = start + timedelta(seconds=5)
            except OverflowError as exc:
                raise ValueError('WINDOW_RANGE') from exc
            lateness = (arrival - end).total_seconds()
            if event['event_id'] not in self.seen and lateness <= 30:
                candidate = self.windows.get((key, start), {}).get('devices', {}).copy()
                previous = candidate.get(p['device_id'])
                if previous is None or (at, p['sequence_number']) > (instant(previous['event_time']), previous['payload']['sequence_number']):
                    candidate[p['device_id']] = event
                load_numbers([e['payload'] for e in candidate.values()], t['capacity_kw'])
        except (ValueError, TypeError, ValidationError) as exc:
            self.metrics['dlq'] += 1
            # Do not expose schema traces or non-JSON NaN values in the audit envelope.
            original = raw if isinstance(raw, str) else json.loads(json.dumps(raw, default=str), parse_constant=lambda _: None)
            payload = dict(original_event=original, original_key=key, failure_stage=stage,
                           attempt_count=1, validation_reasons=[exc.message[:512] if isinstance(exc, ValidationError) else str(exc)[:512]], **metadata)
            result = envelope('telemetry.invalid', payload, now,
                              identity(f'dlq/{partition}/{offset}/{key}/{json.dumps(original, sort_keys=True)}'))
            validate('Dlq', result)
            return [(DLQ, key, result)]
        if event['event_id'] in self.seen:
            self.metrics['duplicates'] += 1
            return []
        # ponytail: finite standalone runs retain identity/window state in full. Fail
        # closed at the run limit; a production retention policy needs a replay horizon.
        if len(self.seen) >= self.max_events:
            raise RuntimeError('RUN_EVENT_LIMIT: start a new run, do not silently evict dedup state')
        self.seen.add(event['event_id'])
        wid = identity(f'window/{key}/{stamp(start)}/5')
        if lateness > 30:
            self.metrics['very_late'] += 1
            payload = dict(original_event=event, window_id=wid,
                           lateness_ms=(arrival-end).total_seconds()*1000,
                           audited_at=stamp(now), reason='BEYOND_ALLOWED_LATENESS', **metadata)
            result = envelope('telemetry.late', payload, now, identity(f'late/{event["event_id"]}'), event['correlation_id'])
            validate('Late', result)
            return [(LATE, key, result)]
        self.greatest = max(self.greatest, at) if self.greatest else at
        w = self.windows.setdefault((key, start), dict(devices={}, late=0, fingerprint=None, revision=-1))
        w['late'] += int(lateness > 0)
        previous = w['devices'].get(p['device_id'])
        rank = lambda e: (instant(e['event_time']), e['payload']['sequence_number'])
        if previous is None or rank(event) > rank(previous):
            w['devices'][p['device_id']] = deepcopy(event)
        return []

    def evaluate(self, now):
        results = []
        if self.greatest is None:
            return results
        for (tx, start), w in sorted(self.windows.items()):
            end = start + timedelta(seconds=5)
            if (self.greatest - end).total_seconds() < 10:
                continue
            fingerprint = tuple(sorted((d, e['event_id']) for d, e in w['devices'].items()))
            if fingerprint == w['fingerprint']:
                continue
            t = self.transformers[tx]
            readings = [e['payload'] for _, e in sorted(w['devices'].items())]
            good = sum(p['quality'] == 'GOOD' for p in readings)
            estimated = len(readings) - good
            missing = len(t['devices']) - len(readings)
            totals, net, utilization = load_numbers(readings, t['capacity_kw'])
            capacity = t['capacity_kw']
            valid = capacity is not None and capacity > 0
            old = (now - end).total_seconds() > 15
            flags = (['ESTIMATED'] if estimated else []) + (['MISSING_DEVICES'] if missing else []) + (['MISSING_CAPACITY'] if not valid else []) + (['OLD_RESULT'] if old else [])
            represented = len(readings) / len(t['devices'])
            quality = 'STALE' if not valid or old or represented < .7 else ('COMPLETE' if good / len(t['devices']) >= .95 and not estimated else 'PARTIAL')
            revision = w['revision'] + 1
            wid = identity(f'window/{tx}/{stamp(start)}/5')
            payload = dict(transformer_id=tx, window_id=wid, window_start=stamp(start), window_end=stamp(end),
                           revision=revision, topology_version=self.topology['topology_version'],
                           meter_demand_kw=totals['SMART_METER'], ev_demand_kw=totals['EV_CHARGER'],
                           solar_generation_kw=-totals['SOLAR'], net_load_kw=net, capacity_kw=capacity,
                           utilization_pct=utilization,
                           expected_device_count=len(t['devices']), good_device_count=good,
                           estimated_device_count=estimated, missing_device_count=missing,
                           late_event_count=w['late'], quality_flags=flags, completeness=quality)
            result = envelope('transformer.load', payload, now, identity(f'load/{wid}/{revision}'))
            validate('Load', result)
            w['fingerprint'], w['revision'] = fingerprint, revision
            self.metrics['corrections'] += int(revision > 0)
            results.append((LOAD, tx, result))
        return results
