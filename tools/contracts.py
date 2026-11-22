"""Offline contract validation and conservative compatibility gates; no services."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
from uuid import NAMESPACE_URL, uuid5

from google.protobuf import descriptor_pb2, json_format
from grpc_tools import protoc
import grpc_tools
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate as validate_openapi

ROOT = Path(__file__).resolve().parents[1]
V1 = ROOT / 'schemas/v1'
BUNDLE = json.loads((V1 / 'bundle.json').read_text())
FORMATS = FormatChecker()


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def window_id(transformer, start):
    return str(uuid5(NAMESPACE_URL, f'smart-grid/v1/window/{transformer}/{start}/5'))


def load_id(window, revision):
    return str(uuid5(NAMESPACE_URL, f'smart-grid/v1/load/{window}/{revision}'))


def fail(code):
    raise ValueError(code)


def finite(value):
    if isinstance(value, float) and not math.isfinite(value):
        fail('NON_FINITE')
    if isinstance(value, dict):
        for v in value.values():
            finite(v)
    if isinstance(value, list):
        for v in value:
            finite(v)


def validate(name, value, *, key=None):
    """Structural checks first; cross-field/time rules have stable domain codes."""
    finite(value)
    Draft202012Validator({'$ref': f'#/$defs/{name}', '$defs': BUNDLE['$defs']}, format_checker=FORMATS).validate(value)
    if name == 'Device':
        capability(value['minimum_rate_kw'], value['maximum_rate_kw'], value['priority'], value['capability_version'], value['device_type'] == 'EV_CHARGER', value['controllable'])
    if name in BUNDLE['x-events'].values():
        p = value['payload']
        if name in ('Action', 'Command'):
            if value['leader_epoch'] != p['leader_epoch']:
                fail('EPOCH_MISMATCH')
            if name == 'Command' and value['saga_version'] != p['saga_version']:
                fail('SAGA_MISMATCH')
        path = BUNDLE['$defs'][name]['x-partition-key'].split('.')
        expected = p
        for part in path:
            expected = expected[part]
        if key != expected:
            fail('PARTITION_KEY')
        if name == 'Telemetry':
            if timestamp(value['event_time']) > timestamp(value['ingested_at']) + timedelta(seconds=5):
                fail('FUTURE_EVENT')
            if p['quality'] == 'INVALID':
                fail('INVALID_QUALITY')
            if (p['device_type'] == 'SOLAR' and p['power_kw'] > 0) or (p['device_type'] != 'SOLAR' and p['power_kw'] < 0):
                fail('POWER_SIGN')
            if p.get('minimum_rate_kw', 0) > p.get('current_rate_kw', float('inf')):
                fail('RATE_RANGE')
        if name == 'Command':
            if timestamp(p['expires_at']) <= timestamp(p['issued_at']):
                fail('COMMAND_TIME')
        if name == 'Load':
            validate_load(p)
            if value['event_id'] != load_id(p['window_id'], p['revision']):
                fail('LOAD_EVENT_ID')
        if name == 'Late':
            original = p['original_event']
            validate('Telemetry', original, key=original['payload']['transformer_id'])
            start = timestamp(original['event_time'])
            start = start.replace(second=(start.second // 5) * 5, microsecond=0)
            end = start + timedelta(seconds=5)
            lateness = (timestamp(original['ingested_at']) - end).total_seconds() * 1000
            if lateness <= 30000 or p['lateness_ms'] != lateness:
                fail('LATE_BOUNDARY')
            if p['window_id'] != window_id(original['payload']['transformer_id'], start.isoformat().replace('+00:00', 'Z')):
                fail('WINDOW_ID')
    if name == 'LoadPayload':
        validate_load(value)
    if name == 'Policy' or name == 'PolicyWrite':
        p = value.get('policy', value)
        if not p['recovery_pct'] < p['warning_pct'] < p['critical_pct']:
            fail('POLICY_THRESHOLDS')
        if p['ack_timeout_seconds'] > p['command_ttl_seconds']:
            fail('POLICY_TIMEOUT')
        if name == 'PolicyWrite' and p['version'] != value['expected_version'] + 1:
            fail('POLICY_VERSION')
    if name in ('TimeRange', 'InvestigationCreate') or name.endswith('Arguments') or name == 'ToolContext':
        p = value.get('time_scope', value)
        if ('start' in p) != ('end' in p):
            fail('TIME_RANGE')
        if 'start' in p and not timedelta(0) < timestamp(p['end']) - timestamp(p['start']) <= timedelta(hours=1):
            fail('TIME_RANGE')
    if name == 'ToolCall':
        validate(value['tool'] + 'Arguments', value['arguments'])
    if name == 'Topology':
        seen = set()
        transformers = set()
        for t in value['transformers']:
            if t['transformer_id'] in transformers:
                fail('DUPLICATE_TRANSFORMER')
            transformers.add(t['transformer_id'])
            for d in t['devices']:
                if d['device_id'] in seen:
                    fail('DUPLICATE_DEVICE')
                seen.add(d['device_id'])


def validate_load(p):
    if timestamp(p['window_end']) - timestamp(p['window_start']) != timedelta(seconds=5) or timestamp(p['window_start']).timestamp() % 5:
        fail('WINDOW_BOUNDS')
    if p['window_id'] != window_id(p['transformer_id'], p['window_start']):
        fail('WINDOW_ID')
    if p['good_device_count'] + p['estimated_device_count'] + p['missing_device_count'] != p['expected_device_count']:
        fail('DEVICE_COUNTS')
    total = p['meter_demand_kw'] + p['ev_demand_kw'] - p['solar_generation_kw']
    if not math.isclose(total, p['net_load_kw'], rel_tol=0, abs_tol=1e-9):
        fail('LOAD_TOTAL')
    valid_capacity = p['capacity_kw'] is not None and p['capacity_kw'] > 0
    if not valid_capacity:
        if p['utilization_pct'] is not None or p['completeness'] != 'STALE' or 'MISSING_CAPACITY' not in p['quality_flags']:
            fail('MISSING_CAPACITY')
    elif p['utilization_pct'] is None or not math.isclose(total / p['capacity_kw'] * 100, p['utilization_pct'], rel_tol=0, abs_tol=1e-9):
        fail('UTILIZATION')
    if p['completeness'] != completeness(p, valid_capacity, 'OLD_RESULT' in p['quality_flags']):
        fail('COMPLETENESS')


def completeness(p, capacity=True, old=False):
    represented = (p['good_device_count'] + p['estimated_device_count']) / p['expected_device_count']
    if not capacity or old or represented < .7:
        return 'STALE'
    if p['good_device_count'] / p['expected_device_count'] >= .95 and p['estimated_device_count'] == 0:
        return 'COMPLETE'
    return 'PARTIAL'


def authorize(call, context, now):
    validate('ToolCall', call)
    validate('ToolContext', context)
    if timestamp(now) >= timestamp(context['deadline']):
        fail('DEADLINE_EXCEEDED')
    if not set(context['roles']) & {'Operator', 'Administrator'}:
        fail('FORBIDDEN')
    a = call['arguments']
    for field, scope in [('transformer_id','transformer_ids'),('action_id','action_ids'),('correlation_id','correlation_ids'),('trace_id','trace_ids')]:
        if field in a and a[field] not in context['entity_scope'][scope]:
            fail('OUT_OF_SCOPE')
    if 'entity_id' in a and a['entity_id'] not in sum(context['entity_scope'].values(), []):
        fail('OUT_OF_SCOPE')
    if 'start' in a and (timestamp(a['start']) < timestamp(context['time_scope']['start']) or timestamp(a['end']) > timestamp(context['time_scope']['end'])):
        fail('OUT_OF_SCOPE')
    bounded = deepcopy(a)
    if 'limit' in bounded:
        bounded['limit'] = min(bounded['limit'], context['max_items'])
    return bounded


def validate_draft(draft, evidence):
    validate('AgentDraft', draft)
    ids = []
    for e in evidence:
        validate('Evidence', e)
        ids.append(e['evidence_id'])
    if len(ids) != len(set(ids)):
        fail('DUPLICATE_EVIDENCE')
    cited = set(draft['evidence_ids'])
    for claim in draft['observations'] + draft['hypotheses']:
        cited.update(claim['evidence_ids'])
    if not cited <= set(ids):
        fail('UNKNOWN_EVIDENCE')
    if not cited <= set(draft['evidence_ids']):
        fail('UNLISTED_EVIDENCE')


def validate_tool_result(result, call, context):
    validate('ToolResult', result)
    if len(json.dumps(result, allow_nan=False).encode('utf-8')) > 65536:
        fail('RESULT_TOO_LARGE')
    evidence = result.get('evidence', [])
    if len(evidence) > context['max_items']:
        fail('RESULT_TOO_LARGE')
    entity = next((call['arguments'][key] for key in ('transformer_id', 'action_id', 'entity_id') if key in call['arguments']), None)
    for item in evidence:
        if item['source_type'] != call['tool'] or (entity and item['entity_id'] != entity):
            fail('OUT_OF_SCOPE')


def compatible(old, new, path=''):
    """Fail closed: allow descriptions and additive optional properties only.

    This intentionally rejects some safe edits. Semantic/access changes still
    require review; it does not claim general JSON Schema subsumption.
    """
    if isinstance(old, dict) and isinstance(new, dict):
        for k, v in old.items():
            if k in ('description', 'summary', 'title'):
                continue
            if k not in new:
                fail('REMOVED ' + path + '/' + k)
            compatible(v, new[k], path + '/' + k)
        for k in new.keys() - old.keys():
            if k in ('description', 'summary', 'title'):
                continue
            if path.endswith('/properties') and 'Arguments' not in path and 'ToolContext' not in path:
                continue
            fail('REVIEW_REQUIRED ' + path + '/' + k)
    elif isinstance(old, list) and isinstance(new, list):
        if len(old) != len(new):
            fail('CHANGED ' + path)
        for index, (before, after) in enumerate(zip(old, new)):
            compatible(before, after, path + '/' + str(index))
    elif type(old) is not type(new) or old != new:
        fail('CHANGED ' + path)


def compile_proto(directory, output):
    include = Path(grpc_tools.__file__).parent / '_proto'
    rc = protoc.main(['protoc', f'-I{directory}', f'-I{include}', f'--python_out={output}', f'--grpc_python_out={output}', f'--descriptor_set_out={output}/registry.pb', '--include_imports', str(Path(directory) / 'registry.proto')])
    if rc:
        fail('PROTO_COMPILE')
    ds = descriptor_pb2.FileDescriptorSet.FromString((Path(output) / 'registry.pb').read_bytes())
    return next(f for f in ds.file if f.name == 'registry.proto')


def proto_compatible(old, new):
    # Compare descriptor objects: comments/whitespace are irrelevant; field
    # numbers/types/presence/names and RPC shapes must remain stable.
    old = json_format.MessageToDict(old)
    new = json_format.MessageToDict(new)
    old.pop('sourceCodeInfo', None)
    new.pop('sourceCodeInfo', None)
    compatible(old, new)


def validate_registry(name, value, module):
    msg = getattr(module, name)()
    json_format.ParseDict(value, msg, ignore_unknown_fields=False)
    def device(d):
        if not d.device_id or not d.transformer_id or d.device_type == 0 or d.capability_version == 0:
            fail('REGISTRY_REQUIRED')
        validate('Id', d.device_id)
        validate('Id', d.transformer_id)
        capability(d.minimum_rate_kw, d.maximum_rate_kw, d.priority, d.capability_version, d.device_type == 1, d.controllable)
        if not d.HasField('updated_at'):
            fail('REGISTRY_REQUIRED')
    if name == 'Device':
        device(msg)
    if name == 'RegisterDeviceRequest':
        if not msg.operation_id or not msg.HasField('device'):
            fail('REGISTRY_REQUIRED')
        device(msg.device)
    if name == 'HeartbeatRequest':
        if not msg.operation_id or not msg.device_id or not msg.capability_version or not msg.HasField('observed_at'):
            fail('REGISTRY_REQUIRED')
    if name == 'GetDeviceRequest' and not msg.device_id:
        fail('REGISTRY_REQUIRED')
    if name == 'ListEligibleChargersRequest' and (not msg.transformer_id or not 1 <= msg.limit <= 200 or not msg.HasField('evaluated_at')):
        fail('REGISTRY_REQUIRED')
    if name == 'ChargerPage':
        if len(msg.devices) > 200 or not msg.HasField('observed_at'):
            fail('REGISTRY_REQUIRED')
        for d in msg.devices:
            device(d)
    # Binary round-trip checks generated bindings, not a hand-authored mirror.
    assert type(msg).FromString(msg.SerializeToString()) == msg


def capability(minimum, maximum, priority, version, ev, controllable):
    if not (math.isfinite(minimum) and math.isfinite(maximum) and 0 <= minimum <= maximum and 0 <= priority <= 100 and version > 0):
        fail('REGISTRY_CAPABILITY')
    if not ev and controllable:
        fail('REGISTRY_CAPABILITY')


def load_generated(output):
    spec = importlib.util.spec_from_file_location('registry_pb2', Path(output) / 'registry_pb2.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def registry_json_definitions(descriptor):
    """Generate the REST capability shape from the canonical protobuf Device.

    REST uses snake_case and numeric integers, both accepted by ProtoJSON.
    Domain limits remain in validate_registry, not duplicated schema rules.
    """
    device = next(m for m in descriptor.message_type if m.name == 'Device')
    properties = {}
    for f in device.field:
        if f.type == f.TYPE_STRING:
            schema = {'$ref': '#/$defs/Id'}
        elif f.type == f.TYPE_BOOL:
            schema = {'type': 'boolean'}
        elif f.type == f.TYPE_ENUM:
            schema = {'$ref': '#/$defs/DeviceType'}
        elif f.type == f.TYPE_MESSAGE:
            schema = {'$ref': '#/$defs/Timestamp'}
        elif f.type == f.TYPE_DOUBLE:
            schema = {'type': 'number', 'x-unit': 'kW'}
        else:
            maximum = 2**32 - 1 if f.type == f.TYPE_UINT32 else 2**64 - 1
            schema = {'type': 'integer', 'minimum': 0, 'maximum': maximum}
        properties[f.name] = schema
    return {
        'Device': {
            '$comment': 'Generated from registry.proto by make bindings; do not edit.',
            'type': 'object', 'properties': properties,
            'required': [f.name for f in device.field if not f.proto3_optional],
            'additionalProperties': False,
        },
        'DeviceType': {
            '$comment': 'Generated from registry.proto; UNSPECIFIED is not a valid registration.',
            'type': 'string',
            'enum': [v.name for e in descriptor.enum_type if e.name == 'DeviceType' for v in e.value if v.number],
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-ref', help='Git base to compare; absent v1 is an initial freeze')
    parser.add_argument('--generate', action='store_true', help='Write generated bindings to ignored .tools/generated')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        descriptor = compile_proto(V1, tmp)
        generated = registry_json_definitions(descriptor)
    if args.generate:
        BUNDLE['$defs'].update(generated)
        (V1 / 'bundle.json').write_text(json.dumps(BUNDLE, indent=2) + '\n')
    elif any(BUNDLE['$defs'][name] != schema for name, schema in generated.items()):
        fail('STALE_GENERATED_SCHEMA: run make bindings')
    Draft202012Validator.check_schema(BUNDLE)
    document = json.loads((V1 / 'openapi.json').read_text())
    # Local refs resolve through the canonical schema; no network is used.
    validate_openapi(document, base_uri=(V1 / 'openapi.json').as_uri())
    for name in BUNDLE['$defs']:
        Draft202012Validator.check_schema(BUNDLE['$defs'][name])
    if args.generate:
        output = ROOT / '.tools/generated'
        output.mkdir(parents=True, exist_ok=True)
        compile_proto(V1, output)
    if args.base_ref:
        for filename in ('bundle.json', 'openapi.json', 'registry.proto'):
            path = f'schemas/v1/{filename}'
            exists = subprocess.run(['git', 'cat-file', '-e', f'{args.base_ref}:{path}'], capture_output=True)
            if exists.returncode:
                # Validate the ref itself; a bad ref must never disable the gate.
                subprocess.run(['git', 'rev-parse', '--verify', args.base_ref + '^{commit}'], check=True, capture_output=True)
                print(f'Initial contract: {path} absent at base')
                continue
            before = subprocess.check_output(['git', 'show', f'{args.base_ref}:{path}'])
            if filename.endswith('.json'):
                compatible(json.loads(before), json.loads((V1 / filename).read_text()))
            else:
                with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as out1, tempfile.TemporaryDirectory() as out2:
                    (Path(tmp) / filename).write_bytes(before)
                    proto_compatible(compile_proto(tmp, out1), compile_proto(V1, out2))
    print('Schemas, OpenAPI and compatibility gate passed')
    return subprocess.call([sys.executable, '-m', 'pytest', '-q', str(ROOT / 'tests/contracts')], cwd=ROOT)


if __name__ == '__main__':
    sys.exit(main())
