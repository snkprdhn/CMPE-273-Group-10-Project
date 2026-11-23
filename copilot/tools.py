"""Seven typed handlers. No executable/query text crosses this boundary."""
import asyncio
import hashlib
import json
import math
from uuid import uuid4
import httpx
from jsonschema import ValidationError
from copilot.contracts import DEFS, validate, now, stamp, instant
from identity.auth import authorize_tool, Denied
from observability import traced

def redacted(value, schema):
    """Drop undeclared fields recursively, including additive event attributes."""
    if '$ref' in schema:
        return redacted(value, DEFS[schema['$ref'].rsplit('/',1)[1]])
    if value is None:
        return None
    if 'anyOf' in schema:
        return redacted(value, next(s for s in schema['anyOf'] if s.get('type') != 'null'))
    if isinstance(value, dict):
        return {key:redacted(item,schema['properties'][key]) for key,item in value.items() if key in schema.get('properties',{})}
    if isinstance(value, list):
        return [redacted(item,schema['items']) for item in value]
    return value

class ToolFailure(Exception):
    def __init__(self, code):
        self.code = code

async def bounded_json(client, path, params=None):
    async with client.stream('GET', path, params=params) as response:
        response.raise_for_status()
        raw = bytearray()
        async for chunk in response.aiter_bytes():
            raw.extend(chunk)
            if len(raw) > 65536:
                raise ToolFailure('RESULT_TOO_LARGE')
        return json.loads(raw)

class Tools:
    def __init__(self, projection, identity, metrics=None, tempo=None):
        self.projection, self.identity = projection, identity
        self.metrics, self.tempo = metrics, tempo

    @traced('investigation.tool')
    async def call(self, token, call, context):
        try:
            if not isinstance(call, dict):
                raise ToolFailure('INVALID_ARGUMENT')
            authorize_tool(self.identity, token, call.get('tool'))
            validate('ToolCall', call)
            validate('ToolContext', context)
            actor = self.identity.authenticate(token)
            if actor['actor'] != context['actor_id']:
                raise ToolFailure('FORBIDDEN')
            remaining = (instant(context['deadline']) - now()).total_seconds()
            if remaining <= 0:
                raise ToolFailure('DEADLINE_EXCEEDED')
            async with asyncio.timeout(min(3, remaining)):
                result = await self.execute(token, call, context)
            validate('ToolResult', result)
            if len(json.dumps(result, allow_nan=False).encode()) > 65536:
                raise ToolFailure('RESULT_TOO_LARGE')
            return result
        except Denied:
            code = 'FORBIDDEN'
        except (ValidationError, ValueError, TypeError, KeyError, RecursionError, OverflowError):
            code = 'INVALID_ARGUMENT'
        except ToolFailure as exc:
            code = exc.code
        except TimeoutError:
            code = 'DEADLINE_EXCEEDED'
        except httpx.HTTPStatusError as exc:
            code = 'NOT_FOUND' if exc.response.status_code == 404 else 'DEPENDENCY_UNAVAILABLE'
        except httpx.HTTPError:
            code = 'DEPENDENCY_UNAVAILABLE'
        result = {'error': dict(schema_version=1, code=code, message='Tool could not provide authorized evidence: ' + code,
                                retryable=code in ('DEADLINE_EXCEEDED', 'DEPENDENCY_UNAVAILABLE'), observed_at=stamp())}
        validate('ToolResult', result)
        return result

    async def execute(self, token, call, context):
        name, args = call['tool'], dict(call['arguments'])
        scope = context['entity_scope']
        for field, collection in [('transformer_id', 'transformer_ids'), ('action_id', 'action_ids'),
                                  ('entity_id', 'transformer_ids'), ('trace_id', 'trace_ids'), ('correlation_id', 'correlation_ids')]:
            if field in args and args[field] not in scope[collection]:
                raise ToolFailure('OUT_OF_SCOPE')
        if 'start' in args:
            start, end = instant(args['start']), instant(args['end'])
            if not 0 < (end-start).total_seconds() <= 3600:
                raise ToolFailure('INVALID_ARGUMENT')
            args['start'] = stamp(max(start, instant(context['time_scope']['start'])))
            args['end'] = stamp(min(end, instant(context['time_scope']['end'])))
            if instant(args['start']) >= instant(args['end']):
                raise ToolFailure('OUT_OF_SCOPE')
        cap = min(200, context['max_items'], args.get('limit', 200))
        entity = args.get('transformer_id', args.get('action_id', args.get('trace_id', args.get('correlation_id', args.get('entity_id', 'system')))))
        kind, fresh, truncated = 'TRANSFORMER', 'UNKNOWN', False
        headers = {'Authorization': 'Bearer ' + token}
        # The client is request-local: identities cannot bleed across investigations.
        async with httpx.AsyncClient(base_url=self.projection, headers=headers, timeout=3, trust_env=False) as client:
            if name == 'get_transformer_snapshot':
                data = await bounded_json(client, '/api/v1/transformers/' + entity)
                if data['transformer_id'] != entity:
                    raise ToolFailure('OUT_OF_SCOPE')
                fresh = data['freshness']
            elif name == 'get_load_history':
                page = await bounded_json(client, '/api/v1/transformers/' + entity + '/loads',
                                          dict(start=args['start'], end=args['end'], limit=cap))
                data, fresh, truncated = {'windows': page['items']}, page['freshness'], bool(page['next_cursor'])
            elif name in ('get_action_timeline', 'get_command_results'):
                packet = await bounded_json(client, '/internal/investigator/projection',
                    dict(transformer_id=entity) if name == 'get_action_timeline' else dict(action_id=entity))
                if scope['transformer_ids'] and any(e['payload']['transformer_id'] not in scope['transformer_ids'] for e in packet['actions']):
                    raise ToolFailure('OUT_OF_SCOPE')
                if name == 'get_action_timeline':
                    data = {key: [e for e in packet[key] if instant(args['start']) <= instant(e['event_time']) < instant(args['end'])][:cap]
                            for key in ('actions', 'alerts')}
                    truncated = any(len(packet[k]) > cap for k in data)
                else:
                    kind = 'ACTION'
                    results = {r['command_id']: r for r in packet['results']}
                    data = dict(commands=[dict(command=c, result=results.get(c['command_id'])) for c in packet['commands'][:cap]],
                                confirmed_reduction_kw=packet['confirmed_reduction_kw'])
                    truncated = len(packet['commands']) > cap
                fresh = 'PARTIAL'  # Retained projection history cannot establish a complete physical ledger.
            elif name == 'get_metric_window':
                kind = 'METRIC'
                data = await self.metric(args, cap)
                fresh = 'PARTIAL' if data['missing_intervals'] else 'FRESH'
            elif name == 'get_stream_health':
                kind = 'STREAM'
                data = await self.stream_health(args['components'])
                fresh = 'FRESH'
            else:
                kind = 'TRACE'
                if not self.tempo or 'trace_id' not in args:
                    raise ToolFailure('DEPENDENCY_UNAVAILABLE')
                async with httpx.AsyncClient(base_url=self.tempo, timeout=3, trust_env=False) as backend:
                    trace = await bounded_json(backend, '/api/traces/' + args['trace_id'])
                spans = []
                for batch in trace.get('batches', trace.get('resourceSpans', [])):
                    for group in batch.get('scopeSpans', []):
                        for span in group.get('spans', []):
                            start, end = int(span['startTimeUnixNano'])/1e9, int(span['endTimeUnixNano'])/1e9
                            from datetime import datetime, timezone
                            spans.append(dict(service='redacted', span='span', duration_ms=max(0, (end-start)*1000),
                                error_category='DEPENDENCY' if span.get('status', {}).get('code') in (2, 'STATUS_CODE_ERROR') else 'NONE',
                                retry_count=0, start=stamp(datetime.fromtimestamp(start, timezone.utc)), end=stamp(datetime.fromtimestamp(end, timezone.utc))))
                data, fresh, truncated = {'spans': spans[:cap]}, 'PARTIAL', len(spans) > cap
        data = redacted(data, DEFS[name+'Evidence']['properties']['data'])
        version = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        evidence = dict(schema_version=1, evidence_id=uuid4().hex, source_type=name, source_version=version,
            entity_type=kind, entity_id=entity, observed_at=stamp(), freshness=fresh, truncated=truncated,
            deep_link='/investigations/' + context['investigation_id'], data=data)
        return {'evidence': [evidence]}

    async def stream_health(self, components):
        if not self.metrics:
            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
        # Full v1 summaries require all six collected signals. Absent/nonapplicable
        # scalars cannot be represented as null in v1, so return an evidence gap.
        # Optional recording rules may provide this normalized read-only view.
        rows = []
        async with httpx.AsyncClient(base_url=self.metrics, timeout=3, trust_env=False) as client:
            for component in components:
                row = dict(component=component, freshness='FRESH')
                for field in ('lag', 'watermark_delay_seconds', 'checkpoint_age_seconds', 'ready', 'restarts'):
                    raw = await bounded_json(client, '/api/v1/query',
                        dict(query='grid_stream_' + field + '{component="' + component + '"}'))
                    series = raw.get('data', {}).get('result', [])
                    if raw.get('status') != 'success' or len(series) != 1:
                        raise ToolFailure('DEPENDENCY_UNAVAILABLE')
                    timestamp, value = series[0]['value']
                    value = float(value)
                    if not math.isfinite(value) or value < 0 or abs(now().timestamp()-float(timestamp)) > 15:
                        raise ToolFailure('DEPENDENCY_UNAVAILABLE')
                    if field == 'ready':
                        if value not in (0,1):
                            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
                        row[field] = bool(value)
                    elif field in ('lag','restarts'):
                        if not value.is_integer():
                            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
                        row[field] = int(value)
                    else:
                        row[field] = value
                rows.append(row)
        return dict(components=rows)

    async def metric(self, args, cap):
        if not self.metrics or args.get('entity_id'):
            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
        # Fixed aggregate queries; entity-specific data is unavailable rather than mislabelled.
        queries = {'INPUT_RATE': 'sum(rate(grid_operations_total{boundary="kafka.process"}[1m]))',
                   'OUTPUT_RATE': 'sum(rate(grid_operations_total{boundary="kafka.delivery"}[1m]))',
                   'KAFKA_LAG': 'sum(kafka_consumergroup_lag)',
                   'WATERMARK_DELAY': 'max(flink_taskmanager_job_task_operator_watermark_delay_ms)/1000',
                   'CHECKPOINT_AGE': 'max(flink_jobmanager_job_lastCheckpointDuration)/1000',
                   'RESTARTS': 'sum(kube_pod_container_status_restarts_total)',
                   'DATA_AGE': 'max(grid_data_age_seconds)'}
        if args['metric'] == 'CHECKPOINT_AGE':
            raise ToolFailure('DEPENDENCY_UNAVAILABLE')  # duration is not age
        start, end = instant(args['start']).timestamp(), instant(args['end']).timestamp()
        step = max(args['resolution_seconds'], math.ceil((end-start)/max(1,cap-1)))
        async with httpx.AsyncClient(base_url=self.metrics, timeout=3, trust_env=False) as client:
            raw = await bounded_json(client, '/api/v1/query_range', dict(query=queries[args['metric']], start=start, end=end, step=step))
        from datetime import datetime, timezone
        series = raw['data']['result']
        if raw.get('status') != 'success' or len(series) > 1:
            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
        points = [dict(time=stamp(datetime.fromtimestamp(float(t), timezone.utc)), value=float(v)) for t,v in (series[0]['values'] if series else [])]
        if any(not math.isfinite(p['value']) or p['value'] < 0 or not start <= instant(p['time']).timestamp() <= end for p in points):
            raise ToolFailure('DEPENDENCY_UNAVAILABLE')
        return dict(points=points[:cap], unit='events_per_second' if args['metric'] in ('INPUT_RATE','OUTPUT_RATE') else 'count' if args['metric'] in ('KAFKA_LAG','RESTARTS') else 'seconds',
                    threshold=None, missing_intervals=[dict(start=args['start'], end=args['end'])], queried_at=stamp())
