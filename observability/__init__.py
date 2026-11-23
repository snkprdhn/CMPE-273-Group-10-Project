"""Member_1-owned explicit instrumentation; disabled exporters need no SDK.

No business data, SQL text, credentials, or arbitrary metric labels live here.
Each service owns its calls to this package and its independent environment.
"""
import atexit
import asyncio
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
import json
import inspect
import os
import re
import socket
import sys
from time import monotonic as clock
import threading

FIELDS = ('correlation_id', 'causation_id', 'event_id', 'transformer_id', 'device_id',
          'action_id', 'command_id', 'scenario_run_id', 'investigation_id', 'tool_name',
          'attempt', 'remaining_deadline_ms')
BOUNDARIES = frozenset(('kafka.publish', 'kafka.delivery', 'kafka.process', 'aggregation.process',
    'control.load', 'control.result', 'control.tick', 'control.reconcile',
    'db.control', 'db.projection', 'db.registry', 'db.gateway', 'grpc.client',
    'grpc.server', 'leadership.step', 'http.client', 'http.request', 'sse.send'))
_fields = ContextVar('grid_log_fields', default={})
_runtime = None
_service = 'smart-grid'
_lock = threading.Lock()


def setup(service):
    """Opt-in bounded OTLP exporters; a broken SDK configuration fails startup."""
    global _runtime, _service
    _service = os.getenv('OTEL_SERVICE_NAME', service)
    if _runtime is not None or not os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'):
        return
    with _lock:
        if _runtime is not None:
            return
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.view import View, ExplicitBucketHistogramAggregation
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        resource = Resource.create({'service.name': os.getenv('OTEL_SERVICE_NAME', service),
            'service.version': os.getenv('GRID_VERSION', 'local'),
            'service.instance.id': os.getenv('HOSTNAME', socket.gethostname()),
            'deployment.environment.name': os.getenv('GRID_ENVIRONMENT', 'local')})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(timeout=2),
            max_queue_size=2048, max_export_batch_size=128, schedule_delay_millis=500))
        reader = PeriodicExportingMetricReader(OTLPMetricExporter(timeout=2),
            export_interval_millis=2000, export_timeout_millis=3000)
        meters = MeterProvider(resource=resource, metric_readers=[reader], views=[View(
            instrument_name='*duration_seconds', aggregation=ExplicitBucketHistogramAggregation(
                boundaries=(.005,.01,.025,.05,.1,.25,.5,1,2,3,5,10)))])
        meter = meters.get_meter('smart-grid')
        trace.set_tracer_provider(provider)
        _runtime = dict(trace=trace, provider=provider, meters=meters,
            tracer=provider.get_tracer('smart-grid'),
            duration=meter.create_histogram('grid_boundary_duration_seconds', unit='s'),
            operations=meter.create_counter('grid_operations'),
            active=meter.create_up_down_counter('grid_sse_clients'),
            resets=meter.create_counter('grid_sse_resets'),
            reconnects=meter.create_counter('grid_sse_reconnects'),
            business=meter.create_gauge('grid_business_kw'),
            leadership=meter.create_gauge('grid_leader'),
            api=meter.create_counter('grid_api_requests'),
            outcomes=meter.create_counter('grid_domain_outcomes'),
            api_duration=meter.create_histogram('grid_api_duration_seconds', unit='s'))
        atexit.register(shutdown)
        for operation in ('query', 'mutation', 'health', 'sse'):
            for outcome in ('ok', 'error'):
                _runtime['api'].add(0, {'operation': operation, 'outcome': outcome})


def shutdown():
    if _runtime:
        _runtime['meters'].shutdown(timeout_millis=3000)
        _runtime['provider'].shutdown()


def carrier(values=None):
    """Only allow W3C trace context and bounded, printable correlation fields."""
    source = dict(values or {})
    result = {}
    for key in ('traceparent', 'tracestate', *FIELDS):
        value = source.get(key)
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = bytes(value).decode('ascii', errors='replace')
        if isinstance(value, str) and len(value) <= 512 and value.isprintable():
            result[key] = value
    parent = result.get('traceparent', '')
    if not re.fullmatch(r'00-(?!0{32})[a-f0-9]{32}-(?!0{16})[a-f0-9]{16}-0[01]', parent):
        result.pop('traceparent', None)
        result.pop('tracestate', None)
    return result


def headers(values=None):
    result = carrier(values)
    result.update({k: str(v) for k, v in _fields.get().items() if v is not None})
    if _runtime:
        from opentelemetry.propagate import inject
        inject(result)
    return result


def log(outcome, **fields):
    context = _fields.get() | {k: v for k, v in fields.items() if k in FIELDS}
    ids = dict(trace_id=None, span_id=None)
    if _runtime:
        span = _runtime['trace'].get_current_span().get_span_context()
        if span.is_valid:
            ids = dict(trace_id=f'{span.trace_id:032x}', span_id=f'{span.span_id:016x}')
    record = dict.fromkeys(FIELDS)
    record.update(context)
    record.update(timestamp=datetime.now(timezone.utc).isoformat(), severity='INFO',
        service=_service, version=os.getenv('GRID_VERSION', 'local'),
        instance=os.getenv('HOSTNAME', socket.gethostname()), environment_id=os.getenv('GRID_ENVIRONMENT', 'local'),
        outcome=outcome, **ids)
    # Explicit allowlist: exception messages, bodies and credentials never enter logs.
    record.update({k: v for k, v in fields.items() if k in ('boundary', 'error_category', 'status_code')})
    sys.stderr.write(json.dumps(record, allow_nan=False, separators=(',', ':')) + '\n')
    sys.stderr.flush()


@contextmanager
def boundary(name, values=None, event=None, *, link=False):
    if name not in BOUNDARIES:
        raise ValueError('unknown metric boundary')
    incoming = carrier(values)
    fields = _fields.get() | {k: v for k, v in incoming.items() if k in FIELDS}
    if isinstance(event, dict):
        fields.update({k: v for k, v in carrier(event).items() if k in FIELDS})
        payload = event.get('payload')
        if isinstance(payload, dict):
            fields.update({k: v for k, v in carrier(payload).items() if k in FIELDS})
    token = _fields.set(fields)
    started, outcome = clock(), 'ok'
    manager = nullcontext(None)
    if _runtime:
        from opentelemetry.propagate import extract
        from opentelemetry.trace import Link
        context = extract(incoming) if 'traceparent' in incoming else None
        links = []
        if link and context is not None:
            parent = _runtime['trace'].get_current_span(context).get_span_context()
            if parent.is_valid:
                links.append(Link(parent))
            context = None
        manager = _runtime['tracer'].start_as_current_span(name, context=context, links=links,
            attributes={k: v for k, v in fields.items() if v is not None},
            record_exception=False, set_status_on_exception=False)
    try:
        with manager as span:
            try:
                yield span
            except BaseException as exc:
                outcome = 'cancelled' if isinstance(exc, (asyncio.CancelledError, GeneratorExit)) else 'error'
                if span and outcome == 'error':
                    from opentelemetry.trace import Status, StatusCode
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute('error.type', type(exc).__name__)
                log(outcome, boundary=name, error_category=type(exc).__name__)
                raise
            else:
                if span and span.status.status_code.name == 'ERROR':
                    outcome = 'error'
                log(outcome, boundary=name)
    finally:
        if _runtime:
            labels = {'boundary': name, 'outcome': outcome}
            _runtime['duration'].record(clock() - started, labels)
            _runtime['operations'].add(1, labels)
        _fields.reset(token)


def traced(name, event_arg=None):
    def decorate(fn):
        if inspect.iscoroutinefunction(fn):
            @wraps(fn)
            async def async_call(*args, **kwargs):
                with boundary(name):
                    return await fn(*args, **kwargs)
            return async_call
        @wraps(fn)
        def call(*args, **kwargs):
            event = kwargs.get('event') or (args[event_arg] if event_arg is not None and len(args) > event_arg else None)
            with boundary(name, event=event):
                return fn(*args, **kwargs)
        return call
    return decorate


def business(event):
    if not _runtime:
        return
    p = event['payload']
    outcome = p.get('outcome', p.get('status'))
    if event['event_type'] in ('command.result', 'action.transition') and outcome in ('APPLIED', 'ALREADY_APPLIED', 'REJECTED', 'EXPIRED', 'FAILED',
                   'PLANNED', 'COMMANDS_DISPATCHED', 'REDUCTION_CONFIRMED', 'PARTIAL',
                   'HOLDING', 'RESTORING', 'COMPLETED', 'NEEDS_REVIEW'):
        _runtime['outcomes'].add(1, {'outcome': outcome, 'kind': event['event_type']})
    tx = p.get('transformer_id')
    if tx not in os.getenv('GRID_TRANSFORMERS', 'tx-001').split(','):
        return  # Unknown inventory never creates new time series.
    for field in ('net_load_kw', 'ev_demand_kw', 'target_reduction_kw', 'expected_reduction_kw', 'confirmed_reduction_kw'):
        if field in p and p[field] is not None:
            _runtime['business'].set(p[field], {'transformer': tx, 'signal': field})


def leader(held):
    if _runtime:
        _runtime['leadership'].set(int(held))


def sse_reset():
    if _runtime:
        _runtime['resets'].add(1)


class HTTPMiddleware:
    """ASGI response-start latency and full streaming spans, including failures."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        values = {k.decode(): v.decode() for k, v in scope['headers']}
        streaming = scope['path'] == '/api/v1/events'
        operation = 'sse' if streaming else ('health' if scope['path'] in ('/api/v1/health', '/api/v1/ready') else 'query' if scope['method'] == 'GET' else 'mutation')
        started = clock()
        with boundary('http.request', values) as span:
            async def observed(message):
                if message['type'] == 'http.response.start':
                    status = message['status']
                    if span:
                        span.set_attribute('http.response.status_code', status)
                        if status >= 500:
                            from opentelemetry.trace import Status, StatusCode
                            span.set_status(Status(StatusCode.ERROR))
                    if _runtime:
                        labels = {'operation': operation, 'outcome': 'error' if status >= 400 else 'ok'}
                        _runtime['api'].add(1, labels)
                        _runtime['api_duration'].record(clock() - started, labels)
                    message = dict(message, headers=[*message.get('headers', []),
                        *[(k.encode(), v.encode()) for k, v in headers().items() if k == 'traceparent']])
                    log('response', boundary='http.request', status_code=status)
                await send(message)
            if streaming and _runtime:
                _runtime['active'].add(1)
                if 'last-event-id' in values:
                    _runtime['reconnects'].add(1)
            try:
                await self.app(scope, receive, observed)
            finally:
                if streaming and _runtime:
                    _runtime['active'].add(-1)
