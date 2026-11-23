"""Real SDK tests: parentage, async isolation, redaction, failure and dimensions."""
import asyncio
import json

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import observability as obs

PARENT = '00-'+'1'*32+'-'+'2'*16+'-01'


@pytest.fixture(scope='module')
def sdk():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    reader = InMemoryMetricReader()
    meters = MeterProvider(metric_readers=[reader])
    meter = meters.get_meter('test')
    obs._runtime = dict(trace=trace, tracer=provider.get_tracer('test'), provider=provider, meters=meters,
        duration=meter.create_histogram('duration'), operations=meter.create_counter('operations'),
        active=meter.create_up_down_counter('active'), api=meter.create_counter('api'),
        reconnects=meter.create_counter('reconnects'), resets=meter.create_counter('resets'),
        api_duration=meter.create_histogram('api_duration'), business=meter.create_gauge('business'),
        outcomes=meter.create_counter('outcomes'), leadership=meter.create_gauge('leader'))
    yield exporter, reader
    obs._runtime = None
    provider.shutdown()
    meters.shutdown()


def test_parentage_failure_and_redacted_correlated_logs(sdk, capsys):
    exporter, reader = sdk
    with pytest.raises(RuntimeError):
        with obs.boundary('kafka.process', [('traceparent', PARENT.encode())],
                          {'correlation_id':'run-273','event_id':'event-1','payload':{'command_id':'command-1'}}):
            with obs.boundary('db.control'):
                raise RuntimeError('password=DO_NOT_LOG SQL query')
    spans = exporter.get_finished_spans()[-2:]
    assert spans[1].parent.span_id == int('2'*16, 16)
    assert spans[0].parent.span_id == spans[1].context.span_id
    assert all(s.status.status_code.name == 'ERROR' and not s.events for s in spans)
    output = capsys.readouterr().err
    assert 'DO_NOT_LOG' not in output and 'SQL query' not in output
    logs = [json.loads(line) for line in output.splitlines()]
    assert all(row['trace_id'] == '1'*32 and row['correlation_id'] == 'run-273' for row in logs)
    assert all(row['command_id'] == 'command-1' for row in logs)
    assert obs.headers() == {}


def test_bad_headers_unknown_labels_and_bounded_inventory(sdk):
    assert 'traceparent' not in obs.carrier({'traceparent':'00-'+'0'*32+'-'+'2'*16+'-01'})
    assert obs.carrier({'authorization':'secret','correlation_id':'x\ny'}) == {}
    assert obs.carrier({'traceparent':bytearray(PARENT.encode())})['traceparent']==PARENT
    with pytest.raises(ValueError):
        with obs.boundary('event-12345'):
            pass
    obs.business({'event_type':'transformer.load','payload':{'transformer_id':'unbounded-123','net_load_kw':9}})
    obs.business({'event_type':'transformer.load','payload':{'transformer_id':'tx-001','net_load_kw':15}})
    obs.business({'event_type':'unbounded-123','payload':{'transformer_id':'tx-001','outcome':'APPLIED'}})
    metrics = sdk[1].get_metrics_data()
    points = [point for r in metrics.resource_metrics for s in r.scope_metrics for m in s.metrics for point in m.data.data_points]
    labels = [dict(p.attributes) for p in points]
    assert {'transformer':'tx-001','signal':'net_load_kw'} in labels
    assert not any('event_id' in p or 'trace_id' in p or 'command_id' in p for p in labels)
    assert not any('unbounded-123' in p.values() for p in labels)


def test_sse_link_and_async_context_isolation(sdk):
    async def request(correlation):
        with obs.boundary('http.request', {'correlation_id':correlation}):
            await asyncio.sleep(0)
            with obs.boundary('sse.send', {'traceparent':PARENT}, link=True):
                assert obs.headers()['correlation_id'] == correlation
    async def run():
        await asyncio.gather(request('a'), request('b'))
    asyncio.run(run())
    sends = [s for s in sdk[0].get_finished_spans() if s.name == 'sse.send']
    assert len(sends) == 2 and all(s.links[0].context.trace_id == int('1'*32,16) for s in sends)
    assert {s.attributes['correlation_id'] for s in sends} == {'a','b'}


def test_asgi_error_and_stream_disconnect(sdk):
    async def app(scope, receive, send):
        await send({'type':'http.response.start','status':503,'headers':[]})
        raise ConnectionError('disconnected with token=SECRET')
    async def run():
        sent = []
        async def send(message):
            sent.append(message)
        with pytest.raises(ConnectionError):
            await obs.HTTPMiddleware(app)({'type':'http','path':'/api/v1/events','method':'GET',
                'headers':[(b'traceparent', PARENT.encode())]}, None, send)
        assert dict(sent[0]['headers'])[b'traceparent'].decode().startswith('00-'+'1'*32)
    asyncio.run(run())
    span = sdk[0].get_finished_spans()[-1]
    assert span.status.status_code.name == 'ERROR'
    metrics = sdk[1].get_metrics_data()
    active = [p.value for r in metrics.resource_metrics for s in r.scope_metrics for m in s.metrics
              if m.name=='active' for p in m.data.data_points]
    assert active == [0]
