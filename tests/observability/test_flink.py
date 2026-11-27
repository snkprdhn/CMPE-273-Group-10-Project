import json
import pickle
from datetime import datetime, timezone
from pathlib import Path

from aggregation.engine import Aggregator
from aggregation.tracing import remember, output_context


def test_context_checkpoints_selected_reading_and_ignores_invalid_input():
    topology = json.loads(Path('devices/fixtures/topology.json').read_text())
    from devices.simulator import scenario
    now = datetime(2026,11,22,18,tzinfo=timezone.utc)
    engine = Aggregator(topology)
    for event in scenario(273, now, steps=21):
        raw = json.dumps(event)
        engine.reading(raw, 'tx-001', now)
        remember(engine, raw, {'traceparent':'00-'+'1'*32+'-'+'2'*16+'-01'})
    remember(engine, '{bad', {'authorization':'secret'})
    restored = pickle.loads(pickle.dumps(engine))
    outputs = restored.evaluate(now)
    assert outputs
    for _, key, event in outputs:
        context = output_context(restored, key, event)
        assert context['traceparent'].endswith('-01') and context['causation_id']
        assert context['correlation_id'] == 'scenario-001'
        assert 'authorization' not in context
