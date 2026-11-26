"""Trace context follows selected readings in the existing checkpointed state."""
import json

from observability import carrier
from member2.aggregation import instant


def remember(engine, raw, values):
    try:
        event = json.loads(raw) if isinstance(raw, str) else raw
        at = instant(event['event_time'])
        start = at.replace(second=at.second // 5 * 5, microsecond=0)
        w = engine.windows.get((event['payload']['transformer_id'], start))
        device = event['payload']['device_id']
        if w and w['devices'].get(device, {}).get('event_id') == event['event_id']:
            # At most one context per selected device/window, bounded by run state.
            w.setdefault('trace_context', {})[device] = carrier(values) | {
                'correlation_id': event['correlation_id'], 'causation_id': event['event_id'],
                'scenario_run_id': event['correlation_id']}
    except (ValueError, KeyError, TypeError):
        pass  # The existing Aggregator, not instrumentation, owns DLQ decisions.


def output_context(engine, key, event):
    if event['event_type'] != 'transformer.load':
        return {}
    w = engine.windows[(key, instant(event['payload']['window_start']))]
    contexts = w.get('trace_context', {})
    # Fan-in is represented by a span link to a contributing reading. Other
    # selected reading IDs remain in checkpoint state, not metric labels.
    return next((contexts[d] for d in sorted(contexts)), {})
