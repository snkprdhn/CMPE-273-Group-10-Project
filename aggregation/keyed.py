"""Per-transformer streaming state around the deterministic Aggregator.

No PyFlink import: the Flink operator passes MapState/ValueState objects, tests pass dicts.
State is small and bounded. One MapState entry per five-second window holds the window
and the event IDs seen for that window. Entries older than RETAIN_SECONDS behind the
transformer's watermark are removed, so deduplication never grows with run length.
A replay older than the retained horizon is audited as late and is never recounted.
"""
from datetime import datetime, timezone

from aggregation.engine import Aggregator, decode_reading, instant

RETAIN_SECONDS = 120   # > 30 s allowed lateness + 10 s watermark delay + replay margin
PERSIST_STEP = 5.0     # the persisted watermark may lag the in-memory one by this many seconds
READY_SECONDS = 15     # watermark distance (seconds) after window start at which a window is evaluated


def window_start(at):
    return at.replace(second=at.second // 5 * 5, microsecond=0)


class Keyed:
    """One instance per operator; all per-key data lives in the state passed to process()."""

    def __init__(self, topology, horizon=RETAIN_SECONDS):
        self.engine = Aggregator(topology, None, horizon)
        self.known = frozenset(t['transformer_id'] for t in topology['transformers'])
        self.memory = {}   # shard -> in-memory watermark; restore falls back to the persisted one

    def shard(self, key):
        # Unknown keys share one audit shard so they cannot create unbounded state.
        return key if key in self.known else ''

    def process(self, key, raw, partition, offset, slots, meta_state, now=None, observe=None):
        """Return (results, metric_deltas, window_late_delta).

        slots: mapping with get/put/remove keyed by window-start epoch seconds.
        meta_state: object with value()/update().
        """
        engine = self.engine
        now = now or datetime.now(timezone.utc)
        meta = meta_state.value()
        fresh_meta = meta is None
        if fresh_meta:
            meta = dict(greatest=None, saved=None, starts=[], pending=[], late=0)
        starts, pending = list(meta['starts']), set(meta['pending'])
        late = meta['late']
        shard = self.shard(key)
        engine.greatest = max((g for g in (meta['greatest'], self.memory.get(shard)) if g is not None), default=None)
        engine.windows = {}
        engine.seen = set()
        before = engine.metrics.copy()
        decoded = None
        try:
            decoded = decode_reading(raw)
            start = window_start(instant(decoded['event_time']))
            epoch = int(start.timestamp())
        except (TypeError, ValueError, KeyError, OverflowError, AttributeError):
            start = epoch = None
        loaded = {}   # epoch -> slot touched in this call
        changed_meta = fresh_meta
        if epoch is not None:
            slot = slots.get(epoch)
            if slot is None:
                slot = dict(w=None, ids=set())
            loaded[epoch] = slot
            engine.seen = slot['ids']
            if slot['w'] is not None:
                engine.windows[(key, start)] = slot['w']
        wkey = (key, start)
        previous_late = engine.windows.get(wkey, {}).get('late', 0)
        results = engine.reading(raw, key, now, partition, offset, decoded=decoded)
        if observe is not None and decoded is not None:
            observe(engine, decoded)   # sampled trace context is stored with the window, before it is saved
        if wkey in engine.windows:
            window = engine.windows[wkey]
            late += window['late'] - previous_late
            if window['revision'] < 0 and epoch not in pending:
                pending.add(epoch)
                changed_meta = True
        if engine.greatest is not None:
            for ready in sorted(pending):
                if ready in loaded:
                    continue
                if (engine.greatest - datetime.fromtimestamp(ready, timezone.utc)).total_seconds() >= READY_SECONDS:
                    other = slots.get(ready)
                    if other is not None and other['w'] is not None:
                        loaded[ready] = other
                        engine.windows[(key, datetime.fromtimestamp(ready, timezone.utc))] = other['w']
        results += engine.evaluate(now)
        for (tx, begun), window in engine.windows.items():
            ep = int(begun.timestamp())
            if window['revision'] >= 0 and ep in pending:
                pending.discard(ep)
                changed_meta = True
            slot = loaded.get(ep)
            if slot is not None:
                slot['w'] = window
        for ep, slot in loaded.items():
            if slot['w'] is not None or slot['ids']:
                slots.put(ep, slot)
                if ep not in starts:
                    starts.append(ep)
                    changed_meta = True
        # Remove state behind the replay horizon.
        if engine.greatest is not None:
            cutoff = engine.greatest.timestamp() - RETAIN_SECONDS
            keep = []
            for ep in sorted(starts):
                if ep < cutoff:
                    slots.remove(ep)
                    pending.discard(ep)
                    changed_meta = True
                else:
                    keep.append(ep)
            starts = keep
        greatest = engine.greatest
        if greatest is not None:
            self.memory[shard] = greatest
        saved = meta['saved']
        if greatest is not None and (saved is None or (greatest - saved).total_seconds() >= PERSIST_STEP):
            saved, changed_meta = greatest, True
        if late != meta['late']:
            changed_meta = True
        if changed_meta:
            # A lagging persisted watermark only delays emission after restore; it never advances early.
            meta_state.update(dict(greatest=greatest, saved=saved, starts=starts, pending=sorted(pending), late=late))
        deltas = {name: engine.metrics[name] - before[name] for name in before}
        return results, deltas, late - meta['late']
