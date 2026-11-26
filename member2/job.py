"""PyFlink entry point. Kafka metadata stays available for validation/audit."""
import argparse
from datetime import datetime, timezone
import json
import hashlib
import pickle
import re

from pyflink.common import Configuration, Row, Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

from member2.aggregation import Aggregator, LOAD, DLQ, LATE, instant, decode_reading
from observability import setup, boundary, headers, business
from member2.tracing import remember, output_context


class EventIds:
    def __init__(self, entries, count):
        self.entries = entries
        self.count = count

    def __contains__(self, event_id):
        return self.entries.contains(event_id)

    def __len__(self):
        return self.count

    def add(self, event_id):
        self.entries.put(event_id, True)
        self.count += 1


class Aggregate(KeyedProcessFunction):
    def __init__(self, topology, max_events):
        self.topology = topology
        self.max_events = max_events

    def open(self, runtime_context):
        setup('member2.aggregation')
        self.engine = Aggregator(self.topology, self.max_events)
        self.signature = hashlib.sha256(json.dumps([self.topology, self.max_events], sort_keys=True).encode()).digest()
        self.ids = runtime_context.get_map_state(MapStateDescriptor('event-ids', Types.STRING(), Types.BOOLEAN()))
        self.windows = runtime_context.get_map_state(MapStateDescriptor('windows', Types.PICKLED_BYTE_ARRAY(), Types.PICKLED_BYTE_ARRAY()))
        self.metadata = runtime_context.get_state(ValueStateDescriptor('run-meta', Types.PICKLED_BYTE_ARRAY()))
        self.state = runtime_context.get_state(ValueStateDescriptor('run-v1', Types.PRIMITIVE_ARRAY(Types.BYTE())))
        metrics = runtime_context.get_metrics_group()
        self.counts = {name: metrics.counter(name) for name in ('duplicates', 'dlq', 'very_late', 'corrections', 'input', 'output')}
        self.watermark = -1
        self.latest = 0
        self.id_count = 0
        self.accepted_late = 0
        metrics.gauge('retained_event_ids', lambda: self.id_count)
        metrics.gauge('semantic_watermark_ms', lambda: self.watermark)
        metrics.gauge('accepted_late_count', lambda: self.accepted_late)
        metrics.gauge('watermark_delay_ms', lambda: int(datetime.now(timezone.utc).timestamp()*1000) - self.watermark if self.watermark != -1 else -1)
        metrics.gauge('latest_output_ms', lambda: self.latest)

    def process_element(self, row, ctx):
        with boundary('aggregation.process', row[4] or {}):
            yield from self._process(row, ctx)

    def _process(self, row, ctx):
        engine = self.engine
        meta = self.metadata.value()
        if meta is None:
            stored = self.state.value()
            if stored is not None:
                old = pickle.loads(bytes(stored))
                if old.topology != self.topology or old.max_events != self.max_events:
                    raise RuntimeError('TOPOLOGY_CHANGED')
                for event_id in old.seen:
                    self.ids.put(event_id, True)
                for key, window in old.windows.items():
                    self.windows.put(key, window)
                meta = self.signature, len(old.seen), old.greatest, {
                    key for key, window in old.windows.items() if window['revision'] < 0
                }, sum(w['late'] for w in old.windows.values())
                self.state.clear()
        signature, count, engine.greatest, pending, late = meta or (self.signature, 0, None, set(), 0)
        if signature != self.signature:
            raise RuntimeError('TOPOLOGY_CHANGED: restore only with identical run fixture')
        engine.seen = EventIds(self.ids, count)
        engine.windows = {}
        decoded = None
        try:
            decoded = decode_reading(row[1])
            at = instant(decoded['event_time'])
            key = row[0], at.replace(second=at.second // 5 * 5, microsecond=0)
        except (TypeError, ValueError, KeyError):
            key = None
        if key is not None:
            window = self.windows.get(key)
            if window is not None:
                engine.windows[key] = window
        previous_late = engine.windows.get(key, {}).get('late', 0)
        before = engine.metrics.copy()
        now = datetime.now(timezone.utc)
        self.counts['input'].inc()
        results = engine.reading(row[1], row[0], now, row[2], row[3], decoded=decoded)
        if key is not None:
            remember(engine, decoded, row[4] or {})
        if key in engine.windows:
            window = engine.windows[key]
            late += window['late'] - previous_late
            if window['revision'] < 0:
                pending.add(key)
        if engine.greatest is not None:
            for ready in pending:
                if ready not in engine.windows and (engine.greatest - ready[1]).total_seconds() >= 15:
                    engine.windows[ready] = self.windows.get(ready)
        results += engine.evaluate(now)
        for changed, window in engine.windows.items():
            self.windows.put(changed, window)
            if window['revision'] >= 0:
                pending.discard(changed)
        self.id_count = len(engine.seen)
        self.metadata.update((self.signature, len(engine.seen), engine.greatest, pending, late))
        for name in before:
            self.counts[name].inc(engine.metrics[name] - before[name])
        self.accepted_late = late
        if engine.greatest:
            self.watermark = int(engine.greatest.timestamp()*1000)-10000
        for topic, key, event in results:
            self.counts['output'].inc()
            self.latest = int(now.timestamp()*1000)
            context = output_context(engine, key, event)
            with boundary('kafka.publish', context, event, link=True):
                business(event)
                yield Row(topic, key, json.dumps(event, allow_nan=False, separators=(',', ':')),
                    {k: v.encode() for k, v in headers().items() if k in ('traceparent', 'correlation_id', 'causation_id', 'scenario_run_id')})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='kafka:9092')
    parser.add_argument('--run', required=True)
    parser.add_argument('--prefix', help='explicit shared integration topic namespace')
    parser.add_argument('--topology', default='/app/member2/topology.json')
    parser.add_argument('--checkpoint', default='file:///checkpoints')
    parser.add_argument('--checkpoint-ms', type=int, default=2000)
    parser.add_argument('--max-events', type=int, default=100000)
    parser.add_argument('--state-backend', choices=('hashmap', 'rocksdb'), default='hashmap')
    parser.add_argument('--demo-transformer-parallelism', type=int,
                        help='isolated demo profile with transformer-local watermarks and deduplication')
    args = parser.parse_args()
    demo_parallelism = args.demo_transformer_parallelism
    if demo_parallelism is not None and (not 2 <= demo_parallelism <= 32 or args.prefix is not None
                                        or args.checkpoint == 'file:///checkpoints'):
        parser.error('parallel demo requires 2–32 subtasks, run-scoped topics and an explicit checkpoint directory')
    if not re.fullmatch(r'[A-Za-z0-9_.:,\[\]-]+', args.bootstrap):
        parser.error('bootstrap must contain Kafka host:port endpoints')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,48}', args.run):
        parser.error('run must be 1-48 ASCII letters/digits/underscore/hyphen')
    if args.prefix is not None and not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]{0,79}\.', args.prefix):
        parser.error('prefix must be a bounded Kafka namespace ending in a period')
    if not 1000 <= args.checkpoint_ms <= 60000:
        parser.error('checkpoint-ms must be between 1000 and 60000')
    with open(args.topology) as fixture:
        topology = json.load(fixture)
    Aggregator(topology, args.max_events)  # Fail before starting a source.
    conf = Configuration()
    conf.set_string('state.backend.type', args.state_backend)
    if args.state_backend == 'rocksdb':
        conf.set_boolean('state.backend.incremental', True)
        conf.set_boolean('state.backend.rocksdb.memory.managed', True)
    conf.set_string('python.executable', '/opt/venv/bin/python')
    conf.set_string('restart-strategy.type', 'fixed-delay')
    conf.set_string('restart-strategy.fixed-delay.attempts', '10')
    conf.set_string('restart-strategy.fixed-delay.delay', '3 s')
    conf.set_string('execution.checkpointing.dir', args.checkpoint)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(demo_parallelism or 1)
    env.enable_checkpointing(args.checkpoint_ms, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().set_externalized_checkpoint_retention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
    tables = StreamTableEnvironment.create(env)
    tables.get_config().get_configuration().set_string('pipeline.name', f'member2-{args.run}')
    prefix = args.prefix if args.prefix is not None else f'm2-{args.run}.'
    # Raw formats permit malformed payloads and null/wrong keys to reach the DLQ.
    tables.execute_sql(f"""CREATE TABLE telemetry (
        kafka_key STRING, body STRING,
        part INT METADATA FROM 'partition' VIRTUAL,
        off BIGINT METADATA FROM 'offset' VIRTUAL,
        trace_headers MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL
    ) WITH (
        'connector'='kafka', 'topic'='{prefix}grid.telemetry.v1',
        'properties.bootstrap.servers'='{args.bootstrap}',
        'properties.group.id'='m2-{args.run}-aggregation-v1',
        'properties.isolation.level'='read_committed',
        'scan.startup.mode'='earliest-offset',
        'key.format'='raw', 'key.fields'='kafka_key',
        'value.format'='raw', 'value.fields-include'='EXCEPT_KEY'
    )""")
    source = tables.to_data_stream(tables.from_path('telemetry'))
    result_type = Types.ROW_NAMED(['topic', 'kafka_key', 'body', 'trace_headers'], [Types.STRING()]*3 + [Types.MAP(Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE()))])
    if demo_parallelism is None:
        keyed = source.key_by(lambda _: 'run', key_type=Types.STRING())
        uid = 'member2-run-v1'
    else:
        # Keep unknown keys in one audit shard so they cannot create unbounded state.
        known = frozenset(tx['transformer_id'] for tx in topology['transformers'])
        keyed = source.key_by(lambda row: row[0] if row[0] in known else '',
                              key_type=Types.STRING())
        uid = 'member2-transformer-demo-v1'
    aggregated = keyed.process(Aggregate(topology, args.max_events),
                              output_type=result_type).uid(uid).name('Member_2 event-time aggregation')
    output = tables.from_data_stream(aggregated)
    tables.create_temporary_view('results', output)
    inserts = tables.create_statement_set()
    for i, topic in enumerate((LOAD, DLQ, LATE)):
        tables.execute_sql(f"""CREATE TABLE sink_{i} (kafka_key STRING, body STRING, trace_headers MAP<STRING, BYTES> METADATA FROM 'headers') WITH (
            'connector'='kafka', 'topic'='{prefix}{topic}',
            'properties.bootstrap.servers'='{args.bootstrap}',
            'key.format'='raw', 'key.fields'='kafka_key',
            'value.format'='raw', 'value.fields-include'='EXCEPT_KEY',
            'sink.delivery-guarantee'='exactly-once',
            'sink.transactional-id-prefix'='m2-{args.run}-{i}-',
            'properties.transaction.timeout.ms'='300000'
        )""")
        inserts.add_insert_sql(f"INSERT INTO sink_{i} SELECT kafka_key, body, trace_headers FROM results WHERE topic='{topic}'")
    inserts.execute()


if __name__ == '__main__':
    main()
