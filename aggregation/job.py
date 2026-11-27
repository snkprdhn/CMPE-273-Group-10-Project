"""PyFlink entry point. Kafka metadata stays available for validation/audit.

The stream is keyed by transformer. Each key keeps bounded state (see aggregation/keyed.py), so
parallelism scales with the number of task slots and a continuous run never reaches a size limit.
"""
import argparse
from datetime import datetime, timezone
import json
import re

from pyflink.common import Configuration, Row, Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

from aggregation.engine import Aggregator, LOAD, DLQ, LATE
from aggregation.keyed import Keyed
from observability import setup, boundary, headers, business
from aggregation.tracing import remember, output_context

SAMPLE_EVERY = 64   # spans and log lines for one in N readings; every output event is always traced


class Slots:
    """Adapter from MapState to the get/put/remove protocol used by Keyed."""
    def __init__(self, state):
        self.state = state

    def get(self, epoch):
        return self.state.get(epoch)

    def put(self, epoch, slot):
        self.state.put(epoch, slot)

    def remove(self, epoch):
        self.state.remove(epoch)


class Aggregate(KeyedProcessFunction):
    def __init__(self, topology):
        self.topology = topology

    def open(self, runtime_context):
        setup('aggregation.job')
        self.keyed = Keyed(self.topology)
        self.slots = Slots(runtime_context.get_map_state(
            MapStateDescriptor('window-slots', Types.LONG(), Types.PICKLED_BYTE_ARRAY())))
        self.meta = runtime_context.get_state(ValueStateDescriptor('shard-meta', Types.PICKLED_BYTE_ARRAY()))
        metrics = runtime_context.get_metrics_group()
        self.counts = {name: metrics.counter(name) for name in ('duplicates', 'dlq', 'very_late', 'corrections', 'input', 'output')}
        self.watermark = -1
        self.latest = 0
        self.accepted_late = 0
        self.seen_records = 0
        metrics.gauge('semantic_watermark_ms', lambda: self.watermark)
        metrics.gauge('accepted_late_count', lambda: self.accepted_late)
        metrics.gauge('watermark_delay_ms', lambda: int(datetime.now(timezone.utc).timestamp()*1000) - self.watermark if self.watermark != -1 else -1)
        metrics.gauge('latest_output_ms', lambda: self.latest)

    def process_element(self, row, ctx):
        self.seen_records += 1
        if self.seen_records % SAMPLE_EVERY == 1:
            with boundary('aggregation.process', row[4] or {}):
                yield from self._process(row, True)
        else:
            yield from self._process(row, False)

    def _process(self, row, sampled):
        now = datetime.now(timezone.utc)
        key, raw = row[0], row[1]
        self.counts['input'].inc()
        trace = row[4] or {}
        results, deltas, late = self.keyed.process(
            key, raw, row[2], row[3], self.slots, self.meta, now,
            observe=(lambda engine, decoded: remember(engine, decoded, trace)) if sampled else None)
        for name, amount in deltas.items():
            if amount:
                self.counts[name].inc(amount)
        self.accepted_late += late
        greatest = self.keyed.engine.greatest
        if greatest:
            self.watermark = int(greatest.timestamp()*1000)-10000
        for topic, out_key, event in results:
            self.counts['output'].inc()
            self.latest = int(now.timestamp()*1000)
            context = output_context(self.keyed.engine, out_key, event)
            if context:
                # A span and a log line only for outputs linked to a sampled reading.
                with boundary('kafka.publish', context, event, link=True):
                    business(event)
                    yield self.row(topic, out_key, event)
            else:
                business(event)
                yield self.row(topic, out_key, event)

    @staticmethod
    def row(topic, key, event):
        return Row(topic, key, json.dumps(event, allow_nan=False, separators=(',', ':')),
                   {k: v.encode() for k, v in headers().items() if k in ('traceparent', 'correlation_id', 'causation_id', 'scenario_run_id')})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='kafka:9092')
    parser.add_argument('--run', required=True)
    parser.add_argument('--prefix', help='explicit shared integration topic namespace')
    parser.add_argument('--topology', default='/app/aggregation/topology.json')
    parser.add_argument('--checkpoint', default='file:///checkpoints')
    parser.add_argument('--checkpoint-ms', type=int, default=2000)
    parser.add_argument('--parallelism', type=int, default=1)
    parser.add_argument('--startup', choices=('earliest-offset', 'latest-offset', 'group-offsets'), default='earliest-offset',
                        help='group-offsets resumes a committed position and starts at the end for a new group')
    parser.add_argument('--state-backend', choices=('hashmap', 'rocksdb'), default='hashmap')
    args = parser.parse_args()
    if not 1 <= args.parallelism <= 32:
        parser.error('parallelism must be 1..32')
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
    Aggregator(topology)  # Fail before starting a source.
    conf = Configuration()
    conf.set_string('state.backend.type', args.state_backend)
    if args.state_backend == 'rocksdb':
        conf.set_boolean('state.backend.incremental', True)
        conf.set_boolean('state.backend.rocksdb.memory.managed', True)
    conf.set_string('python.executable', '/opt/venv/bin/python')
    conf.set_string('restart-strategy.type', 'fixed-delay')
    conf.set_string('restart-strategy.fixed-delay.attempts', '1000')
    conf.set_string('restart-strategy.fixed-delay.delay', '3 s')
    conf.set_string('execution.checkpointing.dir', args.checkpoint)
    conf.set_string('execution.checkpointing.num-retained', '2')
    # Bigger state caches and bundles cut gRPC round trips between the JVM and the Python worker.
    conf.set_string('python.fn-execution.bundle.size', '2000')
    conf.set_string('python.fn-execution.bundle.time', '200')
    conf.set_string('python.state.cache-size', '20000')
    conf.set_string('python.map-state.read-cache-size', '20000')
    conf.set_string('python.map-state.write-cache-size', '20000')
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(args.checkpoint_ms, CheckpointingMode.EXACTLY_ONCE)
    cp = env.get_checkpoint_config()
    cp.set_checkpoint_timeout(60000)
    cp.set_max_concurrent_checkpoints(1)
    # A busy single-core worker can miss one barrier; do not kill the job for it.
    cp.set_tolerable_checkpoint_failure_number(10)
    cp.set_externalized_checkpoint_retention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
    tables = StreamTableEnvironment.create(env)
    tables.get_config().get_configuration().set_string('pipeline.name', f'aggregation-{args.run}')
    prefix = args.prefix if args.prefix is not None else f'agg-{args.run}.'
    # Raw formats permit malformed payloads and null/wrong keys to reach the DLQ.
    tables.execute_sql(f"""CREATE TABLE telemetry (
        kafka_key STRING, body STRING,
        part INT METADATA FROM 'partition' VIRTUAL,
        off BIGINT METADATA FROM 'offset' VIRTUAL,
        trace_headers MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL
    ) WITH (
        'connector'='kafka', 'topic'='{prefix}grid.telemetry.v1',
        'properties.bootstrap.servers'='{args.bootstrap}',
        'properties.group.id'='agg-{args.run}-aggregation-v1',
        'properties.isolation.level'='read_committed',
        'scan.startup.mode'='{args.startup}',
        'properties.auto.offset.reset'='latest',
        'key.format'='raw', 'key.fields'='kafka_key',
        'value.format'='raw', 'value.fields-include'='EXCEPT_KEY'
    )""")
    source = tables.to_data_stream(tables.from_path('telemetry'))
    result_type = Types.ROW_NAMED(['topic', 'kafka_key', 'body', 'trace_headers'], [Types.STRING()]*3 + [Types.MAP(Types.STRING(), Types.PRIMITIVE_ARRAY(Types.BYTE()))])
    known = frozenset(tx['transformer_id'] for tx in topology['transformers'])
    # Unknown keys share one audit shard so they cannot create unbounded state.
    keyed = source.key_by(lambda row: row[0] if row[0] in known else '', key_type=Types.STRING())
    aggregated = keyed.process(Aggregate(topology), output_type=result_type).uid('aggregation-keyed-v2').name('Event-time aggregation')
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
            'sink.transactional-id-prefix'='agg-{args.run}-{i}-',
            'properties.transaction.timeout.ms'='300000'
        )""")
        inserts.add_insert_sql(f"INSERT INTO sink_{i} SELECT kafka_key, body, trace_headers FROM results WHERE topic='{topic}'")
    inserts.execute()


if __name__ == '__main__':
    main()
