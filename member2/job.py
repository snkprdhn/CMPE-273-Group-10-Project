"""PyFlink entry point. Kafka metadata stays available for validation/audit."""
import argparse
from datetime import datetime, timezone
import json
import pickle
import re

from pyflink.common import Configuration, Row, Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

from member2.aggregation import Aggregator, LOAD, DLQ, LATE


class Aggregate(KeyedProcessFunction):
    def __init__(self, topology, max_events):
        self.topology = topology
        self.max_events = max_events

    def open(self, runtime_context):
        self.state = runtime_context.get_state(ValueStateDescriptor('run-v1', Types.PRIMITIVE_ARRAY(Types.BYTE())))
        metrics = runtime_context.get_metrics_group()
        self.counts = {name: metrics.counter(name) for name in ('duplicates', 'dlq', 'very_late', 'corrections', 'input', 'output')}
        self.watermark = -1
        self.latest = 0
        self.accepted_late = 0
        metrics.gauge('semantic_watermark_ms', lambda: self.watermark)
        metrics.gauge('accepted_late_count', lambda: self.accepted_late)
        metrics.gauge('watermark_delay_ms', lambda: int(datetime.now(timezone.utc).timestamp()*1000) - self.watermark if self.watermark != -1 else -1)
        metrics.gauge('latest_output_ms', lambda: self.latest)

    def process_element(self, row, ctx):
        stored = self.state.value()
        engine = pickle.loads(bytes(stored)) if stored is not None else Aggregator(self.topology, self.max_events)
        if engine.topology != self.topology or engine.max_events != self.max_events:
            raise RuntimeError('TOPOLOGY_CHANGED: restore only with identical run fixture')
        before = engine.metrics.copy()
        now = datetime.now(timezone.utc)
        self.counts['input'].inc()
        results = engine.reading(row[1], row[0], now, row[2], row[3])
        results += engine.evaluate(now)
        self.state.update(pickle.dumps(engine, protocol=5))
        for name in before:
            self.counts[name].inc(engine.metrics[name] - before[name])
        self.accepted_late = sum(w['late'] for w in engine.windows.values())
        if engine.greatest:
            self.watermark = int(engine.greatest.timestamp()*1000)-10000
        for topic, key, event in results:
            self.counts['output'].inc()
            self.latest = int(now.timestamp()*1000)
            yield Row(topic, key, json.dumps(event, allow_nan=False, separators=(',', ':')))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='kafka:9092')
    parser.add_argument('--run', required=True)
    parser.add_argument('--prefix', help='explicit shared integration topic namespace')
    parser.add_argument('--topology', default='/app/member2/topology.json')
    parser.add_argument('--checkpoint', default='file:///checkpoints')
    parser.add_argument('--checkpoint-ms', type=int, default=2000)
    parser.add_argument('--max-events', type=int, default=100000)
    args = parser.parse_args()
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
    conf.set_string('python.executable', '/opt/venv/bin/python')
    conf.set_string('restart-strategy.type', 'fixed-delay')
    conf.set_string('restart-strategy.fixed-delay.attempts', '10')
    conf.set_string('restart-strategy.fixed-delay.delay', '3 s')
    conf.set_string('execution.checkpointing.dir', args.checkpoint)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)
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
        off BIGINT METADATA FROM 'offset' VIRTUAL
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
    result_type = Types.ROW_NAMED(['topic', 'kafka_key', 'body'], [Types.STRING()]*3)
    aggregated = source.key_by(lambda _: 'run', key_type=Types.STRING()).process(
        Aggregate(topology, args.max_events), output_type=result_type).uid('member2-run-v1').name('Member_2 event-time aggregation')
    output = tables.from_data_stream(aggregated)
    tables.create_temporary_view('results', output)
    inserts = tables.create_statement_set()
    for i, topic in enumerate((LOAD, DLQ, LATE)):
        tables.execute_sql(f"""CREATE TABLE sink_{i} (kafka_key STRING, body STRING) WITH (
            'connector'='kafka', 'topic'='{prefix}{topic}',
            'properties.bootstrap.servers'='{args.bootstrap}',
            'key.format'='raw', 'key.fields'='kafka_key',
            'value.format'='raw', 'value.fields-include'='EXCEPT_KEY',
            'sink.delivery-guarantee'='exactly-once',
            'sink.transactional-id-prefix'='m2-{args.run}-{i}-',
            'properties.transaction.timeout.ms'='300000'
        )""")
        inserts.add_insert_sql(f"INSERT INTO sink_{i} SELECT kafka_key, body FROM results WHERE topic='{topic}'")
    inserts.execute()


if __name__ == '__main__':
    main()
