"""At-least-once Kafka transport; PostgreSQL is the offset/checkpoint authority."""
import json
import os
from pathlib import Path
import signal
import threading

from confluent_kafka import Consumer
from psycopg.types.json import Jsonb

from member4.contracts import TOPICS


def process(store, message, prefix=''):
    if message.error():
        raise RuntimeError(str(message.error()))
    raw = message.value()
    if raw is None or len(raw) > 65536:
        raise ValueError('missing or oversized event')
    topic = message.topic().removeprefix(prefix)
    store.ingest(topic, message.key().decode('utf-8'), json.loads(raw),
                 partition=message.partition(), offset=message.offset())


def consume(store):
    prefix = os.getenv('KAFKA_PREFIX', '')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    source = {'bootstrap': os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:19092'), 'prefix': prefix}
    with store.transaction() as db:
        old = db.execute("SELECT value FROM projection_context WHERE kind='transport' AND id='kafka'").fetchone()
        if old and old[0] != source:
            raise RuntimeError('Projection store is bound to a different Kafka source')
        db.execute("INSERT INTO projection_context VALUES('transport','kafka',%s) ON CONFLICT DO NOTHING",
                   (Jsonb(source),))

    def assigned(consumer, partitions):
        assign(store, consumer, partitions, prefix)
        if health is not None:
            health.touch()

    health = Path(os.environ['WORKER_HEALTH_FILE']) if 'WORKER_HEALTH_FILE' in os.environ else None

    client = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:19092'),
                       'group.id': os.getenv('PROJECTION_GROUP', 'member4.projection.v1'),
                       'session.timeout.ms': 6000, 'heartbeat.interval.ms': 2000,
                       'socket.timeout.ms': 5000,
                       'enable.auto.commit': False, 'enable.auto.offset.store': False,
                       'auto.offset.reset': 'error', 'isolation.level': 'read_committed',
                       'allow.auto.create.topics': False})

    try:
        client.subscribe([prefix+t for t in TOPICS], on_assign=assigned)
        while not stop.is_set():
            message = client.poll(.2)
            if message is not None:
                process(store, message, prefix)
                client.commit(message=message, asynchronous=False)
            if health is not None and client.assignment():
                health.touch()
    finally:
        client.close()


def assign(store, consumer, partitions, prefix=''):
    with store.transaction() as db:
        offsets = {(prefix+t, p): n+1 for t, p, n in db.execute(
            'SELECT topic,partition,offset_value FROM projection_offsets')}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(p, timeout=3)
        position = offsets.get((p.topic, p.partition), 0)
        if position < low or position > high:
            raise RuntimeError('Projection checkpoint outside Kafka retention; rebuild required')
        p.offset = position
    consumer.assign(partitions)
