"""At-least-once Kafka transport; PostgreSQL is the offset/checkpoint authority."""
import json
import os
from pathlib import Path
import signal
import threading
import time

from confluent_kafka import Consumer
from jsonschema.exceptions import ValidationError
from psycopg.types.json import Jsonb

from observability import setup, boundary, business

from member4.contracts import TOPICS


def process(store, message, prefix=''):
    with boundary('kafka.process', message.headers()):
        return _process(store, message, prefix)


def _process(store, message, prefix=''):
    if message.error():
        raise RuntimeError(str(message.error()))
    raw = message.value()
    if raw is None or len(raw) > 65536:
        raise ValueError('missing or oversized event')
    topic = message.topic().removeprefix(prefix)
    event = json.loads(raw)
    with boundary('kafka.process', event=event):
        changed = store.ingest(topic, message.key().decode('utf-8'), event,
                     partition=message.partition(), offset=message.offset())
    if changed:
        business(event)


def consume(store):
    setup('member4.projection')
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
        pending, commit_at = 0, time.monotonic() + .5
        while not stop.is_set():
            message = client.poll(.2)
            if message is not None:
                try:
                    process(store, message, prefix)
                except (ValidationError, ValueError, KeyError, TypeError) as exc:
                    # Poison record (e.g. schema-invalid payload). Skip it rather
                    # than crash-loop the consumer; offset is still advanced so the
                    # stream makes progress. Transient store/DB errors are not
                    # caught here and still restart the pod.
                    from observability import log as _log
                    _log('projection_skip_invalid', topic=message.topic(),
                         partition=message.partition(), offset=message.offset(),
                         reason=str(exc)[:200])
                client.store_offsets(message=message)
                pending += 1
            if pending and (pending >= 100 or time.monotonic() >= commit_at):
                client.commit(asynchronous=False)
                pending, commit_at = 0, time.monotonic() + .5
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
