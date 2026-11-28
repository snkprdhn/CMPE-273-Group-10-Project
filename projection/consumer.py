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

from projection.contracts import TOPICS


def selected_topics(value):
    """Resolve canonical topic names before binding the store or connecting to Kafka."""
    if value is None:
        return tuple(TOPICS)
    topics = tuple(topic.strip() for topic in value.split(','))
    if len(set(topics)) != len(topics) or any(topic not in TOPICS for topic in topics):
        raise ValueError('PROJECTION_TOPICS must list distinct canonical projection topics')
    return topics


class Scope:
    """Only managed inventory is projected: the transformers and devices in the projection context.

    A fleet publishes readings for hundreds of devices the operations API does not manage. Storing each
    one would grow the journal and the views with run time. Skipped events still advance the source
    offset (see Store.advance), so a restart never replays or loses position.
    """
    def __init__(self, store, ttl=10.0, clock=time.monotonic, telemetry_interval=10.0):
        self.store, self.ttl, self.clock = store, ttl, clock
        self.expires, self.transformers, self.devices = 0.0, set(), set()
        # The projection keeps only the latest reading per device. Storing every report of a charger that
        # reports every five seconds doubles the database work for no visible change.
        self.telemetry_interval, self.accepted = telemetry_interval, {}

    def allows(self, name, payload):
        if self.clock() >= self.expires:
            self.transformers, self.devices = self.store.inventory()
            self.expires = self.clock() + self.ttl
        if name == 'Result':
            return payload.get('device_id') in self.devices
        if name == 'Telemetry':
            device = payload.get('device_id')
            if device not in self.devices:
                return False
            now = self.clock()
            if now - self.accepted.get(device, -1e9) < self.telemetry_interval:
                return False
            self.accepted[device] = now
            return True
        return payload.get('transformer_id') in self.transformers


def process(store, message, prefix='', scope=None):
    """Store one event. Returns 'skipped' for out-of-scope input, which costs no span, log line or row."""
    if message.error():
        raise RuntimeError(str(message.error()))
    raw = message.value()
    if raw is None or len(raw) > 65536:
        raise ValueError('missing or oversized event')
    topic = message.topic().removeprefix(prefix)
    event = json.loads(raw)
    if not isinstance(event, dict) or message.key() is None:
        raise ValueError('expected an event object and a partition key')
    if scope is not None and isinstance(event.get('payload'), dict) and not scope.allows(TOPICS[topic], event['payload']):
        return 'skipped'
    with boundary('kafka.process', message.headers()):
        with boundary('kafka.process', event=event):
            changed = store.ingest(topic, message.key().decode('utf-8'), event,
                                   partition=message.partition(), offset=message.offset())
        if changed:
            business(event)
    return 'stored'


def consume(store):
    topics = selected_topics(os.getenv('PROJECTION_TOPICS'))
    setup('projection.projection')
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
        assign(store, consumer, partitions, prefix, rolling=scope is not None)
        if health is not None:
            health.touch()

    health = Path(os.environ['WORKER_HEALTH_FILE']) if 'WORKER_HEALTH_FILE' in os.environ else None
    scope = Scope(store) if os.getenv('PROJECTION_SCOPE') == 'context' else None
    # The load worker owns retention. Both workers use the same journal and offset table.
    prune = scope is not None and 'grid.transformer-load.v1' in topics

    client = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:19092'),
                       'group.id': os.getenv('PROJECTION_GROUP', 'projection.v1'),
                       'session.timeout.ms': 6000, 'heartbeat.interval.ms': 2000,
                       'socket.timeout.ms': 5000,
                       'enable.auto.commit': False, 'enable.auto.offset.store': False,
                       'auto.offset.reset': 'error', 'isolation.level': 'read_committed',
                       'allow.auto.create.topics': False,
                       # A restart can face a long backlog of out-of-scope records; bound the prefetch buffers.
                       'queued.max.messages.kbytes': 8192, 'fetch.max.bytes': 4194304})

    try:
        client.subscribe([prefix+t for t in topics], on_assign=assigned)
        pending, commit_at = 0, time.monotonic() + .5
        window = int(os.getenv('PROJECTION_WINDOW_MINUTES', '30'))
        skipped, prune_at = {}, time.monotonic() + 60
        while not stop.is_set():
            message = client.poll(.2)
            drained = 0
            while message is not None and not stop.is_set():
                try:
                    if process(store, message, prefix, scope) == 'skipped':
                        skipped[(message.topic().removeprefix(prefix), message.partition())] = message.offset()
                except (ValidationError, ValueError, KeyError, TypeError) as exc:
                    # Poison record (e.g. schema-invalid payload). Skip it rather
                    # than crash-loop the consumer; offset is still advanced so the
                    # stream makes progress. Transient store/DB errors are not
                    # caught here and still restart the pod.
                    from observability import log as _log
                    _log('projection_skip_invalid', topic=message.topic(),
                         partition=message.partition(), offset=message.offset(),
                         reason=str(exc)[:200])
                    skipped[(message.topic().removeprefix(prefix), message.partition())] = message.offset()
                client.store_offsets(message=message)
                pending += 1
                drained += 1
                if pending >= 2000 or drained >= 500 or time.monotonic() >= commit_at:
                    break
                message = client.poll(0)
            if prune and time.monotonic() >= prune_at:
                store.prune(minutes=window, alert_minutes=int(os.getenv('PROJECTION_ALERT_MINUTES', '60')),
                            command_minutes=int(os.getenv('PROJECTION_COMMAND_MINUTES', '180')),
                            action_minutes=int(os.getenv('PROJECTION_ACTION_MINUTES', '1440')))
                prune_at = time.monotonic() + 300
            if pending and (pending >= 2000 or time.monotonic() >= commit_at):
                store.advance(skipped)
                skipped.clear()
                client.commit(asynchronous=False)
                pending, commit_at = 0, time.monotonic() + .5
            if health is not None and client.assignment():
                health.touch()
        store.advance(skipped)
    finally:
        client.close()


def assign(store, consumer, partitions, prefix='', rolling=False):
    with store.transaction() as db:
        offsets = {(prefix+t, p): n+1 for t, p, n in db.execute(
            'SELECT topic,partition,offset_value FROM projection_offsets')}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(p, timeout=3)
        position = offsets.get((p.topic, p.partition), 0)
        if rolling and position < low:
            # Rolling retention deleted the gap (e.g. after a long outage). The projection only
            # keeps a recent window, so resume at the oldest retained record instead of crash-looping.
            from observability import log as _log
            _log('projection_checkpoint_expired', topic=p.topic, partition=p.partition,
                 checkpoint=position, resume=low)
            position = low
        if position < low or position > high:
            raise RuntimeError('Projection checkpoint outside Kafka retention; rebuild required')
        p.offset = position
    consumer.assign(partitions)
