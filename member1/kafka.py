"""Bounded librdkafka delivery; offset commit follows durable effect + delivery."""
import json
import time

from confluent_kafka import Consumer, Producer
from jsonschema.exceptions import ValidationError

from observability import boundary, headers as trace_headers

from member1.common import BUNDLE, log, METRICS, validate


def producer_config(bootstrap, capacity=1000, delivery_ms=10000):
    if capacity < 1 or not 1000 <= delivery_ms <= 30000:
        raise ValueError('invalid queue/delivery bounds')
    return {'bootstrap.servers': bootstrap, 'client.id': 'member1-edge',
            'enable.idempotence': True, 'acks': 'all', 'retries': 3,
            'retry.backoff.ms': 100, 'retry.backoff.max.ms': 1000,
            'delivery.timeout.ms': delivery_ms, 'request.timeout.ms': min(3000, delivery_ms),
            'queue.buffering.max.messages': capacity, 'queue.buffering.max.kbytes': 1024,
            'linger.ms': 5, 'statistics.interval.ms': 1000, 'allow.auto.create.topics': False}


class Publisher:
    def __init__(self, bootstrap, prefix='local-dev.', capacity=1000, delivery_ms=10000, producer=None):
        config = producer_config(bootstrap, capacity, delivery_ms)
        config['stats_cb'] = self.statistics
        self.producer = producer if producer is not None else Producer(config)
        self.prefix, self.timeout = prefix, delivery_ms / 1000 + 1
        self.failures = 0
        self.healthy = True

    def send(self, topic, key, value, headers=None):
        with boundary('kafka.publish', headers, value):
            return self._send(topic, key, value, list(trace_headers(headers).items()))

    def _send(self, topic, key, value, headers=None):
        validate(BUNDLE['x-events'][topic], value, key)
        raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
        if len(raw) > 65536:
            raise ValueError('event exceeds 64KiB')
        end = time.monotonic() + self.timeout
        carried = [(k, v) for k, v in (headers or []) if k == 'traceparent' and v is not None]
        carried = [(k, v.encode() if isinstance(v, str) else v) for k, v in carried]
        carried.extend([('correlation_id', value['correlation_id'].encode()),
                        ('causation_id', (value['causation_id'] or '').encode())])
        context = trace_headers(headers)
        def acknowledged(error, message):
            with boundary('kafka.delivery', context, value) as span:
                self.delivered(error, message)
                if error and span:
                    from opentelemetry.trace import Status, StatusCode
                    span.set_status(Status(StatusCode.ERROR))
        while True:
            try:
                self.producer.produce(self.prefix + topic, key=key.encode(), value=raw,
                                      headers=carried, on_delivery=acknowledged)
                METRICS['enqueued_total'] += 1
                METRICS['buffer_messages'] = len(self.producer)
                self.producer.poll(0)
                return
            except BufferError:
                METRICS['backpressure_total'] += 1
                if time.monotonic() >= end:
                    self.healthy = False
                    METRICS['buffer_rejected_total'] += 1
                    raise TimeoutError('Kafka queue full')
                self.producer.poll(min(.05, max(0, end - time.monotonic())))

    def statistics(self, raw):
        stats = json.loads(raw)
        METRICS['producer_retries_total'] = sum(b.get('txretries', 0) for b in stats.get('brokers', {}).values())
        METRICS['buffer_messages'] = stats.get('msg_cnt', 0)

    def delivered(self, error, message):
        if error:
            self.failures += 1
            self.healthy = False
            METRICS['delivery_failed_total'] += 1
            log('DELIVERY_FAILED', error_category='kafka_delivery')
        else:
            METRICS['published_total'] += 1

    def drain(self):
        outstanding = self.producer.flush(self.timeout)
        METRICS['buffer_messages'] = len(self.producer)
        if outstanding or self.failures:
            self.healthy = False
            raise RuntimeError('Kafka delivery unconfirmed; pending durable results retained')
        self.healthy = True

    def ready(self):
        if not self.healthy:
            return False
        metadata = self.producer.list_topics(timeout=.5)
        return bool(metadata.brokers)


def consumer(bootstrap, prefix='local-dev.', group='local-dev.member1.edge.v1'):
    result = Consumer({'bootstrap.servers': bootstrap, 'group.id': group,
                       'enable.auto.commit': False, 'enable.auto.offset.store': False,
                       'auto.offset.reset': 'earliest', 'allow.auto.create.topics': False,
                       'fetch.message.max.bytes': 65536, 'fetch.max.bytes': 1048576})
    result.subscribe([prefix + 'grid.charger-command.v1'])
    return result


def flush_results(state, publisher, headers=None):
    for identity, raw, carried in state.pending():
        value = json.loads(raw)
        topic = 'grid.command-result.v1' if value['event_type'] == 'command.result' else 'grid.telemetry.v1'
        key = value['payload']['device_id' if topic == 'grid.command-result.v1' else 'transformer_id']
        publisher.send(topic, key, value, json.loads(carried))
        publisher.drain()
        state.acknowledge(identity)


def handle_message(message, state, publisher, receiver, now):
    with boundary('kafka.process', message.headers()):
        return _handle_message(message, state, publisher, receiver, now)


def _handle_message(message, state, publisher, receiver, now):
    """Invalid records stop visibly without committing; no poison-success stub.

    This standalone shard owns the complete topic's fixture fleet (one EV).
    More shards need explicit partition assignment; a shared group alone is unsafe.
    """
    if message.error():
        raise RuntimeError('Kafka command consumption failed')
    try:
        raw = message.value()
        if len(raw) > 65536:
            raise ValueError('command exceeds 64KiB')
        value = json.loads(raw)
        key = message.key().decode()
        state.apply(value, key, now, message.headers())
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, ValidationError):
        METRICS['invalid_commands_total'] += 1
        log('INVALID_COMMAND', error_category='command_validation')
        raise
    flush_results(state, publisher, message.headers())
    receiver.commit(message=message, asynchronous=False)
    METRICS['command_offsets_committed_total'] += 1
