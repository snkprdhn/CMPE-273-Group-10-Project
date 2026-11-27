"""Real local Kafka fixture client; no Flink or Action dependency."""
import json
import os
import time
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import pytest

from devices.common import utc, validate
from devices.kafka import handle_message, Publisher
from devices.simulator import devices, EdgeState, scenario
from tests.devices.test_edge import command

BOOTSTRAP = os.getenv('DEVICES_TEST_KAFKA')
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason='set DEVICES_TEST_KAFKA for live Kafka tests')


def reader(topic, group):
    c = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group,
                  'enable.auto.commit': False, 'enable.auto.offset.store': False})
    deadline = time.monotonic() + 8
    while True:
        try:
            md = c.list_topics(topic, timeout=2)
            if md.topics[topic].error or not md.topics[topic].partitions:
                raise RuntimeError('topic metadata not ready')
            assignments = []
            for partition in md.topics[topic].partitions:
                tp = TopicPartition(topic, partition)
                _, end = c.get_watermark_offsets(tp, timeout=1)
                assignments.append(TopicPartition(topic, partition, end))
            break
        except (KafkaException, RuntimeError):
            if time.monotonic() >= deadline:
                c.close()
                raise
            time.sleep(.1)
    c.assign(assignments)
    return c


def poll(c, predicate, timeout=12):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        m = c.poll(.1)
        if m is None:
            continue
        assert not m.error(), m.error()
        if predicate(json.loads(m.value())):
            return m
    pytest.fail('expected Kafka record not received')


@pytest.fixture
def isolated_topics():
    run = 'test-' + uuid4().hex
    prefix = f'local-dev.devices-test.{run}.'
    topics = [prefix + name for name in ('grid.telemetry.v1', 'grid.charger-command.v1', 'grid.command-result.v1')]
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    try:
        for future in admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1) for name in topics], request_timeout=5).values():
            future.result(8)
        yield run, prefix, topics
    finally:
        for future in admin.delete_topics(topics, request_timeout=5).values():
            future.result(8)


def test_live_telemetry_commands_results_and_offset_restart(tmp_path, isolated_topics):
    run, prefix, topics = isolated_topics
    pub = Publisher(BOOTSTRAP, prefix=prefix)
    telemetry = reader(topics[0], 'local-dev.devices.test.telemetry.' + run)
    commands = reader(topics[1], 'local-dev.devices.test.commands.' + run)
    results = reader(topics[2], 'local-dev.devices.test.results.' + run)
    edge = EdgeState(tmp_path / 'live.sqlite', devices(utc())[0])
    try:
        events = list(scenario(273, utc(), steps=3, run_id=run))
        for e in events:
            pub.send('grid.telemetry.v1', 'tx-001', e)
        pub.drain()
        seen = [poll(telemetry, lambda v: v['correlation_id'] == run) for _ in events]
        assert [json.loads(m.value()) for m in seen] == events
        assert len({m.partition() for m in seen}) == 1
        assert all(m.key() == b'tx-001' for m in seen)
        now = utc()
        value = command(identity=run)
        from datetime import timedelta
        from devices.common import stamp
        value['correlation_id'] = run
        value['payload'].update(issued_at=stamp(now), expires_at=stamp(now + timedelta(seconds=10)))
        for expected in ('APPLIED', 'ALREADY_APPLIED'):
            pub.send('grid.charger-command.v1', 'ev-001', value)
            pub.drain()
            incoming = poll(commands, lambda v: v['correlation_id'] == run)
            handle_message(incoming, edge, pub, commands, utc())
            result = poll(results, lambda v: v['correlation_id'] == run)
            output = json.loads(result.value())
            validate('Result', output, result.key().decode())
            assert output['payload']['outcome'] == expected
            committed = commands.committed([TopicPartition(incoming.topic(), incoming.partition())], timeout=3)[0]
            assert committed.offset == incoming.offset() + 1
            edge.db.close()
            edge = EdgeState(tmp_path / 'live.sqlite', devices(utc())[0])
        assert edge.current() == (3, '42', 1)
        follow = poll(telemetry, lambda v: v['causation_id'] == run)
        assert json.loads(follow.value())['payload']['power_kw'] == 3
    finally:
        edge.db.close()
        for c in (telemetry, commands, results):
            c.close()


def test_unreachable_broker_has_bounded_failure():
    pub = Publisher('127.0.0.1:1', delivery_ms=1000, capacity=2)
    event = next(scenario(1, utc(), steps=3))
    started = time.monotonic()
    pub.send('grid.telemetry.v1', 'tx-001', event)
    with pytest.raises(RuntimeError):
        pub.drain()
    assert time.monotonic() - started < 4
    assert not pub.healthy
