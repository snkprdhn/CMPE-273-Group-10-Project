"""Isolated real broker topics; crash/restart consumes from durable SQL positions."""
from copy import deepcopy
import json
import os
import subprocess
import sys
import time
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import pytest

from member4.consumer import process
from member4.contracts import TOPICS


def test_transport_rejects_poison_without_checkpoint(store):
    class Poison:
        def error(self): return None
        def headers(self): return None
        def value(self): return b''
        def topic(self): return 'grid.action.v1'
        def key(self): return b'tx-001'
        def partition(self): return 0
        def offset(self): return 3
    with pytest.raises(ValueError):
        process(store, Poison())
    with store.transaction() as db:
        assert db.execute('SELECT count(*) FROM projection_offsets').fetchone()[0] == 0


def test_real_kafka_crash_restart_replay(store, records):
    bootstrap = os.getenv('MEMBER4_TEST_KAFKA')
    if not bootstrap:
        pytest.skip('set MEMBER4_TEST_KAFKA for real broker acceptance')
    store.context(records)
    prefix = 'test-m4-'+uuid4().hex+'.'
    admin = AdminClient({'bootstrap.servers': bootstrap})
    topics = [prefix+t for t in TOPICS]
    for future in admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]).values():
        future.result(10)
    producer = Producer({'bootstrap.servers': bootstrap, 'acks':'all'})
    env = dict(os.environ, PROJECTION_DSN=store.dsn, KAFKA_PREFIX=prefix,
               KAFKA_BOOTSTRAP=bootstrap, PROJECTION_GROUP=prefix+'projection')
    worker = None
    log = []
    def publish(events):
        for r in events:
            producer.produce(prefix+r['topic'], key=r['key'], value=json.dumps(r['event']),
                             on_delivery=lambda err, msg: log.append(err))
        assert producer.flush(10) == 0
        assert not any(log)
    def start():
        return subprocess.Popen([sys.executable, '-m', 'member4', 'consume'], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    def wait_for(predicate):
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            assert worker.poll() is None, worker.communicate()
            with store.transaction() as db:
                if predicate(db):
                    return
            time.sleep(.1)
        raise AssertionError('projection did not converge')
    try:
        publish(list(reversed(records['events'])) + records['events'])
        worker = start()
        wait_for(lambda db: db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 6 and
                 db.execute('SELECT min(offset_value) FROM projection_offsets').fetchone()[0] == 1)
        worker.kill()  # durable SQL checkpoint survives process loss and broker retries
        worker.communicate(timeout=5)
        worker = start()
        load = deepcopy(next(r for r in records['events'] if r['topic'] == 'grid.transformer-load.v1'))
        load['event']['event_id'] = str(uuid4())
        load['event']['payload']['revision'] = 12
        publish([load, records['events'][1], load])
        wait_for(lambda db: db.execute("SELECT event->'payload'->>'revision' FROM projection_current "
                                      "WHERE kind='Load'").fetchone()[0] == '12' and
                 db.execute('SELECT count(*) FROM projection_events').fetchone()[0] == 7)
        with store.transaction() as db:
            # Freshness aside, the same authoritative corrected payload is retained once.
            assert db.execute("SELECT event->'payload' FROM projection_current WHERE kind='Load'").fetchone()[0] == load['event']['payload']
        store.rebuild()
        with store.transaction() as db:
            assert db.execute("SELECT event->'payload' FROM projection_current WHERE kind='Load'").fetchone()[0] == load['event']['payload']
    finally:
        if worker and worker.poll() is None:
            worker.terminate()
            try:
                worker.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.communicate(timeout=5)
        for future in admin.delete_topics(topics).values():
            future.result(10)


@pytest.mark.parametrize('low,high,valid', [(0,10,True), (1,10,False), (0,0,True)])
def test_assignment_never_silently_skips_retained_gap(store, low, high, valid):
    from confluent_kafka import TopicPartition
    from member4.consumer import assign
    class FakeConsumer:
        assigned = None
        def get_watermark_offsets(self, partition, timeout): return low, high
        def assign(self, partitions): self.assigned = partitions
    consumer = FakeConsumer()
    partitions = [TopicPartition('grid.action.v1', 0)]
    if valid:
        assign(store, consumer, partitions)
        assert consumer.assigned[0].offset == 0
    else:
        with pytest.raises(RuntimeError, match='retention'):
            assign(store, consumer, partitions)
        assert consumer.assigned is None
