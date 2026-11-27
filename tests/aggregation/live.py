"""Executed inside the aggregation image; real Kafka and Flink, synthetic data only."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import time
from uuid import uuid4

from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
import requests

from aggregation.engine import validate, LOAD, DLQ, LATE


def wait(fn, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(.5)
    raise AssertionError(f'timed out: {description}')


def api(path):
    r = requests.get('http://jobmanager:8081/' + path, timeout=5)
    r.raise_for_status()
    return r.json()


def publish(run, events):
    producer = Producer({'bootstrap.servers': 'kafka:9092', 'acks': 'all'})
    errors = []
    for key, event in events:
        body = json.dumps(event) if not isinstance(event, str) else event
        producer.produce(f'agg-{run}.grid.telemetry.v1', key=key, value=body,
                         on_delivery=lambda err, _: errors.append(str(err)) if err else None)
    assert producer.flush(20) == 0
    assert not errors, errors


def collect(run, expected, timeout=120):
    consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'probe-' + uuid4().hex,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest', 'isolation.level': 'read_committed'})
    consumer.subscribe([f'agg-{run}.{topic}' for topic in (LOAD, DLQ, LATE)])
    found = []
    reached = None
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            record = consumer.poll(.5)
            if record is None:
                if reached is not None and time.monotonic() > reached + 5: return found
                continue
            assert not record.error(), record.error()
            e = json.loads(record.value())
            topic = record.topic().split('.', 1)[1]
            validate({LOAD:'Load', DLQ:'Dlq', LATE:'Late'}[topic], e)
            assert record.key().decode() == 'tx-001'
            found.append((topic, e))
            if expected(found) and reached is None: reached = time.monotonic()
        raise AssertionError(f'timed out collecting committed outputs: {found}')
    finally:
        consumer.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('phase', choices=['topics', 'before', 'pending', 'after', 'metrics'])
    p.add_argument('run')
    args = p.parse_args()
    run = args.run
    if args.phase == 'topics':
        admin = AdminClient({'bootstrap.servers':'kafka:9092'})
        futures = admin.create_topics([NewTopic(f'agg-{run}.{t}', 3, 1) for t in ('grid.telemetry.v1', LOAD, DLQ, LATE)])
        for future in futures.values(): future.result(30)
        print('four isolated topics created, three partitions each')
        return
    job = wait(lambda: next((j['jid'] for j in api('jobs/overview')['jobs'] if j['name']==f'aggregation-{run}' and j['state']=='RUNNING'), None), 'run job RUNNING')
    checkpoint = lambda: api(f'jobs/{job}/checkpoints')
    if args.phase == 'metrics':
        vertex = next(v for v in api(f'jobs/{job}')['vertices'] if v['name'].startswith('Event-time'))
        path = f"jobs/{job}/vertices/{vertex['id']}/subtasks/0/metrics"
        required = ('accepted_late_count', 'watermark_delay_ms', 'semantic_watermark_ms')
        def metrics():
            names = [m['id'] for m in api(path) if any(m['id'].endswith('.' + suffix) for suffix in required)]
            if len(names) != len(required):
                return None  # REST's metric fetcher discovers worker metrics asynchronously.
            values = api(path + '?get=' + ','.join(names))
            return {m['id'].rsplit('.', 1)[-1]: float(m['value']) for m in values} or None
        values = wait(metrics, 'required operator metrics')
        assert values['accepted_late_count'] == 1, values
        assert values['watermark_delay_ms'] > 0, values
        print(json.dumps(dict(phase='metrics', **values)))
        return
    fixture = json.loads(Path('/app/schemas/v1/golden.scenarios.json').read_text())[0]
    readings = [(s['key'], s['event']) for s in fixture['steps'] if s['op']=='reading']
    if args.phase == 'before':
        publish(run, readings[:3] + [readings[1], ('tx-001', '{malformed')] + readings[3:])
        found = collect(run, lambda f: any(t==LOAD for t,e in f) and any(t==DLQ for t,e in f))
        loads = [e for t,e in found if t==LOAD]
        assert len(loads)==1 and loads[0]['payload']['net_load_kw']==15 and loads[0]['payload']['revision']==0
        latest = wait(lambda: checkpoint()['latest'].get('completed'), 'completed checkpoint')
        # Wait for another checkpoint so source offsets AND all sink commits are included.
        latest = wait(lambda: (x if (x:=checkpoint()['latest'].get('completed')) and x['id']>latest['id'] else None), 'next checkpoint')
        Path(f'/checkpoints/{run}.json').write_text(json.dumps(dict(job=job, checkpoint=latest['id'], load=loads[0])))
        print(json.dumps(dict(phase='before', net=15, revision=0, checkpoint=latest['id'])))
    elif args.phase == 'pending':
        proof_path = Path(f'/checkpoints/{run}.json')
        proof = json.loads(proof_path.read_text())
        correction = deepcopy(readings[1][1])
        correction['event_id'] = str(uuid4())
        correction['event_time'] = '2026-11-09T18:30:04Z'
        correction['ingested_at'] = '2026-11-09T18:30:35Z'
        correction['payload'].update(power_kw=9, sequence_number=2)
        checkpoint_before = checkpoint()['latest']['completed']['id']
        publish(run, [('tx-001', correction)])
        consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'pending-' + uuid4().hex,
                             'enable.auto.commit': False, 'isolation.level': 'read_uncommitted'})
        topic = f'agg-{run}.{LOAD}'
        consumer.assign([TopicPartition(topic, n, 0) for n in range(3)])
        deadline = time.monotonic() + 8
        pending = None
        try:
            while time.monotonic() < deadline:
                record = consumer.poll(.1)
                if record is not None and not record.error():
                    event = json.loads(record.value())
                    if event['payload']['revision'] == 1:
                        pending = event
                        break
        finally:
            consumer.close()
        assert pending is not None, 'expected an in-flight transactional correction'
        assert checkpoint()['latest']['completed']['id'] == checkpoint_before, 'checkpoint raced the crash probe; use a fresh run'
        proof['checkpoint'] = checkpoint_before
        proof['correction'] = correction
        proof_path.write_text(json.dumps(proof))
        print(json.dumps(dict(phase='pending', revision=1, checkpoint=checkpoint_before, uncommitted_seen=True)))
    else:
        proof = json.loads(Path(f'/checkpoints/{run}.json').read_text())
        wait(lambda: checkpoint()['counts']['restored'] > 0, 'restore from checkpoint after task loss')
        assert checkpoint()['latest']['restored']['id'] == proof['checkpoint']
        # Redeliver accepted input IDs at new Kafka offsets, then a correction and audit.
        correction = proof['correction']
        late = deepcopy(correction)
        late['event_id'] = str(uuid4())
        late['ingested_at'] = '2026-11-09T18:30:35.001Z'
        late['payload']['power_kw'] = 999
        publish(run, readings + [('tx-001', correction), ('tx-001', late), ('tx-001', correction)])
        found = collect(run, lambda f: any(t==LOAD and e['payload']['revision']==1 for t,e in f) and any(t==LATE for t,e in f))
        loads = [e for t,e in found if t==LOAD]
        assert [(e['payload']['revision'],e['payload']['net_load_kw']) for e in loads] == [(0,15),(1,17)], loads
        assert loads[0] == proof['load']
        assert loads[1]['payload']['window_id']==loads[0]['payload']['window_id']
        assert len({e['event_id'] for e in loads})==2
        # Drain again after a further checkpoint to detect delayed transactional duplicates.
        count = checkpoint()['counts']['completed']
        wait(lambda: checkpoint()['counts']['completed']>count, 'post-recovery checkpoint')
        found = collect(run, lambda f: any(t==LOAD and e['payload']['revision']==1 for t,e in f) and any(t==LATE for t,e in f))
        loads = [e for t,e in found if t==LOAD]
        assert [(e['payload']['revision'], e['payload']['net_load_kw']) for e in loads] == [(0,15),(1,17)], loads
        assert sum(t == DLQ for t,e in found) == 1
        assert sum(t == LATE for t,e in found) == 1
        print(json.dumps(dict(phase='after', totals=[15,17], revisions=[0,1], restored=checkpoint()['counts']['restored'], committed_ids=[e['event_id'] for e in loads])))


if __name__=='__main__': main()
