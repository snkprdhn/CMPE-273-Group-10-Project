"""Local 2,500-device closed-loop acceptance: Registry/Kafka/Flink/Action/CDC/API.

Uses a fresh additive schema/topic namespace. Never deploys to AKS or resets data.
Member_1 owns simulator/Registry; Member_2 aggregation; Member_3 control/CDC;
Member_4 projections/API/UI. Real service boundaries; simulated charger effects.
"""
import argparse
from collections import Counter, defaultdict
from datetime import timedelta
import json
import math
import os
from pathlib import Path
import subprocess
import time
import zlib
from types import SimpleNamespace
from urllib.error import URLError

import psycopg
from confluent_kafka import Consumer, TopicPartition

from member1.common import event, stamp, utc
from member1.fencing import FencedEdgeState
from member1.kafka import Publisher, handle_message
from member1.registry import Client, device_dict, device_message, pb
from member3.domain import default_policy
from member4.live_context import RegistryContext
from tools.integration import Runner, http, from_beginning, wait
from tools.integration_setup import provision


def fleet(run, now):
    kinds=['SMART_METER']*55+['EV_CHARGER']*35+['SOLAR']*10
    return [dict(device_id=f'{run}-d-{i:02}-{j:03}',transformer_id=f'{run}-tx-{i:02}',
        device_type=kind,controllable=kind=='EV_CHARGER',minimum_rate_kw=1 if j==55 else 0,
        maximum_rate_kw=7.2 if kind=='EV_CHARGER' else 100,priority=1,
        capability_version=1,updated_at=stamp(now)) for i in range(25) for j,kind in enumerate(kinds)]


def reporting_transformers(foreground, due, now):
    # Foreground uses the outer frame clock; do not gate it on a second timer.
    return foreground | {tx for tx, at in due.items() if now >= at}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    parser.add_argument('--duration',type=int,default=480)
    parser.add_argument('--interval',type=float,default=4)
    parser.add_argument('--active-transformers',type=int,default=3)
    parser.add_argument('--background-interval',type=float,default=16)
    args=parser.parse_args()
    if not 1<=args.active_transformers<=25 or not args.interval<=args.background_interval<=60:
        parser.error('active transformers 1..25 and background interval between foreground interval and 60 required')
    if not 180<=args.duration<=1200 or not 1<=args.interval<=10:
        parser.error('duration 180..1200 and interval 1..10 required')
    bootstrap='127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092'
    values=provision(args.run,bootstrap,3,3,'http://127.0.0.1:18183')
    options=SimpleNamespace(run=args.run,prefix='integration-'+args.run+'.',bootstrap=bootstrap,
        control_dsn=values['INTEGRATION_CONTROL_DSN'],edge_dsn=values['INTEGRATION_EDGE_DSN'],
        projection_dsn=values['INTEGRATION_PROJECTION_DSN'],registry='127.0.0.1:15051',
        ui_url='http://127.0.0.1:18096',api_url='http://127.0.0.1:18094/api/v1')
    runner=Runner(options);root=runner.directory
    client=Client(options.registry,'simulator','simulator_disposable')
    devices=fleet(args.run,utc());edges={}; readers=[];job=None
    transformers=sorted({d['transformer_id'] for d in devices})
    # Cover each input partition before selecting further simultaneous overloads.
    ranked=sorted(transformers,key=lambda tx:(zlib.crc32(tx.encode())%3,tx))
    first=[next(tx for tx in ranked if zlib.crc32(tx.encode())%3==part) for part in range(3)]
    foreground=set((first+[tx for tx in ranked if tx not in first])[:args.active_transformers])
    report={'run':args.run,'devices':len(devices),'transformers':25,'active_transformers':sorted(foreground),
            'foreground_interval':args.interval,'background_interval':args.background_interval,'status':'RUNNING'}
    try:
        for wanted in devices:
            actual=device_dict(client.call('RegisterDevice',pb.RegisterDeviceRequest(
                operation_id=wanted['device_id'],device=device_message(wanted))))
            assert actual['device_id']==wanted['device_id']
            if wanted['minimum_rate_kw']==1:
                edges[wanted['device_id']]=FencedEdgeState(options.edge_dsn,actual)
        assert len(edges)==25
        topology={'topology_version':args.run,'transformers':[dict(transformer_id=tx,capacity_kw=20,
            devices=[{'device_id':d['device_id'],'device_type':d['device_type']} for d in devices if d['transformer_id']==tx])
            for tx in sorted({d['transformer_id'] for d in devices})]}
        fixture=root/'topology.json';fixture.write_text(json.dumps(topology))
        subprocess.run(['docker','cp',str(fixture),'grid-million-jobmanager-1:/tmp/'+args.run+'.json'],check=True)
        result=subprocess.check_output(['docker','exec','grid-million-jobmanager-1','flink','run','-d','-py','/app/member2/job.py',
            '--run',args.run,'--prefix',options.prefix,'--topology','/tmp/'+args.run+'.json',
            '--bootstrap','kafka1:9092,kafka2:9092,kafka3:9092','--checkpoint','file:///checkpoints/'+args.run,
            '--checkpoint-ms','1000','--max-events','1000000','--state-backend','rocksdb'],text=True,timeout=180)
        job=result.split('JobID ')[-1].strip();report['job']=job
        os.environ['LEASE_API_URL']='http://127.0.0.1:18199'
        runner.start_workers()
        def ready():
            try:
                return http(options.api_url+'/health',headers={'Authorization':'Bearer '+runner.token})
            except (URLError, TimeoutError):
                return False
        wait(ready, 'backend API',30)
        publisher=Publisher(bootstrap,options.prefix,capacity=10000)
        source=Consumer({'bootstrap.servers':bootstrap,'group.id':options.prefix+'evidence','enable.auto.commit':False,
                         'isolation.level':'read_committed','socket.timeout.ms':5000})
        commands=Consumer({'bootstrap.servers':bootstrap,'group.id':options.prefix+'edge','enable.auto.commit':False,
            'enable.auto.offset.store':False,'isolation.level':'read_committed','socket.timeout.ms':5000})
        readers=[source,commands]
        kinds={'grid.transformer-load.v1':'loads','grid.action.v1':'actions','grid.charger-command.v1':'commands','grid.command-result.v1':'results'}
        from_beginning(source,[options.prefix+t for t in kinds]);from_beginning(commands,[options.prefix+'grid.charger-command.v1'])
        latest=defaultdict(set); measured=set();completed=set(); duplicated=set();counts=Counter(); delivered=Counter()
        key_partitions=defaultdict(set)
        original_delivery=publisher.delivered
        def delivery(error,message):
            original_delivery(error,message)
            if not error and message.topic().endswith('grid.telemetry.v1'):
                delivered[message.partition()]+=1
                key_partitions[message.key().decode()].add(message.partition())
        publisher.delivered=delivery
        begin=time.monotonic();next_at=begin;cycle=0;received=[]; rates=[]
        due={tx:begin for tx in transformers}
        with (root/'telemetry.jsonl').open('w') as telemetry:
            while time.monotonic()-begin<args.duration:
                runner.healthy_children();now=utc();elapsed=time.monotonic()-begin
                if time.monotonic()>=next_at:
                    started=time.monotonic();sample_at=now
                    selected=reporting_transformers(foreground,due,started)
                    sampled=0
                    for d in devices:
                        tx=d['transformer_id'];device=d['device_id'];kind=d['device_type']
                        if tx not in selected:continue
                        sampled+=1
                        if device in edges:
                            heartbeat=pb.HeartbeatRequest(operation_id=f'{args.run}-{cycle}-{device}',device_id=device,capability_version=1)
                            heartbeat.observed_at.FromDatetime(now-timedelta(milliseconds=10));client.call('Heartbeat',heartbeat)
                            reading=edges[device].reading(sample_at,args.run)
                        else:
                            power=((8 if elapsed<20 or tx not in foreground or tx in measured else 15)/55 if kind=='SMART_METER' else -.2 if kind=='SOLAR' else 0)
                            payload=dict(device_id=device,transformer_id=tx,device_type=kind,boot_id=args.run,
                                         power_kw=power,quality='GOOD',sequence_number=cycle)
                            if kind=='EV_CHARGER':payload.update(current_rate_kw=0,minimum_rate_kw=0,priority=1)
                            reading=event('telemetry.reading',payload,sample_at,f'{args.run}/{cycle}/{device}',args.run)
                        publisher.send('grid.telemetry.v1',tx,reading)
                        telemetry.write(json.dumps(reading,separators=(',',':'))+'\n');counts['telemetry']+=1
                    publisher.drain();telemetry.flush();cycle+=1
                    rates.append(dict(elapsed=elapsed,seconds=time.monotonic()-started,readings=sampled))
                    for tx in selected:due[tx]=next_at+(args.interval if tx in foreground else args.background_interval)
                    next_at=max(next_at+args.interval,time.monotonic())
                for message in commands.consume(100,timeout=.01):
                    assert not message.error(),message.error()
                    command=json.loads(message.value());device=command['payload']['device_id']
                    assert device in edges,'command selected an idle/non-target charger'
                    handle_message(message,edges[device],publisher,commands,utc())
                    if command['payload']['command_type']=='THROTTLE' and device not in duplicated:
                        publisher.send('grid.charger-command.v1',device,command);publisher.drain();duplicated.add(device)
                for message in source.consume(500,timeout=.01):
                    assert not message.error(),message.error()
                    e=json.loads(message.value());name=kinds[message.topic().removeprefix(options.prefix)]
                    runner.evidence[name].append(e);counts[name]+=1
                    p=e['payload'];tx=p.get('transformer_id')
                    if name=='actions':
                        latest[tx].add(p['status'])
                        if p['status']=='COMPLETED':completed.add(tx)
                    if name=='loads' and 'REDUCTION_CONFIRMED' in latest[tx] and math.isclose(p['net_load_kw'],15,abs_tol=1e-8) and math.isclose(p['ev_demand_kw'],2,abs_tol=1e-8):
                        measured.add(tx)
                    received.append(dict(kind=name,elapsed=time.monotonic()-begin,event_id=e['event_id']))
                progress=dict(elapsed=time.monotonic()-begin,cycles=cycle,counts=dict(counts),reduced=len(measured),completed=len(completed))
                (root/'progress.json').write_text(json.dumps(progress))
                if completed==foreground:break
                time.sleep(.01)
        publisher.drain();report.update(progress,delivered_by_partition=dict(delivered),rounds=rates,
            actual_delivered_eps=counts['telemetry']/(time.monotonic()-begin),received=received)
        (root/'business-events.json').write_text(json.dumps(runner.evidence))
        assert measured==completed==foreground,progress
        assert len(key_partitions)==25 and len(delivered)==3 and all(len(v)==1 for v in key_partitions.values())
        report['transformer_partitions']={k:next(iter(v)) for k,v in key_partitions.items()}
        for tx in foreground:
            assert {'PLAN_CREATED','COMMANDS_DISPATCHED','REDUCTION_CONFIRMED','RECOVERY_MONITORING','RESTORE_DISPATCHED','COMPLETED'}<=latest[tx],(tx,latest[tx])
            assert 'NEEDS_OPERATOR_REVIEW' not in latest[tx]
            loads=[e['payload'] for e in runner.evidence['loads'] if e['payload']['transformer_id']==tx]
            for net in (13.2,20.2,15,8):assert any(math.isclose(p['net_load_kw'],net,abs_tol=1e-8) for p in loads),(tx,net)
        effects={k:v.current() for k,v in edges.items()}
        assert all(math.isclose(v[0],7.2,abs_tol=1e-8) and v[2]==(2 if edges[k].device['transformer_id'] in foreground else 0) for k,v in effects.items()),effects
        assert len(duplicated)==len(foreground)
        report['effects']=effects
        # Publish Registry-owned context using actual retained load outputs, not command targets.
        context=RegistryContext(options.registry,'api_disposable')
        loads={e['payload']['transformer_id']:e['payload'] for e in runner.evidence['loads']}
        baselines=[dict(transformer_id=tx,state='STALE',load=p,policy=default_policy(),freshness='FRESH',data_age_seconds=0) for tx,p in loads.items()]
        try:
            ids=[d['device_id'] for d in devices]
            for index in range(0,len(ids),25):
                context.refresh(runner.store,baselines if index==0 else [],ids[index:index+25])
        finally:context.close()
        def projected():
            with runner.store.transaction() as db:
                return db.execute("SELECT count(*) FROM projection_current WHERE kind='Action' AND event->'payload'->>'status'='COMPLETED'").fetchone()[0]==len(foreground)
        wait(projected,'projected completed actions',180)
        headers={'Authorization':'Bearer '+runner.token}
        def collection(path):
            items=[];cursor=None
            while True:
                value=http(options.api_url+path+('?cursor='+cursor if cursor else ''),headers=headers)
                items.extend(value['items']);cursor=value.get('next_cursor')
                if not cursor:return items
        assert len(collection('/devices'))==2500
        assert len(collection('/transformers'))==25
        actions=collection('/actions');assert len(actions)==len(foreground) and all(a['status']=='COMPLETED' for a in actions)
        report.update(status='PASS',api_devices=2500,api_transformers=25,api_completed_actions=len(foreground))
        print(json.dumps({k:v for k,v in report.items() if k not in ('received','effects','rounds')}),flush=True)
    except BaseException as exc:
        report.update(status='FAIL',error=type(exc).__name__+': '+str(exc)[:1200])
        raise
    finally:
        (root/'report.json').write_text(json.dumps(report,indent=2,default=str))
        for reader in readers:reader.close()
        for name,process,log in reversed(runner.children):
            process.terminate()
            try:process.wait(20)
            except subprocess.TimeoutExpired:process.kill();process.wait()
            log.close()
        for edge in edges.values():edge.db.close()
        client.channel.close()
        if job:
            subprocess.run(['docker','exec','grid-million-jobmanager-1','flink','cancel',job],timeout=60)


if __name__=='__main__':main()
