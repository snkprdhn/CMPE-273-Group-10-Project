"""Real PostgreSQL fixture acceptance; isolated temporary service-owned schemas."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import os
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest
from jsonschema import ValidationError

from member3.control import Conflict, Control
from member3.domain import (allocate, candidates, default_policy, identity,
                            instant, stamp, validate)
from member3.fixtures import FakeRegistry, replay, scenarios

DSN = os.getenv('MEMBER3_TEST_DSN')
CASES = scenarios()


@pytest.fixture
def control():
    if not DSN:
        pytest.skip('set MEMBER3_TEST_DSN for real PostgreSQL acceptance')
    schema = 'test_action_' + uuid4().hex
    with psycopg.connect(DSN) as db:
        db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    dsn = make_conninfo(DSN, options=f'-c search_path={schema}')
    service = Control(dsn, FakeRegistry())
    service.migrate()
    try:
        yield service
    finally:
        with psycopg.connect(DSN) as db:
            db.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def test_all_input_fixtures_validate():
    for case in CASES:
        for step in case['steps']:
            if step['op'] in ('load', 'result'):
                validate('Load' if step['op'] == 'load' else 'Result', step['event'])


@pytest.mark.parametrize('case', CASES, ids=lambda c: c['name'])
def test_durable_fixtures(control, case):
    state = replay(control, case)
    expected = case['expected']
    assert len(state['actions']) == expected['actions']
    assert len(state['commands']) == expected['commands']
    action_rows = [r for r in state['outbox'] if r['topic'] == 'grid.action.v1']
    assert len(action_rows) == expected['action_events']
    assert [r['event']['payload']['status'] for r in action_rows] == expected['transitions']
    assert {c['event']['payload']['device_id'] + '/' + c['event']['payload']['command_type']: c['event']['payload']['target_rate_kw'] for c in state['commands']} == expected['command_targets']
    assert len(state['outbox']) == expected['action_events'] + expected['command_deliveries']
    if state['actions']:
        a = state['actions'][0]
        assert a['status'] == expected['status']
        assert a['version'] == len(action_rows)
        assert [r['event']['saga_version'] for r in action_rows] == list(range(1, len(action_rows)+1))
    assert state['transformers']['tx-fixture']['health'] == expected['health']
    # Outbox retry deliveries are distinct rows with the same immutable logical
    # command envelope, not another logical plan/command or renewed TTL.
    by_command = {}
    for r in state['outbox']:
        event = r['event']
        validate('Action' if r['topic'] == 'grid.action.v1' else 'Command', event)
        if r['topic'] == 'grid.charger-command.v1':
            cid = event['payload']['command_id']
            if cid in by_command:
                assert event == by_command[cid]
            by_command[cid] = event
    assert len(by_command) == expected['commands']
    assert len({(c['event']['payload']['device_id'], c['event']['payload']['command_type']) for c in state['commands']}) == expected['commands']
    before = deepcopy(state)
    # Entire source replay after a restart cannot create new logical plans.
    state = replay(Control(control.dsn, control.registry), dict(steps=[s for s in case['steps'] if s['op'] in ('load','result')]))
    assert state == before


def start(control):
    replay(control, dict(steps=CASES[0]['steps']))
    return control.inspect()


def load_step(n=0):
    return deepcopy(next(s for s in CASES[0]['steps'] if s['op'] == 'load'))


def test_allocation_and_eligibility_boundaries():
    snaps = deepcopy(CASES[0]['steps'][0]['values'])
    now = instant(load_step()['now'])
    d = snaps[0]['device']
    d['updated_at'] = stamp(now-timedelta(seconds=15))
    snaps[0]['telemetry']['observed_at'] = d['updated_at']
    assert len(candidates(snaps,'tx-fixture',now)) == 2
    d['updated_at'] = stamp(now-timedelta(seconds=15,microseconds=1))
    assert [x['device_id'] for x in candidates(snaps,'tx-fixture',now)] == ['ev-1']
    d['updated_at'] = stamp(now)
    d['departure_time'] = stamp(now+timedelta(minutes=5))
    assert len(candidates(snaps,'tx-fixture',now)) == 1
    d['departure_time'] = stamp(now+timedelta(minutes=5,microseconds=1))
    eligible = candidates(snaps,'tx-fixture',now)
    assert allocate(eligible,25) == [dict(device_id='ev-0',original_rate_kw=20,target_rate_kw=0,capability_version=1),dict(device_id='ev-1',original_rate_kw=20,target_rate_kw=15,capability_version=1)]
    # Equal priority: absent departure, latest departure, then lexical ID.
    snaps[1]['device']['priority'] = 0
    assert candidates(snaps,'tx-fixture',now)[0]['device_id'] == 'ev-1'
    snaps[1]['device']['departure_time'] = stamp(now+timedelta(hours=1))
    assert candidates(snaps,'tx-fixture',now)[0]['device_id'] == 'ev-1'
    snaps[0]['device']['departure_time'] = snaps[1]['device']['departure_time']
    assert candidates(snaps,'tx-fixture',now)[0]['device_id'] == 'ev-0'
    for field,value in [('controllable',False),('updated_at',stamp(now+timedelta(seconds=1)))]:
        changed=deepcopy(snaps); changed[0]['device'][field]=value
        assert len(candidates(changed,'tx-fixture',now)) == 1
    for field,value in [('online',False),('charging',False),('observed_at',stamp(now-timedelta(seconds=16)))]:
        changed=deepcopy(snaps); changed[0]['telemetry'][field]=value
        assert len(candidates(changed,'tx-fixture',now)) == 1


def test_threshold_and_freshness_boundaries(control):
    # Use a fresh schema for each load identity by changing only revision and
    # consistently updating all numbers. Below critical never creates a plan.
    step=load_step(); e=step['event'];p=e['payload']
    control.registry.values=deepcopy(CASES[0]['steps'][0]['values'])
    for revision,kw,health in [(0,79.9,'NORMAL'),(1,80,'WARNING'),(2,94.999,'WARNING'),(3,95,'CRITICAL')]:
        p.update(revision=revision,meter_demand_kw=kw-40,net_load_kw=kw,utilization_pct=kw)
        e['event_id']=identity(f"load/{p['window_id']}/{revision}")
        assert control.load(e,'tx-fixture',instant(step['now'])+timedelta(seconds=15)) == health
    assert len(control.inspect()['actions'])==1


def test_conflicts_rollback_and_optimistic_policy(control):
    state=start(control); step=load_step();e=step['event']
    changed=deepcopy(e);changed['correlation_id']='conflicting-id'
    with pytest.raises(Conflict,match='EVENT_ID_REUSED'):
        control.load(changed,'tx-fixture',instant(step['now']))
    assert control.inspect()==state
    policy=default_policy();policy['version']=2
    control.policy('tx-fixture',policy,1)
    with pytest.raises(Conflict,match='POLICY_VERSION'):
        control.policy('tx-fixture',policy,1)
    policy['recovery_pct']=99
    with pytest.raises(ValueError,match='POLICY_THRESHOLDS'):
        control.policy('tx-fixture',policy,2)


def test_registry_outage_and_outbox_failure_are_atomic(control,monkeypatch):
    control.registry.unavailable=True
    step=load_step()
    with pytest.raises(ConnectionError):
        control.load(step['event'],'tx-fixture',instant(step['now']))
    assert control.inspect()['inbox']==0
    assert not control.inspect()['transformers']
    control.registry.unavailable=False
    control.registry.values=deepcopy(CASES[0]['steps'][0]['values'])
    original=control.outbox
    def fail(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError('crash after outbox insert before commit')
    monkeypatch.setattr(control,'outbox',fail)
    with pytest.raises(RuntimeError):
        control.load(step['event'],'tx-fixture',instant(step['now']))
    state=control.inspect()
    assert not state['actions'] and not state['outbox'] and state['inbox']==0
    monkeypatch.setattr(control,'outbox',original)
    start(control)
    assert len(control.inspect()['actions'])==1


def test_concurrent_deliveries_make_one_plan(control):
    control.registry.values=deepcopy(CASES[0]['steps'][0]['values'])
    step=load_step()
    def run(_):
        return Control(control.dsn,control.registry).load(step['event'],'tx-fixture',instant(step['now']))
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(run,range(8)))
    assert results.count('DUPLICATE')==7
    state=control.inspect()
    assert len(state['actions'])==1 and len(state['commands'])==2 and len(state['outbox'])==5


def test_result_validation_versions_and_logical_duplicates(control):
    state=start(control)
    step=deepcopy(next(s for s in CASES[6]['steps'] if s['op']=='result'))
    e,now=step['event'],instant(step['now'])
    with pytest.raises(Conflict,match='SAGA_VERSION'):
        control.result(e,'ev-0',now,expected_version=0)
    assert control.inspect()==state
    bad=deepcopy(e);bad['payload']['device_id']='wrong'
    with pytest.raises(ValueError,match='RESULT_IDENTITY'):
        control.result(bad,'wrong',now)
    control.result(e,'ev-0',now,expected_version=3)
    recorded=control.inspect()
    duplicate=deepcopy(e);duplicate['event_id']=str(uuid4());duplicate['payload']['outcome']='ALREADY_APPLIED'
    assert control.result(duplicate,'ev-0',now)=='KNOWN_RESULT'
    assert control.inspect()['actions']==recorded['actions']
    bad=deepcopy(duplicate);bad['event_id']=str(uuid4());bad['payload']['applied_rate_kw']=20
    with pytest.raises(Conflict,match='CONFLICTING_RESULT'):
        control.result(bad,'ev-0',now)


def test_stale_epoch_cannot_mutate_or_retry(control):
    start(control)
    newer=Control(control.dsn,control.registry,2)
    newer.tick(instant(load_step()['now'])+timedelta(seconds=5))
    state=newer.inspect()
    assert state['actions'][0]['status']=='NEEDS_OPERATOR_REVIEW'
    assert len([r for r in state['outbox'] if r['topic']=='grid.charger-command.v1'])==2
    with pytest.raises(Conflict,match='STALE_EPOCH'):
        control.tick(instant(load_step()['now'])+timedelta(seconds=6))
    assert control.inspect()==state


def test_cooldown_and_completed_trigger_never_replans(control):
    replay(control,CASES[10])  # restore completed at second 21, plan created at 5
    step=load_step(); e=step['event'];p=e['payload'];base=instant(p['window_start'])
    for start in (20,30):
        p['window_start']=stamp(base+timedelta(seconds=start))
        p['window_end']=stamp(base+timedelta(seconds=start+5))
        p['window_id']=identity(f"window/tx-fixture/{p['window_start']}/5")
        e['event_id']=identity(f"load/{p['window_id']}/0")
        now=base+timedelta(seconds=start+5)
        control.registry.values=deepcopy(CASES[0]['steps'][0]['values'])
        for s in control.registry.values:
            s['device']['updated_at']=stamp(now);s['telemetry']['observed_at']=stamp(now)
        control.load(e,'tx-fixture',now)
        assert len(control.inspect()['actions'])==(1 if start==20 else 2)
    # A higher revision cannot erase the previous saga or create a new plan.
    e=load_step()['event'];e['payload']['revision']=2
    e['event_id']=identity(f"load/{e['payload']['window_id']}/2")
    control.load(e,'tx-fixture',base+timedelta(seconds=35))
    assert len(control.inspect()['actions'])==2


@pytest.mark.parametrize('failure', ['missing','stale','changed_capability','changed_rate','assignment'])
def test_restore_requires_authoritative_current_state(control,failure):
    case=deepcopy(CASES[10])
    # Stop before restore trigger, alter fake boundary, then evaluate window two.
    steps=case['steps'][:6]  # base, both results, first recovery window, restart
    replay(control,dict(steps=steps))
    snapshot=deepcopy(next(s for s in case['steps'] if s['op']=='snapshots' and s['values'][0]['telemetry']['current_rate_kw']==0))
    if failure=='missing': snapshot['values']=[]
    elif failure=='stale': snapshot['values'][0]['device']['updated_at']='2026-11-22T18:29:00Z'
    elif failure=='changed_capability': snapshot['values'][0]['device']['capability_version']=2
    elif failure=='changed_rate': snapshot['values'][0]['telemetry']['current_rate_kw']=1
    else: snapshot['values'][0]['device']['transformer_id']='another-transformer'
    trigger=deepcopy([s for s in case['steps'] if s['op']=='load'][-1])
    state=replay(control,dict(steps=[snapshot,trigger]))
    assert state['actions'][0]['status']=='RECOVERY_MONITORING'
    assert len(state['commands'])==2
    assert state['transformers']['tx-fixture']['health']=='SAFE_HOLD'


@pytest.mark.parametrize('change', ['key','number','counts','window','quality','timestamp'])
def test_invalid_load_does_not_enter_inbox(control,change):
    step=load_step();e=step['event'];p=e['payload'];key='tx-fixture'
    if change=='key': key='wrong'
    elif change=='number': p['net_load_kw']=float('nan')
    elif change=='counts': p['missing_device_count']=1
    elif change=='window': p['window_start']='2026-11-22T18:30:01Z'
    elif change=='quality': p['completeness']='PARTIAL'
    else: e['event_time']='not-a-timestampZ'
    with pytest.raises((ValueError,ValidationError)):
        control.load(e,key,instant(step['now']))
    assert control.inspect()['inbox']==0
    assert control.inspect()['actions']==[]


def test_restore_timeout_holds_without_new_throttle(control):
    case=deepcopy(CASES[10]);case['steps']=case['steps'][:-2]
    replay(control,case)
    control.tick(instant('2026-11-22T18:30:25Z'))
    control.tick(instant('2026-11-22T18:30:30Z'))
    state=control.inspect()
    assert state['actions'][0]['status']=='NEEDS_OPERATOR_REVIEW'
    assert state['outbox'][-2]['event']['payload']['status']=='TIMED_OUT'
    assert len(state['commands'])==4
    assert all(c['attempts']<=2 for c in state['commands'])


def test_incorrect_applied_rate_is_not_confirmed(control):
    start(control)
    results=deepcopy([s for s in CASES[10]['steps'] if s['op']=='result'][:2])
    results[0]['event']['payload']['applied_rate_kw']=19
    state=replay(control,dict(steps=results))
    assert state['actions'][0]['status']=='NEEDS_OPERATOR_REVIEW'
    assert state['actions'][0]['confirmed']==5


def test_separate_process_reads_committed_recovery(control):
    import json
    import subprocess
    import sys
    replay(control,CASES[10])
    output=subprocess.run([sys.executable,'-m','member3','inspect'],
        env=dict(os.environ,CONTROL_DSN=control.dsn),check=True,capture_output=True,text=True)
    assert json.loads(output.stdout)==control.inspect()


def test_review_hold_survives_same_window_correction(control):
    case = deepcopy(CASES[10])
    replay(control, dict(steps=case['steps'][:-2]))  # restore awaiting results
    control.tick(instant('2026-11-22T18:30:30Z'))
    corrected = deepcopy([s for s in case['steps'] if s['op'] == 'load'][-1])
    p = corrected['event']['payload']
    p['revision'] += 1
    corrected['event']['event_id'] = identity(f"load/{p['window_id']}/{p['revision']}")
    corrected['now'] = '2026-11-22T18:30:30Z'
    state = replay(control, dict(steps=[corrected]))
    assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'
    assert state['transformers']['tx-fixture']['health'] == 'SAFE_HOLD'
    assert len(state['commands']) == 4


def test_late_results_cannot_bypass_timeout_after_process_downtime(control):
    start(control)
    results = deepcopy([s for s in CASES[10]['steps'] if s['op'] == 'result'][:2])
    # No timer ran while the process was down. Even an on-time device effect
    # reported after TTL must be reconciled instead of auto-releasing the saga.
    for step in results:
        step['now'] = '2026-11-22T18:30:16Z'
    restarted = Control(control.dsn, control.registry)
    state = replay(restarted, dict(steps=results))
    assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'
    assert state['transformers']['tx-fixture']['health'] == 'SAFE_HOLD'
    assert state['actions'][0]['confirmed'] == 25
    assert not any(r['event']['payload'].get('status') == 'RECOVERY_MONITORING' for r in state['outbox'])


@pytest.mark.parametrize('scale', [1e-12, 1e306])
def test_extreme_finite_loads_never_dispatch_an_empty_plan(control, scale):
    step = load_step()
    p = step['event']['payload']
    for field in ('meter_demand_kw', 'ev_demand_kw', 'net_load_kw', 'capacity_kw'):
        p[field] *= scale
    control.registry.values = deepcopy(CASES[0]['steps'][0]['values'])
    state = replay(control, dict(steps=[step]))
    assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'
    assert state['actions'][0]['target'] == pytest.approx(25 * scale)
    assert state['commands'] == []


def test_final_retry_deadline_requires_review_before_ttl(control):
    policy = default_policy()
    policy.update(version=2, command_ttl_seconds=20)
    control.policy('tx-fixture', policy, expected_version=1)
    state = start(control)
    control.tick(instant('2026-11-22T18:30:10Z'))  # final retry, deadline 15, TTL 25
    results = deepcopy([s for s in CASES[10]['steps'] if s['op'] == 'result'][:2])
    for step in results:
        p = step['event']['payload']
        command = next(c for c in state['commands'] if c['event']['payload']['device_id'] == p['device_id'])
        p.update(command_id=command['id'], action_id=state['actions'][0]['id'])
        step['now'] = '2026-11-22T18:30:16Z'
    state = replay(Control(control.dsn, control.registry), dict(steps=results))
    assert state['actions'][0]['status'] == 'NEEDS_OPERATOR_REVIEW'
    assert state['actions'][0]['confirmed'] == 25
    assert state['transformers']['tx-fixture']['health'] == 'SAFE_HOLD'
