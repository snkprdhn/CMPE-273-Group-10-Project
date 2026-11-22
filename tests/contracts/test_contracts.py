"""Contract examples, boundaries and deliberate breaking changes."""
from copy import deepcopy
import json
import tempfile

import pytest
from jsonschema import ValidationError
from tools import contracts as c
from tests.contracts.reference import replay


def read(name):
    return json.loads((c.V1 / name).read_text())


VALID = read('examples.valid.json')
BY = {v['schema']: v['value'] for v in VALID}


@pytest.mark.parametrize('case', VALID, ids=lambda x:x['name'])
def test_valid_examples(case):
    c.validate(case['schema'], case['value'], key=case.get('key'))


@pytest.mark.parametrize('case', read('examples.invalid.json'), ids=lambda x:x['name'])
def test_invalid_examples(case):
    expected = case['error']
    if 'validator' in expected:
        with pytest.raises(ValidationError) as error:
            c.validate(case['schema'], case['value'], key=case.get('key'))
        assert error.value.validator == expected['validator']
        assert list(error.value.path) == expected['path']
    else:
        with pytest.raises(ValueError, match='^'+expected['code']+'$'):
            c.validate(case['schema'], case['value'], key=case.get('key'))


@pytest.mark.parametrize('case', read('golden.scenarios.json'), ids=lambda x:x['name'])
def test_golden(case):
    c.validate('Topology',case['topology'])
    assert replay(case) == case['expected']


def test_every_definition_has_an_example_and_all_topics_are_covered():
    assert set(BY) == set(c.BUNDLE['$defs'])
    assert len(c.BUNDLE['x-events']) == 8
    assert {v['schema'] for v in VALID} >= set(c.BUNDLE['x-events'].values())


@pytest.fixture(scope='module')
def proto():
    with tempfile.TemporaryDirectory() as tmp:
        descriptor = c.compile_proto(c.V1,tmp)
        yield descriptor,c.load_generated(tmp)


@pytest.mark.parametrize('case', read('registry.examples.json')['valid'])
def test_registry_valid(proto,case):
    c.validate_registry(case['message'],case['value'],proto[1])


@pytest.mark.parametrize('case', read('registry.examples.json')['invalid'])
def test_registry_invalid(proto,case):
    with pytest.raises(ValueError,match=case['error']):
        c.validate_registry(case['message'],case['value'],proto[1])


@pytest.mark.parametrize('mutation', ['remove','type','required','enum','key','unit','authorization'])
def test_incompatible_json_detected(mutation):
    old=deepcopy(c.BUNDLE);new=deepcopy(old)
    if mutation=='remove': del new['$defs']['Telemetry']['properties']['event_id']
    elif mutation=='type': new['$defs']['TelemetryPayload']['properties']['power_kw']['type']='string'
    elif mutation=='required': new['$defs']['Telemetry']['required'].append('new_field')
    elif mutation=='enum': new['$defs']['DeviceType']['enum'].append('WIND')
    elif mutation=='key': new['$defs']['Telemetry']['x-partition-key']='device_id'
    elif mutation=='unit': new['$defs']['LoadPayload']['properties']['net_load_kw']['x-unit']='W'
    elif mutation=='authorization':new['$defs']['get_transformer_snapshotArguments']['additionalProperties']=True
    with pytest.raises(ValueError):c.compatible(old,new)


def test_optional_event_addition_and_comments_are_compatible():
    new=deepcopy(c.BUNDLE)
    new['$defs']['TelemetryPayload']['properties']['firmware_version']={'type':'string'}
    new['$defs']['Telemetry']['description']='Documentation'
    c.compatible(c.BUNDLE,new)


@pytest.mark.parametrize('mutation',['number','type','rpc','enum','remove'])
def test_incompatible_proto_detected(proto,mutation):
    old=proto[0];new=deepcopy(old)
    if mutation=='number':new.message_type[0].field[0].number=99
    elif mutation=='type':new.message_type[0].field[0].type=5
    elif mutation=='rpc':new.service[0].method[0].server_streaming=True
    elif mutation=='enum':new.enum_type[0].value[1].number=9
    else:del new.message_type[0].field[0]
    with pytest.raises(ValueError):c.proto_compatible(old,new)


def test_rest_removal_and_access_widening_detected():
    old=read('openapi.json')
    for mutation in ('remove','role','schema'):
        new=deepcopy(old)
        if mutation=='remove':del new['paths']['/overview']
        elif mutation=='role':new['paths']['/investigations']['post']['x-minimum-role']='Viewer'
        else:new['paths']['/overview']['get']['responses']['200']['content']['application/json']['schema']={'type':'string'}
        with pytest.raises(ValueError):c.compatible(old,new)


def test_tool_scope_and_clamped_limit():
    call=deepcopy(BY['get_load_historyCall']);context=deepcopy(BY['ToolContext'])
    call['arguments'].update(transformer_id='tx-001',limit=200)
    context['max_items']=10
    assert c.authorize(call,context,'2026-11-09T18:30:01Z')['limit']==10
    for mutate,code in [('id','OUT_OF_SCOPE'),('role','FORBIDDEN'),('deadline','DEADLINE_EXCEEDED'),('time','OUT_OF_SCOPE')]:
        ctx=deepcopy(context); request=deepcopy(call)
        if mutate=='id':request['arguments']['transformer_id']='tx-999'
        elif mutate=='role':ctx['roles']=['Viewer']
        elif mutate=='deadline':ctx['deadline']='2026-11-09T18:30:01Z'
        else:ctx['time_scope']['start']='2026-11-09T18:30:02Z'
        with pytest.raises(ValueError,match=code):c.authorize(request,ctx,'2026-11-09T18:30:01Z')


def test_evidence_must_exist_in_current_investigation():
    draft=deepcopy(BY['AgentDraft']);evidence=[deepcopy(BY['Evidence'])]
    c.validate_draft(draft,evidence)
    with pytest.raises(ValueError,match='UNKNOWN_EVIDENCE'):c.validate_draft(draft,[])
    with pytest.raises(ValueError,match='DUPLICATE_EVIDENCE'):c.validate_draft(draft,evidence*2)
    draft['observations'][0]['evidence_ids']=['invented']
    with pytest.raises(ValueError,match='UNKNOWN_EVIDENCE'):c.validate_draft(draft,evidence)


def test_invalid_timestamp_uuid_nonfinite_and_non_utc():
    for field,value in [('event_id','abc'),('event_time','2026-02-30T00:00:00Z'),('event_time','2026-11-09T18:30:00+00:00')]:
        event=deepcopy(BY['Telemetry']);event[field]=value
        with pytest.raises(ValidationError):c.validate('Telemetry',event,key='tx-001')
    event=deepcopy(BY['Telemetry']);event['payload']['power_kw']=float('nan')
    with pytest.raises(ValueError,match='NON_FINITE'):c.validate('Telemetry',event,key='tx-001')


def test_all_openapi_refs_resolve_and_paths_have_errors():
    document=read('openapi.json')
    def walk(value):
        if isinstance(value,dict):
            if '$ref' in value:
                prefix,pointer=value['$ref'].split('#')
                assert prefix=='bundle.json'
                target=c.BUNDLE
                for key in pointer.strip('/').split('/'):target=target[key]
            for v in value.values():walk(v)
        elif isinstance(value,list):
            for v in value:walk(v)
    walk(document)
    assert len(document['paths'])==24
    for methods in document['paths'].values():
        for op in methods.values():
            assert {'400','401','403','404','409','422','429','503'} <= op['responses'].keys()


@pytest.mark.parametrize('case', read('agent.scenarios.json'), ids=lambda x:x['name'])
def test_agent_golden(case):
    if 'error' in case:
        with pytest.raises(ValueError,match='^'+case['error']+'$'):
            c.authorize(case['call'],case['context'],case['now'])
    else:
        assert c.authorize(case['call'],case['context'],case['now']) == case['expected_arguments']
        c.validate_tool_result(case['result'],case['call'],case['context'])
        c.validate_draft(case['draft'],case['result']['evidence'])


def test_tool_results_have_size_and_scope_limits():
    call=BY['get_transformer_snapshotCall'];context=deepcopy(BY['ToolContext'])
    result={'evidence':[deepcopy(BY['Evidence'])]}
    c.validate_tool_result(result,call,context)
    result['evidence'][0]['entity_id']='tx-unauthorized'
    with pytest.raises(ValueError,match='OUT_OF_SCOPE'):c.validate_tool_result(result,call,context)
    result['evidence'][0]['entity_id']='tx-001';context['max_items']=1
    result['evidence']*=2
    with pytest.raises(ValueError,match='RESULT_TOO_LARGE'):c.validate_tool_result(result,call,context)
    history=deepcopy(BY['get_load_historyEvidence'])
    history['data']['windows']=[deepcopy(BY['LoadPayload']) for _ in range(200)]
    context['max_items']=200
    with pytest.raises(ValueError,match='RESULT_TOO_LARGE'):
        c.validate_tool_result({'evidence':[history]},BY['get_load_historyCall'],context)
    evidence=deepcopy(BY['Evidence']);evidence['deep_link']='https://backend.example/secrets'
    with pytest.raises(ValidationError):c.validate('Evidence',evidence)


def test_tool_argument_additions_require_review():
    new=deepcopy(c.BUNDLE)
    new['$defs']['get_transformer_snapshotArguments']['properties']['actor_id']={'type':'string'}
    with pytest.raises(ValueError):c.compatible(c.BUNDLE,new)


def test_write_tool_is_rejected():
    with pytest.raises(ValidationError):
        c.validate('ToolCall',{'schema_version':1,'tool':'set_charger_rate','arguments':{'target_rate_kw':0}})


def test_generated_capabilities_match_proto(proto):
    for name,schema in c.registry_json_definitions(proto[0]).items():
        assert c.BUNDLE['$defs'][name] == schema


@pytest.mark.parametrize('old,new', [(1, True), ([1], [True]), (1, 1.0)])
def test_compatibility_preserves_json_value_types(old, new):
    with pytest.raises(ValueError, match='CHANGED'):
        c.compatible({'const': old}, {'const': new})


def test_load_checks_use_absolute_tolerance():
    payload = deepcopy(BY['LoadPayload'])
    payload.update(meter_demand_kw=1_000_000_000, ev_demand_kw=0,
                   solar_generation_kw=0, net_load_kw=1_000_000_000.5)
    with pytest.raises(ValueError, match='LOAD_TOTAL'):
        c.validate('LoadPayload', payload)
    payload.update(net_load_kw=1_000_000_000, capacity_kw=100,
                   utilization_pct=1_000_000_000.5)
    with pytest.raises(ValueError, match='UTILIZATION'):
        c.validate('LoadPayload', payload)
