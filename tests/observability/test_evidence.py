import base64
from copy import deepcopy
import pytest
from observability.evidence import validate_packet


def packet():
    return {'trace_id':'11'*16,'trace':{'batches':[{'scopeSpans':[{'spans':[{
        'name':'db.projection','traceId':base64.b64encode(bytes.fromhex('11'*16)).decode(),
        'startTimeUnixNano':'100','endTimeUnixNano':'3000000100','status':{'code':'STATUS_CODE_ERROR'}}]}]}]},
        'correlated_logs':[{'boundary':'db.projection','outcome':'error','trace_id':'11'*16}],
        'metric':{'data':{'result':[{'metric':{'outcome':'error'},'value':[123,'1']}]}},
        'fault_response':{'status':503}, 'recovery':[{'status':200,'seconds':.02}]*3,
        'baseline':[{'status':200,'seconds':.02}]*5,'recovery_seconds':3.4}


def test_missing_or_mismatched_evidence_is_never_success():
    original=packet()
    validate_packet(original)
    mutations=[lambda p:p.update(trace_id='22'*16),
        lambda p:p.update(correlated_logs=[]),
        lambda p:p['metric']['data'].update(result=[]),
        lambda p:p.update(recovery=[{'status':200,'seconds':2}]*3),
        lambda p:p.update(baseline=[]),lambda p:p.update(recovery_seconds=0),
        lambda p:p.update(fault_response={'status':200}),
        lambda p:p['trace']['batches'][0]['scopeSpans'][0]['spans'][0].update(status={})]
    for mutation in mutations:
        changed=deepcopy(original)
        mutation(changed)
        with pytest.raises(ValueError):
            validate_packet(changed)
