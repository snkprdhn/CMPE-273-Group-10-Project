"""Keep SDK compatibility, native boundaries and provisioned queries reviewable."""
import json
from pathlib import Path
import subprocess
from observability.dashboards import dashboards


def test_dashboards_use_fixed_dimensions_and_do_not_hide_missing_data():
    views = list(dashboards())
    assert len(views)==5
    for uid, body in views:
        assert body['uid']==uid and body['panels']
        for panel in body['panels']:
            expr=panel['targets'][0]['expr']
            assert 'or vector(0)' not in expr
            assert not any(field in expr for field in ('trace_id','action_id','command_id','device_id','correlation_id'))
            assert panel['fieldConfig']['defaults']['noValue']=='unknown'


def test_deployment_propagates_headers_without_changing_event_contract():
    job=Path('aggregation/job.py').read_text()
    assert "METADATA FROM 'headers' VIRTUAL" in job
    assert "MAP<STRING, BYTES> METADATA FROM 'headers'" in job
    assert "'sink.delivery-guarantee'='exactly-once'" in job
    from action.cdc import configuration
    placement=configuration()['transforms.outbox.table.fields.additional.placement']
    assert placement=='traceparent:header:traceparent,correlation_id:header:correlation_id,causation_id:header:causation_id'
    assert '1.45.2' in Path('observability/requirements.txt').read_text()
    before=subprocess.check_output(['git','show','HEAD:schemas/v1/bundle.json'],text=True)
    assert json.loads(before)==json.loads(Path('schemas/v1/bundle.json').read_text())
