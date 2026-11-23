"""Fail-closed checks on collected backend responses, not injector expectations."""
import base64

def trace_spans(trace):
    return [span for batch in trace.get('batches', trace.get('resourceSpans', []))
        for scope in batch.get('scopeSpans', batch.get('instrumentationLibrarySpans', []))
        for span in scope.get('spans', [])]


def validate_packet(packet):
    spans = trace_spans(packet['trace'])
    if not any(s['name']=='db.projection' and s.get('status',{}).get('code') in (2, 'STATUS_CODE_ERROR') and int(s['endTimeUnixNano'])-int(s['startTimeUnixNano']) >= 2_500_000_000 for s in spans):
        raise ValueError('Missing real slow database span')
    ids = {base64.b64decode(s['traceId']).hex() for s in spans}
    if ids != {packet['trace_id']}:
        raise ValueError('Trace identity mismatch')
    logs = packet['correlated_logs']
    if not logs or any(row.get('trace_id') != packet['trace_id'] for row in logs):
        raise ValueError('Log correlation mismatch')
    if not any(r.get('boundary')=='db.projection' and r.get('outcome')=='error' for r in logs):
        raise ValueError('Missing correlated database error log')
    if not any(row['metric'].get('outcome')=='error' and float(row['value'][1])>=1 for row in packet['metric']['data']['result']):
        raise ValueError('Missing collected metric')
    if packet['fault_response']['status']!=503 or not all(r['status']==200 and r['seconds']<1 for r in packet['recovery']):
        raise ValueError('Failure/recovery criterion not met')
    if len(packet['recovery']) < 3 or not packet['baseline'] or not all(r['status']==200 and r['seconds']<1 for r in packet['baseline']):
        raise ValueError('Baseline/recovery evidence incomplete')
    if packet['recovery_seconds']<=0:
        raise ValueError('Invalid measured recovery')
