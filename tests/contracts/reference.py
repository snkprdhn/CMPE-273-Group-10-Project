"""Small executable specification for fixtures, not a Flink/control service.

Inputs deliberately use explicit evaluation steps so arrival, event time, and
watermark advancement cannot be confused. Component implementations must run
these same fixtures against their actual engines in later milestones.
"""
from datetime import timedelta
from jsonschema import ValidationError
from tools.contracts import timestamp, window_id, load_id, validate, completeness


def replay(case):
    selected, seen, emitted, revisions = {}, set(), {}, {}
    topics, results = [], []
    metrics = dict(duplicates=0, dlq=0, very_late=0, corrections=0)
    late_counts = {}
    greatest = None
    epochs, applied, rates = {}, {}, {}
    commands = []
    history = {}
    for step in case['steps']:
        op = step['op']
        if op == 'reading':
            event = step['event']
            p = event['payload']
            try:
                validate('Telemetry', event, key=step['key'])
            except (ValueError, ValidationError):
                topics.append('grid.telemetry.dlq.v1'); metrics['dlq'] += 1
                continue
            if event['event_id'] in seen:
                metrics['duplicates'] += 1
                continue
            seen.add(event['event_id'])
            event_time = timestamp(event['event_time'])
            start = event_time.replace(second=event_time.second // 5 * 5, microsecond=0)
            end = start + timedelta(seconds=5)
            if timestamp(event['ingested_at']) > end + timedelta(seconds=30):
                topics.append('grid.telemetry.late.v1'); metrics['very_late'] += 1
                continue
            greatest = max(greatest, event_time) if greatest else event_time
            key = (p['transformer_id'], start)
            late_counts[key] = late_counts.get(key, 0) + int(timestamp(event['ingested_at']) > end)
            device = selected.setdefault(key, {})
            previous = device.get(p['device_id'])
            rank = (timestamp(event['event_time']), p['sequence_number'])
            if previous is None or rank > (timestamp(previous['event_time']), previous['payload']['sequence_number']):
                device[p['device_id']] = event
        elif op == 'evaluate':
            for (tx, start), devices in sorted(selected.items()):
                end = start + timedelta(seconds=5)
                watermark = greatest - timedelta(seconds=10)
                if watermark < end:
                    continue
                topology = next(t for t in case['topology']['transformers'] if t['transformer_id'] == tx)
                expected = {d['device_id'] for d in topology['devices']}
                readings = [v['payload'] for k, v in devices.items() if k in expected]
                good = sum(p['quality'] == 'GOOD' for p in readings)
                estimated = sum(p['quality'] == 'ESTIMATED' for p in readings)
                totals = {typ:sum(p['power_kw'] for p in readings if p['device_type'] == typ) for typ in ('SMART_METER','EV_CHARGER','SOLAR')}
                net = sum(totals.values())
                capacity = topology['capacity_kw']
                valid = capacity is not None and capacity > 0
                old = timestamp(step['now']) > end + timedelta(seconds=15)
                flags = []
                if estimated: flags.append('ESTIMATED')
                if len(readings) < len(expected): flags.append('MISSING_DEVICES')
                if not valid: flags.append('MISSING_CAPACITY')
                if old: flags.append('OLD_RESULT')
                payload = dict(transformer_id=tx, window_id=window_id(tx,start.isoformat().replace('+00:00','Z')),window_start=start.isoformat().replace('+00:00','Z'),window_end=end.isoformat().replace('+00:00','Z'),topology_version=case['topology']['topology_version'],meter_demand_kw=totals['SMART_METER'],ev_demand_kw=totals['EV_CHARGER'],solar_generation_kw=-totals['SOLAR'],net_load_kw=net,capacity_kw=capacity,utilization_pct=round(net/capacity*100,9) if valid else None,expected_device_count=len(expected),good_device_count=good,estimated_device_count=estimated,missing_device_count=len(expected)-len(readings),late_event_count=late_counts[(tx,start)],quality_flags=flags)
                payload['completeness'] = completeness(payload,valid,old)
                key = payload['window_id']
                # Revision changes only when selected samples change, not on replay.
                fingerprint = tuple(sorted((k,v['event_id']) for k,v in devices.items()))
                if emitted.get(key) == fingerprint: continue
                emitted[key] = fingerprint
                revision = revisions.get(key, -1) + 1
                revisions[key] = revision
                if revision: metrics['corrections'] += 1
                payload['revision'] = revision
                validate('LoadPayload',payload)
                results.append(dict(event_id=load_id(key,revision),payload=payload))
                topics.append('grid.transformer-load.v1')
        elif op == 'command':
            event, now = step['event'], timestamp(step['now'])
            p = event['payload']; d = p['device_id']; c = p['command_id']
            validate('Command',event,key=d)
            # A known command returns its recorded outcome even after expiry or
            # epoch change; it never re-applies the physical effect.
            if c in applied: outcome = 'ALREADY_APPLIED'
            elif p['leader_epoch'] < epochs.get(d,0): outcome = 'REJECTED'
            elif now >= timestamp(p['expires_at']): outcome = 'EXPIRED'
            else:
                outcome = 'APPLIED'; epochs[d] = p['leader_epoch']; rates[d] = p['target_rate_kw']; applied[c] = p['target_rate_kw']
            commands.append(dict(command_id=c,outcome=outcome,applied_rate_kw=rates.get(d,7.2)))
            topics.append('grid.command-result.v1')
        elif op == 'project':
            p = step['load']
            key = p['window_id']
            if p['revision'] > history.get(key,-1): history[key] = p['revision']
        elif op == 'checkpoint_replay':
            # Re-deliver committed output without re-computing IDs.
            results.append(results[step['index']])
            topics.append('grid.transformer-load.v1')
        else:
            raise AssertionError(op)
    return dict(topics=topics,loads=results,metrics=metrics,commands=commands,projection=history,epochs=epochs)
