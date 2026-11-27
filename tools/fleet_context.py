"""Projection context for the managed (hero) part of the fleet.

Prints the JSON that `python -m projection.live_context --context` expects: baseline Transformer
snapshots and the controllable charger IDs. Existing transformers are skipped because baselines are
immutable. Transformers outside the managed set stay visible through metrics only.

    .venv/bin/python -m tools.fleet_context --skip tx-001 > context.json
"""
import argparse
import json
from uuid import NAMESPACE_URL, uuid5

from devices.fleet import fleet

POLICY = dict(version=1, warning_pct=80, critical_pct=95, recovery_pct=75, cooldown_seconds=30,
              recovery_windows=2, ack_timeout_seconds=5, command_ttl_seconds=10)


def baseline(tx):
    devices = len(tx['devices'])
    return dict(
        transformer_id=tx['transformer_id'], state='STALE', freshness='STALE', data_age_seconds=0, policy=POLICY,
        load=dict(transformer_id=tx['transformer_id'], window_id=str(uuid5(NAMESPACE_URL, 'baseline/' + tx['transformer_id'])),
                  revision=0, window_start='2026-11-26T00:00:00Z', window_end='2026-11-26T00:00:05Z',
                  topology_version=f'fleet-{devices}', meter_demand_kw=0.0, ev_demand_kw=0.0, solar_generation_kw=0.0,
                  net_load_kw=0.0, capacity_kw=tx['capacity_kw'], utilization_pct=0.0, expected_device_count=devices,
                  good_device_count=0, estimated_device_count=0, missing_device_count=devices, late_event_count=0,
                  quality_flags=['MISSING_DEVICES'], completeness='STALE'))


def context(transformers=25, per=40, heroes=5, skip=()):
    topology, capabilities, managed = fleet(transformers, per, heroes)
    wanted = [t for t in topology['transformers'] if t['transformer_id'] in managed and t['transformer_id'] not in skip]
    return dict(transformers=[baseline(t) for t in wanted],
                device_ids=[c['device_id'] for c in capabilities if c['controllable']])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transformers', type=int, default=25)
    parser.add_argument('--devices', type=int, default=40)
    parser.add_argument('--heroes', type=int, default=5)
    parser.add_argument('--skip', nargs='*', default=[])
    parser.add_argument('--slice', default='0:200', help='START:STOP of the charger IDs; the Registry deadline allows about 25 per call')
    args = parser.parse_args()
    result = context(args.transformers, args.devices, args.heroes, args.skip)
    start, stop = (int(v) for v in args.slice.split(':'))
    result['device_ids'] = result['device_ids'][start:stop]
    print(json.dumps(result))


if __name__ == '__main__':
    main()
