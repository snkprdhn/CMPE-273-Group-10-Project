"""Deterministic, session-local Flow Map playback for presentations."""
from datetime import timedelta

SCENARIOS = {
    'overload': 'Evening EV overload',
    'solar': 'Sudden solar drop',
    'steady': 'Steady demand',
}
INTERVAL = 4
PHASES = {
    'overload': ('Normal demand', 'EV charging surge', 'Throttle acknowledged',
                 'Measured reduction', 'Restore acknowledged', 'Measured restoration'),
    'solar': ('Normal solar output', 'Solar generation drops', 'Throttle acknowledged',
              'Measured reduction', 'Restore acknowledged', 'Measured restoration'),
    'steady': ('Normal demand', 'Household demand rises', 'Demand stabilizes'),
}


def frame(scenario, phase, started):
    """The control acknowledgement and the next measurement are separate frames."""
    phase = min(phase, len(PHASES[scenario]) - 1)
    at = started + timedelta(seconds=phase * INTERVAL)
    house = (10, 12, 11)[phase] if scenario == 'steady' else (
        (10, 16, 16, 16, 10, 10)[phase] if scenario == 'overload' else 15)
    solar = (5, 0, 0, 0, 5, 5)[phase] if scenario == 'solar' else 2
    ev = (4, 4, 4)[phase] if scenario == 'steady' else (
        (4, 7.2, 7.2, 2, 2, 7.2)[phase] if scenario == 'overload' else (7.2, 7.2, 7.2, 2, 2, 7.2)[phase])
    result = []
    if scenario != 'steady' and phase >= 2:
        result.append(dict(action_id='demo-action', outcome='APPLIED', previous_rate_kw=7.2,
                           applied_rate_kw=2, result_time=(started + timedelta(seconds=2 * INTERVAL)).isoformat()))
    if scenario != 'steady' and phase >= 4:
        result.append(dict(action_id='demo-action', outcome='APPLIED', previous_rate_kw=2,
                           applied_rate_kw=7.2, result_time=(started + timedelta(seconds=4 * INTERVAL)).isoformat()))
    tx = 'demo-tx'
    devices = [dict(device=dict(device_id=device, transformer_id=tx, device_type=kind),
                    freshness='FRESH', latest_telemetry=dict(power_kw=power, quality='GOOD'),
                    command_history=result if kind == 'EV_CHARGER' else [])
               for device, kind, power in [('demo-meter', 'SMART_METER', house),
                                            ('demo-solar', 'SOLAR', -solar), ('demo-ev', 'EV_CHARGER', ev)]]
    net = house + ev - solar
    transformer = dict(transformer_id=tx, state='CRITICAL' if net > 20 else 'NORMAL', freshness='FRESH',
                       load=dict(meter_demand_kw=house, solar_generation_kw=solar, ev_demand_kw=ev,
                                 net_load_kw=net, capacity_kw=20, window_end=at.strftime('%Y-%m-%dT%H:%M:%S')))
    actions = [{'action_id': 'demo-action', 'status': 'ACKS_PENDING' if phase < 3 else
                'REDUCTION_CONFIRMED' if phase < 4 else 'RESTORE_DISPATCHED' if phase < 5 else 'COMPLETED'}] if result else []
    return transformer, devices, actions
