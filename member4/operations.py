"""Contract mocks for Member_1/3 boundaries; never access their stores."""
from uuid import NAMESPACE_URL, uuid5

from psycopg.types.json import Jsonb

from member4.contracts import ApiError, stamp, validate


class ContractMocks:
    """Transactional deterministic standalone replacements, explicitly not live adapters."""
    def __init__(self, failure=None):
        if failure not in (None, 'conflict', 'unavailable', 'timeout'):
            raise ValueError('unknown mock failure')
        self.failure = failure

    def call(self, db, operation, entity, body, context, now):
        for field in ('actor', 'request_id', 'idempotency_key'):
            validate('Id', context[field])
        validate('Role', context['role'])
        if context['service_identity'] != 'member4.api.mock' or context['deadline_ms'] != 3000:
            raise ApiError(503, 'Invalid internal call context')
        if self.failure:
            code = {'conflict': 409, 'unavailable': 503, 'timeout': 504}[self.failure]
            raise ApiError(code, 'Contract mock injected ' + self.failure)
        if operation == 'policy':
            validate('PolicyWrite', body)
            p = body['policy']
            if not (p['recovery_pct'] < p['warning_pct'] < p['critical_pct'] and
                    p['ack_timeout_seconds'] <= p['command_ttl_seconds'] and
                    p['version'] == body['expected_version'] + 1):
                raise ApiError(422, 'Invalid policy thresholds, deadlines or version')
            row = db.execute("SELECT value FROM projection_context WHERE kind='mock_policy' AND id=%s",
                             (entity,)).fetchone()
            if row is None:
                row = db.execute("SELECT value->'policy' FROM projection_context "
                                 "WHERE kind='transformers' AND id=%s", (entity,)).fetchone()
            if row is None:
                raise ApiError(404, 'Transformer not found')
            if row[0]['version'] != body['expected_version']:
                raise ApiError(409, 'Policy version conflict')
            db.execute("INSERT INTO projection_context VALUES('mock_policy',%s,%s) ON CONFLICT(kind,id) "
                       'DO UPDATE SET value=excluded.value', (entity, Jsonb(p)))
            return 200, p
        if operation == 'start':
            validate('ScenarioStart', body)
            if db.execute("SELECT 1 FROM projection_context WHERE kind='mock_scenario' "
                          "AND value->>'status'='RUNNING'").fetchone():
                raise ApiError(409, 'A scenario is already running')
            run = dict(run_id=str(uuid5(NAMESPACE_URL, context['actor'] + '/' + context['idempotency_key'])),
                       status='RUNNING', configuration=body, updated_at=stamp(now))
            db.execute("INSERT INTO projection_context VALUES('mock_scenario',%s,%s)",
                       (run['run_id'], Jsonb(run)))
            return 202, run
        if operation == 'stop':
            row = db.execute("SELECT value FROM projection_context WHERE kind='mock_scenario' AND id=%s",
                             (entity,)).fetchone()
            if row is None:
                raise ApiError(404, 'Scenario run not found')
            run = row[0]
            if run['status'] == 'RUNNING':
                run.update(status='STOPPED', updated_at=stamp(now))
                db.execute("UPDATE projection_context SET value=%s WHERE kind='mock_scenario' AND id=%s",
                           (Jsonb(run), entity))
            return 200, run
        raise ApiError(503, 'Unsupported internal operation')
