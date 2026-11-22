# Member_3 Action domain and distributed delivery

Implemented: deterministic policy/allocation and the PostgreSQL durable saga using
versioned [load/result scenarios](fixtures/scenarios.json) and a [fake Registry](fixtures.py).
Member_1 retains simulation/Registry/observability; Member_2 Kafka/PyFlink;
Member_3 control/PostgreSQL/Debezium; Member_4 projection/API/UI/identity/Copilot.
The standalone domain profile requires no live simulator, aggregation job, Kafka,
model or Registry. The completed outbox/fencing profile additionally uses local
Kafka Connect and the kind API; see the [delivery runbook](../infra/member3/README.md)
and [ADR-029](../docs/decisions/ADR-029-outbox-and-fencing.md).

## Run and inspect

From the repository root, create the independent environment (Python 3.12.9):

```sh
.venv/bin/python -m venv .tools/member3-venv
.tools/member3-venv/bin/python -m pip install --require-hashes -r member3/requirements.txt
```

The lock reuses accepted versions/hashes; offline installation can add
`--no-index --find-links .tools/member1-wheels-macos` when those cached wheels exist.
Only PostgreSQL is required. If it is not already running, the following is a
**planned convenience command**, not a new deployment acceptance claim:

```sh
docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml up -d --build --wait postgres
```

Use the existing disposable local control role, never Registry/projection credentials:

```sh
export CONTROL_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control
MEMBER3_TEST_DSN="$CONTROL_DSN" make member3-test
.tools/member3-venv/bin/python -m member3 cases
.tools/member3-venv/bin/python -m member3 replay --schema member3_demo --case 'safe restore and restart'
.tools/member3-venv/bin/python -m member3 inspect --schema member3_demo
```

The replay prints transformer state, actions, commands, complete ordered outbox
rows and inbox count. `inspect` starts a new process reading committed state.
Select a **new schema name** to repeat a demo: replay refuses nonempty state;
it never truncates existing history. Tests create/drop only their random
`test_action_*` schemas. The demo schema persists for inspection. Tables use the
`action_` prefix; [state.sql](state.sql) is additive/idempotent. `migrate` applies
it without replay; `tick --at 2026-11-22T18:30:30Z` explicitly evaluates deadlines.
Without `--schema`, commands use the DSN's search path.

`make member3-test` without `MEMBER3_TEST_DSN` runs two pure checks and explicitly
skips database acceptance; that result is insufficient to accept this milestone.

## Behavior and safety decisions

- Frozen ADR-025 defaults: warning ≥80%, critical ≥95%; target reduction to 75%;
  cooldown 30s from plan creation; two distinct consecutive fresh COMPLETE
  five-second windows at/below 75% before restore; TTL 10s, acknowledgement 5s.
  Exactly 15 seconds old is fresh. Future windows, incomplete/missing capacity,
  missing charger observations and stale capabilities cannot justify control.
- Registry v1 has **capabilities only**, not live rates or session/online state.
  Fake snapshots contain a canonical `device` plus explicit `telemetry` with
  `observed_at`, `current_rate_kw`, `online`, `charging`. Both timestamps must be
  fresh and not in the future. Never substitute maximum capability for current
  load. A live authoritative observation adapter remains an integration gate.
- Priority 0 first, absent departure least urgent, latest departure next, lexical
  ID last; departure ≤5 minutes away is excluded. Reduction respects minimum
  rate. Insufficient capacity creates an audited review/hold plan and **no
  commands**; it does not silently claim adequate reduction. Reductions below
  the numerical allocation resolution also hold instead of dispatching an empty plan.
- Each load revision enters the inbox atomically with state/outbox. Lower revisions
  and old windows cannot create a second plan. Corrections retain previous history;
  historical corrections conservatively reset recovery evidence. A correction
  cannot count as another recovery window. One active saga per transformer is a
  database constraint (stronger than one per policy epoch).
- Policies use expected versions and pin their full accepted policy into each
  plan. Actions use optimistic database versions; result callers may supply
  `expected_version`. Every result/state transition has an ordered Action event.
  IDs are UUIDv5: `smart-grid/v1/action/{transformer}/{window}/{policy_version}`,
  `smart-grid/v1/command/{action}/{device}/{THROTTLE|RESTORE}` and
  `smart-grid/v1/action-event/{action}/{version}` in NAMESPACE_URL.
- Every input and outbox payload is validated against the canonical schema;
  loads also enforce identity, totals, utilization, window and quality invariants.
  A reused event ID with different content conflicts. Results must match known
  action/device/command identities and requested rates; mismatched applied rates
  fail the command. Duplicate logical results cannot change its recorded outcome.
- Domain updates, inbox, commands and action/command outbox rows commit in one
  transaction. The small standalone store serializes transactions through a
  PostgreSQL advisory lock; optimistic versions and uniqueness remain enforced.
  Registry failure or any write failure rolls the transaction back for safe replay.
- `COMMANDS_DISPATCHED` means committed to the outbox, **delivery unconfirmed**.
  Publication is provided by the separate CDC profile. Router columns are `id`, `aggregatetype`
  (canonical topic), `aggregateid` (transformer/device key), `type`, `payload`.
  Debezium uses that stored topic/key and unwrapped payload; its header ID is
  physical, while payload event_id is the stable logical identity.
- There is at most **one retransmission** (two attempts total) within the original
  TTL, only while results are unknown and the command epoch matches. A retry has
  a new physical outbox delivery ID but the exact same logical event/command,
  target, issued time and expiry. Timeout is unknown, never successful/zero load.
  Exhaustion/partial failure preserves limits in NEEDS_OPERATOR_REVIEW. Late
  results update evidence but cannot automatically exit review or restore. Result
  handling checks exhausted deadlines itself, including after process downtime
  when no timer ran. Corrected windows cannot clear the visible review hold.
- Confirmed throttle results enter recovery monitoring. Recovery windows must
  begin after confirmation; missing/stale windows reset the count. Restore also
  requires fresh matching Registry capabilities/current rates and projected load
  after full restore **strictly below critical**, preventing immediate rebound.
  Restore to minimum/zero-rate chargers is supported. Completion requires every
  restore acknowledgement; restore timeout/partial failure returns to safe hold.

## Acceptance boundary

The tests cover all 14 fixed scenarios with expected transition sequences,
command targets, outbox counts and logical identities, plus concurrent delivery,
atomic rollback after an outbox insert, version conflicts, safety boundaries,
restore failures, cooldown, deadline recovery after downtime, corrected review
hold, extreme finite loads and separate-process recovery. The reviewed suite has
42 passing tests against real PostgreSQL. See the exact executed
results in [implementation status](../docs/implementation/IMPLEMENTATION_STATUS.md).

The distributed profile now acquires Kubernetes Leases, allocates durable epochs,
checks authority before/after every control transaction and holds old active
sagas on restart. Once authority exists, supplied-epoch standalone callers cannot
bypass it. Debezium publishes committed outbox rows; the fenced Member_1 gateway
commits authorization/effect/dedup/result outbox in one PostgreSQL transaction.
See [delivery runbook](../infra/member3/README.md) for executed failure checks and
local commands. The SQLite standalone simulator retains its narrower guarantee.

Live observation acquisition, authenticated operator reconciliation/compensation,
full kind workload packaging, metrics, backup/export and database-server restart
acceptance remain future milestones. Reviewed sagas deliberately remain held
until that explicit operator workflow exists. No cloud/model calls are needed.
