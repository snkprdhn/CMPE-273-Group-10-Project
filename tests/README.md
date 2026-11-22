# Test strategy

Preparation and executable contract checks are implemented. Member_1, local infrastructure and Member_2 standalone behavior are verified; other components and full platform behavior remain future gates.

- `make contracts`: canonical schema/OpenAPI/Protobuf validation, valid/invalid fixtures with exact failure reasons, deterministic stream/control/agent golden cases, generated capability consistency and deliberate compatibility mutations. See [the bundle guide](../schemas/README.md).
- `make contracts BASE_REF=HEAD`: also compare the old contracts from Git; the first introduction explicitly reports an absent executable baseline.
- `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py`: ten local documentation/environment checks, requiring the prepared Mac ARM64 environment. These are separate from portable contract CI.

Every shared fixture has fixed expected results. Reference replay is an offline specification; it does not establish actual Kafka/Flink checkpoint recovery, persistent fencing, authentication, SSE transport, cluster persistence or workload performance. Future component tests must run the same fixtures against real behavior without another member’s unpublished branch. See [independent testing](../docs/testing/INDEPENDENT_COMPONENT_TESTING.md) and [acceptance](../docs/demo/ACCEPTANCE_PLAN.md).

- `make member2-test`: production aggregation engine against frozen stream golden cases and failure/replay boundaries.
- `make member2-build`; `make member2-live`: hash-locked native image and real Kafka/Flink TaskManager SIGKILL with an open Kafka transaction. Committed revisions remain 0/1 at 15/17 kW. See [Member_2 runbook](../member2/README.md).

- `MEMBER3_TEST_DSN=postgresql://control:control_disposable@127.0.0.1:15432/local_dev_control make member3-test`: real PostgreSQL Action domain acceptance using isolated disposable schemas, fixed load/result fixtures and a fake Registry; no Kafka, simulator or Flink dependency. Omitting the DSN skips database tests explicitly. See [Member_3](../member3/README.md).
