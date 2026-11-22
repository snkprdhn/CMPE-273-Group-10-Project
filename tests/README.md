# Test strategy

Preparation and executable contract checks are implemented. Application/component and infrastructure behavior remain future gates.

- `make contracts`: canonical schema/OpenAPI/Protobuf validation, valid/invalid fixtures with exact failure reasons, deterministic stream/control/agent golden cases, generated capability consistency and deliberate compatibility mutations. See [the bundle guide](../schemas/README.md).
- `make contracts BASE_REF=HEAD`: also compare the old contracts from Git; the first introduction explicitly reports an absent executable baseline.
- `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py`: ten local documentation/environment checks, requiring the prepared Mac ARM64 environment. These are separate from portable contract CI.

Every shared fixture has fixed expected results. Reference replay is an offline specification; it does not establish actual Kafka/Flink checkpoint recovery, persistent fencing, authentication, SSE transport, cluster persistence or workload performance. Future component tests must run the same fixtures against real behavior without another member’s unpublished branch. See [independent testing](../docs/testing/INDEPENDENT_COMPONENT_TESTING.md) and [acceptance](../docs/demo/ACCEPTANCE_PLAN.md).
