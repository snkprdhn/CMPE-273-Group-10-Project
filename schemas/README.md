# Executable contract bundle v1

The canonical wire definitions are [bundle.json](v1/bundle.json), [registry.proto](v1/registry.proto), and [openapi.json](v1/openapi.json). Approved prose describes intent; [ADR-025](../docs/decisions/ADR-025-executable-contract-freeze.md) resolves exact defaults and ambiguous boundaries. No runtime schema registry or application services are needed.

| Contract | Canonical location |
|---|---|
| All eight Kafka topics | `bundle.json`: `x-events` maps topic to `$defs` name; event metadata defines key and units |
| Topology/capabilities | `$defs/Topology`; Protobuf Device generates `$defs/Device` and `DeviceType` |
| REST/internal operations and roles | OpenAPI paths; bodies reference `bundle.json#/$defs/...` |
| SSE event union | `$defs/SseEvent`; OpenAPI text/event-stream responses document framing/replay |
| Seven model tool calls | `$defs/ToolCall`, tool-specific `Arguments`/`Call` definitions |
| Server-owned context, evidence and errors | `$defs/ToolContext`, `Evidence`, `ToolResult`, `ToolError`, `HttpError` |
| Structured agent result | `$defs/AgentDraft`, `Claim`, `Investigation` |

Run from the repository root with the prepared Python 3.12.9 venv. The separate contract environment avoids mixing compiler/Protobuf dependencies into future PyFlink environments:

```sh
.venv/bin/python -m venv .tools/contracts-venv
.tools/contracts-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r requirements-contracts.txt
make contracts
make bindings
make contracts BASE_REF=HEAD
```

`make contracts` checks schemas, OpenAPI, canonical generated capability shapes, compiles Protobuf, parses and binary-round-trips Registry fixtures, validates every valid/invalid example, replays golden scenarios, and tests deliberately incompatible changes. `make bindings` also writes Python bindings/descriptors to ignored `.tools/generated`; generated artifacts are not hand-maintained. A different venv can be selected with `CONTRACT_PYTHON=/path/to/venv/bin/python`.

[Valid examples](v1/examples.valid.json) cover every named JSON definition; [invalid examples](v1/examples.invalid.json) declare the precise validation keyword/path or domain error code. [Registry examples](v1/registry.examples.json) use ProtoJSON and the generated messages. [Stream/control golden scenarios](v1/golden.scenarios.json) explicitly contain topology, ordered inputs, expected topic order, full load payloads/IDs, command results, projection state, epochs, and metrics. [Agent scenarios](v1/agent.scenarios.json) cover authorized evidence and entity/role/deadline/trace-scope failures. Fixture outputs are fixed data, not generated from the implementation being tested. Their [offline reference](../tests/contracts/reference.py) establishes deterministic semantics for later component implementations.

JSON Schema handles structure, formats, enums and bounds. [tools/contracts.py](../tools/contracts.py) adds cross-field, partition-key, finite-number, window/count/unit, time, scope, and evidence checks. Event consumers may ignore additive fields; model inputs/context are strict. Schema-valid `quality: INVALID` telemetry still fails domain validation and routes to DLQ. Missing/stale data, invalid input, and very-late audit are different outcomes.

The compatibility command reads the old definitions directly from Git. It fails closed on existing field/type/required/enum/key/unit changes, RPC descriptor changes, REST removals or access changes; optional ordinary JSON properties are allowed. Tool argument/context additions require review. Semantic changes still require review and a new major version when incompatible. On the first introduction, the command reports the absent baseline and still runs the complete suite. CI uses standard Ubuntu GitHub runners; host-specific preparation tests are deliberately separate.

The reference suite is contract evidence only. Real broker ordering, checkpoints, crash recovery, persistent dedup/fencing, Registry authorization/deadlines, SSE delivery and OIDC require component/integration evidence later.
