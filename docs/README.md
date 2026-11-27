# Documentation

Start with the [master guide](MASTER_GUIDE.md). It covers the problem, architecture, data flow, design decisions, the hosted demo, operations, the demo script and the limits.

| Folder | Contents |
|---|---|
| [contracts](contracts/EVENT_CONTRACTS.md) | Event, API, gRPC, agent-tool and stream-semantics contracts. The executable form is in [schemas](../schemas/README.md). `make contracts` validates it. |
| [decisions](decisions/README.md) | Architecture decision records and the decision register |
| [design](design/DATA_MODEL.md) | Deep dives: [data model](design/DATA_MODEL.md), [UI design](design/UI_DESIGN.md), [Grid Incident Investigator](design/AGENTIC_COPILOT.md) |
| [implementation](implementation/IMPLEMENTATION_STATUS.md) | [Status and executed evidence](implementation/IMPLEMENTATION_STATUS.md), [version matrix](implementation/VERSION_MATRIX.md), [local prerequisites](implementation/LOCAL_PREREQUISITES.md) |
| [testing](testing/LOAD_TEST_REPORT.md) | [Measured local load](testing/LOAD_TEST_REPORT.md), [agent evaluation plan](testing/AGENT_EVALUATION_PLAN.md) |

Runbooks live next to the code: [AKS](../infra/aks/AGENT_RUNBOOK.md), [kind](../infra/kind/README.md), [observability](../infra/observability/README.md), [tests](../tests/README.md), and a README in each component package.

## Rules

1. Describe planned behavior as planned until it is implemented and verified.
2. Keep field names and API paths in the contract documents. Do not copy competing definitions into READMEs.
3. Record significant architecture changes as ADRs in `decisions/`.
4. Record exact commands, environment and results in the status document when a check is run. Label commands that were not run as planned.
5. Update diagrams and acceptance criteria in the same change as the design.
