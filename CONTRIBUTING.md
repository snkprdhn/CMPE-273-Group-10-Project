# Contributing

## Before implementation

Read the root README, documentation index, team overview, event contract, and API contract. Close the required architecture decisions before creating incompatible component assumptions.

## Workflow

1. Create a focused branch.
2. Keep changes inside the assigned workstream where possible.
3. Update documentation and tests with behavior changes.
4. Open a pull request with purpose, contract impact, verification, and screenshots for UI work.
5. Obtain review from at least one teammate; contract changes require producer and consumer review.

## Commit and review expectations

- Do not commit secrets, generated data, database volumes, or dependency caches.
- Do not describe planned behavior as implemented.
- Keep shared event/API names consistent with `docs/contracts/`.
- Treat agent tool, evidence, and output schemas as shared contracts; model prompts cannot bypass their authorization or bounds.
- Record architecture changes in `docs/decisions/`.
- Reuse the approved projects in `docs/design/OPEN_SOURCE_STACK.md`; adding overlapping infrastructure or custom platform plumbing requires an ADR.
- Pin container images, charts, operators, and application dependencies. Do not use `latest` tags.
- Preserve required license notices and update the software bill of materials when dependencies change.
- Add commands to component READMEs only after another teammate reproduces them.
- Prefer small pull requests with a clear owner and acceptance criterion.
