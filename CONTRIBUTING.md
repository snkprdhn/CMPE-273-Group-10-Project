# Contributing

Read the [master guide](docs/MASTER_GUIDE.md), the [event contracts](docs/contracts/EVENT_CONTRACTS.md) and the [API contracts](docs/contracts/API_CONTRACTS.md) first.

## Workflow

1. Create a focused branch.
2. Update documentation and tests with every behavior change.
3. Open a pull request with the purpose, contract impact, verification and screenshots for UI work. Contract changes need producer and consumer review.

## Expectations

- Do not commit secrets, generated data, database volumes or dependency caches.
- Do not describe planned behavior as implemented. Label commands as unverified until they have run, and record the command, environment and result when they have.
- Keep shared event and API names consistent with `docs/contracts/`. Agent tool, evidence and output schemas are shared contracts. Model prompts cannot bypass their authorization or bounds.
- Test against versioned contracts and fixtures. Do not require another component's unpublished branch or running service.
- Do not import another service's internal package or read or write another service's database schema.
- Each component keeps its Kubernetes resources, security policy, instrumentation, dashboard panel and failure test.
- Record architecture changes in `docs/decisions/`. Adding overlapping infrastructure or custom platform plumbing needs an ADR.
- Pin container images, charts, operators and application dependencies. Do not use `latest` tags.
- Preserve required license notices and update the software bill of materials when dependencies change.
- Prefer small pull requests with one acceptance criterion.
