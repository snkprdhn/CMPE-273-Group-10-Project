# Test Strategy

**Status: design only; tests not implemented.**

Planned suites include unit tests, contract validation, independently runnable component profiles, pairwise integration gates, end-to-end event flows, restart/failure tests, load tests, and Playwright UI acceptance tests. Every shared fixture must have an expected result, and performance reports must name the machine and configuration used. No component test may require another member's unpublished branch or manually running service. See the [independent testing plan](../docs/testing/INDEPENDENT_COMPONENT_TESTING.md) and [acceptance plan](../docs/demo/ACCEPTANCE_PLAN.md).
