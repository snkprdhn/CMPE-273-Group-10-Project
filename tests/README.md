# Test Strategy

**Status: preparation documentation/environment smoke checks implemented; application/contract behavior tests not implemented.**

Run `.venv/bin/python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py`. Ten checks cover documentation links and shell syntax (including negative cases), isolated Python 3.12.9, selected native/Python tools, and valid/invalid JSON Schema behavior. The environment suite requires the prepared macOS ARM64 venv and makes no network calls. These tests do not establish application behavior, cluster persistence, or full deployment correctness.

Planned suites include unit tests, contract validation, independently runnable component profiles, pairwise integration gates, end-to-end event flows, restart/failure tests, load tests, and Playwright Python UI acceptance tests. Every shared fixture must have an expected result, and performance reports must name the machine and configuration used. No component test may require another member's unpublished branch or manually running service. See the [independent testing plan](../docs/testing/INDEPENDENT_COMPONENT_TESTING.md) and [acceptance plan](../docs/demo/ACCEPTANCE_PLAN.md).
