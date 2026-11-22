# Aggregation Service

**Status: design only; implementation not started.**

Planned home of the PyFlink job that validates telemetry, keys it by transformer, applies event-time windows and watermarks, checkpoints state, and publishes `grid.transformer-load.v1`. The Apache Flink Kubernetes Operator manages job lifecycle; this directory owns the grid aggregation logic and its tests, not a custom controller. It does not own control policy or write PostgreSQL. It is tested independently with telemetry fixtures and golden load outputs. See the [data-flow design](../../docs/design/DATA_FLOW.md), [independent testing plan](../../docs/testing/INDEPENDENT_COMPONENT_TESTING.md), and [Member_2's work plan](../../docs/team/02_MEMBER_2_STREAMING.md).
