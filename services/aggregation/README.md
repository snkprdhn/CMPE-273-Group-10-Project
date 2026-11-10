# Aggregation Service

**Status: design only; implementation not started.**

Planned home of the Flink/PyFlink job that validates telemetry, keys it by transformer, applies event-time windows and watermarks, checkpoints state, and publishes `grid.transformer-load.v1`. The Apache Flink Kubernetes Operator manages job lifecycle; this directory owns the grid aggregation logic and its tests, not a custom controller. It does not own control policy. See the [data-flow design](../../docs/design/DATA_FLOW.md), [open-source reuse plan](../../docs/design/OPEN_SOURCE_STACK.md), and [Member_2's work plan](../../docs/team/02_MEMBER_2_STREAMING.md).
