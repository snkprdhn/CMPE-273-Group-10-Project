# ADR-002 — Python-authored stream processing

- Status: Accepted, 2026-11-21
- Authority: User-approved Python application code with containerized infrastructure

## Context

The design left PyFlink versus Java unresolved. The host is Apple M5/ARM64 with 16 GiB RAM. The user requires Python application code and no host Java or Node.js development dependency. Kafka/Flink JVM internals inside containers are explicitly permitted.

## Decision

Use the PyFlink DataStream API, Flink/PyFlink **2.2.1**, Python **3.12.9**, and the Flink Kafka connector **5.0.0-2.2**. Run Flink-aware tests and job submission inside the same project-built container used for deployment. Keep pure aggregation/domain tests independent of a running cluster. Preserve every rule in [Stream Semantics v1](../contracts/STREAM_SEMANTICS_V1.md), including revisions, quality, deduplication, and late audit events.

[Flink's Python installation requirements](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/installation/) include Python 3.12. The earlier Flink 1.20/2.0 documentation inspected during preparation listed only Python through 3.11; that restriction must not be generalized to 2.2. The [Apache connector compatibility listing](https://flink.apache.org/downloads/#apache-flink-kafka-connector-500) supports 2.2.x with connector 5.0.0. [Operator 1.15.0](https://flink.apache.org/downloads/) supports Flink 2.2.x.

Use native linux/arm64 containers. The [PyPI release file listing](https://pypi.org/project/apache-flink/2.2.1/#files) contains macOS ARM64 wheels and Linux x86_64 wheels, but no Linux ARM64 wheel. Therefore the future image build must build the PyFlink wheel from its published source distribution inside an ARM64 build stage, with build tools confined to that stage. This is an accepted build strategy, **not a verified build**. Do not assume a macOS wheel works in Linux or silently switch to emulation. The first aggregation image gate must install dependencies, import PyFlink, and run a bounded source-to-sink/checkpoint test on ARM64. Record wheel hashes and the resolved dependency lock then.

## Alternatives and consequences

- Java would simplify access to Flink's native test harnesses, but violates the approved application-language constraint.
- Replacing Kafka/Flink with a Python queue/worker would change required partitioning, event-time, state, and recovery demonstrations; rejected as a scope redesign.
- Whole-stack x86_64 emulation adds CPU/memory overhead on this laptop; rejected as the default.
- Python adds a worker process, packaging work, and JVM/Python serialization overhead. Bound simulator scale and test the real job; pure Python tests alone do not establish checkpoint correctness.

There is no host JDK or Maven prerequisite. The container's Flink distribution supplies its own Java runtime; the image build may use upstream Java build tooling without introducing project Java source. Library JARs are downloaded upstream artifacts, not custom services. See the [matrix](../implementation/VERSION_MATRIX.md).
