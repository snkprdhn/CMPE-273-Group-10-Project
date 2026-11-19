# Schemas

**Status: contracts documented; machine-readable schemas not implemented.**

This directory will hold versioned JSON Schemas and valid/invalid example fixtures for Kafka events. During implementation, CI will validate every fixture and reject incompatible contract changes; no runtime schema-registry service is required. Protocol Buffers are used separately for gRPC contracts. Until executable schemas exist, the canonical definitions are in [EVENT_CONTRACTS.md](../docs/contracts/EVENT_CONTRACTS.md).
