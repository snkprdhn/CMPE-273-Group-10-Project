# Implemented local architecture

Both kind profiles run on one Docker Desktop node with Calico. The dev profile runs one Kafka broker. Review uses three combined KRaft brokers on the same node and adds application replicas. Flink runs one aggregation task in either profile. PostgreSQL uses separate databases and roles, with a narrow fenced gateway in the control database.

```mermaid
flowchart LR
    browser[Operator browser]
    subgraph local[Docker Desktop · kind · Calico]
        direction LR
        sim[Member_1 · Simulator Job]
        reg[Member_1 · gRPC Registry]
        kafka[(Member_2 · Strimzi Kafka)]
        flink[Member_2 · PyFlink]
        cp[(Checkpoint PVC)]
        action[Member_3 · Action workers]
        lease[Kubernetes Lease]
        db[(Member_3 · PostgreSQL primary)]
        cdc[Member_3 · Debezium Connect]
        gateway[Member_1 · Fenced simulated gateway]
        projection[Member_4 · Projection consumer]
        api[Member_4 · FastAPI REST/SSE]
        ui[Member_4 · NiceGUI BFF]
        oidc[Member_4 · Keycloak]
        copilot[Member_4 · Copilot / read-only tools]
        mock[Deterministic mock]
        audit[(Copilot SQLite PVC)]
        signals[Member_1 · Collector / Prometheus / Tempo / Grafana]
        sim -->|register and heartbeat| reg
        sim -->|telemetry| kafka
        kafka --> flink
        flink -->|checkpoint| cp
        flink -->|committed load| kafka
        kafka -->|loads and results| action
        action --> reg
        action --> lease
        action -->|control and outbox| db
        reg -->|Registry database| db
        db -->|pgoutput outbox| cdc
        cdc -->|alert, action, command| kafka
        kafka -->|command| gateway
        gateway -->|atomic simulated effect| db
        gateway -->|result and subsequent telemetry| kafka
        kafka -->|read committed| projection
        projection -->|journal and positions| db
        api -->|projections| db
        ui -->|REST and SSE| api
        ui <-->|OIDC| oidc
        oidc -->|identity database| db
        ui --> copilot
        copilot -->|bounded evidence| api
        copilot --> mock
        copilot --> audit
        action -.-> signals
        api -.-> signals
        flink -.-> signals
    end
    browser -->|loopback only| ui
```

```mermaid
sequenceDiagram
    participant Edge as Simulator and fenced gateway
    participant Kafka
    participant Flink
    participant Action
    participant SQL as PostgreSQL
    participant CDC as Debezium
    participant View as Projection / API
    Edge->>Kafka: EV, meter and solar events keyed by transformer
    Kafka->>Flink: Event-time aggregation
    Flink->>Kafka: Commit transformer load through checkpoint
    Kafka->>Action: Critical complete load
    Action->>SQL: Commit inbox, saga and outbox under durable epoch
    SQL->>CDC: Capture committed outbox
    CDC->>Kafka: Publish stable logical command and action IDs
    Kafka->>Edge: Deliver throttle command
    Edge->>SQL: Authorize epoch, apply simulated rate and record result
    Edge->>Kafka: Publish result and measured demand
    Kafka->>View: Commit journal, views, source positions and SSE event
    Note over Edge,Action: Safe subsequent windows permit restore through the same fenced path
```

## Contribution and presentation mapping

These labels identify ownership on the shared implementation computer.

| Owner | Source and deployment | Evidence to present |
|---|---|---|
| Member_1 | [Registry and simulator](../../member1/README.md), [observability](../../infra/observability/README.md) | Registered edge inputs, measured rate reduction, duplicate-safe effects, incident metric/trace/log packet |
| Member_2 | [Aggregation](../../member2/README.md), Kafka/Topic/Flink resources in [kind](../../infra/kind/README.md) | Fixture totals, event-time revisions, broker profile, checkpoint and TaskManager recovery |
| Member_3 | [Action](../../member3/README.md), PostgreSQL, CDC, Lease/RBAC, fenced gateway transaction | Action and command IDs, stale-epoch rejection evidence, outbox delivery, logical backup/restore |
| Member_4 | [Operations API](../../member4/README.md), [UI](../../dashboard/README.md), [identity](../../identity/README.md), [Investigator](../../services/copilot/README.md) | Replay equality, REST/SSE, live Viewer and Operator sessions, cited evidence and role denial |
