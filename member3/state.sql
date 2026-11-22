-- Member_3 only. Apply with the control role; never another service's credentials.
CREATE TABLE IF NOT EXISTS action_transformers (
    id TEXT PRIMARY KEY, version BIGINT NOT NULL, state JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS action_inbox (
    consumer TEXT NOT NULL, id UUID NOT NULL, event JSONB NOT NULL,
    PRIMARY KEY (consumer, id)
);
CREATE TABLE IF NOT EXISTS action_windows (
    transformer_id TEXT NOT NULL REFERENCES action_transformers(id),
    window_id UUID NOT NULL, revision BIGINT NOT NULL,
    PRIMARY KEY (transformer_id, window_id)
);
CREATE TABLE IF NOT EXISTS action_plans (
    id UUID PRIMARY KEY, transformer_id TEXT NOT NULL REFERENCES action_transformers(id),
    window_id UUID NOT NULL, active BOOLEAN NOT NULL, version BIGINT NOT NULL,
    state JSONB NOT NULL, UNIQUE (transformer_id, window_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS action_one_active ON action_plans(transformer_id) WHERE active;
CREATE TABLE IF NOT EXISTS action_commands (
    id UUID PRIMARY KEY, action_id UUID NOT NULL REFERENCES action_plans(id),
    device_id TEXT NOT NULL, command_type TEXT NOT NULL, state JSONB NOT NULL,
    UNIQUE (action_id, device_id, command_type)
);
-- Outbox Event Router-compatible columns. Repeated physical sends retain payload.event_id.
-- The row id is a delivery identity; event_id in payload is the logical event identity.
CREATE TABLE IF NOT EXISTS action_outbox (
    sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id UUID PRIMARY KEY, aggregatetype TEXT NOT NULL, aggregateid TEXT NOT NULL,
    type TEXT NOT NULL, payload JSONB NOT NULL
);
