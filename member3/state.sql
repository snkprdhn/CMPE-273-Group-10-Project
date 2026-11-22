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
-- One local control shard. Never truncate/reset while command history survives.
CREATE TABLE IF NOT EXISTS action_authority (
    shard INTEGER PRIMARY KEY CHECK (shard=1), epoch BIGINT NOT NULL CHECK (epoch>0),
    lease_uid TEXT NOT NULL, generation BIGINT NOT NULL, holder TEXT NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL
);
-- Narrow gateway service interface. Grant EXECUTE to a gateway role, never
-- SELECT on Control tables. Search path is pinned to the installing schema.
CREATE OR REPLACE FUNCTION authorize_command(command_id UUID, requested_epoch BIGINT, envelope JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(273003);
    RETURN EXISTS (
        SELECT 1 FROM action_authority a, action_commands c
        WHERE a.shard=1 AND a.epoch=requested_epoch AND a.valid_until > clock_timestamp()
          AND c.id=command_id AND c.state->'event'=envelope
    );
END;
$$;
REVOKE ALL ON FUNCTION authorize_command(UUID,BIGINT,JSONB) FROM PUBLIC;
