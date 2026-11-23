CREATE TABLE IF NOT EXISTS projection_migrations(version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS projection_meta (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    generation text NOT NULL, sequence bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS projection_events (
    event_id text PRIMARY KEY, topic text NOT NULL, event jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_current (
    kind text NOT NULL, id text NOT NULL, ordering jsonb NOT NULL,
    event jsonb NOT NULL, PRIMARY KEY(kind,id)
);
CREATE TABLE IF NOT EXISTS projection_context (
    kind text NOT NULL, id text NOT NULL, value jsonb NOT NULL, PRIMARY KEY(kind,id)
);
CREATE TABLE IF NOT EXISTS projection_offsets (
    topic text NOT NULL, partition integer NOT NULL, offset_value bigint NOT NULL,
    PRIMARY KEY(topic,partition)
);
CREATE TABLE IF NOT EXISTS projection_stream (
    sequence bigint PRIMARY KEY, created_at timestamptz NOT NULL, event jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_trace (
    sequence bigint PRIMARY KEY REFERENCES projection_stream(sequence) ON DELETE CASCADE,
    context jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_pages (
    token text PRIMARY KEY, scope text NOT NULL, created_at timestamptz NOT NULL,
    response jsonb NOT NULL, cursor text NOT NULL
);
CREATE TABLE IF NOT EXISTS projection_operations (
    actor text NOT NULL, key text NOT NULL, fingerprint jsonb NOT NULL,
    status integer NOT NULL, response jsonb NOT NULL, context jsonb NOT NULL,
    PRIMARY KEY(actor,key)
);
INSERT INTO projection_migrations VALUES (1) ON CONFLICT DO NOTHING;
