-- Migration 002: Create telemetry_logs hypertable
-- Requirement 13: Time-Series Storage
-- Stores all telemetry events partitioned by timestamp for historical querying

CREATE TABLE telemetry_logs (
    timestamp TIMESTAMPTZ NOT NULL,
    log_id UUID DEFAULT uuid_generate_v4(),
    agent_id UUID NOT NULL,
    org_id UUID NOT NULL,
    prompt_tokens INT NOT NULL,
    completion_tokens INT NOT NULL,
    total_cost NUMERIC(10, 6) NOT NULL,
    latency_ms INT NOT NULL,
    tool_name TEXT,
    prompt_hash TEXT,
    session_id UUID,
    batch_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable with 1-hour chunk intervals for optimal query performance
SELECT create_hypertable('telemetry_logs', 'timestamp', chunk_time_interval => INTERVAL '1 hour');

-- Indexes for common query patterns
CREATE INDEX idx_telemetry_logs_agent_id ON telemetry_logs (agent_id, timestamp DESC);
CREATE INDEX idx_telemetry_logs_org_id ON telemetry_logs (org_id, timestamp DESC);
CREATE INDEX idx_telemetry_logs_batch_id ON telemetry_logs (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX idx_telemetry_logs_session_id ON telemetry_logs (session_id, timestamp DESC) WHERE session_id IS NOT NULL;

-- Configure data retention policy: automatically drop chunks older than 90 days
SELECT add_retention_policy('telemetry_logs', INTERVAL '90 days');
