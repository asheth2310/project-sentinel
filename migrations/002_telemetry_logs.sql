-- Migration 002: Create telemetry_logs hypertable
-- Requirement 13: Time-Series Storage
-- Stores all telemetry events partitioned by timestamp for historical querying

CREATE TABLE telemetry_logs (
    timestamp TIMESTAMPTZ NOT NULL,
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_id UUID NOT NULL,
    org_id UUID NOT NULL,
    prompt_tokens INTEGER NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens INTEGER NOT NULL CHECK (completion_tokens >= 0),
    total_cost NUMERIC(18, 6) NOT NULL CHECK (total_cost >= 0),
    latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
    tool_name TEXT,
    prompt_hash TEXT,
    session_id UUID,
    batch_id UUID
);

-- Convert to hypertable with 1-hour chunk intervals for optimal query performance
SELECT create_hypertable('telemetry_logs', 'timestamp', chunk_time_interval => INTERVAL '1 hour');

-- Indexes for common query patterns
CREATE INDEX idx_telemetry_logs_agent_id ON telemetry_logs (agent_id);
CREATE INDEX idx_telemetry_logs_org_id ON telemetry_logs (org_id);
CREATE INDEX idx_telemetry_logs_agent_timestamp ON telemetry_logs (agent_id, timestamp DESC);

-- Configure data retention policy: automatically drop chunks older than 90 days
SELECT add_retention_policy('telemetry_logs', INTERVAL '90 days');

COMMENT ON TABLE telemetry_logs IS 'Time-series telemetry data from AI agent SDKs, partitioned by timestamp';
COMMENT ON COLUMN telemetry_logs.total_cost IS 'Total cost of the LLM call in USD with 6 decimal precision';
COMMENT ON COLUMN telemetry_logs.prompt_hash IS 'Hash of prompt content for deduplication and cascade detection';
