-- Migration 005: Create anomaly_events table
-- Requirements 4, 5, 6: Anomaly Detection
-- Stores detected anomalies for historical querying and dashboard visualization

CREATE TABLE anomaly_events (
    anomaly_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_id UUID NOT NULL,
    org_id UUID NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    metric_value DOUBLE PRECISION,
    threshold_value DOUBLE PRECISION,
    description TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Indexes for common query patterns (dashboard queries, agent investigation)
CREATE INDEX idx_anomaly_events_agent_detected ON anomaly_events (agent_id, detected_at DESC);
CREATE INDEX idx_anomaly_events_org_severity ON anomaly_events (org_id, severity);

COMMENT ON TABLE anomaly_events IS 'Detected anomalies from the real-time anomaly detection engine';
COMMENT ON COLUMN anomaly_events.anomaly_type IS 'Type of anomaly: token_spike, infinite_loop, prompt_cascade, latency_spike, cost_runaway';
COMMENT ON COLUMN anomaly_events.severity IS 'Severity level: low, medium, high, critical';
COMMENT ON COLUMN anomaly_events.metadata IS 'Additional context (e.g., repeated tool name, growth rate details)';
