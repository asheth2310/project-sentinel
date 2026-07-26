-- Migration 004: Create audit_log table
-- Requirement 10: Circuit Breaker Management
-- Records all circuit breaker activation and deactivation events for accountability

CREATE TABLE audit_log (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_id UUID NOT NULL,
    org_id UUID NOT NULL,
    action_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX idx_audit_log_agent_id ON audit_log (agent_id, created_at DESC);
CREATE INDEX idx_audit_log_org_id ON audit_log (org_id, created_at DESC);
CREATE INDEX idx_audit_log_action_type ON audit_log (action_type, created_at DESC);

COMMENT ON TABLE audit_log IS 'Audit trail for circuit breaker activations and deactivations';
COMMENT ON COLUMN audit_log.action_type IS 'Action type: circuit_breaker_activated, circuit_breaker_deactivated, etc.';
COMMENT ON COLUMN audit_log.actor IS 'User ID or "system" for automated actions';
