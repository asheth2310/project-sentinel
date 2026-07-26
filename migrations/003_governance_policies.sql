-- Migration 003: Create governance_policies table
-- Requirement 8: Governance Policy Configuration
-- Stores organization-level governance policies with thresholds and notification channels

CREATE TABLE governance_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id UUID NOT NULL UNIQUE,
    thresholds JSONB NOT NULL DEFAULT '[]'::jsonb,
    notification_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
    auto_kill_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for org_id lookups (unique constraint already creates an index, but explicit for clarity)
CREATE INDEX idx_governance_policies_org_id ON governance_policies (org_id);

-- Comment on table for documentation
COMMENT ON TABLE governance_policies IS 'Organization-level governance policies defining thresholds, notification channels, and auto-kill behavior';
COMMENT ON COLUMN governance_policies.thresholds IS 'JSON array of threshold configs: [{metric, soft_limit, hard_limit, window_seconds, cooldown_seconds}]';
COMMENT ON COLUMN governance_policies.notification_channels IS 'JSON array of notification channels: [{type, webhook_url?, routing_key?}]';
