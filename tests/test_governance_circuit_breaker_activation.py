"""Tests for governance engine circuit breaker activation (Task 42).

Verifies that when the governance engine determines a KILL_SWITCH action,
it activates the circuit breaker service and writes to the audit log.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from src.governance.engine import GovernanceEngine, ActionType, GovernanceAction
from src.governance.audit import AuditLogger
from src.models.anomaly import AnomalyEvent, AnomalyType, Severity
from src.models.governance import (
    GovernancePolicy,
    ThresholdConfig,
    SupportedMetric,
)


def _make_anomaly(agent_id=None, org_id=None, metric_value=150.0):
    """Create a test anomaly event."""
    return AnomalyEvent(
        agent_id=agent_id or uuid4(),
        org_id=org_id or uuid4(),
        anomaly_type=AnomalyType.TOKEN_SPIKE,
        severity=Severity.HIGH,
        detected_at=datetime.now(timezone.utc),
        window_start=datetime.now(timezone.utc),
        window_end=datetime.now(timezone.utc),
        metric_value=metric_value,
        threshold_value=100.0,
        description="Test anomaly",
    )


def _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True):
    """Create a test governance policy."""
    return GovernancePolicy(
        org_id=org_id,
        thresholds=[
            ThresholdConfig(
                metric=SupportedMetric.TOTAL_TOKENS,
                soft_limit=soft_limit,
                hard_limit=hard_limit,
                window_seconds=60,
                cooldown_seconds=300,
            )
        ],
        auto_kill_enabled=auto_kill,
    )


@pytest.mark.asyncio
async def test_kill_switch_activates_circuit_breaker():
    """When KILL_SWITCH is determined, circuit breaker should be activated."""
    org_id = uuid4()
    agent_id = uuid4()
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=150.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True)

    cb_service = AsyncMock()
    cb_service.activate = AsyncMock()
    audit_logger = AuditLogger()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        audit_logger=audit_logger,
    )

    actions = await engine.evaluate_anomaly(anomaly)

    # Should have WARNING + KILL_SWITCH
    kill_actions = [a for a in actions if a.action_type == ActionType.KILL_SWITCH]
    assert len(kill_actions) == 1

    # Circuit breaker should have been activated
    cb_service.activate.assert_called_once_with(
        agent_id=agent_id,
        reason=kill_actions[0].reason,
        activated_by="system",
    )


@pytest.mark.asyncio
async def test_kill_switch_writes_audit_log():
    """When KILL_SWITCH is determined, audit log should be written."""
    org_id = uuid4()
    agent_id = uuid4()
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=150.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True)

    cb_service = AsyncMock()
    audit_logger = AuditLogger()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        audit_logger=audit_logger,
    )

    await engine.evaluate_anomaly(anomaly)

    # Audit log should have an entry for circuit breaker activation
    entries = audit_logger.get_entries(agent_id=agent_id)
    assert len(entries) == 1
    assert entries[0].action_type == "circuit_breaker_activated"
    assert entries[0].actor == "system"
    assert entries[0].org_id == org_id
    assert "threshold_metric" in entries[0].metadata


@pytest.mark.asyncio
async def test_warning_only_does_not_activate_circuit_breaker():
    """When only WARNING (not KILL_SWITCH), circuit breaker should NOT activate."""
    org_id = uuid4()
    agent_id = uuid4()
    # Value above soft limit but below hard limit
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=90.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True)

    cb_service = AsyncMock()
    audit_logger = AuditLogger()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        audit_logger=audit_logger,
    )

    actions = await engine.evaluate_anomaly(anomaly)

    # Should only have WARNING, no KILL_SWITCH
    assert all(a.action_type == ActionType.WARNING for a in actions)
    cb_service.activate.assert_not_called()
    assert len(audit_logger.get_entries()) == 0


@pytest.mark.asyncio
async def test_auto_kill_disabled_does_not_activate_circuit_breaker():
    """When auto_kill is disabled, hard limit breach does NOT activate circuit breaker."""
    org_id = uuid4()
    agent_id = uuid4()
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=150.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=False)

    cb_service = AsyncMock()
    audit_logger = AuditLogger()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        audit_logger=audit_logger,
    )

    actions = await engine.evaluate_anomaly(anomaly)

    # No KILL_SWITCH action, only WARNINGs
    assert all(a.action_type == ActionType.WARNING for a in actions)
    cb_service.activate.assert_not_called()
    assert len(audit_logger.get_entries()) == 0


@pytest.mark.asyncio
async def test_circuit_breaker_failure_does_not_crash_engine():
    """If circuit breaker activation fails, engine still returns actions."""
    org_id = uuid4()
    agent_id = uuid4()
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=150.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True)

    cb_service = AsyncMock()
    cb_service.activate.side_effect = Exception("Redis connection error")
    audit_logger = AuditLogger()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        audit_logger=audit_logger,
    )

    # Should not raise
    actions = await engine.evaluate_anomaly(anomaly)
    assert len(actions) > 0
    kill_actions = [a for a in actions if a.action_type == ActionType.KILL_SWITCH]
    assert len(kill_actions) == 1


@pytest.mark.asyncio
async def test_engine_works_without_audit_logger():
    """Engine should work even without audit logger (backwards compatibility)."""
    org_id = uuid4()
    agent_id = uuid4()
    anomaly = _make_anomaly(agent_id=agent_id, org_id=org_id, metric_value=150.0)
    policy = _make_policy(org_id, soft_limit=80.0, hard_limit=100.0, auto_kill=True)

    cb_service = AsyncMock()

    engine = GovernanceEngine(
        policy_store={org_id: policy},
        circuit_breaker_service=cb_service,
        # No audit_logger
    )

    # Should not raise
    actions = await engine.evaluate_anomaly(anomaly)
    assert len(actions) > 0
