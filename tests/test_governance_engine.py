"""Unit tests for the Governance Engine.

Tests cover:
- Task 37: Threshold evaluation logic (soft -> WARNING, hard -> KILL_SWITCH)
- Task 38: Monotonicity guarantee (warning before/simultaneously with kill-switch)
- Task 39: Cooldown period enforcement
- Task 40: auto_kill_enabled behavior (CRITICAL warning when disabled)
- Task 41: Kafka consumer for anomaly-events topic
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from src.governance.engine import ActionType, GovernanceAction, GovernanceEngine
from src.models.anomaly import AnomalyEvent, AnomalyType, Severity
from src.models.governance import (
    GovernancePolicy,
    SupportedMetric,
    ThresholdConfig,
)


# --- Fixtures ---


@pytest.fixture
def org_id() -> UUID:
    return uuid4()


@pytest.fixture
def agent_id() -> UUID:
    return uuid4()


@pytest.fixture
def threshold_config() -> ThresholdConfig:
    """A threshold with soft_limit=80, hard_limit=100, cooldown=60s."""
    return ThresholdConfig(
        metric=SupportedMetric.TOTAL_TOKENS,
        soft_limit=80.0,
        hard_limit=100.0,
        window_seconds=60,
        cooldown_seconds=60,
    )


@pytest.fixture
def policy(org_id: UUID, threshold_config: ThresholdConfig) -> GovernancePolicy:
    """A governance policy with auto_kill enabled and a single threshold."""
    return GovernancePolicy(
        org_id=org_id,
        thresholds=[threshold_config],
        auto_kill_enabled=True,
    )


@pytest.fixture
def policy_no_auto_kill(org_id: UUID, threshold_config: ThresholdConfig) -> GovernancePolicy:
    """A governance policy with auto_kill disabled."""
    return GovernancePolicy(
        org_id=org_id,
        thresholds=[threshold_config],
        auto_kill_enabled=False,
    )


@pytest.fixture
def circuit_breaker_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def engine(
    policy: GovernancePolicy, circuit_breaker_service: MagicMock
) -> GovernanceEngine:
    policy_store = {policy.org_id: policy}
    return GovernanceEngine(policy_store, circuit_breaker_service)


@pytest.fixture
def engine_no_auto_kill(
    policy_no_auto_kill: GovernancePolicy, circuit_breaker_service: MagicMock
) -> GovernanceEngine:
    policy_store = {policy_no_auto_kill.org_id: policy_no_auto_kill}
    return GovernanceEngine(policy_store, circuit_breaker_service)


def make_anomaly(
    agent_id: UUID,
    org_id: UUID,
    metric_value: float,
    threshold_value: float = 80.0,
) -> AnomalyEvent:
    """Helper to create an AnomalyEvent with given metric value."""
    now = datetime.now(timezone.utc)
    return AnomalyEvent(
        agent_id=agent_id,
        org_id=org_id,
        anomaly_type=AnomalyType.TOKEN_SPIKE,
        severity=Severity.HIGH,
        detected_at=now,
        window_start=now - timedelta(seconds=60),
        window_end=now,
        metric_value=metric_value,
        threshold_value=threshold_value,
        description=f"Token spike detected: {metric_value} tokens",
    )


# --- Task 37: Threshold evaluation logic ---


class TestThresholdEvaluation:
    """Tests for soft limit -> WARNING and hard limit -> KILL_SWITCH."""

    @pytest.mark.asyncio
    async def test_below_soft_limit_no_action(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Metric below soft limit should produce no actions."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=50.0)
        actions = await engine.evaluate_anomaly(anomaly)
        assert actions == []

    @pytest.mark.asyncio
    async def test_at_soft_limit_generates_warning(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Metric at soft limit (80) should generate WARNING."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=80.0)
        actions = await engine.evaluate_anomaly(anomaly)
        assert len(actions) == 1
        assert actions[0].action_type == ActionType.WARNING
        assert actions[0].severity == Severity.HIGH
        assert actions[0].agent_id == agent_id
        assert actions[0].threshold_metric == "total_tokens"

    @pytest.mark.asyncio
    async def test_above_soft_below_hard_generates_warning(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Metric between soft and hard limit should generate WARNING only."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)
        actions = await engine.evaluate_anomaly(anomaly)
        assert len(actions) == 1
        assert actions[0].action_type == ActionType.WARNING

    @pytest.mark.asyncio
    async def test_at_hard_limit_generates_kill_switch(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Metric at hard limit (100) should generate KILL_SWITCH (plus WARNING for monotonicity)."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=100.0)
        actions = await engine.evaluate_anomaly(anomaly)
        # Expect both WARNING and KILL_SWITCH
        kill_actions = [a for a in actions if a.action_type == ActionType.KILL_SWITCH]
        assert len(kill_actions) == 1
        assert kill_actions[0].severity == Severity.CRITICAL

    @pytest.mark.asyncio
    async def test_above_hard_limit_generates_kill_switch(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Metric above hard limit should generate KILL_SWITCH."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=150.0)
        actions = await engine.evaluate_anomaly(anomaly)
        kill_actions = [a for a in actions if a.action_type == ActionType.KILL_SWITCH]
        assert len(kill_actions) == 1

    @pytest.mark.asyncio
    async def test_no_policy_returns_empty(
        self, circuit_breaker_service: MagicMock, agent_id: UUID
    ):
        """If no policy exists for the org, return empty actions."""
        engine = GovernanceEngine({}, circuit_breaker_service)
        anomaly = make_anomaly(agent_id, uuid4(), metric_value=200.0)
        actions = await engine.evaluate_anomaly(anomaly)
        assert actions == []


# --- Task 38: Monotonicity guarantee ---


class TestMonotonicity:
    """Tests that WARNING is always issued before/simultaneously with KILL_SWITCH."""

    @pytest.mark.asyncio
    async def test_kill_switch_always_accompanied_by_warning(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """When KILL_SWITCH is triggered, WARNING must appear first in the list."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=100.0)
        actions = await engine.evaluate_anomaly(anomaly)

        action_types = [a.action_type for a in actions]
        assert ActionType.WARNING in action_types
        assert ActionType.KILL_SWITCH in action_types

        # WARNING must come before KILL_SWITCH (monotonicity)
        warning_idx = action_types.index(ActionType.WARNING)
        kill_idx = action_types.index(ActionType.KILL_SWITCH)
        assert warning_idx <= kill_idx

    @pytest.mark.asyncio
    async def test_monotonicity_on_extreme_value(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Even with an extreme metric value, WARNING is issued first."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=10000.0)
        actions = await engine.evaluate_anomaly(anomaly)

        action_types = [a.action_type for a in actions]
        assert ActionType.WARNING in action_types
        assert ActionType.KILL_SWITCH in action_types
        assert action_types.index(ActionType.WARNING) <= action_types.index(
            ActionType.KILL_SWITCH
        )

    @pytest.mark.asyncio
    async def test_warning_only_when_below_hard_limit(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Below hard limit, only WARNING is issued (no KILL_SWITCH)."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)
        actions = await engine.evaluate_anomaly(anomaly)

        action_types = [a.action_type for a in actions]
        assert ActionType.WARNING in action_types
        assert ActionType.KILL_SWITCH not in action_types


# --- Task 39: Cooldown period enforcement ---


class TestCooldownEnforcement:
    """Tests that cooldown prevents repeated threshold triggers."""

    @pytest.mark.asyncio
    async def test_cooldown_blocks_repeated_trigger(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """After a threshold triggers, same threshold is blocked during cooldown."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)

        # First trigger
        actions1 = await engine.evaluate_anomaly(anomaly)
        assert len(actions1) == 1

        # Second trigger within cooldown should be blocked
        actions2 = await engine.evaluate_anomaly(anomaly)
        assert actions2 == []

    @pytest.mark.asyncio
    async def test_cooldown_elapsed_allows_retrigger(
        self, engine: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """After cooldown expires, threshold can trigger again."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)

        # First trigger
        actions1 = await engine.evaluate_anomaly(anomaly)
        assert len(actions1) == 1

        # Manually set cooldown to past time (simulate elapsed cooldown)
        metric = "total_tokens"
        key = (agent_id, metric)
        engine._cooldown_tracker[key] = datetime.now(timezone.utc) - timedelta(
            seconds=120
        )

        # Should trigger again
        actions2 = await engine.evaluate_anomaly(anomaly)
        assert len(actions2) == 1

    @pytest.mark.asyncio
    async def test_cooldown_independent_per_metric(
        self, circuit_breaker_service: MagicMock, agent_id: UUID, org_id: UUID
    ):
        """Cooldown is independent per (agent_id, metric) pair."""
        threshold_tokens = ThresholdConfig(
            metric=SupportedMetric.TOTAL_TOKENS,
            soft_limit=80.0,
            hard_limit=100.0,
            window_seconds=60,
            cooldown_seconds=60,
        )
        threshold_cost = ThresholdConfig(
            metric=SupportedMetric.TOTAL_COST,
            soft_limit=50.0,
            hard_limit=100.0,
            window_seconds=60,
            cooldown_seconds=60,
        )
        policy = GovernancePolicy(
            org_id=org_id,
            thresholds=[threshold_tokens, threshold_cost],
            auto_kill_enabled=True,
        )
        engine = GovernanceEngine({org_id: policy}, circuit_breaker_service)

        # Trigger with value that breaches tokens soft limit
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)
        actions1 = await engine.evaluate_anomaly(anomaly)
        # Should get actions for token threshold (90 >= 80) and cost threshold (90 >= 50)
        assert len(actions1) >= 1

        # Second evaluation - both should be in cooldown now
        actions2 = await engine.evaluate_anomaly(anomaly)
        assert actions2 == []

    @pytest.mark.asyncio
    async def test_cooldown_independent_per_agent(
        self, engine: GovernanceEngine, org_id: UUID
    ):
        """Different agents have independent cooldowns."""
        agent1 = uuid4()
        agent2 = uuid4()

        anomaly1 = make_anomaly(agent1, org_id, metric_value=90.0)
        anomaly2 = make_anomaly(agent2, org_id, metric_value=90.0)

        # Agent1 triggers
        actions1 = await engine.evaluate_anomaly(anomaly1)
        assert len(actions1) == 1

        # Agent2 should still be able to trigger (different agent)
        actions2 = await engine.evaluate_anomaly(anomaly2)
        assert len(actions2) == 1

    def test_check_cooldown_no_prior_trigger(
        self, engine: GovernanceEngine, agent_id: UUID
    ):
        """First check with no prior trigger should return True."""
        assert engine._check_cooldown(agent_id, "total_tokens", 60) is True

    def test_check_cooldown_within_period(
        self, engine: GovernanceEngine, agent_id: UUID
    ):
        """Check within cooldown period should return False."""
        engine._record_cooldown(agent_id, "total_tokens")
        assert engine._check_cooldown(agent_id, "total_tokens", 60) is False

    def test_check_cooldown_after_period(
        self, engine: GovernanceEngine, agent_id: UUID
    ):
        """Check after cooldown period should return True."""
        key = (agent_id, "total_tokens")
        engine._cooldown_tracker[key] = datetime.now(timezone.utc) - timedelta(
            seconds=120
        )
        assert engine._check_cooldown(agent_id, "total_tokens", 60) is True


# --- Task 40: auto_kill_enabled behavior ---


class TestAutoKillDisabled:
    """Tests that auto_kill_enabled=False produces CRITICAL warning instead of KILL_SWITCH."""

    @pytest.mark.asyncio
    async def test_hard_limit_no_auto_kill_gives_critical_warning(
        self, engine_no_auto_kill: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Hard limit breach with auto_kill disabled -> CRITICAL warning, not KILL_SWITCH."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=100.0)
        actions = await engine_no_auto_kill.evaluate_anomaly(anomaly)

        # Should have no KILL_SWITCH actions
        kill_actions = [a for a in actions if a.action_type == ActionType.KILL_SWITCH]
        assert len(kill_actions) == 0

        # Should have WARNING actions, including one with CRITICAL severity
        critical_warnings = [
            a
            for a in actions
            if a.action_type == ActionType.WARNING and a.severity == Severity.CRITICAL
        ]
        assert len(critical_warnings) == 1
        assert "auto_kill disabled" in critical_warnings[0].reason

    @pytest.mark.asyncio
    async def test_hard_limit_no_auto_kill_still_has_monotonicity_warning(
        self, engine_no_auto_kill: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Even with auto_kill disabled, the monotonicity WARNING is issued."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=120.0)
        actions = await engine_no_auto_kill.evaluate_anomaly(anomaly)

        # Should have at least 2 warnings: soft-limit WARNING + CRITICAL warning
        assert len(actions) == 2
        assert actions[0].action_type == ActionType.WARNING
        assert actions[0].severity == Severity.HIGH
        assert actions[1].action_type == ActionType.WARNING
        assert actions[1].severity == Severity.CRITICAL

    @pytest.mark.asyncio
    async def test_soft_limit_same_regardless_of_auto_kill(
        self, engine_no_auto_kill: GovernanceEngine, agent_id: UUID, org_id: UUID
    ):
        """Soft limit breach behavior is the same whether auto_kill is on or off."""
        anomaly = make_anomaly(agent_id, org_id, metric_value=85.0)
        actions = await engine_no_auto_kill.evaluate_anomaly(anomaly)

        assert len(actions) == 1
        assert actions[0].action_type == ActionType.WARNING
        assert actions[0].severity == Severity.HIGH


# --- Task 41: Kafka consumer for anomaly-events topic ---


class TestGovernanceEngineConsumer:
    """Tests for the GovernanceEngineConsumer."""

    @pytest.mark.asyncio
    async def test_consumer_initialization(self):
        """Consumer initializes with correct topic and group."""
        from src.config.settings import KafkaSettings
        from src.governance.consumer import GovernanceEngineConsumer

        settings = KafkaSettings()
        engine = MagicMock()
        consumer = GovernanceEngineConsumer(settings, engine)

        assert consumer._topic == "anomaly-events"
        assert consumer._group_id == "sentinel-governance-engine"

    @pytest.mark.asyncio
    async def test_consumer_processes_valid_message(self):
        """Consumer correctly parses and evaluates a valid anomaly event message."""
        from src.governance.consumer import GovernanceEngineConsumer
        from src.config.settings import KafkaSettings

        settings = KafkaSettings()
        mock_engine = MagicMock()
        mock_engine.evaluate_anomaly = AsyncMock(return_value=[])

        consumer = GovernanceEngineConsumer(settings, mock_engine)

        now = datetime.now(timezone.utc)
        message = {
            "anomaly_id": str(uuid4()),
            "agent_id": str(uuid4()),
            "org_id": str(uuid4()),
            "anomaly_type": "token_spike",
            "severity": "high",
            "detected_at": now.isoformat(),
            "window_start": (now - timedelta(seconds=60)).isoformat(),
            "window_end": now.isoformat(),
            "metric_value": 150.0,
            "threshold_value": 100.0,
            "description": "Token spike detected",
            "metadata": {},
        }

        await consumer.process_message(message)
        mock_engine.evaluate_anomaly.assert_called_once()

    @pytest.mark.asyncio
    async def test_consumer_rejects_invalid_message(self):
        """Consumer raises ValueError for invalid messages."""
        from src.governance.consumer import GovernanceEngineConsumer
        from src.config.settings import KafkaSettings

        settings = KafkaSettings()
        mock_engine = MagicMock()
        consumer = GovernanceEngineConsumer(settings, mock_engine)

        with pytest.raises(ValueError, match="Invalid anomaly event message"):
            await consumer.process_message({"invalid": "data"})

    @pytest.mark.asyncio
    async def test_consumer_passes_anomaly_to_engine(self):
        """Consumer passes parsed AnomalyEvent to governance engine."""
        from src.governance.consumer import GovernanceEngineConsumer
        from src.config.settings import KafkaSettings

        settings = KafkaSettings()
        mock_engine = MagicMock()

        action = GovernanceAction(
            action_type=ActionType.WARNING,
            agent_id=uuid4(),
            reason="test",
            severity=Severity.HIGH,
            threshold_metric="total_tokens",
        )
        mock_engine.evaluate_anomaly = AsyncMock(return_value=[action])

        consumer = GovernanceEngineConsumer(settings, mock_engine)

        now = datetime.now(timezone.utc)
        agent_id = uuid4()
        message = {
            "agent_id": str(agent_id),
            "org_id": str(uuid4()),
            "anomaly_type": "token_spike",
            "severity": "high",
            "detected_at": now.isoformat(),
            "window_start": (now - timedelta(seconds=60)).isoformat(),
            "window_end": now.isoformat(),
            "metric_value": 95.0,
            "threshold_value": 80.0,
            "description": "Token spike",
            "metadata": {},
        }

        await consumer.process_message(message)

        # Verify the engine was called with an AnomalyEvent
        call_args = mock_engine.evaluate_anomaly.call_args
        anomaly_arg = call_args[0][0]
        assert anomaly_arg.agent_id == agent_id


# --- Multiple thresholds ---


class TestMultipleThresholds:
    """Tests for multiple threshold evaluation."""

    @pytest.mark.asyncio
    async def test_multiple_thresholds_evaluated_independently(
        self, circuit_breaker_service: MagicMock
    ):
        """Each threshold is evaluated independently against the metric value."""
        org_id = uuid4()
        agent_id = uuid4()

        threshold_tokens = ThresholdConfig(
            metric=SupportedMetric.TOTAL_TOKENS,
            soft_limit=80.0,
            hard_limit=100.0,
            window_seconds=60,
            cooldown_seconds=60,
        )
        threshold_cost = ThresholdConfig(
            metric=SupportedMetric.TOTAL_COST,
            soft_limit=50.0,
            hard_limit=75.0,
            window_seconds=60,
            cooldown_seconds=60,
        )

        policy = GovernancePolicy(
            org_id=org_id,
            thresholds=[threshold_tokens, threshold_cost],
            auto_kill_enabled=True,
        )
        engine = GovernanceEngine({org_id: policy}, circuit_breaker_service)

        # Metric value 90 breaches tokens soft (80) and cost hard (75)
        anomaly = make_anomaly(agent_id, org_id, metric_value=90.0)
        actions = await engine.evaluate_anomaly(anomaly)

        # Should have actions from both thresholds
        token_actions = [a for a in actions if a.threshold_metric == "total_tokens"]
        cost_actions = [a for a in actions if a.threshold_metric == "total_cost"]

        # Token: 90 >= 80 (soft) but < 100 (hard) -> WARNING
        assert len(token_actions) == 1
        assert token_actions[0].action_type == ActionType.WARNING

        # Cost: 90 >= 75 (hard) -> WARNING + KILL_SWITCH
        assert len(cost_actions) == 2
        assert any(a.action_type == ActionType.KILL_SWITCH for a in cost_actions)
