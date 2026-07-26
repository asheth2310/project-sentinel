"""Governance Policy Evaluation Engine for Project Sentinel.

Evaluates anomaly events against organization governance policies to determine
appropriate actions (WARNING, KILL_SWITCH, or NO_ACTION).

Implements:
- Threshold evaluation (soft limit -> WARNING, hard limit -> KILL_SWITCH)
- Monotonicity guarantee (warning always issued before/simultaneously with kill-switch)
- Cooldown period enforcement to prevent repeated threshold triggers
- auto_kill_enabled behavior (CRITICAL warning instead of kill-switch when disabled)
- Circuit breaker activation on KILL_SWITCH with audit logging (Requirement 10.2)
"""

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from src.models.anomaly import AnomalyEvent, Severity
from src.models.governance import GovernancePolicy, ThresholdConfig

logger = logging.getLogger(__name__)


class ActionType(str, Enum):
    """Types of governance actions that can be taken."""

    NO_ACTION = "no_action"
    WARNING = "warning"
    KILL_SWITCH = "kill_switch"


class GovernanceAction:
    """Represents an action determined by the governance engine.

    Attributes:
        action_type: The type of action to take.
        agent_id: The agent this action applies to.
        reason: Human-readable explanation.
        severity: The severity level of the action.
        threshold_metric: The metric that triggered this action (if applicable).
    """

    def __init__(
        self,
        action_type: ActionType,
        agent_id: UUID,
        reason: str,
        severity: Severity,
        threshold_metric: str | None = None,
    ) -> None:
        self.action_type = action_type
        self.agent_id = agent_id
        self.reason = reason
        self.severity = severity
        self.threshold_metric = threshold_metric

    def __repr__(self) -> str:
        return (
            f"GovernanceAction(action_type={self.action_type!r}, "
            f"agent_id={self.agent_id!r}, severity={self.severity!r}, "
            f"metric={self.threshold_metric!r})"
        )


class GovernanceEngine:
    """Evaluates anomaly events against governance policies.

    Determines appropriate actions based on threshold breaches, cooldown
    periods, and auto-kill configuration. Activates circuit breakers
    and writes audit logs when KILL_SWITCH actions are triggered.

    Args:
        policy_store: Dictionary mapping org_id to GovernancePolicy.
        circuit_breaker_service: Service for activating circuit breakers.
        notification_service: Optional service for sending notifications.
        audit_logger: Optional audit logger for recording actions.
    """

    def __init__(
        self,
        policy_store: dict[UUID, GovernancePolicy],
        circuit_breaker_service: Any,
        notification_service: Any | None = None,
        audit_logger: Any | None = None,
    ) -> None:
        self._policy_store = policy_store
        self._circuit_breaker_service = circuit_breaker_service
        self._notification_service = notification_service
        self._audit_logger = audit_logger
        # (agent_id, metric) -> last_trigger_time
        self._cooldown_tracker: dict[tuple[UUID, str], datetime] = {}
        # (agent_id, metric) -> True if warning was issued
        self._warning_issued: dict[tuple[UUID, str], bool] = {}

    async def evaluate_anomaly(self, anomaly: AnomalyEvent) -> list[GovernanceAction]:
        """Evaluate an anomaly event against the org's governance policy.

        Checks each threshold independently and returns the list of actions taken.
        Ensures monotonicity: a WARNING is always issued before or simultaneously
        with a KILL_SWITCH action. Activates circuit breaker on KILL_SWITCH.

        Args:
            anomaly: The anomaly event to evaluate.

        Returns:
            List of GovernanceAction objects. Empty if no thresholds breached
            or if all relevant thresholds are in cooldown.
        """
        policy = self._policy_store.get(anomaly.org_id)
        if policy is None:
            logger.debug(
                "No governance policy found for org_id=%s, skipping evaluation",
                anomaly.org_id,
            )
            return []

        actions: list[GovernanceAction] = []

        for threshold in policy.thresholds:
            threshold_actions = self._evaluate_threshold(
                anomaly=anomaly,
                threshold=threshold,
                auto_kill_enabled=policy.auto_kill_enabled,
            )
            actions.extend(threshold_actions)

        # Execute circuit breaker activation for KILL_SWITCH actions
        for action in actions:
            if action.action_type == ActionType.KILL_SWITCH:
                await self._activate_circuit_breaker(action, anomaly.org_id)

        return actions

    async def _activate_circuit_breaker(
        self, action: GovernanceAction, org_id: UUID
    ) -> None:
        """Activate circuit breaker and write audit log for a KILL_SWITCH action.

        Called automatically when the governance engine determines a KILL_SWITCH
        action. Writes to Redis via CircuitBreakerService and records the event
        in the audit log.

        Args:
            action: The KILL_SWITCH governance action.
            org_id: The organization ID for audit logging.
        """
        try:
            if self._circuit_breaker_service is not None:
                await self._circuit_breaker_service.activate(
                    agent_id=action.agent_id,
                    reason=action.reason,
                    activated_by="system",
                )
                logger.info(
                    "Circuit breaker activated for agent_id=%s, reason=%s",
                    action.agent_id,
                    action.reason,
                )
        except Exception as exc:
            logger.error(
                "Failed to activate circuit breaker for agent_id=%s: %s",
                action.agent_id,
                exc,
            )

        # Write audit log
        try:
            if self._audit_logger is not None:
                await self._audit_logger.log(
                    agent_id=action.agent_id,
                    action_type="circuit_breaker_activated",
                    actor="system",
                    reason=action.reason,
                    org_id=org_id,
                    metadata={
                        "threshold_metric": action.threshold_metric,
                        "severity": action.severity.value,
                    },
                )
        except Exception as exc:
            logger.error(
                "Failed to write audit log for circuit breaker activation: %s",
                exc,
            )

    def _evaluate_threshold(
        self,
        anomaly: AnomalyEvent,
        threshold: ThresholdConfig,
        auto_kill_enabled: bool,
    ) -> list[GovernanceAction]:
        """Evaluate a single threshold against the anomaly metric value.

        Returns actions for this threshold, enforcing cooldown and monotonicity.

        Args:
            anomaly: The anomaly event being evaluated.
            threshold: The threshold configuration to check against.
            auto_kill_enabled: Whether automatic kill-switch is enabled.

        Returns:
            List of actions for this threshold evaluation.
        """
        metric = threshold.metric.value
        agent_id = anomaly.agent_id
        metric_value = anomaly.metric_value
        hard_limit = threshold.hard_limit
        soft_limit = threshold.soft_limit

        # Check cooldown - if in cooldown, skip this threshold
        if not self._check_cooldown(agent_id, metric, threshold.cooldown_seconds):
            return []

        actions: list[GovernanceAction] = []

        # Calculate ratios
        # Soft limit breach: metric_value >= soft_limit
        soft_breached = metric_value >= soft_limit
        # Hard limit breach: metric_value >= hard_limit
        hard_breached = metric_value >= hard_limit

        if hard_breached:
            # Monotonicity guarantee: WARNING always issued before/simultaneously with KILL_SWITCH
            # Issue a WARNING first
            actions.append(
                GovernanceAction(
                    action_type=ActionType.WARNING,
                    agent_id=agent_id,
                    reason=(
                        f"Soft limit breached for {metric}: "
                        f"value={metric_value}, soft_limit={soft_limit}"
                    ),
                    severity=Severity.HIGH,
                    threshold_metric=metric,
                )
            )
            self._warning_issued[(agent_id, metric)] = True

            if auto_kill_enabled:
                # Hard limit + auto_kill -> KILL_SWITCH
                actions.append(
                    GovernanceAction(
                        action_type=ActionType.KILL_SWITCH,
                        agent_id=agent_id,
                        reason=(
                            f"Hard limit breached for {metric}: "
                            f"value={metric_value}, hard_limit={hard_limit}"
                        ),
                        severity=Severity.CRITICAL,
                        threshold_metric=metric,
                    )
                )
            else:
                # auto_kill disabled -> CRITICAL warning instead of kill-switch
                actions.append(
                    GovernanceAction(
                        action_type=ActionType.WARNING,
                        agent_id=agent_id,
                        reason=(
                            f"Hard limit breached for {metric} (auto_kill disabled): "
                            f"value={metric_value}, hard_limit={hard_limit}"
                        ),
                        severity=Severity.CRITICAL,
                        threshold_metric=metric,
                    )
                )

            # Record cooldown after triggering
            self._record_cooldown(agent_id, metric)

        elif soft_breached:
            # Soft limit only -> WARNING
            actions.append(
                GovernanceAction(
                    action_type=ActionType.WARNING,
                    agent_id=agent_id,
                    reason=(
                        f"Soft limit breached for {metric}: "
                        f"value={metric_value}, soft_limit={soft_limit}"
                    ),
                    severity=Severity.HIGH,
                    threshold_metric=metric,
                )
            )
            self._warning_issued[(agent_id, metric)] = True

            # Record cooldown after triggering
            self._record_cooldown(agent_id, metric)

        return actions

    def _check_cooldown(self, agent_id: UUID, metric: str, cooldown_seconds: int) -> bool:
        """Check if cooldown has elapsed for a given agent/metric pair.

        Args:
            agent_id: The agent identifier.
            metric: The metric name.
            cooldown_seconds: Required cooldown period in seconds.

        Returns:
            True if cooldown has elapsed (action can be taken), False otherwise.
        """
        key = (agent_id, metric)
        last_trigger = self._cooldown_tracker.get(key)

        if last_trigger is None:
            return True

        now = datetime.now(timezone.utc)
        elapsed = (now - last_trigger).total_seconds()
        return elapsed >= cooldown_seconds

    def _record_cooldown(self, agent_id: UUID, metric: str) -> None:
        """Record that an action was triggered, starting the cooldown period.

        Args:
            agent_id: The agent identifier.
            metric: The metric name.
        """
        key = (agent_id, metric)
        self._cooldown_tracker[key] = datetime.now(timezone.utc)
