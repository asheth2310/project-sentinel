"""Governance policy models for Project Sentinel.

Defines GovernancePolicy, ThresholdConfig, NotificationChannel, and
CircuitBreakerState models with validation rules per Requirements 8 and 10.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class SupportedMetric(str, Enum):
    """Supported metrics for governance thresholds (Requirement 8.4)."""

    TOTAL_TOKENS = "total_tokens"
    TOTAL_COST = "total_cost"
    LATENCY_P99 = "latency_p99"
    CONSECUTIVE_IDENTICAL_CALLS = "consecutive_identical_calls"


class NotificationChannelType(str, Enum):
    """Supported notification channel types (Requirement 8.6)."""

    SLACK = "slack"
    PAGERDUTY = "pagerduty"


class NotificationChannel(BaseModel):
    """Notification channel configuration (Requirement 8.6).

    Supports Slack webhooks and PagerDuty routing keys.
    """

    type: NotificationChannelType
    webhook_url: str | None = Field(
        default=None,
        description="Slack webhook URL. Required when type is 'slack'.",
    )
    routing_key: str | None = Field(
        default=None,
        description="PagerDuty routing key. Required when type is 'pagerduty'.",
    )

    @model_validator(mode="after")
    def validate_channel_config(self) -> "NotificationChannel":
        """Ensure the appropriate config field is provided for the channel type."""
        if self.type == NotificationChannelType.SLACK:
            if not self.webhook_url:
                raise ValueError(
                    "webhook_url is required for Slack notification channels"
                )
        elif self.type == NotificationChannelType.PAGERDUTY:
            if not self.routing_key:
                raise ValueError(
                    "routing_key is required for PagerDuty notification channels"
                )
        return self


class ThresholdConfig(BaseModel):
    """Threshold configuration for a single metric (Requirement 8.2, 8.3, 8.4).

    Defines soft (warning) and hard (kill-switch) limits for a supported metric.
    """

    metric: SupportedMetric = Field(
        description="The metric to evaluate against thresholds."
    )
    soft_limit: float = Field(
        description="Warning threshold. Triggers a notification when breached."
    )
    hard_limit: float = Field(
        description="Kill-switch threshold. Triggers circuit breaker when breached."
    )
    window_seconds: int = Field(
        gt=0,
        description="Evaluation window duration in seconds. Must be > 0.",
    )
    cooldown_seconds: int = Field(
        ge=0,
        description="Cooldown period in seconds before re-evaluation. Must be >= 0.",
    )

    @model_validator(mode="after")
    def validate_soft_less_than_hard(self) -> "ThresholdConfig":
        """Soft limit must be strictly less than hard limit (Requirement 8.3)."""
        if self.soft_limit >= self.hard_limit:
            raise ValueError(
                f"soft_limit ({self.soft_limit}) must be less than "
                f"hard_limit ({self.hard_limit})"
            )
        return self


class GovernancePolicy(BaseModel):
    """Organization governance policy (Requirement 8).

    Defines thresholds, notification channels, and kill-switch behavior
    for an organization's AI agent fleet.
    """

    policy_id: UUID = Field(default_factory=uuid4)
    org_id: UUID
    thresholds: list[ThresholdConfig] = Field(
        min_length=1,
        description="List of threshold configurations. Must contain at least one.",
    )
    notification_channels: list[NotificationChannel] = Field(
        default_factory=list,
        description="Notification channels for alerts (Slack, PagerDuty).",
    )
    auto_kill_enabled: bool = Field(
        default=True,
        description="Whether automatic kill-switch activation is enabled (Requirement 8.5).",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Policy creation timestamp.",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Policy last update timestamp.",
    )


class CircuitBreakerState(BaseModel):
    """Circuit breaker (kill-switch) state for an agent (Requirement 10).

    Tracks whether an agent is currently circuit-broken, who activated it,
    and when it should auto-deactivate (if TTL is configured).
    """

    agent_id: UUID = Field(..., description="ID of the agent this circuit breaker applies to")
    is_active: bool = Field(..., description="Whether the circuit breaker is currently active")
    activated_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the circuit breaker was activated. Required when is_active=True.",
    )
    activated_by: str = Field(
        ...,
        min_length=1,
        description="Actor who activated the breaker: 'system' for automated or a user ID for manual activation.",
    )
    reason: str = Field(
        ...,
        min_length=1,
        description="Human-readable reason for circuit breaker activation.",
    )
    ttl_seconds: Optional[int] = Field(
        default=None,
        description="Optional TTL in seconds for auto-deactivation. Must be > 0 if provided.",
    )

    @field_validator("ttl_seconds")
    @classmethod
    def validate_ttl_positive(cls, v: Optional[int]) -> Optional[int]:
        """TTL must be > 0 if provided (Requirement 10.3)."""
        if v is not None and v <= 0:
            raise ValueError("ttl_seconds must be greater than 0 if provided")
        return v

    @model_validator(mode="after")
    def validate_activated_at_when_active(self) -> "CircuitBreakerState":
        """activated_at is required when is_active=True (Requirement 10.2)."""
        if self.is_active and self.activated_at is None:
            raise ValueError("activated_at is required when is_active is True")
        return self
