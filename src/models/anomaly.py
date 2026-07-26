"""Anomaly event models and enums for Project Sentinel."""

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class AnomalyType(StrEnum):
    """Enumeration of detectable anomaly types."""

    TOKEN_SPIKE = "token_spike"
    INFINITE_LOOP = "infinite_loop"
    PROMPT_CASCADE = "prompt_cascade"
    LATENCY_SPIKE = "latency_spike"
    COST_RUNAWAY = "cost_runaway"


class Severity(StrEnum):
    """Severity levels with ordering: LOW < MEDIUM < HIGH < CRITICAL.

    Supports comparison operators for severity-based logic.
    """

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    def __lt__(self, other: "Severity") -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self._severity_order() < other._severity_order()

    def __le__(self, other: "Severity") -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self._severity_order() <= other._severity_order()

    def __gt__(self, other: "Severity") -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self._severity_order() > other._severity_order()

    def __ge__(self, other: "Severity") -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self._severity_order() >= other._severity_order()

    def _severity_order(self) -> int:
        """Return numeric order for comparison."""
        order = {
            Severity.LOW: 0,
            Severity.MEDIUM: 1,
            Severity.HIGH: 2,
            Severity.CRITICAL: 3,
        }
        return order[self]


class AnomalyEvent(BaseModel):
    """Represents a detected anomaly in agent behavior.

    Produced by the anomaly detection engine when metrics exceed
    configured thresholds (token spikes, infinite loops, prompt cascades).
    """

    anomaly_id: UUID = Field(default_factory=uuid4, description="Unique identifier for this anomaly event")
    agent_id: UUID = Field(..., description="ID of the agent that triggered the anomaly")
    org_id: UUID = Field(..., description="Organization the agent belongs to")
    anomaly_type: AnomalyType = Field(..., description="Type of anomaly detected")
    severity: Severity = Field(..., description="Severity level of the anomaly")
    detected_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the anomaly was detected",
    )
    window_start: datetime = Field(..., description="Start of the sliding window that detected the anomaly")
    window_end: datetime = Field(..., description="End of the sliding window that detected the anomaly")
    metric_value: float = Field(..., description="Measured metric value that triggered the anomaly")
    threshold_value: float = Field(..., description="Threshold value that was exceeded")
    description: str = Field(..., description="Human-readable description of the anomaly")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional context about the anomaly")
