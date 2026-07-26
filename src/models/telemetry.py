"""Telemetry data models for Project Sentinel."""

import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class TelemetryEvent(BaseModel):
    """A single telemetry observation from an AI agent.

    Validates field constraints per Requirement 2.
    """

    timestamp: datetime
    log_id: UUID = Field(default_factory=uuid4)
    agent_id: UUID
    org_id: UUID
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_cost: Decimal = Field(ge=0)
    latency_ms: int = Field(ge=0)
    tool_name: Optional[str] = None
    prompt_hash: Optional[str] = None
    session_id: Optional[UUID] = None

    @field_validator("total_cost")
    @classmethod
    def validate_cost_precision(cls, v: Decimal) -> Decimal:
        """Ensure total_cost has at most 6 decimal places."""
        exponent = v.as_tuple().exponent
        # Only check negative exponents (digits after the decimal point)
        if isinstance(exponent, int) and exponent < -6:
            raise ValueError("total_cost must have at most 6 decimal places")
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp_not_future(cls, v: datetime) -> datetime:
        """Ensure timestamp is not more than 5 minutes in the future."""
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > now + timedelta(minutes=5):
            raise ValueError("timestamp must not be more than 5 minutes in the future")
        return v

    model_config = {"frozen": True}


_SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


class TelemetryBatch(BaseModel):
    """A batch of telemetry events submitted by an agent SDK.

    Validates batch-level constraints per Requirements 1 and 2:
    - 1-1000 events per batch
    - All events must share the same agent_id as the batch
    - sdk_version must follow semver format (major.minor.patch)
    """

    agent_id: UUID
    org_id: UUID
    events: list[TelemetryEvent] = Field(min_length=1, max_length=1000)
    sdk_version: str
    batch_id: UUID = Field(default_factory=uuid4)

    @field_validator("sdk_version")
    @classmethod
    def validate_sdk_version_semver(cls, v: str) -> str:
        """Ensure sdk_version follows semantic versioning (major.minor.patch)."""
        if not _SEMVER_PATTERN.match(v):
            raise ValueError(
                "sdk_version must follow semver format: major.minor.patch "
                "(e.g., '1.2.3')"
            )
        return v

    @model_validator(mode="after")
    def validate_events_agent_id(self) -> "TelemetryBatch":
        """Ensure all events in the batch have the same agent_id as the batch."""
        for i, event in enumerate(self.events):
            if event.agent_id != self.agent_id:
                raise ValueError(
                    f"Event at index {i} has agent_id={event.agent_id}, "
                    f"but batch agent_id={self.agent_id}. "
                    "All events must have the same agent_id as the batch."
                )
        return self

    model_config = {"frozen": True}
