"""Tests for TelemetryEvent pydantic model validation."""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from src.models.telemetry import TelemetryEvent


def _valid_event_data(**overrides) -> dict:
    """Return a valid TelemetryEvent payload with optional overrides."""
    data = {
        "timestamp": datetime.now(timezone.utc),
        "log_id": uuid4(),
        "agent_id": uuid4(),
        "org_id": uuid4(),
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_cost": Decimal("0.003400"),
        "latency_ms": 245,
    }
    data.update(overrides)
    return data


class TestTelemetryEventValid:
    """Tests for valid TelemetryEvent construction."""

    def test_creates_with_required_fields(self):
        data = _valid_event_data()
        event = TelemetryEvent(**data)
        assert event.prompt_tokens == 100
        assert event.completion_tokens == 50
        assert event.total_cost == Decimal("0.003400")
        assert event.latency_ms == 245

    def test_creates_with_all_optional_fields(self):
        session_id = uuid4()
        data = _valid_event_data(
            tool_name="web_search",
            prompt_hash="abc123",
            session_id=session_id,
        )
        event = TelemetryEvent(**data)
        assert event.tool_name == "web_search"
        assert event.prompt_hash == "abc123"
        assert event.session_id == session_id

    def test_optional_fields_default_to_none(self):
        event = TelemetryEvent(**_valid_event_data())
        assert event.tool_name is None
        assert event.prompt_hash is None
        assert event.session_id is None

    def test_zero_tokens_valid(self):
        event = TelemetryEvent(**_valid_event_data(prompt_tokens=0, completion_tokens=0))
        assert event.prompt_tokens == 0
        assert event.completion_tokens == 0

    def test_zero_cost_valid(self):
        event = TelemetryEvent(**_valid_event_data(total_cost=Decimal("0")))
        assert event.total_cost == Decimal("0")

    def test_zero_latency_valid(self):
        event = TelemetryEvent(**_valid_event_data(latency_ms=0))
        assert event.latency_ms == 0

    def test_cost_with_six_decimal_places_valid(self):
        event = TelemetryEvent(**_valid_event_data(total_cost=Decimal("0.123456")))
        assert event.total_cost == Decimal("0.123456")

    def test_timestamp_in_past_valid(self):
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        event = TelemetryEvent(**_valid_event_data(timestamp=past))
        assert event.timestamp == past

    def test_timestamp_slightly_in_future_valid(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=4)
        event = TelemetryEvent(**_valid_event_data(timestamp=future))
        assert event.timestamp == future

    def test_model_is_frozen(self):
        event = TelemetryEvent(**_valid_event_data())
        with pytest.raises(ValidationError):
            event.prompt_tokens = 200


class TestTelemetryEventTokenValidation:
    """Tests for prompt_tokens and completion_tokens validation."""

    def test_negative_prompt_tokens_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(prompt_tokens=-1))
        assert "prompt_tokens" in str(exc_info.value)

    def test_negative_completion_tokens_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(completion_tokens=-1))
        assert "completion_tokens" in str(exc_info.value)


class TestTelemetryEventCostValidation:
    """Tests for total_cost validation (non-negative, max 6 decimal places)."""

    def test_negative_cost_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(total_cost=Decimal("-0.01")))
        assert "total_cost" in str(exc_info.value)

    def test_cost_with_seven_decimal_places_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(total_cost=Decimal("0.1234567")))
        assert "total_cost" in str(exc_info.value)

    def test_cost_with_many_decimal_places_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(total_cost=Decimal("0.00000001")))
        assert "total_cost" in str(exc_info.value)


class TestTelemetryEventLatencyValidation:
    """Tests for latency_ms validation."""

    def test_negative_latency_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(latency_ms=-1))
        assert "latency_ms" in str(exc_info.value)


class TestTelemetryEventTimestampValidation:
    """Tests for timestamp future-bound validation."""

    def test_timestamp_more_than_5_minutes_in_future_rejected(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=6)
        with pytest.raises(ValidationError) as exc_info:
            TelemetryEvent(**_valid_event_data(timestamp=future))
        assert "timestamp" in str(exc_info.value)

    def test_timestamp_exactly_at_boundary_accepted(self):
        # Just under 5 minutes should be accepted
        future = datetime.now(timezone.utc) + timedelta(minutes=4, seconds=59)
        event = TelemetryEvent(**_valid_event_data(timestamp=future))
        assert event.timestamp is not None

    def test_naive_timestamp_treated_as_utc(self):
        # Naive timestamp in the past should be accepted (treated as UTC)
        past = datetime.now(timezone.utc) - timedelta(minutes=1)
        naive_past = past.replace(tzinfo=None)
        event = TelemetryEvent(**_valid_event_data(timestamp=naive_past))
        assert event.timestamp.tzinfo == timezone.utc
