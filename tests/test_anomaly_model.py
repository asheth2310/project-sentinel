"""Tests for AnomalyEvent model, AnomalyType enum, and Severity enum."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from src.models.anomaly import AnomalyEvent, AnomalyType, Severity


class TestAnomalyTypeEnum:
    """Tests for AnomalyType string enum."""

    def test_all_values_are_strings(self):
        for member in AnomalyType:
            assert isinstance(member, str)

    def test_has_expected_members(self):
        expected = {"token_spike", "infinite_loop", "prompt_cascade", "latency_spike", "cost_runaway"}
        actual = {member.value for member in AnomalyType}
        assert actual == expected

    def test_token_spike_value(self):
        assert AnomalyType.TOKEN_SPIKE == "token_spike"

    def test_infinite_loop_value(self):
        assert AnomalyType.INFINITE_LOOP == "infinite_loop"

    def test_prompt_cascade_value(self):
        assert AnomalyType.PROMPT_CASCADE == "prompt_cascade"

    def test_latency_spike_value(self):
        assert AnomalyType.LATENCY_SPIKE == "latency_spike"

    def test_cost_runaway_value(self):
        assert AnomalyType.COST_RUNAWAY == "cost_runaway"


class TestSeverityEnum:
    """Tests for Severity string enum with ordering."""

    def test_all_values_are_strings(self):
        for member in Severity:
            assert isinstance(member, str)

    def test_has_expected_members(self):
        expected = {"low", "medium", "high", "critical"}
        actual = {member.value for member in Severity}
        assert actual == expected

    def test_ordering_low_less_than_medium(self):
        assert Severity.LOW < Severity.MEDIUM

    def test_ordering_medium_less_than_high(self):
        assert Severity.MEDIUM < Severity.HIGH

    def test_ordering_high_less_than_critical(self):
        assert Severity.HIGH < Severity.CRITICAL

    def test_ordering_critical_greater_than_low(self):
        assert Severity.CRITICAL > Severity.LOW

    def test_ordering_equal_to_self(self):
        assert Severity.HIGH >= Severity.HIGH
        assert Severity.HIGH <= Severity.HIGH

    def test_ordering_not_less_than_self(self):
        assert not (Severity.MEDIUM < Severity.MEDIUM)

    def test_full_ordering_chain(self):
        ordered = sorted([Severity.CRITICAL, Severity.LOW, Severity.HIGH, Severity.MEDIUM])
        assert ordered == [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]


def _valid_anomaly_data(**overrides) -> dict:
    """Return a valid AnomalyEvent payload with optional overrides."""
    now = datetime.now(timezone.utc)
    data = {
        "agent_id": uuid4(),
        "org_id": uuid4(),
        "anomaly_type": AnomalyType.TOKEN_SPIKE,
        "severity": Severity.HIGH,
        "detected_at": now,
        "window_start": now,
        "window_end": now,
        "metric_value": 3.5,
        "threshold_value": 2.0,
        "description": "Token spike detected",
    }
    data.update(overrides)
    return data


class TestAnomalyEventDefaults:
    """Tests for AnomalyEvent default values."""

    def test_anomaly_id_auto_generated(self):
        event = AnomalyEvent(**_valid_anomaly_data())
        assert isinstance(event.anomaly_id, UUID)

    def test_anomaly_id_unique_per_instance(self):
        event1 = AnomalyEvent(**_valid_anomaly_data())
        event2 = AnomalyEvent(**_valid_anomaly_data())
        assert event1.anomaly_id != event2.anomaly_id

    def test_detected_at_is_required(self):
        data = _valid_anomaly_data()
        del data["detected_at"]
        with pytest.raises(ValidationError):
            AnomalyEvent(**data)

    def test_metadata_defaults_to_empty_dict(self):
        event = AnomalyEvent(**_valid_anomaly_data())
        assert event.metadata == {}

    def test_metadata_default_not_shared_between_instances(self):
        event1 = AnomalyEvent(**_valid_anomaly_data())
        event2 = AnomalyEvent(**_valid_anomaly_data())
        assert event1.metadata is not event2.metadata


class TestAnomalyEventExplicitValues:
    """Tests for AnomalyEvent with explicit field values."""

    def test_explicit_anomaly_id_used(self):
        custom_id = uuid4()
        event = AnomalyEvent(**_valid_anomaly_data(anomaly_id=custom_id))
        assert event.anomaly_id == custom_id

    def test_explicit_detected_at_used(self):
        custom_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        event = AnomalyEvent(**_valid_anomaly_data(detected_at=custom_time))
        assert event.detected_at == custom_time

    def test_explicit_metadata_used(self):
        meta = {"tool_name": "web_search", "count": 15}
        event = AnomalyEvent(**_valid_anomaly_data(metadata=meta))
        assert event.metadata == meta

    def test_all_anomaly_types_accepted(self):
        for anomaly_type in AnomalyType:
            event = AnomalyEvent(**_valid_anomaly_data(anomaly_type=anomaly_type))
            assert event.anomaly_type == anomaly_type

    def test_all_severity_levels_accepted(self):
        for severity in Severity:
            event = AnomalyEvent(**_valid_anomaly_data(severity=severity))
            assert event.severity == severity


class TestAnomalyEventSerialization:
    """Tests for AnomalyEvent JSON serialization."""

    def test_serializes_to_json(self):
        event = AnomalyEvent(**_valid_anomaly_data())
        json_str = event.model_dump_json()
        assert "token_spike" in json_str
        assert "high" in json_str

    def test_round_trip_serialization(self):
        event = AnomalyEvent(**_valid_anomaly_data())
        json_str = event.model_dump_json()
        restored = AnomalyEvent.model_validate_json(json_str)
        assert restored.anomaly_id == event.anomaly_id
        assert restored.anomaly_type == event.anomaly_type
        assert restored.severity == event.severity
        assert restored.metric_value == event.metric_value

    def test_string_values_accepted_for_enums(self):
        """Pydantic should accept raw string values for StrEnum fields."""
        event = AnomalyEvent(**_valid_anomaly_data(
            anomaly_type="infinite_loop",
            severity="critical",
        ))
        assert event.anomaly_type == AnomalyType.INFINITE_LOOP
        assert event.severity == Severity.CRITICAL


class TestAnomalyEventValidation:
    """Tests for AnomalyEvent validation errors."""

    def test_missing_required_field_rejected(self):
        data = _valid_anomaly_data()
        del data["agent_id"]
        with pytest.raises(ValidationError):
            AnomalyEvent(**data)

    def test_invalid_anomaly_type_rejected(self):
        with pytest.raises(ValidationError):
            AnomalyEvent(**_valid_anomaly_data(anomaly_type="not_a_type"))

    def test_invalid_severity_rejected(self):
        with pytest.raises(ValidationError):
            AnomalyEvent(**_valid_anomaly_data(severity="extreme"))

    def test_invalid_uuid_rejected(self):
        with pytest.raises(ValidationError):
            AnomalyEvent(**_valid_anomaly_data(agent_id="not-a-uuid"))

    def test_frozen_model_rejects_mutation(self):
        event = AnomalyEvent(**_valid_anomaly_data())
        with pytest.raises(ValidationError):
            event.severity = Severity.LOW
