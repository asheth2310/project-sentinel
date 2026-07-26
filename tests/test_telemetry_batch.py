"""Unit tests for TelemetryBatch model validation."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from src.models.telemetry import TelemetryBatch, TelemetryEvent


def _make_event(agent_id: UUID, org_id: UUID) -> TelemetryEvent:
    """Helper to create a valid TelemetryEvent."""
    return TelemetryEvent(
        timestamp=datetime.now(timezone.utc),
        agent_id=agent_id,
        org_id=org_id,
        prompt_tokens=100,
        completion_tokens=50,
        total_cost=Decimal("0.001500"),
        latency_ms=200,
    )


class TestTelemetryBatchCreation:
    def test_valid_batch_single_event(self):
        agent_id = uuid4()
        org_id = uuid4()
        event = _make_event(agent_id, org_id)
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=[event],
            sdk_version="1.0.0",
        )
        assert batch.agent_id == agent_id
        assert batch.org_id == org_id
        assert len(batch.events) == 1
        assert batch.sdk_version == "1.0.0"
        assert batch.batch_id is not None

    def test_valid_batch_multiple_events(self):
        agent_id = uuid4()
        org_id = uuid4()
        events = [_make_event(agent_id, org_id) for _ in range(10)]
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=events,
            sdk_version="2.1.3",
        )
        assert len(batch.events) == 10

    def test_batch_id_defaults_to_new_uuid(self):
        agent_id = uuid4()
        org_id = uuid4()
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=[_make_event(agent_id, org_id)],
            sdk_version="1.0.0",
        )
        assert isinstance(batch.batch_id, UUID)

    def test_batch_id_can_be_provided(self):
        agent_id = uuid4()
        org_id = uuid4()
        custom_id = uuid4()
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=[_make_event(agent_id, org_id)],
            sdk_version="1.0.0",
            batch_id=custom_id,
        )
        assert batch.batch_id == custom_id


class TestTelemetryBatchEventsValidation:
    def test_empty_events_rejected(self):
        agent_id = uuid4()
        org_id = uuid4()
        with pytest.raises(Exception) as exc_info:
            TelemetryBatch(
                agent_id=agent_id,
                org_id=org_id,
                events=[],
                sdk_version="1.0.0",
            )
        assert "too_short" in str(exc_info.value).lower() or "min_length" in str(
            exc_info.value
        ).lower()

    def test_max_1000_events_accepted(self):
        agent_id = uuid4()
        org_id = uuid4()
        events = [_make_event(agent_id, org_id) for _ in range(1000)]
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=events,
            sdk_version="1.0.0",
        )
        assert len(batch.events) == 1000

    def test_over_1000_events_rejected(self):
        agent_id = uuid4()
        org_id = uuid4()
        events = [_make_event(agent_id, org_id) for _ in range(1001)]
        with pytest.raises(Exception) as exc_info:
            TelemetryBatch(
                agent_id=agent_id,
                org_id=org_id,
                events=events,
                sdk_version="1.0.0",
            )
        assert "too_long" in str(exc_info.value).lower() or "max_length" in str(
            exc_info.value
        ).lower()


class TestTelemetryBatchAgentIdConsistency:
    def test_mismatched_agent_id_rejected(self):
        batch_agent_id = uuid4()
        event_agent_id = uuid4()
        org_id = uuid4()
        event = TelemetryEvent(
            timestamp=datetime.now(timezone.utc),
            agent_id=event_agent_id,
            org_id=org_id,
            prompt_tokens=100,
            completion_tokens=50,
            total_cost=Decimal("0.001000"),
            latency_ms=200,
        )
        with pytest.raises(Exception) as exc_info:
            TelemetryBatch(
                agent_id=batch_agent_id,
                org_id=org_id,
                events=[event],
                sdk_version="1.0.0",
            )
        assert "agent_id" in str(exc_info.value).lower()

    def test_mixed_agent_ids_rejected(self):
        agent_id_1 = uuid4()
        agent_id_2 = uuid4()
        org_id = uuid4()
        events = [
            _make_event(agent_id_1, org_id),
            _make_event(agent_id_2, org_id),  # Different agent_id
        ]
        with pytest.raises(Exception) as exc_info:
            TelemetryBatch(
                agent_id=agent_id_1,
                org_id=org_id,
                events=events,
                sdk_version="1.0.0",
            )
        assert "agent_id" in str(exc_info.value).lower()

    def test_all_matching_agent_ids_accepted(self):
        agent_id = uuid4()
        org_id = uuid4()
        events = [_make_event(agent_id, org_id) for _ in range(5)]
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=events,
            sdk_version="1.0.0",
        )
        assert all(e.agent_id == agent_id for e in batch.events)


class TestTelemetryBatchSdkVersion:
    def test_valid_semver_accepted(self):
        agent_id = uuid4()
        org_id = uuid4()
        for version in ["0.0.1", "1.0.0", "2.1.3", "10.20.30", "0.0.0"]:
            batch = TelemetryBatch(
                agent_id=agent_id,
                org_id=org_id,
                events=[_make_event(agent_id, org_id)],
                sdk_version=version,
            )
            assert batch.sdk_version == version

    def test_invalid_semver_rejected(self):
        agent_id = uuid4()
        org_id = uuid4()
        invalid_versions = [
            "1.0",  # Missing patch
            "1",  # Only major
            "v1.0.0",  # Prefix
            "1.0.0-alpha",  # Pre-release
            "1.0.0+build",  # Build metadata
            "a.b.c",  # Non-numeric
            "",  # Empty
            "1.0.0.0",  # Too many parts
            "1.0.0 ",  # Trailing space
            " 1.0.0",  # Leading space
        ]
        for version in invalid_versions:
            with pytest.raises(Exception, match="sdk_version"):
                TelemetryBatch(
                    agent_id=agent_id,
                    org_id=org_id,
                    events=[_make_event(agent_id, org_id)],
                    sdk_version=version,
                )


class TestTelemetryBatchFrozen:
    def test_batch_is_immutable(self):
        agent_id = uuid4()
        org_id = uuid4()
        batch = TelemetryBatch(
            agent_id=agent_id,
            org_id=org_id,
            events=[_make_event(agent_id, org_id)],
            sdk_version="1.0.0",
        )
        with pytest.raises(Exception):
            batch.sdk_version = "2.0.0"
