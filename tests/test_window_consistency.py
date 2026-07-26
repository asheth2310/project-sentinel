"""Tests for WindowManager.validate_consistency method.

Validates Requirement 7.4: Window aggregates are always consistent
with the contained events.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from src.anomaly.window_manager import WindowManager
from src.models.telemetry import TelemetryEvent


@pytest.fixture
def agent_id():
    return uuid4()


@pytest.fixture
def org_id():
    return uuid4()


@pytest.fixture
def manager():
    return WindowManager(window_duration_seconds=60)


def _make_event(
    agent_id,
    org_id,
    timestamp=None,
    prompt_tokens=100,
    completion_tokens=50,
    total_cost=Decimal("0.001"),
    latency_ms=200,
    tool_name=None,
    prompt_hash=None,
):
    """Helper to create TelemetryEvent instances for tests."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    return TelemetryEvent(
        timestamp=timestamp,
        agent_id=agent_id,
        org_id=org_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_cost=total_cost,
        latency_ms=latency_ms,
        tool_name=tool_name,
        prompt_hash=prompt_hash,
    )


class TestValidateConsistencyBasic:
    """Basic tests for validate_consistency."""

    def test_raises_for_unknown_agent(self, manager):
        """Raises ValueError if agent has no window."""
        with pytest.raises(ValueError, match="No window exists"):
            manager.validate_consistency(uuid4())

    def test_consistent_after_single_event(self, manager, agent_id, org_id):
        """Single event window is consistent."""
        event = _make_event(agent_id, org_id)
        manager.add_event(event)
        assert manager.validate_consistency(agent_id) is True

    def test_consistent_after_multiple_events(self, manager, agent_id, org_id):
        """Multiple event window is consistent."""
        now = datetime.now(timezone.utc)
        for i in range(5):
            event = _make_event(
                agent_id, org_id,
                timestamp=now + timedelta(seconds=i * 5),
                prompt_tokens=100 + i * 10,
                completion_tokens=50 + i * 5,
                total_cost=Decimal(f"0.00{i + 1}"),
                latency_ms=100 + i * 50,
            )
            manager.add_event(event)

        assert manager.validate_consistency(agent_id) is True

    def test_consistent_after_eviction(self, manager, agent_id, org_id):
        """Window remains consistent after stale events are evicted."""
        now = datetime.now(timezone.utc)

        # Add old events that will be evicted
        for i in range(3):
            old_event = _make_event(
                agent_id, org_id,
                timestamp=now - timedelta(seconds=100 - i),
                prompt_tokens=500,
                completion_tokens=500,
                latency_ms=999,
            )
            manager.add_event(old_event)

        # Add new event to trigger eviction
        new_event = _make_event(agent_id, org_id, timestamp=now, latency_ms=50)
        manager.add_event(new_event)

        assert manager.validate_consistency(agent_id) is True

    def test_consistent_empty_window(self, manager, agent_id):
        """Empty window (after get_or_create) is consistent."""
        manager.get_or_create_window(agent_id)
        assert manager.validate_consistency(agent_id) is True


class TestValidateConsistencyTokens:
    """Tests for total_tokens consistency check."""

    def test_tokens_match_sum_of_prompt_and_completion(self, manager, agent_id, org_id):
        """total_tokens == sum(prompt_tokens + completion_tokens) for all events."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=100,
            completion_tokens=50,
        )
        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=5),
            prompt_tokens=200,
            completion_tokens=75,
        )

        manager.add_event(e1)
        manager.add_event(e2)

        # Should pass - total_tokens = (100+50) + (200+75) = 425
        assert manager.validate_consistency(agent_id) is True

    def test_detects_token_mismatch(self, manager, agent_id, org_id):
        """Detects if stored total_tokens doesn't match events."""
        event = _make_event(agent_id, org_id, prompt_tokens=100, completion_tokens=50)
        manager.add_event(event)

        # Corrupt the stored value
        manager._windows[agent_id].total_tokens = 999

        with pytest.raises(ValueError, match="total_tokens mismatch"):
            manager.validate_consistency(agent_id)


class TestValidateConsistencyCost:
    """Tests for total_cost consistency check."""

    def test_cost_matches_sum(self, manager, agent_id, org_id):
        """total_cost == sum of all event costs."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(
            agent_id, org_id,
            timestamp=now,
            total_cost=Decimal("0.005"),
        )
        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=5),
            total_cost=Decimal("0.010"),
        )

        manager.add_event(e1)
        manager.add_event(e2)

        assert manager.validate_consistency(agent_id) is True

    def test_detects_cost_mismatch(self, manager, agent_id, org_id):
        """Detects if stored total_cost doesn't match events."""
        event = _make_event(agent_id, org_id, total_cost=Decimal("0.005"))
        manager.add_event(event)

        # Corrupt the stored value
        manager._windows[agent_id].total_cost = Decimal("99.99")

        with pytest.raises(ValueError, match="total_cost mismatch"):
            manager.validate_consistency(agent_id)


class TestValidateConsistencyEventCount:
    """Tests for event_count consistency check."""

    def test_event_count_matches_len(self, manager, agent_id, org_id):
        """event_count == len(events)."""
        now = datetime.now(timezone.utc)
        for i in range(3):
            event = _make_event(
                agent_id, org_id, timestamp=now + timedelta(seconds=i)
            )
            manager.add_event(event)

        assert manager.validate_consistency(agent_id) is True

    def test_detects_event_count_mismatch(self, manager, agent_id, org_id):
        """Detects if stored event_count doesn't match actual count."""
        event = _make_event(agent_id, org_id)
        manager.add_event(event)

        # Corrupt the stored value
        manager._windows[agent_id].event_count = 42

        with pytest.raises(ValueError, match="event_count mismatch"):
            manager.validate_consistency(agent_id)


class TestValidateConsistencyLatency:
    """Tests for latency consistency checks."""

    def test_max_latency_matches(self, manager, agent_id, org_id):
        """max_latency_ms == max of all event latencies."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, latency_ms=100)
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=5), latency_ms=500
        )
        e3 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=10), latency_ms=200
        )

        manager.add_event(e1)
        manager.add_event(e2)
        manager.add_event(e3)

        assert manager.validate_consistency(agent_id) is True

    def test_detects_max_latency_mismatch(self, manager, agent_id, org_id):
        """Detects if stored max_latency_ms doesn't match events."""
        event = _make_event(agent_id, org_id, latency_ms=200)
        manager.add_event(event)

        # Corrupt the stored value
        manager._windows[agent_id].max_latency_ms = 9999

        with pytest.raises(ValueError, match="max_latency_ms mismatch"):
            manager.validate_consistency(agent_id)

    def test_avg_latency_matches(self, manager, agent_id, org_id):
        """avg_latency_ms == mean of all event latencies."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, latency_ms=100)
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=5), latency_ms=300
        )

        manager.add_event(e1)
        manager.add_event(e2)

        # avg = (100 + 300) / 2 = 200.0
        assert manager.validate_consistency(agent_id) is True

    def test_detects_avg_latency_mismatch(self, manager, agent_id, org_id):
        """Detects if stored avg_latency_ms doesn't match events."""
        event = _make_event(agent_id, org_id, latency_ms=200)
        manager.add_event(event)

        # Corrupt the stored value
        manager._windows[agent_id].avg_latency_ms = 555.5

        with pytest.raises(ValueError, match="avg_latency_ms mismatch"):
            manager.validate_consistency(agent_id)


class TestValidateConsistencyWindowBounds:
    """Tests for window duration boundary validation."""

    def test_all_events_within_window(self, manager, agent_id, org_id):
        """All events within window duration pass validation."""
        now = datetime.now(timezone.utc)
        for i in range(5):
            event = _make_event(
                agent_id, org_id, timestamp=now + timedelta(seconds=i * 10)
            )
            manager.add_event(event)

        assert manager.validate_consistency(agent_id) is True

    def test_detects_event_outside_window(self, manager, agent_id, org_id):
        """Detects if an event timestamp is outside the window duration."""
        now = datetime.now(timezone.utc)
        event = _make_event(
            agent_id, org_id, timestamp=now,
            prompt_tokens=100, completion_tokens=50,
            total_cost=Decimal("0.001"), latency_ms=200,
        )
        manager.add_event(event)

        # Manually inject a stale event into the deque (bypassing eviction)
        stale_event = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=120),
            prompt_tokens=100, completion_tokens=50,
            total_cost=Decimal("0.001"), latency_ms=200,
        )
        manager._events[agent_id].appendleft(stale_event)

        # Fix stored aggregates to match the injected events so the bounds
        # check is what actually triggers the failure
        window = manager._windows[agent_id]
        window.event_count = 2
        window.total_tokens = 300  # (100+50) * 2
        window.total_cost = Decimal("0.002")
        window.max_latency_ms = 200
        window.avg_latency_ms = 200.0

        with pytest.raises(ValueError, match="outside window duration"):
            manager.validate_consistency(agent_id)


class TestValidateConsistencyMultipleInconsistencies:
    """Tests for reporting multiple inconsistencies."""

    def test_reports_all_inconsistencies_at_once(self, manager, agent_id, org_id):
        """All inconsistencies are reported in a single error."""
        event = _make_event(
            agent_id, org_id,
            prompt_tokens=100,
            completion_tokens=50,
            total_cost=Decimal("0.001"),
            latency_ms=200,
        )
        manager.add_event(event)

        # Corrupt multiple values
        window = manager._windows[agent_id]
        window.total_tokens = 999
        window.total_cost = Decimal("99.99")
        window.max_latency_ms = 9999

        with pytest.raises(ValueError) as exc_info:
            manager.validate_consistency(agent_id)

        error_msg = str(exc_info.value)
        assert "total_tokens mismatch" in error_msg
        assert "total_cost mismatch" in error_msg
        assert "max_latency_ms mismatch" in error_msg
