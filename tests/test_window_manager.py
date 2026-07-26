"""Tests for WindowManager sliding window aggregation.

Validates Requirements 7, 4, 5:
- Sliding window update with event addition and metric recomputation
- Time-based eviction of stale events
- Consecutive identical tool call tracking (Requirement 4)
- Token growth rate computation (Requirement 5)
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


class TestWindowManagerInit:
    """Tests for WindowManager initialization."""

    def test_default_window_duration(self):
        mgr = WindowManager()
        assert mgr.window_duration_seconds == 60

    def test_custom_window_duration(self):
        mgr = WindowManager(window_duration_seconds=120)
        assert mgr.window_duration_seconds == 120

    def test_invalid_window_duration_zero(self):
        with pytest.raises(ValueError, match="positive"):
            WindowManager(window_duration_seconds=0)

    def test_invalid_window_duration_negative(self):
        with pytest.raises(ValueError, match="positive"):
            WindowManager(window_duration_seconds=-10)


class TestGetOrCreateWindow:
    """Tests for get_or_create_window."""

    def test_creates_new_window(self, manager, agent_id):
        window = manager.get_or_create_window(agent_id)
        assert window.agent_id == agent_id
        assert window.total_tokens == 0
        assert window.event_count == 0
        assert window.total_cost == Decimal("0")

    def test_returns_existing_window(self, manager, agent_id, org_id):
        # Create by adding an event
        event = _make_event(agent_id, org_id, prompt_tokens=50, completion_tokens=25)
        manager.add_event(event)

        # Get should return the same window
        window = manager.get_or_create_window(agent_id)
        assert window.event_count == 1
        assert window.total_tokens == 75

    def test_separate_windows_per_agent(self, manager, org_id):
        agent1 = uuid4()
        agent2 = uuid4()

        manager.get_or_create_window(agent1)
        manager.get_or_create_window(agent2)

        assert manager._windows[agent1].agent_id == agent1
        assert manager._windows[agent2].agent_id == agent2


class TestAddEvent:
    """Tests for add_event - basic event addition and metric updates."""

    def test_single_event_updates_totals(self, manager, agent_id, org_id):
        event = _make_event(
            agent_id, org_id, prompt_tokens=100, completion_tokens=50
        )
        window = manager.add_event(event)

        assert window.total_tokens == 150
        assert window.event_count == 1

    def test_single_event_updates_cost(self, manager, agent_id, org_id):
        event = _make_event(agent_id, org_id, total_cost=Decimal("0.005"))
        window = manager.add_event(event)

        assert window.total_cost == Decimal("0.005")

    def test_multiple_events_accumulate_tokens(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, prompt_tokens=100, completion_tokens=50)
        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=5),
            prompt_tokens=200, completion_tokens=100,
        )

        manager.add_event(e1)
        window = manager.add_event(e2)

        assert window.total_tokens == 450  # (100+50) + (200+100)
        assert window.event_count == 2

    def test_multiple_events_accumulate_cost(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, total_cost=Decimal("0.001"))
        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=5),
            total_cost=Decimal("0.002"),
        )

        manager.add_event(e1)
        window = manager.add_event(e2)

        assert window.total_cost == Decimal("0.003")

    def test_max_latency_tracked(self, manager, agent_id, org_id):
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
        window = manager.add_event(e3)

        assert window.max_latency_ms == 500

    def test_avg_latency_computed(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, latency_ms=100)
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=5), latency_ms=300
        )

        manager.add_event(e1)
        window = manager.add_event(e2)

        assert window.avg_latency_ms == 200.0  # (100 + 300) / 2

    def test_unique_tool_calls_tracked(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, tool_name="search")
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=5), tool_name="search"
        )
        e3 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=10), tool_name="write"
        )

        manager.add_event(e1)
        manager.add_event(e2)
        window = manager.add_event(e3)

        assert window.unique_tool_calls == {"search", "write"}

    def test_unique_prompt_hashes_tracked(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, prompt_hash="abc123")
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=5), prompt_hash="abc123"
        )
        e3 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=10), prompt_hash="def456"
        )

        manager.add_event(e1)
        manager.add_event(e2)
        window = manager.add_event(e3)

        assert window.unique_prompt_hashes == {"abc123", "def456"}

    def test_window_end_matches_latest_event(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now)
        e2 = _make_event(agent_id, org_id, timestamp=now + timedelta(seconds=30))

        manager.add_event(e1)
        window = manager.add_event(e2)

        assert window.window_end == now + timedelta(seconds=30)


class TestTimeBasedEviction:
    """Tests for time-based event eviction."""

    def test_old_events_evicted(self, manager, agent_id, org_id):
        """Events older than window_duration are removed."""
        now = datetime.now(timezone.utc)
        old_event = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=90),
            prompt_tokens=100, completion_tokens=100,
        )
        new_event = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=50, completion_tokens=50,
        )

        manager.add_event(old_event)
        window = manager.add_event(new_event)

        # Old event should be evicted (> 60s before now)
        assert window.event_count == 1
        assert window.total_tokens == 100  # only new event's tokens

    def test_boundary_event_not_evicted(self, manager, agent_id, org_id):
        """Events exactly at the boundary are kept."""
        now = datetime.now(timezone.utc)
        boundary_event = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=60),
            prompt_tokens=100, completion_tokens=0,
        )
        new_event = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=50, completion_tokens=0,
        )

        manager.add_event(boundary_event)
        window = manager.add_event(new_event)

        # Boundary event (exactly at cutoff) should be kept
        assert window.event_count == 2
        assert window.total_tokens == 150

    def test_multiple_old_events_evicted(self, manager, agent_id, org_id):
        """Multiple stale events are all evicted."""
        now = datetime.now(timezone.utc)
        events = [
            _make_event(agent_id, org_id, timestamp=now - timedelta(seconds=100)),
            _make_event(agent_id, org_id, timestamp=now - timedelta(seconds=80)),
            _make_event(agent_id, org_id, timestamp=now - timedelta(seconds=70)),
            _make_event(agent_id, org_id, timestamp=now),
        ]

        for e in events:
            window = manager.add_event(e)

        assert window.event_count == 1  # only the last event remains

    def test_eviction_updates_aggregates(self, manager, agent_id, org_id):
        """After eviction, aggregates reflect only remaining events."""
        now = datetime.now(timezone.utc)
        old = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=90),
            latency_ms=999,
            total_cost=Decimal("1.0"),
        )
        new = _make_event(
            agent_id, org_id,
            timestamp=now,
            latency_ms=100,
            total_cost=Decimal("0.01"),
        )

        manager.add_event(old)
        window = manager.add_event(new)

        # Old event evicted - aggregates only reflect new event
        assert window.max_latency_ms == 100
        assert window.total_cost == Decimal("0.01")

    def test_custom_window_duration_eviction(self, agent_id, org_id):
        """Custom window duration is respected for eviction."""
        mgr = WindowManager(window_duration_seconds=10)
        now = datetime.now(timezone.utc)

        old = _make_event(agent_id, org_id, timestamp=now - timedelta(seconds=15))
        new = _make_event(agent_id, org_id, timestamp=now)

        mgr.add_event(old)
        window = mgr.add_event(new)

        assert window.event_count == 1


class TestConsecutiveIdenticalCalls:
    """Tests for consecutive identical tool call tracking (Requirement 4)."""

    def test_single_tool_call_sets_count_to_one(self, manager, agent_id, org_id):
        event = _make_event(agent_id, org_id, tool_name="search")
        window = manager.add_event(event)

        assert window.consecutive_identical_calls == 1
        assert window.last_tool_name == "search"

    def test_consecutive_same_tool_increments(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        for i in range(5):
            event = _make_event(
                agent_id, org_id,
                timestamp=now + timedelta(seconds=i),
                tool_name="search",
            )
            window = manager.add_event(event)

        assert window.consecutive_identical_calls == 5
        assert window.last_tool_name == "search"

    def test_different_tool_resets_count(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, tool_name="search")
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=1), tool_name="search"
        )
        e3 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=2), tool_name="write"
        )

        manager.add_event(e1)
        manager.add_event(e2)
        window = manager.add_event(e3)

        assert window.consecutive_identical_calls == 1
        assert window.last_tool_name == "write"

    def test_no_tool_name_resets_count(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, tool_name="search")
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=1), tool_name=None
        )

        manager.add_event(e1)
        window = manager.add_event(e2)

        assert window.consecutive_identical_calls == 0
        assert window.last_tool_name is None

    def test_consecutive_after_no_tool(self, manager, agent_id, org_id):
        now = datetime.now(timezone.utc)
        e1 = _make_event(agent_id, org_id, timestamp=now, tool_name=None)
        e2 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=1), tool_name="read"
        )
        e3 = _make_event(
            agent_id, org_id, timestamp=now + timedelta(seconds=2), tool_name="read"
        )

        manager.add_event(e1)
        manager.add_event(e2)
        window = manager.add_event(e3)

        assert window.consecutive_identical_calls == 2
        assert window.last_tool_name == "read"

    def test_eviction_resets_consecutive_count(self, manager, agent_id, org_id):
        """When old consecutive events are evicted, count is recomputed."""
        now = datetime.now(timezone.utc)
        # Add old consecutive calls that will be evicted
        for i in range(3):
            event = _make_event(
                agent_id, org_id,
                timestamp=now - timedelta(seconds=90 - i),
                tool_name="search",
            )
            manager.add_event(event)

        # Add a new event with different tool
        new_event = _make_event(agent_id, org_id, timestamp=now, tool_name="write")
        window = manager.add_event(new_event)

        # Old events evicted, only new event remains
        assert window.consecutive_identical_calls == 1
        assert window.last_tool_name == "write"


class TestTokenGrowthRate:
    """Tests for token growth rate computation (Requirement 5)."""

    def test_growth_rate_single_event(self, manager, agent_id, org_id):
        """Single event has growth rate = total_tokens / window_duration."""
        event = _make_event(agent_id, org_id, prompt_tokens=600, completion_tokens=0)
        window = manager.add_event(event)

        # window_duration = 60s, total_tokens = 600
        # rate = 600 / 60 = 10.0
        assert window.token_growth_rate == 10.0

    def test_growth_rate_multiple_events(self, manager, agent_id, org_id):
        """Growth rate accumulates tokens over elapsed time."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=300, completion_tokens=0,
        )
        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=30),
            prompt_tokens=300, completion_tokens=0,
        )

        manager.add_event(e1)
        window = manager.add_event(e2)

        # total_tokens = 600, elapsed = window_duration = 60s
        # rate = 600 / 60 = 10.0
        assert window.token_growth_rate == 10.0

    def test_growth_rate_increases_with_more_tokens(self, manager, agent_id, org_id):
        """Adding more tokens increases the growth rate."""
        now = datetime.now(timezone.utc)
        e1 = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=100, completion_tokens=0,
        )
        window1 = manager.add_event(e1)

        e2 = _make_event(
            agent_id, org_id,
            timestamp=now + timedelta(seconds=10),
            prompt_tokens=500, completion_tokens=0,
        )
        window2 = manager.add_event(e2)

        assert window2.token_growth_rate > window1.token_growth_rate

    def test_growth_rate_after_eviction(self, manager, agent_id, org_id):
        """Growth rate is recomputed after old events are evicted."""
        now = datetime.now(timezone.utc)

        # Old event with lots of tokens (will be evicted)
        old = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=90),
            prompt_tokens=10000, completion_tokens=0,
        )
        manager.add_event(old)

        # New event with few tokens
        new = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=60, completion_tokens=0,
        )
        window = manager.add_event(new)

        # Only new event remains: 60 tokens / 60s = 1.0
        assert window.token_growth_rate == 1.0

    def test_growth_rate_uses_window_duration_as_elapsed(self, agent_id, org_id):
        """Elapsed time for growth rate is window_duration (window_end - window_start)."""
        mgr = WindowManager(window_duration_seconds=30)
        now = datetime.now(timezone.utc)

        event = _make_event(
            agent_id, org_id,
            timestamp=now,
            prompt_tokens=300, completion_tokens=0,
        )
        window = mgr.add_event(event)

        # 300 tokens / 30s = 10.0
        assert window.token_growth_rate == 10.0


class TestWindowConsistency:
    """Tests for window state consistency after operations."""

    def test_empty_window_after_all_evicted(self, manager, agent_id, org_id):
        """If all events are evicted, window resets to empty state."""
        now = datetime.now(timezone.utc)
        old = _make_event(
            agent_id, org_id,
            timestamp=now - timedelta(seconds=120),
        )
        manager.add_event(old)

        # Add event far in the future to evict the old one
        new = _make_event(agent_id, org_id, timestamp=now)
        window = manager.add_event(new)

        # Only the new event remains
        assert window.event_count == 1

    def test_window_start_is_end_minus_duration(self, manager, agent_id, org_id):
        """window_start = window_end - window_duration_seconds."""
        now = datetime.now(timezone.utc)
        event = _make_event(agent_id, org_id, timestamp=now)
        window = manager.add_event(event)

        expected_start = now - timedelta(seconds=60)
        assert window.window_start == expected_start
        assert window.window_end == now

    def test_none_tool_not_added_to_unique_set(self, manager, agent_id, org_id):
        """Events with tool_name=None don't add None to unique_tool_calls."""
        event = _make_event(agent_id, org_id, tool_name=None)
        window = manager.add_event(event)

        assert len(window.unique_tool_calls) == 0

    def test_none_prompt_hash_not_added_to_unique_set(self, manager, agent_id, org_id):
        """Events with prompt_hash=None don't add None to unique_prompt_hashes."""
        event = _make_event(agent_id, org_id, prompt_hash=None)
        window = manager.add_event(event)

        assert len(window.unique_prompt_hashes) == 0
