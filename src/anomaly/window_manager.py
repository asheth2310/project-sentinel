"""Sliding window manager for per-agent telemetry aggregation.

Implements the Sliding Window Update Algorithm from the design document.
Handles event addition, time-based eviction, and metric recomputation
for real-time anomaly detection (Requirement 7).
"""

from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

from src.models.telemetry import TelemetryEvent
from src.models.window import WindowState


class WindowManager:
    """Manages per-agent sliding windows for anomaly detection.

    Maintains a time-bounded window of telemetry events per agent,
    automatically evicting stale events and recomputing aggregate
    metrics on each update.

    Attributes:
        window_duration_seconds: Duration of the sliding window in seconds.
    """

    def __init__(self, window_duration_seconds: int = 60) -> None:
        if window_duration_seconds <= 0:
            raise ValueError("window_duration_seconds must be positive")
        self.window_duration_seconds = window_duration_seconds
        self._windows: dict[UUID, WindowState] = {}
        self._events: dict[UUID, deque[TelemetryEvent]] = {}

    def get_or_create_window(self, agent_id: UUID) -> WindowState:
        """Get existing window for agent or create a new empty one."""
        if agent_id not in self._windows:
            now = datetime.now(timezone.utc)
            self._windows[agent_id] = WindowState(
                agent_id=agent_id,
                window_start=now,
                window_end=now,
            )
            self._events[agent_id] = deque()
        return self._windows[agent_id]

    def add_event(self, event: TelemetryEvent) -> WindowState:
        """Add event to agent's window, evict stale entries, recompute metrics.

        This is the main entry point for the sliding window update algorithm.
        Events are expected to arrive roughly in order, though late arrivals
        within the window duration are handled correctly.

        Args:
            event: A validated TelemetryEvent to incorporate.

        Returns:
            Updated WindowState with recomputed aggregates.
        """
        agent_id = event.agent_id

        # Ensure window exists
        self.get_or_create_window(agent_id)

        # Add event to the deque
        self._events[agent_id].append(event)

        # Evict stale events based on the latest timestamp
        event_ts = event.timestamp
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)
        self._evict_stale_events(agent_id, event_ts)

        # Recompute all metrics from the remaining events
        return self._recompute_metrics(agent_id)

    def _evict_stale_events(self, agent_id: UUID, current_time: datetime) -> None:
        """Remove events older than window_duration from the window.

        Events are evicted from the left of the deque (oldest first)
        based on the cutoff time derived from current_time.

        Args:
            agent_id: The agent whose window to evict from.
            current_time: The reference time (usually the latest event timestamp).
        """
        cutoff = current_time - timedelta(seconds=self.window_duration_seconds)
        events = self._events[agent_id]

        while events:
            oldest = events[0]
            oldest_ts = oldest.timestamp
            if oldest_ts.tzinfo is None:
                oldest_ts = oldest_ts.replace(tzinfo=timezone.utc)
            if oldest_ts < cutoff:
                events.popleft()
            else:
                break

    def validate_consistency(self, agent_id: UUID) -> bool:
        """Verify that stored WindowState aggregates match recomputed values from events.

        Returns True if consistent, raises ValueError with details if inconsistent.
        Used for debugging and property-based testing.

        Validates (Requirement 7.4):
        - total_tokens matches sum of (prompt_tokens + completion_tokens) for all events
        - total_cost matches sum of all event costs
        - event_count matches len(events)
        - max_latency_ms matches max of all event latencies
        - avg_latency_ms matches mean of all event latencies
        - All events are within the window duration
        """
        if agent_id not in self._windows:
            raise ValueError(f"No window exists for agent_id={agent_id}")

        window = self._windows[agent_id]
        events = self._events[agent_id]

        # Check event_count
        actual_event_count = len(events)
        if window.event_count != actual_event_count:
            raise ValueError(
                f"event_count mismatch: stored={window.event_count}, "
                f"actual={actual_event_count}"
            )

        if actual_event_count == 0:
            # Empty window should have zeroed aggregates
            inconsistencies = []
            if window.total_tokens != 0:
                inconsistencies.append(
                    f"total_tokens should be 0, got {window.total_tokens}"
                )
            if window.total_cost != Decimal("0"):
                inconsistencies.append(
                    f"total_cost should be 0, got {window.total_cost}"
                )
            if window.max_latency_ms != 0:
                inconsistencies.append(
                    f"max_latency_ms should be 0, got {window.max_latency_ms}"
                )
            if window.avg_latency_ms != 0.0:
                inconsistencies.append(
                    f"avg_latency_ms should be 0.0, got {window.avg_latency_ms}"
                )
            if inconsistencies:
                raise ValueError(
                    "Empty window has non-zero aggregates: "
                    + "; ".join(inconsistencies)
                )
            return True

        # Recompute aggregates from events
        expected_total_tokens = 0
        expected_total_cost = Decimal("0")
        expected_max_latency_ms = 0
        total_latency_ms = 0

        for evt in events:
            expected_total_tokens += evt.prompt_tokens + evt.completion_tokens
            expected_total_cost += evt.total_cost
            expected_max_latency_ms = max(expected_max_latency_ms, evt.latency_ms)
            total_latency_ms += evt.latency_ms

        expected_avg_latency_ms = total_latency_ms / actual_event_count

        # Collect all inconsistencies
        inconsistencies = []

        if window.total_tokens != expected_total_tokens:
            inconsistencies.append(
                f"total_tokens mismatch: stored={window.total_tokens}, "
                f"expected={expected_total_tokens}"
            )

        if window.total_cost != expected_total_cost:
            inconsistencies.append(
                f"total_cost mismatch: stored={window.total_cost}, "
                f"expected={expected_total_cost}"
            )

        if window.max_latency_ms != expected_max_latency_ms:
            inconsistencies.append(
                f"max_latency_ms mismatch: stored={window.max_latency_ms}, "
                f"expected={expected_max_latency_ms}"
            )

        if abs(window.avg_latency_ms - expected_avg_latency_ms) > 1e-9:
            inconsistencies.append(
                f"avg_latency_ms mismatch: stored={window.avg_latency_ms}, "
                f"expected={expected_avg_latency_ms}"
            )

        # Validate all events are within window duration
        cutoff = window.window_end - timedelta(seconds=self.window_duration_seconds)
        for i, evt in enumerate(events):
            evt_ts = evt.timestamp
            if evt_ts.tzinfo is None:
                evt_ts = evt_ts.replace(tzinfo=timezone.utc)
            if evt_ts < cutoff:
                inconsistencies.append(
                    f"Event at index {i} (timestamp={evt_ts}) is outside "
                    f"window duration (cutoff={cutoff})"
                )

        if inconsistencies:
            raise ValueError(
                "Window consistency check failed: " + "; ".join(inconsistencies)
            )

        return True

    def _recompute_metrics(self, agent_id: UUID) -> WindowState:
        """Recompute all window metrics from contained events.

        Rebuilds aggregate metrics from scratch to ensure consistency
        after eviction. Tracks:
        - total_tokens, total_cost, event_count
        - unique_tool_calls, unique_prompt_hashes
        - max_latency_ms, avg_latency_ms
        - token_growth_rate (tokens per second)
        - consecutive_identical_calls and last_tool_name

        Args:
            agent_id: The agent whose metrics to recompute.

        Returns:
            The updated WindowState stored for this agent.
        """
        events = self._events[agent_id]

        if not events:
            # Window is empty after eviction
            now = datetime.now(timezone.utc)
            window = WindowState(
                agent_id=agent_id,
                window_start=now,
                window_end=now,
            )
            self._windows[agent_id] = window
            return window

        # Compute aggregates
        total_tokens = 0
        total_cost = Decimal("0")
        unique_tool_calls: set[str] = set()
        unique_prompt_hashes: set[str] = set()
        max_latency_ms = 0
        total_latency_ms = 0
        token_sum_squares = 0.0
        event_count = len(events)

        # Track consecutive identical calls by iterating in order
        consecutive_identical_calls = 0
        last_tool_name: Optional[str] = None

        for evt in events:
            tokens = evt.prompt_tokens + evt.completion_tokens
            total_tokens += tokens
            token_sum_squares += float(tokens) ** 2
            total_cost += evt.total_cost
            max_latency_ms = max(max_latency_ms, evt.latency_ms)
            total_latency_ms += evt.latency_ms

            if evt.tool_name is not None:
                unique_tool_calls.add(evt.tool_name)
                if evt.tool_name == last_tool_name:
                    consecutive_identical_calls += 1
                else:
                    consecutive_identical_calls = 1
                last_tool_name = evt.tool_name
            else:
                # No tool_name: reset consecutive tracking
                consecutive_identical_calls = 0
                last_tool_name = None

            if evt.prompt_hash is not None:
                unique_prompt_hashes.add(evt.prompt_hash)

        # Determine window boundaries from actual events
        timestamps = []
        for evt in events:
            ts = evt.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            timestamps.append(ts)

        window_end = max(timestamps)
        window_start = window_end - timedelta(seconds=self.window_duration_seconds)

        # Compute avg latency
        avg_latency_ms = total_latency_ms / event_count if event_count > 0 else 0.0

        # Compute token growth rate (tokens per second)
        elapsed = (window_end - window_start).total_seconds()
        token_growth_rate = total_tokens / elapsed if elapsed > 0 else 0.0

        window = WindowState(
            agent_id=agent_id,
            window_start=window_start,
            window_end=window_end,
            total_tokens=total_tokens,
            total_cost=total_cost,
            event_count=event_count,
            unique_tool_calls=unique_tool_calls,
            unique_prompt_hashes=unique_prompt_hashes,
            max_latency_ms=max_latency_ms,
            avg_latency_ms=avg_latency_ms,
            token_growth_rate=token_growth_rate,
            consecutive_identical_calls=consecutive_identical_calls,
            last_tool_name=last_tool_name,
            token_sum_squares=token_sum_squares,
        )
        self._windows[agent_id] = window
        return window
