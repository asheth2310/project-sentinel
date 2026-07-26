"""Property-based tests for Project Sentinel using Hypothesis.

Validates Requirements 1 and 7:
- Ingestion idempotency (batch deduplication)
- Window consistency (sliding window aggregation invariants)
- Telemetry model validation roundtrip (serialization/deserialization)

**Validates: Requirements 1.7, 7**
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import (
    booleans,
    composite,
    datetimes,
    integers,
    just,
    lists,
    none,
    one_of,
    text,
    uuids,
)

from src.anomaly.window_manager import WindowManager
from src.gateway.deduplication import DeduplicationService
from src.models.telemetry import TelemetryEvent


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@composite
def decimal_costs(draw):
    """Strategy to generate valid Decimal cost values (0 to 999, max 6 decimal places)."""
    # Generate an integer and divide by 1_000_000 to ensure at most 6 decimal places
    raw = draw(integers(min_value=0, max_value=999_999_999))
    return Decimal(raw) / Decimal("1000000")


@composite
def telemetry_events(draw, agent_id=None, org_id=None):
    """Strategy to generate valid TelemetryEvent instances.

    All generated events have timestamps within the last 60 seconds to
    ensure they pass the 'not in the future' validator and stay within
    a reasonable sliding window.
    """
    now = datetime.now(timezone.utc)

    # Generate timestamp within the last 60 seconds (well within bounds)
    offset_seconds = draw(integers(min_value=0, max_value=55))
    ts = now - timedelta(seconds=offset_seconds)

    _agent_id = agent_id if agent_id is not None else draw(uuids())
    _org_id = org_id if org_id is not None else draw(uuids())

    prompt_tokens = draw(integers(min_value=0, max_value=10000))
    completion_tokens = draw(integers(min_value=0, max_value=10000))
    total_cost = draw(decimal_costs())
    latency_ms = draw(integers(min_value=0, max_value=5000))

    # Optional tool_name: either None or a short alphanumeric string
    tool_name: Optional[str] = draw(
        one_of(
            none(),
            text(
                alphabet="abcdefghijklmnopqrstuvwxyz_",
                min_size=1,
                max_size=20,
            ),
        )
    )

    # Optional prompt_hash
    prompt_hash: Optional[str] = draw(
        one_of(
            none(),
            text(
                alphabet="0123456789abcdef",
                min_size=32,
                max_size=32,
            ),
        )
    )

    return TelemetryEvent(
        timestamp=ts,
        agent_id=_agent_id,
        org_id=_org_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_cost=total_cost,
        latency_ms=latency_ms,
        tool_name=tool_name,
        prompt_hash=prompt_hash,
    )


# ---------------------------------------------------------------------------
# Test 1: Window consistency property
# ---------------------------------------------------------------------------


class TestWindowConsistency:
    """Property: window is always consistent regardless of event sequence.

    **Validates: Requirements 7**
    """

    @given(events=lists(telemetry_events(), min_size=1, max_size=50))
    @settings(max_examples=100, deadline=None)
    def test_window_always_consistent_after_events(self, events: list[TelemetryEvent]):
        """For any sequence of valid TelemetryEvents added to a WindowManager,
        the resulting WindowState always passes validate_consistency().
        """
        # Use a single agent_id for all events in this test
        agent_id = events[0].agent_id

        # Rewrite all events to share the same agent_id
        normalized_events = []
        for evt in events:
            normalized_events.append(
                TelemetryEvent(
                    timestamp=evt.timestamp,
                    agent_id=agent_id,
                    org_id=evt.org_id,
                    prompt_tokens=evt.prompt_tokens,
                    completion_tokens=evt.completion_tokens,
                    total_cost=evt.total_cost,
                    latency_ms=evt.latency_ms,
                    tool_name=evt.tool_name,
                    prompt_hash=evt.prompt_hash,
                )
            )

        wm = WindowManager(window_duration_seconds=60)

        for event in normalized_events:
            wm.add_event(event)

        # The window must always be consistent after processing all events
        assert wm.validate_consistency(agent_id) is True


# ---------------------------------------------------------------------------
# Test 2: Window idempotency under eviction
# ---------------------------------------------------------------------------


class TestWindowEviction:
    """Property: event_count always matches events within the time window after eviction.

    **Validates: Requirements 7**
    """

    @given(events=lists(telemetry_events(), min_size=1, max_size=50))
    @settings(max_examples=100, deadline=None)
    def test_window_event_count_matches_retained_events(
        self, events: list[TelemetryEvent]
    ):
        """After adding events and triggering eviction, the window's event_count
        always matches the number of events within the time window.

        Events are sorted by timestamp before insertion to simulate realistic
        in-order arrival, which is the documented assumption of the WindowManager.
        """
        agent_id = events[0].agent_id

        # Normalize all events to the same agent_id and sort by timestamp
        # (WindowManager expects roughly in-order arrival)
        normalized_events = []
        for evt in events:
            normalized_events.append(
                TelemetryEvent(
                    timestamp=evt.timestamp,
                    agent_id=agent_id,
                    org_id=evt.org_id,
                    prompt_tokens=evt.prompt_tokens,
                    completion_tokens=evt.completion_tokens,
                    total_cost=evt.total_cost,
                    latency_ms=evt.latency_ms,
                    tool_name=evt.tool_name,
                    prompt_hash=evt.prompt_hash,
                )
            )
        normalized_events.sort(key=lambda e: e.timestamp)

        window_duration = 30  # Use a shorter window to encourage more eviction
        wm = WindowManager(window_duration_seconds=window_duration)

        window_state = None
        for event in normalized_events:
            window_state = wm.add_event(event)

        # After all events are added, the event_count in the window state
        # must equal the actual number of retained events in the internal deque
        assert window_state is not None
        actual_events_in_deque = len(wm._events[agent_id])
        assert window_state.event_count == actual_events_in_deque

        # With in-order arrival, all retained events must be within the window
        if actual_events_in_deque > 0:
            cutoff = window_state.window_end - timedelta(seconds=window_duration)
            for evt in wm._events[agent_id]:
                evt_ts = evt.timestamp
                if evt_ts.tzinfo is None:
                    evt_ts = evt_ts.replace(tzinfo=timezone.utc)
                assert evt_ts >= cutoff


# ---------------------------------------------------------------------------
# Test 3: Telemetry model validation roundtrip
# ---------------------------------------------------------------------------


class TestTelemetryRoundtrip:
    """Property: any valid TelemetryEvent survives JSON serialization roundtrip.

    **Validates: Requirements 1.7**
    """

    @given(event=telemetry_events())
    @settings(max_examples=100, deadline=None)
    def test_telemetry_event_serialization_roundtrip(self, event: TelemetryEvent):
        """Any TelemetryEvent that passes validation can be serialized to JSON
        and deserialized back without data loss.
        """
        # Serialize to JSON
        json_str = event.model_dump_json()

        # Deserialize back
        restored = TelemetryEvent.model_validate_json(json_str)

        # All fields must match exactly
        assert restored.timestamp == event.timestamp
        assert restored.log_id == event.log_id
        assert restored.agent_id == event.agent_id
        assert restored.org_id == event.org_id
        assert restored.prompt_tokens == event.prompt_tokens
        assert restored.completion_tokens == event.completion_tokens
        assert restored.total_cost == event.total_cost
        assert restored.latency_ms == event.latency_ms
        assert restored.tool_name == event.tool_name
        assert restored.prompt_hash == event.prompt_hash
        assert restored.session_id == event.session_id


# ---------------------------------------------------------------------------
# Test 4: Deduplication idempotency
# ---------------------------------------------------------------------------


class TestDeduplicationIdempotency:
    """Property: marking a batch_id as processed and checking is_duplicate
    always returns True for the same batch_id.

    **Validates: Requirements 1.7**
    """

    @given(batch_id=uuids())
    @settings(max_examples=100, deadline=None)
    def test_mark_processed_then_is_duplicate(self, batch_id: UUID):
        """Marking a batch_id as processed and checking is_duplicate always
        returns True for the same batch_id.
        """
        # Create a mock Redis service that stores values in memory
        store: dict[str, str] = {}

        mock_redis = AsyncMock()

        async def mock_get(key: str):
            return store.get(key)

        async def mock_set(key: str, value, ttl=None):
            store[key] = value
            return True

        mock_redis.get = AsyncMock(side_effect=mock_get)
        mock_redis.set = AsyncMock(side_effect=mock_set)

        dedup = DeduplicationService(redis_service=mock_redis)

        loop = asyncio.new_event_loop()
        try:
            # Mark as processed
            loop.run_until_complete(dedup.mark_processed(batch_id))

            # Check is_duplicate - must always return True
            result = loop.run_until_complete(dedup.is_duplicate(batch_id))
            assert result is True
        finally:
            loop.close()

    @given(batch_id=uuids())
    @settings(max_examples=100, deadline=None)
    def test_unprocessed_batch_is_not_duplicate(self, batch_id: UUID):
        """A batch_id that was never marked as processed should not be
        detected as a duplicate.
        """
        store: dict[str, str] = {}

        mock_redis = AsyncMock()

        async def mock_get(key: str):
            return store.get(key)

        mock_redis.get = AsyncMock(side_effect=mock_get)

        dedup = DeduplicationService(redis_service=mock_redis)

        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(dedup.is_duplicate(batch_id))
            assert result is False
        finally:
            loop.close()
