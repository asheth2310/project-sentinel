"""
Unit tests for the EventBuffer class.

Tests cover:
- Basic add/drain operations
- Buffer capacity enforcement
- Thread-safe concurrent access
- Flush mechanism with producer retries
- Edge cases (empty buffer, max_size validation)
"""

import asyncio

import pytest

from src.gateway.event_buffer import EventBuffer


@pytest.fixture
def buffer():
    """Create an EventBuffer with small max_size for testing."""
    return EventBuffer(max_size=5)


@pytest.fixture
def default_buffer():
    """Create an EventBuffer with default max_size (10,000)."""
    return EventBuffer()


class TestEventBufferInit:
    """Tests for EventBuffer initialization."""

    def test_default_max_size(self, default_buffer):
        """Default buffer should have max_size of 10,000."""
        assert default_buffer.max_size == 10_000

    def test_custom_max_size(self, buffer):
        """Custom max_size should be respected."""
        assert buffer.max_size == 5

    def test_initial_size_is_zero(self, buffer):
        """New buffer should have size 0."""
        assert buffer.size == 0

    def test_initial_is_not_full(self, buffer):
        """New buffer should not be full."""
        assert buffer.is_full is False

    def test_invalid_max_size_zero(self):
        """max_size of 0 should raise ValueError."""
        with pytest.raises(ValueError, match="must be a positive integer"):
            EventBuffer(max_size=0)

    def test_invalid_max_size_negative(self):
        """Negative max_size should raise ValueError."""
        with pytest.raises(ValueError, match="must be a positive integer"):
            EventBuffer(max_size=-1)


class TestEventBufferAdd:
    """Tests for adding events to the buffer."""

    @pytest.mark.asyncio
    async def test_add_single_event(self, buffer):
        """Adding a single event should succeed and increase size."""
        event = {"agent_id": "agent-1", "tokens": 100}
        result = await buffer.add(event)
        assert result is True
        assert buffer.size == 1

    @pytest.mark.asyncio
    async def test_add_up_to_max(self, buffer):
        """Adding events up to max_size should all succeed."""
        for i in range(5):
            result = await buffer.add({"event_id": i})
            assert result is True
        assert buffer.size == 5
        assert buffer.is_full is True

    @pytest.mark.asyncio
    async def test_add_when_full_returns_false(self, buffer):
        """Adding to a full buffer should return False."""
        for i in range(5):
            await buffer.add({"event_id": i})

        result = await buffer.add({"event_id": "overflow"})
        assert result is False
        assert buffer.size == 5

    @pytest.mark.asyncio
    async def test_add_preserves_event_data(self, buffer):
        """Buffered events should retain their original data."""
        event = {"agent_id": "agent-1", "tokens": 500, "cost": 0.01}
        await buffer.add(event)
        events = await buffer.drain()
        assert events == [event]


class TestEventBufferDrain:
    """Tests for draining the buffer."""

    @pytest.mark.asyncio
    async def test_drain_empty_buffer(self, buffer):
        """Draining empty buffer should return empty list."""
        events = await buffer.drain()
        assert events == []

    @pytest.mark.asyncio
    async def test_drain_returns_all_events(self, buffer):
        """Drain should return all buffered events."""
        for i in range(3):
            await buffer.add({"event_id": i})

        events = await buffer.drain()
        assert len(events) == 3
        assert events == [{"event_id": 0}, {"event_id": 1}, {"event_id": 2}]

    @pytest.mark.asyncio
    async def test_drain_clears_buffer(self, buffer):
        """Buffer should be empty after drain."""
        for i in range(3):
            await buffer.add({"event_id": i})

        await buffer.drain()
        assert buffer.size == 0
        assert buffer.is_full is False

    @pytest.mark.asyncio
    async def test_drain_preserves_order(self, buffer):
        """Events should be drained in FIFO order."""
        for i in range(5):
            await buffer.add({"order": i})

        events = await buffer.drain()
        for i, event in enumerate(events):
            assert event["order"] == i

    @pytest.mark.asyncio
    async def test_add_after_drain(self, buffer):
        """Buffer should accept new events after being drained."""
        for i in range(5):
            await buffer.add({"event_id": i})
        assert buffer.is_full is True

        await buffer.drain()
        result = await buffer.add({"event_id": "new"})
        assert result is True
        assert buffer.size == 1


class TestEventBufferFlush:
    """Tests for the flush mechanism."""

    @pytest.mark.asyncio
    async def test_flush_empty_buffer(self, buffer):
        """Flushing empty buffer should return 0."""

        async def producer_fn(event):
            pass

        sent = await buffer.flush(producer_fn)
        assert sent == 0

    @pytest.mark.asyncio
    async def test_flush_all_success(self, buffer):
        """All events should be sent when producer succeeds."""
        sent_events = []

        async def producer_fn(event):
            sent_events.append(event)

        for i in range(3):
            await buffer.add({"event_id": i})

        sent = await buffer.flush(producer_fn)
        assert sent == 3
        assert buffer.size == 0
        assert len(sent_events) == 3

    @pytest.mark.asyncio
    async def test_flush_partial_failure_rebuffers(self, buffer):
        """Failed events should be re-buffered."""
        call_count = 0

        async def producer_fn(event):
            nonlocal call_count
            call_count += 1
            if event["event_id"] == 1:
                raise ConnectionError("Kafka unavailable")

        for i in range(3):
            await buffer.add({"event_id": i})

        sent = await buffer.flush(producer_fn)
        assert sent == 2
        assert buffer.size == 1
        # The failed event should be re-buffered
        remaining = await buffer.drain()
        assert remaining == [{"event_id": 1}]

    @pytest.mark.asyncio
    async def test_flush_all_failure_rebuffers_all(self, buffer):
        """All events should be re-buffered on total failure."""

        async def producer_fn(event):
            raise ConnectionError("Kafka unavailable")

        for i in range(3):
            await buffer.add({"event_id": i})

        sent = await buffer.flush(producer_fn)
        assert sent == 0
        assert buffer.size == 3


class TestEventBufferConcurrency:
    """Tests for concurrent buffer access."""

    @pytest.mark.asyncio
    async def test_concurrent_adds(self):
        """Concurrent adds should not exceed max_size."""
        buffer = EventBuffer(max_size=100)

        async def add_events(start, count):
            results = []
            for i in range(start, start + count):
                result = await buffer.add({"event_id": i})
                results.append(result)
            return results

        # Launch concurrent add tasks
        tasks = [add_events(i * 50, 50) for i in range(4)]
        results = await asyncio.gather(*tasks)

        # Flatten results
        all_results = [r for batch in results for r in batch]

        # Exactly 100 should succeed, 100 should fail
        assert sum(1 for r in all_results if r is True) == 100
        assert sum(1 for r in all_results if r is False) == 100
        assert buffer.size == 100

    @pytest.mark.asyncio
    async def test_concurrent_add_and_drain(self):
        """Add and drain operations should not corrupt buffer state."""
        buffer = EventBuffer(max_size=50)

        # Fill buffer partially
        for i in range(25):
            await buffer.add({"event_id": i})

        async def adder():
            for i in range(25, 75):
                await buffer.add({"event_id": i})

        async def drainer():
            await asyncio.sleep(0)  # yield control
            return await buffer.drain()

        await asyncio.gather(adder(), drainer())
        # Buffer should be in a valid state (size >= 0, <= max_size)
        assert 0 <= buffer.size <= 50


class TestEventBufferProperties:
    """Tests for buffer properties."""

    @pytest.mark.asyncio
    async def test_is_full_at_boundary(self):
        """is_full should be True exactly at max_size."""
        buffer = EventBuffer(max_size=3)

        await buffer.add({"event_id": 0})
        assert buffer.is_full is False

        await buffer.add({"event_id": 1})
        assert buffer.is_full is False

        await buffer.add({"event_id": 2})
        assert buffer.is_full is True

    @pytest.mark.asyncio
    async def test_size_tracks_additions(self):
        """Size should accurately reflect buffer contents."""
        buffer = EventBuffer(max_size=10)

        for i in range(7):
            await buffer.add({"event_id": i})
        assert buffer.size == 7

        await buffer.drain()
        assert buffer.size == 0
