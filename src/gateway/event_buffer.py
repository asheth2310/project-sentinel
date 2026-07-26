"""
In-memory event buffer for transient Kafka failures.

Buffers up to a configurable maximum number of events (default 10,000)
during transient Kafka broker unavailability. When the buffer is full,
new events are rejected and the gateway should return HTTP 503
Service Unavailable.

The buffer provides a flush mechanism that retries sending buffered
events when Kafka recovers.
"""

import asyncio
import logging
from collections import deque
from typing import Any

logger = logging.getLogger(__name__)


class EventBuffer:
    """
    Thread-safe in-memory buffer for telemetry events during Kafka outages.

    Uses collections.deque for O(1) append/popleft operations and an
    asyncio.Lock to guard concurrent access in async contexts.

    Attributes:
        max_size: Maximum number of events the buffer can hold (default 10,000).
    """

    def __init__(self, max_size: int = 10_000) -> None:
        """
        Initialize the event buffer.

        Args:
            max_size: Maximum number of events to buffer. When this limit
                      is reached, add() returns False indicating the buffer
                      is full and the caller should reject with 503.
        """
        if max_size <= 0:
            raise ValueError("max_size must be a positive integer")
        self._max_size = max_size
        self._buffer: deque[dict[str, Any]] = deque(maxlen=None)
        self._lock = asyncio.Lock()

    @property
    def max_size(self) -> int:
        """Maximum number of events the buffer can hold."""
        return self._max_size

    @property
    def is_full(self) -> bool:
        """Check if the buffer has reached its maximum capacity."""
        return len(self._buffer) >= self._max_size

    @property
    def size(self) -> int:
        """Current number of events in the buffer."""
        return len(self._buffer)

    async def add(self, event: dict[str, Any]) -> bool:
        """
        Add an event to the buffer.

        Args:
            event: Serialized event data (dict) to buffer.

        Returns:
            True if the event was successfully added to the buffer.
            False if the buffer is full (caller should return 503).
        """
        async with self._lock:
            if len(self._buffer) >= self._max_size:
                logger.warning(
                    "Event buffer full (%d/%d). Rejecting event.",
                    len(self._buffer),
                    self._max_size,
                )
                return False
            self._buffer.append(event)
            logger.debug(
                "Event buffered (%d/%d).",
                len(self._buffer),
                self._max_size,
            )
            return True

    async def drain(self) -> list[dict[str, Any]]:
        """
        Drain all events from the buffer and return them.

        Returns a list of all buffered events and clears the buffer.
        This is used when Kafka recovers to flush buffered events.

        Returns:
            List of all buffered event dicts. Empty list if buffer is empty.
        """
        async with self._lock:
            events = list(self._buffer)
            self._buffer.clear()
            if events:
                logger.info(
                    "Drained %d events from buffer.",
                    len(events),
                )
            return events

    async def flush(self, producer_fn) -> int:
        """
        Flush buffered events by sending them via the provided producer function.

        This method drains the buffer and attempts to send each event
        through the producer function. Events that fail to send are
        re-buffered for a subsequent retry.

        Args:
            producer_fn: An async callable that accepts a single event dict
                         and sends it to Kafka. Should raise an exception
                         on failure.

        Returns:
            Number of events successfully flushed.
        """
        events = await self.drain()
        if not events:
            return 0

        sent_count = 0
        failed_events: list[dict[str, Any]] = []

        for event in events:
            try:
                await producer_fn(event)
                sent_count += 1
            except Exception as exc:
                logger.warning(
                    "Failed to flush event from buffer: %s",
                    exc,
                )
                failed_events.append(event)

        # Re-buffer events that failed to send
        if failed_events:
            async with self._lock:
                for event in failed_events:
                    if len(self._buffer) < self._max_size:
                        self._buffer.appendleft(event)
                    else:
                        logger.error(
                            "Buffer full during re-buffer. Dropping event."
                        )
            logger.warning(
                "Re-buffered %d events that failed to flush.",
                len(failed_events),
            )

        if sent_count > 0:
            logger.info(
                "Successfully flushed %d/%d buffered events.",
                sent_count,
                len(events),
            )

        return sent_count
