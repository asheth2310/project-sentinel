"""Tests for the StorageWriterConsumer (Task 49).

Tests batch buffering and flush logic for writing telemetry to TimescaleDB.
"""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from src.config.settings import KafkaSettings
from src.storage.consumer import StorageWriterConsumer


class MockPool:
    """Mock asyncpg pool for testing."""

    def __init__(self):
        self.conn = MockConnection()
        self._acquired = False

    def acquire(self):
        return MockAcquireContext(self.conn)


class MockAcquireContext:
    """Mock async context manager for pool.acquire()."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        pass


class MockConnection:
    """Mock asyncpg connection."""

    def __init__(self):
        self.executemany_calls: list = []

    async def executemany(self, query, args):
        self.executemany_calls.append((query, args))


@pytest.fixture
def kafka_settings():
    """Create KafkaSettings with defaults for testing."""
    return KafkaSettings()


@pytest.fixture
def mock_pool():
    """Create a mock database pool."""
    return MockPool()


@pytest.fixture
def consumer(kafka_settings, mock_pool):
    """Create a StorageWriterConsumer with small batch size for testing."""
    return StorageWriterConsumer(
        settings=kafka_settings,
        db_pool=mock_pool,
        batch_size=3,
        flush_interval_seconds=60.0,
    )


class TestStorageWriterConsumer:
    """Tests for StorageWriterConsumer message processing and batching."""

    def test_init_correct_topic_and_group(self, consumer, kafka_settings):
        """Consumer subscribes to telemetry-enriched with storage group."""
        assert consumer._topic == kafka_settings.topic_telemetry_enriched
        assert consumer._group_id == kafka_settings.consumer_group_storage

    def test_init_empty_batch(self, consumer):
        """Consumer starts with empty batch."""
        assert consumer.pending_batch_size == 0
        assert consumer.total_written == 0
        assert consumer.total_errors == 0

    @pytest.mark.asyncio
    async def test_process_message_buffers(self, consumer):
        """Messages are buffered until batch_size is reached."""
        msg = _make_telemetry_message()
        await consumer.process_message(msg)
        assert consumer.pending_batch_size == 1

        await consumer.process_message(msg)
        assert consumer.pending_batch_size == 2

    @pytest.mark.asyncio
    async def test_flush_on_batch_full(self, consumer, mock_pool):
        """Batch is flushed when batch_size messages are buffered."""
        msg = _make_telemetry_message()

        # Process batch_size messages (3)
        await consumer.process_message(msg)
        await consumer.process_message(msg)
        await consumer.process_message(msg)

        # Batch should have been flushed
        assert consumer.pending_batch_size == 0
        assert consumer.total_written == 3
        assert len(mock_pool.conn.executemany_calls) == 1

    @pytest.mark.asyncio
    async def test_flush_batch_directly(self, consumer, mock_pool):
        """_flush_batch writes pending messages to database."""
        msg = _make_telemetry_message()
        await consumer.process_message(msg)
        await consumer.process_message(msg)

        # Force flush
        await consumer._flush_batch()

        assert consumer.pending_batch_size == 0
        assert consumer.total_written == 2

    @pytest.mark.asyncio
    async def test_flush_empty_batch_no_op(self, consumer, mock_pool):
        """Flushing an empty batch does nothing."""
        await consumer._flush_batch()
        assert consumer.total_written == 0
        assert len(mock_pool.conn.executemany_calls) == 0

    @pytest.mark.asyncio
    async def test_parse_row_complete_message(self, consumer):
        """A complete message is parsed into the correct tuple."""
        msg = _make_telemetry_message()
        row = consumer._parse_row(msg)

        assert row is not None
        assert len(row) == 11
        assert isinstance(row[0], datetime)  # timestamp
        assert row[4] == 100  # prompt_tokens
        assert row[5] == 200  # completion_tokens
        assert isinstance(row[6], Decimal)  # total_cost
        assert row[7] == 150  # latency_ms

    @pytest.mark.asyncio
    async def test_parse_row_handles_string_uuids(self, consumer):
        """String UUIDs are parsed correctly."""
        agent_id = uuid4()
        msg = _make_telemetry_message()
        msg["agent_id"] = str(agent_id)
        row = consumer._parse_row(msg)
        assert row is not None
        assert row[2] == agent_id  # agent_id

    @pytest.mark.asyncio
    async def test_parse_row_handles_missing_optionals(self, consumer):
        """Missing optional fields are set to None."""
        msg = _make_telemetry_message()
        del msg["tool_name"]
        del msg["prompt_hash"]
        del msg["session_id"]
        row = consumer._parse_row(msg)
        assert row is not None
        assert row[8] is None  # tool_name
        assert row[9] is None  # prompt_hash
        assert row[10] is None  # session_id

    @pytest.mark.asyncio
    async def test_parse_row_invalid_message(self, consumer):
        """Invalid messages return None instead of raising."""
        # agent_id is not a valid UUID
        msg = {"agent_id": "not-a-uuid"}
        row = consumer._parse_row(msg)
        assert row is None

    @pytest.mark.asyncio
    async def test_flush_batch_db_error_counts_errors(self, consumer):
        """Database errors increment error counter and discard batch."""
        # Replace pool with one that raises
        class FailingPool:
            def acquire(self):
                return FailingContext()

        class FailingContext:
            async def __aenter__(self):
                raise ConnectionError("DB unavailable")

            async def __aexit__(self, *args):
                pass

        consumer._db_pool = FailingPool()

        msg = _make_telemetry_message()
        consumer._batch = [msg, msg]
        await consumer._flush_batch()

        assert consumer.total_errors == 2
        assert consumer.total_written == 0
        assert consumer.pending_batch_size == 0

    @pytest.mark.asyncio
    async def test_multiple_flushes_accumulate_total(self, consumer, mock_pool):
        """Multiple flushes accumulate the total written count."""
        msg = _make_telemetry_message()

        # First batch
        for _ in range(3):
            await consumer.process_message(msg)
        assert consumer.total_written == 3

        # Second batch
        for _ in range(3):
            await consumer.process_message(msg)
        assert consumer.total_written == 6


def _make_telemetry_message() -> dict:
    """Create a sample telemetry message for testing."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "log_id": str(uuid4()),
        "agent_id": str(uuid4()),
        "org_id": str(uuid4()),
        "prompt_tokens": 100,
        "completion_tokens": 200,
        "total_cost": "0.003000",
        "latency_ms": 150,
        "tool_name": "code_search",
        "prompt_hash": "abc123",
        "session_id": str(uuid4()),
    }
