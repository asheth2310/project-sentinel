"""Tests for the RetentionManager (Task 50).

Tests configurable data retention policies and automated purge logic.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from src.storage.retention import RetentionManager


class MockPool:
    """Mock asyncpg pool for testing retention manager."""

    def __init__(self, execute_result="DELETE 5", fetchval_raises=True):
        self._execute_result = execute_result
        self._fetchval_raises = fetchval_raises
        self.conn = MockConnection(execute_result, fetchval_raises)

    def acquire(self):
        return MockAcquireContext(self.conn)


class MockAcquireContext:
    """Mock async context manager."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        pass


class MockConnection:
    """Mock asyncpg connection with execute and fetchval."""

    def __init__(self, execute_result="DELETE 5", fetchval_raises=True):
        self._execute_result = execute_result
        self._fetchval_raises = fetchval_raises
        self.execute_calls: list = []
        self.fetchval_calls: list = []

    async def execute(self, query, *args):
        self.execute_calls.append((query, args))
        return self._execute_result

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if self._fetchval_raises:
            raise Exception("Not a hypertable")
        return 3


@pytest.fixture
def mock_pool():
    """Create a mock pool that returns DELETE results."""
    return MockPool(execute_result="DELETE 10")


@pytest.fixture
def retention(mock_pool):
    """Create a RetentionManager with mock pool."""
    return RetentionManager(db_pool=mock_pool, default_retention_days=90)


class TestRetentionManager:
    """Tests for RetentionManager purge operations."""

    def test_init_default_retention(self, retention):
        """Default retention period is set correctly."""
        assert retention.default_retention_days == 90

    def test_init_custom_retention(self, mock_pool):
        """Custom retention period is stored."""
        manager = RetentionManager(db_pool=mock_pool, default_retention_days=30)
        assert manager.default_retention_days == 30

    def test_set_retention_days(self, retention):
        """Retention days can be updated."""
        retention.default_retention_days = 60
        assert retention.default_retention_days == 60

    def test_set_retention_days_invalid(self, retention):
        """Setting non-positive retention raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            retention.default_retention_days = 0

        with pytest.raises(ValueError, match="must be positive"):
            retention.default_retention_days = -10

    def test_retention_targets_defined(self):
        """All expected tables are defined as retention targets."""
        assert "telemetry_logs" in RetentionManager.RETENTION_TARGETS
        assert "anomaly_events" in RetentionManager.RETENTION_TARGETS
        assert "audit_log" in RetentionManager.RETENTION_TARGETS

    @pytest.mark.asyncio
    async def test_purge_old_data_uses_default_days(self, retention, mock_pool):
        """purge_old_data uses default retention when no arg provided."""
        result = await retention.purge_old_data()
        # 3 tables × 10 rows each = 30 total
        assert result == 30

    @pytest.mark.asyncio
    async def test_purge_old_data_custom_days(self, retention, mock_pool):
        """purge_old_data accepts custom retention_days override."""
        result = await retention.purge_old_data(retention_days=7)
        # Should still work - 3 tables × 10 rows = 30
        assert result == 30

    @pytest.mark.asyncio
    async def test_purge_old_data_executes_delete(self, retention, mock_pool):
        """Purge executes DELETE queries for each table."""
        await retention.purge_old_data()
        # Each table should have had execute called (after fetchval fails)
        assert len(mock_pool.conn.execute_calls) == 3

    @pytest.mark.asyncio
    async def test_purge_table_specific(self, retention, mock_pool):
        """purge_table targets a single table."""
        result = await retention.purge_table("telemetry_logs")
        assert result == 10
        assert len(mock_pool.conn.execute_calls) == 1

    @pytest.mark.asyncio
    async def test_purge_table_invalid_raises(self, retention):
        """purge_table raises ValueError for unknown tables."""
        with pytest.raises(ValueError, match="Unknown retention target"):
            await retention.purge_table("nonexistent_table")

    @pytest.mark.asyncio
    async def test_purge_uses_drop_chunks_for_hypertable(self):
        """When drop_chunks succeeds, it's used instead of DELETE."""
        pool = MockPool(fetchval_raises=False)
        manager = RetentionManager(db_pool=pool, default_retention_days=90)

        result = await manager.purge_table("telemetry_logs")
        # fetchval returns 3 (chunks dropped)
        assert result == 3
        # DELETE should not have been called
        assert len(pool.conn.execute_calls) == 0

    @pytest.mark.asyncio
    async def test_purge_handles_db_errors_gracefully(self):
        """Database errors for one table don't stop others."""

        class FailingPool:
            def __init__(self):
                self._call_count = 0

            def acquire(self):
                self._call_count += 1
                return FailingContext(self._call_count)

        class FailingContext:
            def __init__(self, call_count):
                self._call_count = call_count

            async def __aenter__(self):
                if self._call_count == 1:
                    raise ConnectionError("DB error")
                return SuccessConn()

            async def __aexit__(self, *args):
                pass

        class SuccessConn:
            async def fetchval(self, *args):
                raise Exception("Not hypertable")

            async def execute(self, *args):
                return "DELETE 5"

        pool = FailingPool()
        manager = RetentionManager(db_pool=pool, default_retention_days=90)
        # Should not raise even if first table fails
        result = await manager.purge_old_data()
        # Only 2 tables succeed × 5 rows = 10
        assert result == 10

    @pytest.mark.asyncio
    async def test_purge_delete_zero_rows(self):
        """Handles DELETE 0 result correctly."""
        pool = MockPool(execute_result="DELETE 0")
        manager = RetentionManager(db_pool=pool, default_retention_days=90)
        result = await manager.purge_old_data()
        assert result == 0
