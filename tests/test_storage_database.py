"""Tests for the DatabaseManager (Task 54).

Tests separate connection pool management for ingestion writes and query reads.
"""

import pytest

from src.config.settings import DatabaseSettings
from src.storage.database import DatabaseManager


class TestDatabaseManager:
    """Tests for DatabaseManager initialization and pool access."""

    def test_init_not_started(self):
        """DatabaseManager initializes in stopped state."""
        settings = DatabaseSettings()
        manager = DatabaseManager(settings)
        assert manager.is_started is False

    def test_write_pool_raises_before_start(self):
        """Accessing write_pool before start() raises RuntimeError."""
        settings = DatabaseSettings()
        manager = DatabaseManager(settings)
        with pytest.raises(RuntimeError, match="Write pool not initialized"):
            _ = manager.write_pool

    def test_read_pool_raises_before_start(self):
        """Accessing read_pool before start() raises RuntimeError."""
        settings = DatabaseSettings()
        manager = DatabaseManager(settings)
        with pytest.raises(RuntimeError, match="Read pool not initialized"):
            _ = manager.read_pool

    def test_pool_settings_stored(self):
        """Settings are stored on initialization."""
        settings = DatabaseSettings(
            host="custom-host",
            port=5433,
            name="test_db",
            user="tester",
            password="secret",
            min_pool_size=3,
            max_pool_size=15,
        )
        manager = DatabaseManager(settings)
        assert manager._settings.host == "custom-host"
        assert manager._settings.port == 5433
        assert manager._settings.name == "test_db"
        assert manager._settings.max_pool_size == 15

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """Stopping when not started should be a no-op."""
        settings = DatabaseSettings()
        manager = DatabaseManager(settings)
        # Should not raise
        await manager.stop()
        assert manager.is_started is False

    @pytest.mark.asyncio
    async def test_start_sets_pools_with_mock(self, monkeypatch):
        """Start creates both pools when asyncpg is available."""

        class MockPool:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            async def close(self):
                pass

        async def mock_create_pool(**kwargs):
            return MockPool(**kwargs)

        # Mock asyncpg.create_pool
        import types
        mock_asyncpg = types.ModuleType("asyncpg")
        mock_asyncpg.create_pool = mock_create_pool
        monkeypatch.setitem(
            __import__("sys").modules, "asyncpg", mock_asyncpg
        )

        settings = DatabaseSettings(min_pool_size=5, max_pool_size=20)
        manager = DatabaseManager(settings)
        await manager.start()

        assert manager.is_started is True
        assert manager._write_pool is not None
        assert manager._read_pool is not None

        # Cleanup
        await manager.stop()
        assert manager.is_started is False
        assert manager._write_pool is None
        assert manager._read_pool is None
