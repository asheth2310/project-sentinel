"""Tests for RedisService with fail-open behavior."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as redis_exc

from src.config.settings import RedisSettings
from src.gateway.redis_service import RedisService


@pytest.fixture
def redis_settings():
    """Default Redis settings for testing."""
    return RedisSettings()


@pytest.fixture
def redis_service(redis_settings):
    """Create a RedisService instance without starting it."""
    return RedisService(redis_settings)


class TestRedisServiceInit:
    def test_initial_state(self, redis_service):
        assert redis_service.is_available is False
        assert redis_service._pool is None
        assert redis_service._client is None

    def test_settings_stored(self, redis_settings):
        service = RedisService(redis_settings)
        assert service._settings is redis_settings


class TestRedisServiceStart:
    @pytest.mark.asyncio
    async def test_start_success(self, redis_service):
        """Successful start sets available to True."""
        with patch("src.gateway.redis_service.ConnectionPool") as mock_pool_cls, \
             patch("src.gateway.redis_service.Redis") as mock_redis_cls:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(return_value=True)
            mock_redis_cls.return_value = mock_client

            await redis_service.start()

            assert redis_service.is_available is True
            mock_pool_cls.assert_called_once_with(
                host="localhost",
                port=6379,
                db=0,
                password=None,
                max_connections=20,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
                decode_responses=True,
            )

    @pytest.mark.asyncio
    async def test_start_connection_error_fails_open(self, redis_service):
        """Start with connection error sets available to False (fail-open)."""
        with patch("src.gateway.redis_service.ConnectionPool") as mock_pool_cls, \
             patch("src.gateway.redis_service.Redis") as mock_redis_cls:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(
                side_effect=redis_exc.ConnectionError("Connection refused")
            )
            mock_redis_cls.return_value = mock_client

            await redis_service.start()

            assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_start_timeout_error_fails_open(self, redis_service):
        """Start with timeout sets available to False (fail-open)."""
        with patch("src.gateway.redis_service.ConnectionPool") as mock_pool_cls, \
             patch("src.gateway.redis_service.Redis") as mock_redis_cls:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(
                side_effect=redis_exc.TimeoutError("Timeout")
            )
            mock_redis_cls.return_value = mock_client

            await redis_service.start()

            assert redis_service.is_available is False


class TestRedisServiceStop:
    @pytest.mark.asyncio
    async def test_stop_closes_resources(self, redis_service):
        """Stop closes client and pool."""
        mock_client = AsyncMock()
        mock_pool = AsyncMock()
        redis_service._client = mock_client
        redis_service._pool = mock_pool
        redis_service._available = True

        await redis_service.stop()

        mock_client.aclose.assert_called_once()
        mock_pool.aclose.assert_called_once()
        assert redis_service._client is None
        assert redis_service._pool is None
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_stop_without_start(self, redis_service):
        """Stop on unstarted service is safe."""
        await redis_service.stop()
        assert redis_service.is_available is False


class TestRedisServiceHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_success(self, redis_service):
        """Health check returns True when Redis is healthy."""
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(return_value=True)
        redis_service._client = mock_client

        result = await redis_service.health_check()

        assert result is True
        assert redis_service.is_available is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, redis_service):
        """Health check returns False and marks unavailable on error."""
        mock_client = AsyncMock()
        mock_client.ping = AsyncMock(
            side_effect=redis_exc.ConnectionError("Connection lost")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.health_check()

        assert result is False
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_health_check_no_client(self, redis_service):
        """Health check returns False when no client exists."""
        result = await redis_service.health_check()

        assert result is False
        assert redis_service.is_available is False


class TestRedisServiceGet:
    @pytest.mark.asyncio
    async def test_get_returns_value(self, redis_service):
        """GET returns the stored value."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value="active")
        redis_service._client = mock_client

        result = await redis_service.get("circuit_breaker:agent-123")

        assert result == "active"
        assert redis_service.is_available is True
        mock_client.get.assert_called_once_with("circuit_breaker:agent-123")

    @pytest.mark.asyncio
    async def test_get_returns_none_for_missing_key(self, redis_service):
        """GET returns None for non-existent key."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=None)
        redis_service._client = mock_client

        result = await redis_service.get("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_fails_open_on_connection_error(self, redis_service):
        """GET returns None on connection error (fail-open)."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=redis_exc.ConnectionError("Connection refused")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.get("circuit_breaker:agent-123")

        assert result is None
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_get_fails_open_on_timeout(self, redis_service):
        """GET returns None on timeout (fail-open)."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=redis_exc.TimeoutError("Timed out")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.get("some_key")

        assert result is None
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_get_no_client_fails_open(self, redis_service):
        """GET returns None when no client (fail-open)."""
        result = await redis_service.get("some_key")
        assert result is None


class TestRedisServiceSet:
    @pytest.mark.asyncio
    async def test_set_without_ttl(self, redis_service):
        """SET stores value without TTL."""
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        redis_service._client = mock_client

        result = await redis_service.set("key", "value")

        assert result is True
        assert redis_service.is_available is True
        mock_client.set.assert_called_once_with("key", "value")

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, redis_service):
        """SET stores value with TTL."""
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        redis_service._client = mock_client

        result = await redis_service.set("key", "value", ttl=300)

        assert result is True
        mock_client.set.assert_called_once_with("key", "value", ex=300)

    @pytest.mark.asyncio
    async def test_set_fails_open_on_connection_error(self, redis_service):
        """SET returns False on connection error (fail-open)."""
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(
            side_effect=redis_exc.ConnectionError("Connection refused")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.set("key", "value")

        assert result is False
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_set_fails_open_on_timeout(self, redis_service):
        """SET returns False on timeout (fail-open)."""
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(
            side_effect=redis_exc.TimeoutError("Timed out")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.set("key", "value")

        assert result is False
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_set_no_client_fails_open(self, redis_service):
        """SET returns False when no client (fail-open)."""
        result = await redis_service.set("key", "value")
        assert result is False


class TestRedisServiceDelete:
    @pytest.mark.asyncio
    async def test_delete_success(self, redis_service):
        """DELETE removes key and returns True."""
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(return_value=1)
        redis_service._client = mock_client

        result = await redis_service.delete("key")

        assert result is True
        assert redis_service.is_available is True
        mock_client.delete.assert_called_once_with("key")

    @pytest.mark.asyncio
    async def test_delete_fails_open_on_connection_error(self, redis_service):
        """DELETE returns False on connection error (fail-open)."""
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(
            side_effect=redis_exc.ConnectionError("Connection refused")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.delete("key")

        assert result is False
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_delete_fails_open_on_timeout(self, redis_service):
        """DELETE returns False on timeout (fail-open)."""
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(
            side_effect=redis_exc.TimeoutError("Timed out")
        )
        redis_service._client = mock_client
        redis_service._available = True

        result = await redis_service.delete("key")

        assert result is False
        assert redis_service.is_available is False

    @pytest.mark.asyncio
    async def test_delete_no_client_fails_open(self, redis_service):
        """DELETE returns False when no client (fail-open)."""
        result = await redis_service.delete("key")
        assert result is False
