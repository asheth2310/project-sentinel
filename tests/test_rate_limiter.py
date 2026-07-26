"""Tests for RateLimiter with Redis sliding window implementation."""

import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
import redis.asyncio as redis_exc

from src.config.settings import RedisSettings
from src.gateway.rate_limiter import RateLimiter, RateLimitResult
from src.gateway.redis_service import RedisService


@pytest.fixture
def redis_settings():
    """Default Redis settings for testing."""
    return RedisSettings()


@pytest.fixture
def redis_service(redis_settings):
    """Create a RedisService instance with mocked client."""
    service = RedisService(redis_settings)
    service._available = True
    service._client = AsyncMock()
    return service


@pytest.fixture
def rate_limiter(redis_service):
    """Create a RateLimiter with default settings."""
    return RateLimiter(
        redis_service=redis_service,
        agent_limit=100,
        agent_window=60,
        org_limit=1000,
        org_window=60,
    )


@pytest.fixture
def agent_id():
    return uuid4()


@pytest.fixture
def org_id():
    return uuid4()


class TestRateLimitResult:
    def test_result_is_frozen(self):
        """RateLimitResult is immutable."""
        result = RateLimitResult(
            allowed=True,
            remaining=99,
            reset_at=datetime.now(timezone.utc),
        )
        with pytest.raises(Exception):
            result.allowed = False

    def test_result_fields(self):
        """RateLimitResult stores all fields."""
        now = datetime.now(timezone.utc)
        result = RateLimitResult(allowed=False, remaining=0, reset_at=now)
        assert result.allowed is False
        assert result.remaining == 0
        assert result.reset_at == now


class TestRateLimiterInit:
    def test_default_limits(self, redis_service):
        """Default limits are set correctly."""
        limiter = RateLimiter(redis_service)
        assert limiter._agent_limit == 100
        assert limiter._agent_window == 60
        assert limiter._org_limit == 1000
        assert limiter._org_window == 60

    def test_custom_limits(self, redis_service):
        """Custom limits are stored."""
        limiter = RateLimiter(
            redis_service,
            agent_limit=50,
            agent_window=30,
            org_limit=500,
            org_window=120,
        )
        assert limiter._agent_limit == 50
        assert limiter._agent_window == 30
        assert limiter._org_limit == 500
        assert limiter._org_window == 120


class TestRateLimiterFailOpen:
    @pytest.mark.asyncio
    async def test_fails_open_when_client_is_none(
        self, redis_settings, agent_id, org_id
    ):
        """Returns allowed=True when Redis client is None."""
        service = RedisService(redis_settings)
        # Client is None (not started)
        limiter = RateLimiter(service)

        result = await limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True
        assert result.remaining == 100

    @pytest.mark.asyncio
    async def test_fails_open_when_redis_unavailable(
        self, redis_service, agent_id, org_id
    ):
        """Returns allowed=True when Redis is marked unavailable."""
        redis_service._available = False
        limiter = RateLimiter(redis_service)

        result = await limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_fails_open_on_connection_error(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Returns allowed=True on Redis ConnectionError."""
        mock_pipe = AsyncMock()
        mock_pipe.zremrangebyscore = MagicMock()
        mock_pipe.zcard = MagicMock()
        mock_pipe.execute = AsyncMock(
            side_effect=redis_exc.ConnectionError("Connection refused")
        )
        redis_service._client.pipeline = MagicMock(return_value=mock_pipe)

        result = await rate_limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True
        assert redis_service._available is False

    @pytest.mark.asyncio
    async def test_fails_open_on_timeout_error(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Returns allowed=True on Redis TimeoutError."""
        mock_pipe = AsyncMock()
        mock_pipe.zremrangebyscore = MagicMock()
        mock_pipe.zcard = MagicMock()
        mock_pipe.execute = AsyncMock(
            side_effect=redis_exc.TimeoutError("Timeout")
        )
        redis_service._client.pipeline = MagicMock(return_value=mock_pipe)

        result = await rate_limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True
        assert redis_service._available is False


class TestRateLimiterAgentLimit:
    @pytest.mark.asyncio
    async def test_allows_request_under_limit(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Allows requests when agent is under the limit."""
        # Agent pipeline: zremrangebyscore + zcard returns count=5
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 5])

        # Org pipeline: same pattern, count=10
        org_pipe = AsyncMock()
        org_pipe.zremrangebyscore = MagicMock()
        org_pipe.zcard = MagicMock()
        org_pipe.execute = AsyncMock(return_value=[0, 10])

        # Add pipeline for zadd + expire
        add_pipe = AsyncMock()
        add_pipe.zadd = MagicMock()
        add_pipe.expire = MagicMock()
        add_pipe.execute = AsyncMock(return_value=[1, True])

        # Pipeline calls: first for agent check, second for agent add,
        # third for org check, fourth for org add
        redis_service._client.pipeline = MagicMock(
            side_effect=[agent_pipe, add_pipe, org_pipe, add_pipe]
        )

        result = await rate_limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True
        # Agent: 100 - 5 - 1 = 94
        assert result.remaining == 94

    @pytest.mark.asyncio
    async def test_denies_request_at_agent_limit(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Denies requests when agent has reached the limit."""
        # Agent pipeline: count=100 (at limit)
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 100])

        redis_service._client.pipeline = MagicMock(return_value=agent_pipe)

        # Oldest entry for reset_at calculation
        oldest_time = time.time() - 30  # 30 seconds ago
        redis_service._client.zrange = AsyncMock(
            return_value=[("member", oldest_time)]
        )

        result = await rate_limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is False
        assert result.remaining == 0
        # Reset at should be oldest_time + window (60)
        expected_reset = datetime.fromtimestamp(
            oldest_time + 60, tz=timezone.utc
        )
        assert abs((result.reset_at - expected_reset).total_seconds()) < 1


class TestRateLimiterOrgLimit:
    @pytest.mark.asyncio
    async def test_denies_request_at_org_limit(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Denies requests when org has reached the limit."""
        # Agent passes: count=5
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 5])

        # Agent add pipeline
        agent_add_pipe = AsyncMock()
        agent_add_pipe.zadd = MagicMock()
        agent_add_pipe.expire = MagicMock()
        agent_add_pipe.execute = AsyncMock(return_value=[1, True])

        # Org exceeds: count=1000
        org_pipe = AsyncMock()
        org_pipe.zremrangebyscore = MagicMock()
        org_pipe.zcard = MagicMock()
        org_pipe.execute = AsyncMock(return_value=[0, 1000])

        redis_service._client.pipeline = MagicMock(
            side_effect=[agent_pipe, agent_add_pipe, org_pipe]
        )

        # Oldest entry for org
        oldest_time = time.time() - 45
        redis_service._client.zrange = AsyncMock(
            return_value=[("member", oldest_time)]
        )

        result = await rate_limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is False
        assert result.remaining == 0


class TestRateLimiterKeyPatterns:
    @pytest.mark.asyncio
    async def test_agent_key_pattern(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Uses correct key pattern for agent rate limits."""
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 0])

        add_pipe = AsyncMock()
        add_pipe.zadd = MagicMock()
        add_pipe.expire = MagicMock()
        add_pipe.execute = AsyncMock(return_value=[1, True])

        org_pipe = AsyncMock()
        org_pipe.zremrangebyscore = MagicMock()
        org_pipe.zcard = MagicMock()
        org_pipe.execute = AsyncMock(return_value=[0, 0])

        org_add_pipe = AsyncMock()
        org_add_pipe.zadd = MagicMock()
        org_add_pipe.expire = MagicMock()
        org_add_pipe.execute = AsyncMock(return_value=[1, True])

        redis_service._client.pipeline = MagicMock(
            side_effect=[agent_pipe, add_pipe, org_pipe, org_add_pipe]
        )

        await rate_limiter.check_rate_limit(agent_id, org_id)

        # Verify agent key was used in zremrangebyscore
        expected_agent_key = f"rate_limit:agent:{agent_id}"
        agent_pipe.zremrangebyscore.assert_called_once()
        call_args = agent_pipe.zremrangebyscore.call_args
        assert call_args[0][0] == expected_agent_key

    @pytest.mark.asyncio
    async def test_org_key_pattern(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Uses correct key pattern for org rate limits."""
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 0])

        add_pipe = AsyncMock()
        add_pipe.zadd = MagicMock()
        add_pipe.expire = MagicMock()
        add_pipe.execute = AsyncMock(return_value=[1, True])

        org_pipe = AsyncMock()
        org_pipe.zremrangebyscore = MagicMock()
        org_pipe.zcard = MagicMock()
        org_pipe.execute = AsyncMock(return_value=[0, 0])

        org_add_pipe = AsyncMock()
        org_add_pipe.zadd = MagicMock()
        org_add_pipe.expire = MagicMock()
        org_add_pipe.execute = AsyncMock(return_value=[1, True])

        redis_service._client.pipeline = MagicMock(
            side_effect=[agent_pipe, add_pipe, org_pipe, org_add_pipe]
        )

        await rate_limiter.check_rate_limit(agent_id, org_id)

        # Verify org key was used
        expected_org_key = f"rate_limit:org:{org_id}"
        org_pipe.zremrangebyscore.assert_called_once()
        call_args = org_pipe.zremrangebyscore.call_args
        assert call_args[0][0] == expected_org_key


class TestRateLimiterResetAt:
    @pytest.mark.asyncio
    async def test_reset_at_when_denied_with_empty_set(
        self, rate_limiter, redis_service, agent_id, org_id
    ):
        """Reset time defaults to now + window when sorted set is empty."""
        # Agent at limit but no oldest entries returned
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 100])

        redis_service._client.pipeline = MagicMock(return_value=agent_pipe)
        redis_service._client.zrange = AsyncMock(return_value=[])

        before = time.time()
        result = await rate_limiter.check_rate_limit(agent_id, org_id)
        after = time.time()

        assert result.allowed is False
        # reset_at should be approximately now + 60s
        reset_ts = result.reset_at.timestamp()
        assert before + 60 <= reset_ts <= after + 60 + 1


class TestRateLimiterReturnsMostRestrictive:
    @pytest.mark.asyncio
    async def test_returns_agent_result_when_more_restrictive(
        self, redis_service, agent_id, org_id
    ):
        """When agent has fewer remaining than org, returns agent result."""
        limiter = RateLimiter(
            redis_service, agent_limit=10, agent_window=60,
            org_limit=1000, org_window=60
        )

        # Agent: 8 requests used (2 remaining after adding)
        agent_pipe = AsyncMock()
        agent_pipe.zremrangebyscore = MagicMock()
        agent_pipe.zcard = MagicMock()
        agent_pipe.execute = AsyncMock(return_value=[0, 8])

        agent_add_pipe = AsyncMock()
        agent_add_pipe.zadd = MagicMock()
        agent_add_pipe.expire = MagicMock()
        agent_add_pipe.execute = AsyncMock(return_value=[1, True])

        # Org: 50 requests used (949 remaining after adding)
        org_pipe = AsyncMock()
        org_pipe.zremrangebyscore = MagicMock()
        org_pipe.zcard = MagicMock()
        org_pipe.execute = AsyncMock(return_value=[0, 50])

        org_add_pipe = AsyncMock()
        org_add_pipe.zadd = MagicMock()
        org_add_pipe.expire = MagicMock()
        org_add_pipe.execute = AsyncMock(return_value=[1, True])

        redis_service._client.pipeline = MagicMock(
            side_effect=[agent_pipe, agent_add_pipe, org_pipe, org_add_pipe]
        )

        result = await limiter.check_rate_limit(agent_id, org_id)

        assert result.allowed is True
        # Agent remaining: 10 - 8 - 1 = 1
        assert result.remaining == 1
