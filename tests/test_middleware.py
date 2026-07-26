"""Tests for circuit breaker check middleware.

Validates Requirement 3: Circuit Breaker Enforcement at Ingestion.
- 3.1 Before processing any batch, gateway checks Redis for circuit breaker state
- 3.2 If active, returns HTTP 429 Too Many Requests
- 3.4 If Redis unavailable, fails open (accepts telemetry) and logs warning
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from src.gateway.circuit_breaker import CircuitBreakerService
from src.gateway.middleware import CircuitBreakerMiddleware, _DEFAULT_RETRY_AFTER_SECONDS


@pytest.fixture
def mock_redis_service():
    """Mock RedisService for testing."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=True)
    return redis


@pytest.fixture
def circuit_breaker_service(mock_redis_service):
    """CircuitBreakerService with mocked Redis."""
    return CircuitBreakerService(mock_redis_service)


@pytest.fixture
def middleware(circuit_breaker_service):
    """CircuitBreakerMiddleware instance."""
    return CircuitBreakerMiddleware(circuit_breaker_service)


@pytest.fixture
def agent_id():
    return uuid4()


class TestCircuitBreakerMiddlewareCheck:
    """Tests for the check() method (direct agent_id invocation)."""

    @pytest.mark.asyncio
    async def test_allows_request_when_breaker_inactive(self, middleware, agent_id):
        """Request passes through when circuit breaker is not active (Req 3.1)."""
        # Default mock returns None from Redis.get -> check_agent_status returns False
        result = await middleware.check(agent_id)
        # Should return None (no exception) indicating request is allowed
        assert result is None

    @pytest.mark.asyncio
    async def test_raises_429_when_breaker_active(
        self, middleware, mock_redis_service, agent_id
    ):
        """Returns HTTP 429 when circuit breaker is active (Req 3.2)."""
        # Simulate active circuit breaker in Redis
        active_state = json.dumps({
            "agent_id": str(agent_id),
            "is_active": True,
            "activated_at": "2024-01-01T00:00:00+00:00",
            "activated_by": "system",
            "reason": "token spike",
            "ttl_seconds": None,
        })
        mock_redis_service.get = AsyncMock(return_value=active_state)

        with pytest.raises(HTTPException) as exc_info:
            await middleware.check(agent_id)

        assert exc_info.value.status_code == 429
        assert exc_info.value.detail["error"] == "circuit_breaker_active"
        assert str(agent_id) in exc_info.value.detail["message"]
        assert exc_info.value.detail["agent_id"] == str(agent_id)

    @pytest.mark.asyncio
    async def test_429_response_includes_retry_after_header(
        self, middleware, mock_redis_service, agent_id
    ):
        """429 response includes Retry-After header (Req 3.2)."""
        active_state = json.dumps({
            "agent_id": str(agent_id),
            "is_active": True,
            "activated_at": "2024-01-01T00:00:00+00:00",
            "activated_by": "system",
            "reason": "cost overrun",
            "ttl_seconds": None,
        })
        mock_redis_service.get = AsyncMock(return_value=active_state)

        with pytest.raises(HTTPException) as exc_info:
            await middleware.check(agent_id)

        assert "Retry-After" in exc_info.value.headers
        assert exc_info.value.headers["Retry-After"] == str(_DEFAULT_RETRY_AFTER_SECONDS)

    @pytest.mark.asyncio
    async def test_fails_open_when_redis_unavailable(
        self, middleware, mock_redis_service, agent_id
    ):
        """Fails open (accepts telemetry) when Redis is unavailable (Req 3.4)."""
        # RedisService.get returns None when Redis is unavailable (fail-open)
        mock_redis_service.get = AsyncMock(return_value=None)

        # Should NOT raise — request is allowed
        result = await middleware.check(agent_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_allows_request_when_breaker_state_is_inactive(
        self, middleware, mock_redis_service, agent_id
    ):
        """Explicitly inactive circuit breaker allows request."""
        inactive_state = json.dumps({
            "agent_id": str(agent_id),
            "is_active": False,
            "activated_at": None,
            "activated_by": "admin",
            "reason": "deactivated",
            "ttl_seconds": None,
        })
        mock_redis_service.get = AsyncMock(return_value=inactive_state)

        result = await middleware.check(agent_id)
        assert result is None


class TestCircuitBreakerMiddlewareCallable:
    """Tests for the __call__ method (middleware-style with request body parsing)."""

    def _make_request(self, body: dict | None = None, body_bytes: bytes | None = None):
        """Create a mock FastAPI Request object."""
        request = MagicMock()
        request.state = MagicMock()

        if body is not None:
            request.json = AsyncMock(return_value=body)
        elif body_bytes is not None:
            request.json = AsyncMock(side_effect=json.JSONDecodeError("", "", 0))
        else:
            request.json = AsyncMock(return_value={})

        return request

    @pytest.mark.asyncio
    async def test_extracts_agent_id_and_checks_breaker(
        self, middleware, mock_redis_service, agent_id
    ):
        """Extracts agent_id from body and performs circuit breaker check."""
        request = self._make_request(body={"agent_id": str(agent_id)})

        # Breaker inactive — should pass through
        result = await middleware(request)
        assert result is None

    @pytest.mark.asyncio
    async def test_raises_429_when_breaker_active_via_call(
        self, middleware, mock_redis_service, agent_id
    ):
        """Returns 429 when invoked as middleware and breaker is active."""
        active_state = json.dumps({
            "agent_id": str(agent_id),
            "is_active": True,
            "activated_at": "2024-01-01T00:00:00+00:00",
            "activated_by": "system",
            "reason": "infinite loop",
            "ttl_seconds": None,
        })
        mock_redis_service.get = AsyncMock(return_value=active_state)

        request = self._make_request(body={"agent_id": str(agent_id)})

        with pytest.raises(HTTPException) as exc_info:
            await middleware(request)

        assert exc_info.value.status_code == 429

    @pytest.mark.asyncio
    async def test_stores_parsed_body_on_request_state(
        self, middleware, mock_redis_service, agent_id
    ):
        """Parsed body is stored on request.state for downstream reuse."""
        body = {"agent_id": str(agent_id), "events": []}
        request = self._make_request(body=body)

        await middleware(request)

        assert request.state.parsed_body == body

    @pytest.mark.asyncio
    async def test_skips_check_when_no_agent_id_in_body(self, middleware):
        """Skips circuit breaker check when body has no agent_id."""
        request = self._make_request(body={"some_field": "value"})

        # Should not raise — skips the check gracefully
        result = await middleware(request)
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_check_when_invalid_uuid(self, middleware):
        """Skips circuit breaker check when agent_id is not a valid UUID."""
        request = self._make_request(body={"agent_id": "not-a-uuid"})

        # Should not raise — invalid UUID is skipped and left for
        # payload validation to catch
        result = await middleware(request)
        assert result is None

    @pytest.mark.asyncio
    async def test_skips_check_when_body_parse_fails(self, middleware):
        """Skips circuit breaker check when request body cannot be parsed."""
        request = self._make_request(body_bytes=b"not json")

        # Should not raise — malformed body is skipped
        result = await middleware(request)
        assert result is None


class TestCircuitBreakerMiddlewareLogging:
    """Tests verifying correct logging behavior."""

    @pytest.mark.asyncio
    async def test_logs_info_when_rejecting(
        self, middleware, mock_redis_service, agent_id
    ):
        """Logs at INFO level when rejecting a request due to active breaker."""
        active_state = json.dumps({
            "agent_id": str(agent_id),
            "is_active": True,
            "activated_at": "2024-01-01T00:00:00+00:00",
            "activated_by": "system",
            "reason": "test",
            "ttl_seconds": None,
        })
        mock_redis_service.get = AsyncMock(return_value=active_state)

        with patch("src.gateway.middleware.logger") as mock_logger:
            with pytest.raises(HTTPException):
                await middleware.check(agent_id)

            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            assert "Circuit breaker active" in call_args[0]
