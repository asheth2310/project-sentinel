"""Tests for rate limiting middleware integration in the telemetry ingestion endpoint.

Validates: Requirement 14.5 - Rate limiting is enforced per-agent and per-organization.

Tests verify:
- Rate limited requests get HTTP 429 with correct headers
- Allowed requests include rate limit headers on 202 response
- Fail-open behavior when Redis is unavailable
"""

import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config.settings import KafkaSettings
from src.gateway.auth import AgentIdentity
from src.gateway.deduplication import DeduplicationService
from src.gateway.dependencies import (
    authenticate_agent,
    get_circuit_breaker_middleware,
    get_deduplication_service,
    get_event_buffer,
    get_kafka_producer,
    get_kafka_settings,
    get_rate_limiter,
)
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter, RateLimitResult
from src.gateway.routes import router


AGENT_ID = UUID("550e8400-e29b-41d4-a716-446655440000")
ORG_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")


@pytest.fixture
def kafka_settings():
    return KafkaSettings()


@pytest.fixture
def mock_kafka_producer():
    producer = AsyncMock(spec=KafkaProducerService)
    producer.produce_batch = AsyncMock(return_value=None)
    return producer


@pytest.fixture
def event_buffer():
    return EventBuffer(max_size=100)


@pytest.fixture
def mock_identity():
    return AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)


@pytest.fixture
def mock_cb_middleware():
    middleware = AsyncMock(spec=CircuitBreakerMiddleware)
    middleware.check = AsyncMock(return_value=None)
    return middleware


@pytest.fixture
def mock_dedup_service():
    service = AsyncMock(spec=DeduplicationService)
    service.is_duplicate = AsyncMock(return_value=False)
    service.mark_processed = AsyncMock(return_value=None)
    return service


@pytest.fixture
def mock_rate_limiter_allowed():
    """Rate limiter that allows all requests."""
    limiter = AsyncMock(spec=RateLimiter)
    limiter.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(
            allowed=True,
            remaining=94,
            reset_at=datetime.fromtimestamp(
                time.time() + 60, tz=timezone.utc
            ),
        )
    )
    return limiter


@pytest.fixture
def mock_rate_limiter_denied():
    """Rate limiter that denies requests (limit exceeded)."""
    reset_time = time.time() + 45  # Resets in 45 seconds
    limiter = AsyncMock(spec=RateLimiter)
    limiter.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(
            allowed=False,
            remaining=0,
            reset_at=datetime.fromtimestamp(reset_time, tz=timezone.utc),
        )
    )
    return limiter


@pytest.fixture
def mock_rate_limiter_fail_open():
    """Rate limiter that fails open (Redis unavailable)."""
    limiter = AsyncMock(spec=RateLimiter)
    limiter.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(
            allowed=True,
            remaining=100,
            reset_at=datetime.now(timezone.utc),
        )
    )
    return limiter


def _create_app(
    mock_kafka_producer,
    event_buffer,
    kafka_settings,
    mock_identity,
    mock_cb_middleware,
    mock_dedup_service,
    rate_limiter,
):
    """Helper to create a FastAPI app with dependency overrides."""
    test_app = FastAPI()
    test_app.include_router(router)

    test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    test_app.dependency_overrides[get_event_buffer] = lambda: event_buffer
    test_app.dependency_overrides[get_kafka_settings] = lambda: kafka_settings
    test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
    test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: mock_cb_middleware
    test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup_service
    test_app.dependency_overrides[get_rate_limiter] = lambda: rate_limiter

    return test_app


def _make_batch(agent_id=None):
    """Create a valid telemetry batch payload."""
    aid = str(agent_id or AGENT_ID)
    oid = str(ORG_ID)
    return {
        "agent_id": aid,
        "org_id": oid,
        "sdk_version": "1.0.0",
        "batch_id": str(uuid4()),
        "events": [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent_id": aid,
                "org_id": oid,
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_cost": "0.001500",
                "latency_ms": 200,
                "tool_name": "web_search",
            }
        ],
    }


class TestRateLimitedRequestsGet429:
    """Rate limited requests receive HTTP 429 with correct headers."""

    def test_rate_limited_returns_429(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """When rate limit is exceeded, endpoint returns 429."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        assert response.status_code == 429

    def test_rate_limited_has_x_ratelimit_remaining_header(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """429 response includes X-RateLimit-Remaining: 0."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        assert response.headers["x-ratelimit-remaining"] == "0"

    def test_rate_limited_has_x_ratelimit_reset_header(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """429 response includes X-RateLimit-Reset as Unix timestamp."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        reset_header = response.headers["x-ratelimit-reset"]
        # Should be a valid Unix timestamp (parseable as integer)
        reset_ts = int(reset_header)
        # Should be in the future
        assert reset_ts > int(time.time()) - 1

    def test_rate_limited_has_retry_after_header(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """429 response includes Retry-After header with seconds until reset."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        retry_after = int(response.headers["retry-after"])
        # Should be a non-negative number of seconds
        assert retry_after >= 0
        # Given our mock resets in ~45 seconds, it should be around that range
        assert retry_after <= 60

    def test_rate_limited_does_not_produce_to_kafka(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """When rate limited, no events are produced to Kafka."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        client.post("/v1/telemetry", json=_make_batch())

        mock_kafka_producer.produce_batch.assert_not_called()

    def test_rate_limited_response_body(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_denied,
    ):
        """429 response body contains rate limit exceeded detail."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_denied,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        data = response.json()
        assert "rate limit" in data["detail"].lower()


class TestAllowedRequestsIncludeRateLimitHeaders:
    """Allowed requests include rate limit info headers on 202 responses."""

    def test_allowed_request_has_x_ratelimit_remaining(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_allowed,
    ):
        """202 response includes X-RateLimit-Remaining header."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_allowed,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        assert response.status_code == 202
        assert "x-ratelimit-remaining" in response.headers
        remaining = int(response.headers["x-ratelimit-remaining"])
        assert remaining == 94  # Matches our mock

    def test_allowed_request_has_x_ratelimit_reset(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_allowed,
    ):
        """202 response includes X-RateLimit-Reset header."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_allowed,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        assert response.status_code == 202
        assert "x-ratelimit-reset" in response.headers
        reset_ts = int(response.headers["x-ratelimit-reset"])
        # Should be a valid future timestamp
        assert reset_ts > int(time.time()) - 1

    def test_rate_limiter_called_with_identity_ids(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_allowed,
    ):
        """Rate limiter is called with agent_id and org_id from identity."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_allowed,
        )
        client = TestClient(app)

        client.post("/v1/telemetry", json=_make_batch())

        mock_rate_limiter_allowed.check_rate_limit.assert_called_once_with(
            AGENT_ID, ORG_ID
        )


class TestFailOpenBehavior:
    """Rate limiter fails open when Redis is unavailable."""

    def test_fail_open_allows_request(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_fail_open,
    ):
        """When Redis is down, rate limiter fails open and request succeeds."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_fail_open,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        # Should succeed (fail-open behavior)
        assert response.status_code == 202

    def test_fail_open_includes_rate_limit_headers(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_fail_open,
    ):
        """Even on fail-open, response includes rate limit headers."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_fail_open,
        )
        client = TestClient(app)

        response = client.post("/v1/telemetry", json=_make_batch())

        assert response.status_code == 202
        # Remaining should be 100 (full limit since Redis is down)
        remaining = int(response.headers["x-ratelimit-remaining"])
        assert remaining == 100

    def test_fail_open_produces_to_kafka(
        self,
        mock_kafka_producer,
        event_buffer,
        kafka_settings,
        mock_identity,
        mock_cb_middleware,
        mock_dedup_service,
        mock_rate_limiter_fail_open,
    ):
        """On fail-open, events are still produced to Kafka."""
        app = _create_app(
            mock_kafka_producer,
            event_buffer,
            kafka_settings,
            mock_identity,
            mock_cb_middleware,
            mock_dedup_service,
            mock_rate_limiter_fail_open,
        )
        client = TestClient(app)

        client.post("/v1/telemetry", json=_make_batch())

        mock_kafka_producer.produce_batch.assert_called_once()
