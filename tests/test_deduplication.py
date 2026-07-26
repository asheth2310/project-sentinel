"""Tests for batch deduplication service.

Validates Requirement 1.7: Duplicate batch submissions (same batch_id)
are idempotent and do not create duplicate events.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.gateway.deduplication import (
    BATCH_PROCESSED_KEY_PREFIX,
    DEFAULT_DEDUP_TTL_SECONDS,
    DeduplicationService,
)
from src.gateway.redis_service import RedisService


# ============================================================================
# Unit tests for DeduplicationService
# ============================================================================


class TestDeduplicationServiceIsKeyFormat:
    """Tests for the Redis key format."""

    def test_key_format_includes_prefix_and_batch_id(self):
        """Key should follow the pattern batch_processed:{batch_id}."""
        redis_svc = AsyncMock(spec=RedisService)
        dedup = DeduplicationService(redis_svc)
        batch_id = UUID("550e8400-e29b-41d4-a716-446655440000")

        key = dedup._make_key(batch_id)

        assert key == f"{BATCH_PROCESSED_KEY_PREFIX}:{batch_id}"
        assert key == "batch_processed:550e8400-e29b-41d4-a716-446655440000"


class TestDeduplicationServiceIsDuplicate:
    """Tests for is_duplicate method."""

    @pytest.fixture
    def mock_redis(self):
        """Mock RedisService."""
        return AsyncMock(spec=RedisService)

    @pytest.fixture
    def dedup_service(self, mock_redis):
        """DeduplicationService with mocked Redis."""
        return DeduplicationService(mock_redis)

    @pytest.mark.asyncio
    async def test_not_duplicate_when_key_absent(self, dedup_service, mock_redis):
        """Returns False when batch_id has not been processed before."""
        mock_redis.get.return_value = None
        batch_id = uuid4()

        result = await dedup_service.is_duplicate(batch_id)

        assert result is False
        mock_redis.get.assert_called_once_with(f"{BATCH_PROCESSED_KEY_PREFIX}:{batch_id}")

    @pytest.mark.asyncio
    async def test_duplicate_when_key_exists(self, dedup_service, mock_redis):
        """Returns True when batch_id is already in Redis."""
        mock_redis.get.return_value = "1"
        batch_id = uuid4()

        result = await dedup_service.is_duplicate(batch_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_fails_open_when_redis_unavailable(self, dedup_service, mock_redis):
        """Returns False (not duplicate) when Redis returns None due to failure.

        RedisService.get already fails open by returning None on connection errors.
        """
        mock_redis.get.return_value = None  # Redis unavailable returns None
        batch_id = uuid4()

        result = await dedup_service.is_duplicate(batch_id)

        assert result is False


class TestDeduplicationServiceMarkProcessed:
    """Tests for mark_processed method."""

    @pytest.fixture
    def mock_redis(self):
        """Mock RedisService."""
        return AsyncMock(spec=RedisService)

    @pytest.fixture
    def dedup_service(self, mock_redis):
        """DeduplicationService with mocked Redis."""
        return DeduplicationService(mock_redis)

    @pytest.mark.asyncio
    async def test_marks_batch_in_redis_with_default_ttl(self, dedup_service, mock_redis):
        """Sets Redis key with default 24-hour TTL."""
        mock_redis.set.return_value = True
        batch_id = uuid4()

        await dedup_service.mark_processed(batch_id)

        mock_redis.set.assert_called_once_with(
            f"{BATCH_PROCESSED_KEY_PREFIX}:{batch_id}",
            "1",
            ttl=DEFAULT_DEDUP_TTL_SECONDS,
        )

    @pytest.mark.asyncio
    async def test_marks_batch_with_custom_ttl(self, dedup_service, mock_redis):
        """Supports custom TTL for deduplication window."""
        mock_redis.set.return_value = True
        batch_id = uuid4()

        await dedup_service.mark_processed(batch_id, ttl_seconds=3600)

        mock_redis.set.assert_called_once_with(
            f"{BATCH_PROCESSED_KEY_PREFIX}:{batch_id}",
            "1",
            ttl=3600,
        )

    @pytest.mark.asyncio
    async def test_does_not_raise_when_redis_unavailable(self, dedup_service, mock_redis):
        """Fails open: does not raise if Redis SET fails."""
        mock_redis.set.return_value = False  # Redis unavailable
        batch_id = uuid4()

        # Should not raise
        await dedup_service.mark_processed(batch_id)

        mock_redis.set.assert_called_once()


# ============================================================================
# Integration tests: deduplication in the telemetry ingestion route
# ============================================================================

from src.config.settings import KafkaSettings
from src.gateway.auth import AgentIdentity
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


def _make_batch(batch_id=None, agent_id=None, event_count=1):
    """Helper: create a valid telemetry batch payload."""
    aid = str(agent_id or AGENT_ID)
    oid = str(ORG_ID)
    return {
        "agent_id": aid,
        "org_id": oid,
        "sdk_version": "1.0.0",
        "batch_id": str(batch_id or uuid4()),
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
            for _ in range(event_count)
        ],
    }


class TestIdempotentIngestion:
    """Tests for idempotent batch ingestion via the route."""

    @pytest.fixture
    def mock_kafka_producer(self):
        producer = AsyncMock(spec=KafkaProducerService)
        producer.produce_batch = AsyncMock(return_value=None)
        return producer

    @pytest.fixture
    def mock_dedup_service(self):
        dedup = AsyncMock(spec=DeduplicationService)
        dedup.is_duplicate = AsyncMock(return_value=False)
        dedup.mark_processed = AsyncMock(return_value=None)
        return dedup

    @pytest.fixture
    def mock_rate_limiter(self):
        limiter = AsyncMock(spec=RateLimiter)
        limiter.check_rate_limit = AsyncMock(
            return_value=RateLimitResult(allowed=True, remaining=99, reset_at=datetime.now(timezone.utc))
        )
        return limiter

    @pytest.fixture
    def app(self, mock_kafka_producer, mock_dedup_service, mock_rate_limiter):
        test_app = FastAPI()
        test_app.include_router(router)

        mock_identity = AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)
        mock_cb = AsyncMock(spec=CircuitBreakerMiddleware)
        mock_cb.check = AsyncMock(return_value=None)
        event_buffer = EventBuffer(max_size=100)
        kafka_settings = KafkaSettings()

        test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
        test_app.dependency_overrides[get_event_buffer] = lambda: event_buffer
        test_app.dependency_overrides[get_kafka_settings] = lambda: kafka_settings
        test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
        test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: mock_cb
        test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup_service
        test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_limiter

        return test_app

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_first_submission_produces_to_kafka_and_marks_processed(
        self, client, mock_kafka_producer, mock_dedup_service
    ):
        """First submission of a batch_id: produces to Kafka and marks as processed."""
        mock_dedup_service.is_duplicate.return_value = False

        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["batch_id"] == batch["batch_id"]

        # Kafka should have been called
        mock_kafka_producer.produce_batch.assert_called_once()

        # Dedup service should mark as processed
        mock_dedup_service.mark_processed.assert_called_once()

    def test_duplicate_submission_returns_202_without_kafka_produce(
        self, client, mock_kafka_producer, mock_dedup_service
    ):
        """Duplicate batch_id: returns same 202 response without re-producing."""
        mock_dedup_service.is_duplicate.return_value = True

        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["batch_id"] == batch["batch_id"]
        assert data["event_count"] == 1

        # Kafka should NOT have been called for duplicate
        mock_kafka_producer.produce_batch.assert_not_called()

        # mark_processed should NOT be called for duplicate
        mock_dedup_service.mark_processed.assert_not_called()

    def test_same_batch_id_submitted_twice_is_idempotent(
        self, client, mock_kafka_producer, mock_dedup_service
    ):
        """Same batch_id submitted twice: first produces, second is idempotent."""
        batch_id = uuid4()
        batch = _make_batch(batch_id=batch_id)

        # First submission: not a duplicate
        mock_dedup_service.is_duplicate.return_value = False
        response1 = client.post("/v1/telemetry", json=batch)
        assert response1.status_code == 202

        # Second submission: duplicate detected
        mock_dedup_service.is_duplicate.return_value = True
        response2 = client.post("/v1/telemetry", json=batch)
        assert response2.status_code == 202

        # Both responses should have same content
        assert response1.json()["batch_id"] == response2.json()["batch_id"]
        assert response1.json()["event_count"] == response2.json()["event_count"]

        # Kafka called only once (first submission)
        assert mock_kafka_producer.produce_batch.call_count == 1

    def test_dedup_fails_open_allows_request_through(
        self, client, mock_kafka_producer, mock_dedup_service
    ):
        """When Redis is unavailable for dedup check, request proceeds normally."""
        # is_duplicate returns False when Redis is unavailable (fail-open)
        mock_dedup_service.is_duplicate.return_value = False

        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202
        mock_kafka_producer.produce_batch.assert_called_once()

    def test_different_batch_ids_both_produce(
        self, client, mock_kafka_producer, mock_dedup_service
    ):
        """Different batch_ids are treated as separate submissions."""
        mock_dedup_service.is_duplicate.return_value = False

        batch1 = _make_batch(batch_id=uuid4())
        batch2 = _make_batch(batch_id=uuid4())

        response1 = client.post("/v1/telemetry", json=batch1)
        response2 = client.post("/v1/telemetry", json=batch2)

        assert response1.status_code == 202
        assert response2.status_code == 202
        assert mock_kafka_producer.produce_batch.call_count == 2
