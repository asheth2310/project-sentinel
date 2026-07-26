"""Tests for telemetry ingestion routes - atomic batch production to Kafka.

Validates Requirement 1.6: Batch processing is atomic (all-or-nothing).
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
from src.gateway.deduplication import DeduplicationService
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import (
    KafkaProducerError,
    KafkaProducerService,
    KafkaUnavailableError,
)
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
    """Mock KafkaProducerService with produce_batch as AsyncMock."""
    producer = AsyncMock(spec=KafkaProducerService)
    producer.produce_batch = AsyncMock(return_value=None)
    return producer


@pytest.fixture
def event_buffer():
    """Real EventBuffer instance for testing."""
    return EventBuffer(max_size=100)


@pytest.fixture
def mock_identity():
    """Mock agent identity for authentication bypass."""
    return AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)


@pytest.fixture
def mock_cb_middleware():
    """Mock CircuitBreakerMiddleware that always passes (no active breaker)."""
    middleware = AsyncMock(spec=CircuitBreakerMiddleware)
    middleware.check = AsyncMock(return_value=None)
    return middleware


@pytest.fixture
def mock_dedup_service():
    """Mock DeduplicationService that always allows (no duplicates)."""
    dedup = AsyncMock(spec=DeduplicationService)
    dedup.is_duplicate = AsyncMock(return_value=False)
    dedup.mark_processed = AsyncMock(return_value=None)
    return dedup


@pytest.fixture
def mock_rate_limiter():
    """Mock RateLimiter that always allows."""
    limiter = AsyncMock(spec=RateLimiter)
    limiter.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(allowed=True, remaining=99, reset_at=datetime.now(timezone.utc))
    )
    return limiter


@pytest.fixture
def app(mock_kafka_producer, event_buffer, kafka_settings, mock_identity, mock_cb_middleware, mock_dedup_service, mock_rate_limiter):
    """Create a FastAPI app with the telemetry router and overridden dependencies."""
    test_app = FastAPI()
    test_app.include_router(router)

    # Override dependencies for testing
    test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    test_app.dependency_overrides[get_event_buffer] = lambda: event_buffer
    test_app.dependency_overrides[get_kafka_settings] = lambda: kafka_settings
    test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
    test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: mock_cb_middleware
    test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup_service
    test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_limiter

    return test_app


@pytest.fixture
def client(app):
    """Synchronous test client for the FastAPI app."""
    return TestClient(app)


def _make_batch(agent_id=None, event_count=1):
    """Helper: create a valid telemetry batch payload."""
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
            for _ in range(event_count)
        ],
    }


class TestIngestTelemetrySuccess:
    """Tests for successful batch ingestion (202 Accepted)."""

    def test_valid_batch_returns_202(self, client, mock_kafka_producer):
        """Valid batch is accepted and returns 202 with batch_id."""
        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202
        data = response.json()
        assert data["batch_id"] == batch["batch_id"]
        assert data["status"] == "accepted"
        assert data["event_count"] == 1

    def test_produce_batch_called_with_correct_args(
        self, client, mock_kafka_producer, kafka_settings
    ):
        """produce_batch is called with serialized events, topic, and agent_id key."""
        batch = _make_batch()
        client.post("/v1/telemetry", json=batch)

        mock_kafka_producer.produce_batch.assert_called_once()
        call_args = mock_kafka_producer.produce_batch.call_args[0][0]

        # Each event should be a (topic, key, value_bytes) tuple
        assert len(call_args) == 1
        topic, key, value = call_args[0]
        assert topic == kafka_settings.topic_telemetry_raw
        assert key == str(AGENT_ID)
        # Value should be valid JSON bytes
        event_data = json.loads(value)
        assert event_data["prompt_tokens"] == 100

    def test_multi_event_batch_serializes_all_events(self, client, mock_kafka_producer):
        """All events in a multi-event batch are serialized and sent."""
        batch = _make_batch(event_count=5)
        client.post("/v1/telemetry", json=batch)

        call_args = mock_kafka_producer.produce_batch.call_args[0][0]
        assert len(call_args) == 5

    def test_agent_id_used_as_partition_key(self, client, mock_kafka_producer):
        """agent_id from the batch is used as the Kafka partition key."""
        batch = _make_batch()
        client.post("/v1/telemetry", json=batch)

        call_args = mock_kafka_producer.produce_batch.call_args[0][0]
        for topic, key, value in call_args:
            assert key == str(AGENT_ID)


class TestIngestTelemetryKafkaUnavailable:
    """Tests for Kafka unavailability (buffer or 503)."""

    def test_kafka_unavailable_buffers_events_returns_202(
        self, client, mock_kafka_producer, event_buffer
    ):
        """When Kafka is down, events are buffered and 202 is returned."""
        mock_kafka_producer.produce_batch.side_effect = KafkaUnavailableError(
            "Broker down"
        )
        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "buffered"
        assert data["event_count"] == 1
        assert event_buffer.size == 1

    def test_kafka_unavailable_buffer_full_returns_503(
        self, mock_kafka_producer, kafka_settings, mock_identity, mock_cb_middleware, mock_dedup_service, mock_rate_limiter
    ):
        """When Kafka is down and buffer is full, returns 503 with Retry-After."""
        # Use a tiny buffer that's already full
        small_buffer = EventBuffer(max_size=1)

        import asyncio

        asyncio.run(small_buffer.add({"dummy": "event"}))

        # Build a fresh app with the full buffer
        test_app = FastAPI()
        test_app.include_router(router)
        test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
        test_app.dependency_overrides[get_event_buffer] = lambda: small_buffer
        test_app.dependency_overrides[get_kafka_settings] = lambda: kafka_settings
        test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
        test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: mock_cb_middleware
        test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup_service
        test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_limiter

        client = TestClient(test_app)

        mock_kafka_producer.produce_batch.side_effect = KafkaUnavailableError(
            "Broker down"
        )
        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 503
        assert "Retry-After" in response.headers
        assert int(response.headers["Retry-After"]) > 0
        data = response.json()
        assert "buffer" in data["detail"].lower() or "unavailable" in data["detail"].lower()


class TestIngestTelemetryKafkaProducerError:
    """Tests for KafkaProducerError (no partial writes)."""

    def test_producer_error_returns_503(self, client, mock_kafka_producer):
        """KafkaProducerError results in 503 with no partial writes guarantee."""
        mock_kafka_producer.produce_batch.side_effect = KafkaProducerError(
            "Batch production failed: 1/3 messages failed"
        )
        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 503
        assert "Retry-After" in response.headers
        data = response.json()
        assert "atomic" in data["detail"].lower() or "no events were written" in data["detail"].lower()

    def test_producer_error_no_events_buffered(
        self, client, mock_kafka_producer, event_buffer
    ):
        """On KafkaProducerError, events are NOT buffered (produce_batch handles atomicity)."""
        mock_kafka_producer.produce_batch.side_effect = KafkaProducerError(
            "Delivery failed"
        )
        batch = _make_batch()
        client.post("/v1/telemetry", json=batch)

        # Events should NOT be buffered on produce error - only on unavailable
        assert event_buffer.size == 0


class TestAtomicBatchSemantics:
    """Tests verifying all-or-nothing batch semantics."""

    def test_all_events_sent_in_single_produce_batch_call(
        self, client, mock_kafka_producer
    ):
        """All events go through a single produce_batch call (atomic unit)."""
        batch = _make_batch(event_count=10)
        client.post("/v1/telemetry", json=batch)

        # produce_batch should be called exactly once with all events
        assert mock_kafka_producer.produce_batch.call_count == 1
        call_args = mock_kafka_producer.produce_batch.call_args[0][0]
        assert len(call_args) == 10

    def test_no_partial_produce_on_failure(self, client, mock_kafka_producer):
        """On failure, no events are partially produced (all-or-nothing)."""
        mock_kafka_producer.produce_batch.side_effect = KafkaProducerError(
            "Partial failure impossible - all-or-nothing"
        )
        batch = _make_batch(event_count=5)
        response = client.post("/v1/telemetry", json=batch)

        # The route doesn't attempt individual retries - relies on produce_batch atomicity
        assert response.status_code == 503
        assert mock_kafka_producer.produce_batch.call_count == 1

    def test_events_serialized_as_json_bytes(self, client, mock_kafka_producer):
        """Each event is serialized to JSON bytes for Kafka production."""
        batch = _make_batch(event_count=3)
        client.post("/v1/telemetry", json=batch)

        call_args = mock_kafka_producer.produce_batch.call_args[0][0]
        for topic, key, value in call_args:
            # value should be bytes
            assert isinstance(value, bytes)
            # should be valid JSON
            parsed = json.loads(value)
            assert "prompt_tokens" in parsed
            assert "completion_tokens" in parsed
            assert "timestamp" in parsed

    def test_topic_from_settings(self, client, mock_kafka_producer, kafka_settings):
        """Topic name comes from KafkaSettings.topic_telemetry_raw."""
        batch = _make_batch()
        client.post("/v1/telemetry", json=batch)

        call_args = mock_kafka_producer.produce_batch.call_args[0][0]
        for topic, key, value in call_args:
            assert topic == "telemetry-raw"
