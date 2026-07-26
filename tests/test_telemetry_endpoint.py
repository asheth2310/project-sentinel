"""Unit tests for POST /v1/telemetry endpoint.

Tests authentication, authorization, and successful ingestion scenarios.
Validates: Requirements 1, 14 (Telemetry Ingestion API, Agent Authentication).
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from src.gateway.app import create_app
from src.gateway.auth import get_api_key_registry
from src.gateway.deduplication import DeduplicationService
from src.gateway.dependencies import (
    set_circuit_breaker_middleware,
    set_deduplication_service,
    set_event_buffer,
    set_kafka_producer,
    set_rate_limiter,
)
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter, RateLimitResult


@pytest.fixture(autouse=True)
def setup_dependencies():
    """Set up mock dependencies that the route handler requires."""
    # Mock Kafka producer
    mock_kafka = AsyncMock(spec=KafkaProducerService)
    mock_kafka.produce_batch = AsyncMock(return_value=None)
    set_kafka_producer(mock_kafka)

    # Real event buffer
    buffer = EventBuffer(max_size=100)
    set_event_buffer(buffer)

    # Mock circuit breaker middleware (always allows through)
    mock_cb = AsyncMock(spec=CircuitBreakerMiddleware)
    mock_cb.check = AsyncMock(return_value=None)
    set_circuit_breaker_middleware(mock_cb)

    # Mock deduplication service (no duplicates)
    mock_dedup = AsyncMock(spec=DeduplicationService)
    mock_dedup.is_duplicate = AsyncMock(return_value=False)
    mock_dedup.mark_processed = AsyncMock(return_value=None)
    set_deduplication_service(mock_dedup)

    # Mock rate limiter (always allows)
    mock_rl = AsyncMock(spec=RateLimiter)
    mock_rl.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(allowed=True, remaining=99, reset_at=datetime.now(timezone.utc))
    )
    set_rate_limiter(mock_rl)

    yield

    # Cleanup singletons
    set_kafka_producer(None)
    set_event_buffer(None)
    set_circuit_breaker_middleware(None)
    set_deduplication_service(None)
    set_rate_limiter(None)


@pytest.fixture
def app():
    """Create a test app with lifespan disabled (dependencies set via fixture)."""
    from fastapi import FastAPI

    from src.gateway.routes import router
    from src.gateway.validation import register_validation_handler

    test_app = FastAPI()
    register_validation_handler(test_app)
    test_app.include_router(router)
    return test_app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def registered_agent():
    """Register a test agent and return its credentials."""
    registry = get_api_key_registry()
    agent_id = uuid4()
    org_id = uuid4()
    api_key = f"test-api-key-{uuid4().hex[:8]}"
    registry.register(api_key, agent_id, org_id)
    yield {"api_key": api_key, "agent_id": agent_id, "org_id": org_id}
    # Cleanup
    registry.revoke(api_key)


@pytest.fixture
def valid_batch_payload(registered_agent):
    """Build a valid telemetry batch payload matching the registered agent."""
    agent_id = str(registered_agent["agent_id"])
    org_id = str(registered_agent["org_id"])
    return {
        "agent_id": agent_id,
        "org_id": org_id,
        "sdk_version": "1.0.0",
        "batch_id": str(uuid4()),
        "events": [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent_id": agent_id,
                "org_id": org_id,
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_cost": "0.001500",
                "latency_ms": 200,
            }
        ],
    }


class TestAuthenticationMissing:
    """Test 401 responses for missing or invalid authentication."""

    def test_no_authorization_header(self, client, valid_batch_payload):
        """Missing Authorization header returns 401."""
        response = client.post("/v1/telemetry", json=valid_batch_payload)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Missing Authorization header" in response.json()["detail"]

    def test_empty_authorization_header(self, client, valid_batch_payload):
        """Empty Authorization header returns 401."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": ""},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_auth_scheme(self, client, valid_batch_payload):
        """Non-Bearer auth scheme returns 401."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": "Basic abc123"},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Invalid Authorization header format" in response.json()["detail"]

    def test_bearer_without_token(self, client, valid_batch_payload):
        """Bearer scheme with no token value returns 401."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": "Bearer "},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_invalid_token(self, client, valid_batch_payload):
        """Invalid/unknown API key returns 401."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": "Bearer invalid-key-xyz"},
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Invalid API key" in response.json()["detail"]

    def test_www_authenticate_header_present(self, client, valid_batch_payload):
        """401 responses include WWW-Authenticate: Bearer header."""
        response = client.post("/v1/telemetry", json=valid_batch_payload)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert response.headers.get("www-authenticate") == "Bearer"


class TestAuthorizationAgentMismatch:
    """Test 403 responses when batch agent_id doesn't match token's agent."""

    def test_agent_id_mismatch_returns_403(self, client, registered_agent):
        """Batch with different agent_id than token returns 403."""
        different_agent_id = str(uuid4())
        org_id = str(registered_agent["org_id"])
        payload = {
            "agent_id": different_agent_id,
            "org_id": org_id,
            "sdk_version": "1.0.0",
            "batch_id": str(uuid4()),
            "events": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "agent_id": different_agent_id,
                    "org_id": org_id,
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_cost": "0.001500",
                    "latency_ms": 200,
                }
            ],
        }
        response = client.post(
            "/v1/telemetry",
            json=payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "does not match" in response.json()["detail"]


class TestSuccessfulIngestion:
    """Test 202 responses for valid authenticated requests."""

    def test_valid_request_returns_202(
        self, client, registered_agent, valid_batch_payload
    ):
        """Valid authenticated request returns 202 Accepted."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_202_ACCEPTED

    def test_response_contains_batch_id(
        self, client, registered_agent, valid_batch_payload
    ):
        """202 response contains the batch_id from the request."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        data = response.json()
        assert data["batch_id"] == valid_batch_payload["batch_id"]

    def test_response_contains_event_count(
        self, client, registered_agent, valid_batch_payload
    ):
        """202 response reports correct event count."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        data = response.json()
        assert data["event_count"] == len(valid_batch_payload["events"])

    def test_response_status_is_accepted(
        self, client, registered_agent, valid_batch_payload
    ):
        """202 response has status field set to 'accepted'."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        data = response.json()
        assert data["status"] == "accepted"

    def test_multiple_events_in_batch(self, client, registered_agent):
        """Batch with multiple events is accepted correctly."""
        agent_id = str(registered_agent["agent_id"])
        org_id = str(registered_agent["org_id"])
        events = [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent_id": agent_id,
                "org_id": org_id,
                "prompt_tokens": i * 100,
                "completion_tokens": i * 50,
                "total_cost": f"0.00{i}000",
                "latency_ms": 200 + i,
            }
            for i in range(1, 6)
        ]
        payload = {
            "agent_id": agent_id,
            "org_id": org_id,
            "sdk_version": "2.1.0",
            "batch_id": str(uuid4()),
            "events": events,
        }
        response = client.post(
            "/v1/telemetry",
            json=payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_202_ACCEPTED
        assert response.json()["event_count"] == 5

    def test_bearer_token_case_insensitive_scheme(
        self, client, registered_agent, valid_batch_payload
    ):
        """Bearer scheme matching is case-insensitive."""
        response = client.post(
            "/v1/telemetry",
            json=valid_batch_payload,
            headers={"Authorization": f"BEARER {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_202_ACCEPTED


class TestPayloadValidation:
    """Test 422 responses for invalid payloads (FastAPI built-in validation)."""

    def test_missing_required_field(self, client, registered_agent):
        """Missing required fields return 422."""
        response = client.post(
            "/v1/telemetry",
            json={"agent_id": str(registered_agent["agent_id"])},
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_invalid_sdk_version(self, client, registered_agent):
        """Invalid sdk_version format returns 422."""
        agent_id = str(registered_agent["agent_id"])
        org_id = str(registered_agent["org_id"])
        payload = {
            "agent_id": agent_id,
            "org_id": org_id,
            "sdk_version": "invalid",
            "batch_id": str(uuid4()),
            "events": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "agent_id": agent_id,
                    "org_id": org_id,
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_cost": "0.001500",
                    "latency_ms": 200,
                }
            ],
        }
        response = client.post(
            "/v1/telemetry",
            json=payload,
            headers={"Authorization": f"Bearer {registered_agent['api_key']}"},
        )
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
