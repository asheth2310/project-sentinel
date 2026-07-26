"""Tests for the payload validation pipeline.

Verifies that invalid telemetry payloads return HTTP 422 with structured
ErrorResponse containing field-level validation details.

Validates: Requirement 1.4, 2.1-2.7
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.config.settings import KafkaSettings
from src.gateway.app import create_app
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
from src.gateway.rate_limiter import RateLimiter, RateLimitResult

# Test constants
TEST_AGENT_ID = "550e8400-e29b-41d4-a716-446655440000"
TEST_ORG_ID = "7c9e6679-7425-40de-944b-e07fc1f90ae7"


@pytest.fixture
def app():
    """Create a test app with all dependencies overridden."""
    application = create_app()

    # Override authentication to return a fixed identity
    async def mock_authenticate():
        return AgentIdentity(
            agent_id=uuid.UUID(TEST_AGENT_ID),
            org_id=uuid.UUID(TEST_ORG_ID),
        )

    # Mock circuit breaker middleware (no-op)
    mock_cb = MagicMock()
    mock_cb.check = AsyncMock(return_value=None)

    # Mock kafka producer
    mock_kafka = MagicMock()
    mock_kafka.produce_batch = AsyncMock(return_value=None)

    # Mock event buffer
    mock_buffer = MagicMock()
    mock_buffer.add = AsyncMock(return_value=True)

    application.dependency_overrides[authenticate_agent] = mock_authenticate
    application.dependency_overrides[get_circuit_breaker_middleware] = lambda: mock_cb
    application.dependency_overrides[get_kafka_producer] = lambda: mock_kafka
    application.dependency_overrides[get_event_buffer] = lambda: mock_buffer
    application.dependency_overrides[get_kafka_settings] = lambda: KafkaSettings()

    # Mock deduplication service (no duplicates)
    mock_dedup = MagicMock(spec=DeduplicationService)
    mock_dedup.is_duplicate = AsyncMock(return_value=False)
    mock_dedup.mark_processed = AsyncMock(return_value=None)
    application.dependency_overrides[get_deduplication_service] = lambda: mock_dedup

    # Mock rate limiter (always allows)
    mock_rl = MagicMock(spec=RateLimiter)
    mock_rl.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(allowed=True, remaining=99, reset_at=datetime.now(timezone.utc))
    )
    application.dependency_overrides[get_rate_limiter] = lambda: mock_rl

    yield application
    application.dependency_overrides.clear()


@pytest.fixture
def client(app):
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def valid_event():
    """Return a valid telemetry event payload."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent_id": TEST_AGENT_ID,
        "org_id": TEST_ORG_ID,
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_cost": "0.001500",
        "latency_ms": 200,
        "tool_name": "web_search",
    }


@pytest.fixture
def valid_batch(valid_event):
    """Return a valid telemetry batch payload."""
    return {
        "agent_id": TEST_AGENT_ID,
        "org_id": TEST_ORG_ID,
        "sdk_version": "1.2.3",
        "batch_id": str(uuid.uuid4()),
        "events": [valid_event],
    }


class TestValidBatchAccepted:
    """Verify valid batches return 202."""

    def test_valid_batch_returns_202(self, client, valid_batch):
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["event_count"] == 1


class TestErrorResponseStructure:
    """Verify 422 responses have structured ErrorResponse format."""

    def test_error_response_has_detail_field(self, client):
        response = client.post("/v1/telemetry", json={})
        assert response.status_code == 422
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], str)

    def test_error_response_has_errors_list(self, client):
        response = client.post("/v1/telemetry", json={})
        assert response.status_code == 422
        data = response.json()
        assert "errors" in data
        assert isinstance(data["errors"], list)
        assert len(data["errors"]) > 0

    def test_error_detail_has_field_message_type(self, client):
        response = client.post("/v1/telemetry", json={})
        assert response.status_code == 422
        data = response.json()
        for error in data["errors"]:
            assert "field" in error
            assert "message" in error
            assert "type" in error

    def test_request_id_included_when_header_present(self, client):
        request_id = str(uuid.uuid4())
        response = client.post(
            "/v1/telemetry",
            json={},
            headers={"X-Request-ID": request_id},
        )
        assert response.status_code == 422
        data = response.json()
        assert data["request_id"] == request_id

    def test_request_id_null_when_header_absent(self, client):
        response = client.post("/v1/telemetry", json={})
        assert response.status_code == 422
        data = response.json()
        assert data["request_id"] is None


class TestTokenValidation:
    """Verify non-negative token validation (Requirement 2.1)."""

    def test_negative_prompt_tokens_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["prompt_tokens"] = -1
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("prompt_tokens" in f for f in field_names)

    def test_negative_completion_tokens_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["completion_tokens"] = -5
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("completion_tokens" in f for f in field_names)


class TestCostValidation:
    """Verify cost precision validation (Requirement 2.2)."""

    def test_negative_cost_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["total_cost"] = "-0.01"
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("total_cost" in f for f in field_names)

    def test_cost_exceeding_6_decimal_places_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["total_cost"] = "0.1234567"
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("total_cost" in f for f in field_names)


class TestLatencyValidation:
    """Verify non-negative latency validation (Requirement 2.3)."""

    def test_negative_latency_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["latency_ms"] = -10
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("latency_ms" in f for f in field_names)


class TestTimestampValidation:
    """Verify future timestamp validation (Requirement 2.4)."""

    def test_timestamp_more_than_5_minutes_future_returns_422(self, client, valid_batch):
        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        valid_batch["events"][0]["timestamp"] = future.isoformat()
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("timestamp" in f for f in field_names)


class TestSdkVersionValidation:
    """Verify semver sdk_version validation (Requirement 2.6)."""

    def test_invalid_sdk_version_returns_422(self, client, valid_batch):
        valid_batch["sdk_version"] = "not-semver"
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("sdk_version" in f for f in field_names)

    def test_partial_semver_returns_422(self, client, valid_batch):
        valid_batch["sdk_version"] = "1.2"
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422


class TestAgentIdConsistency:
    """Verify batch agent_id consistency validation (Requirement 2.7)."""

    def test_mismatched_event_agent_id_returns_422(self, client, valid_batch):
        valid_batch["events"][0]["agent_id"] = str(uuid.uuid4())
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        # Model validator error shows up in errors list
        assert len(errors) > 0


class TestBatchSizeValidation:
    """Verify batch size constraints (1-1000 events)."""

    def test_empty_events_list_returns_422(self, client, valid_batch):
        valid_batch["events"] = []
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        field_names = [e["field"] for e in errors]
        assert any("events" in f for f in field_names)


class TestMultipleFieldErrors:
    """Verify all field-level errors are returned when multiple fields fail."""

    def test_multiple_validation_errors_all_returned(self, client, valid_batch):
        # Break multiple fields at once
        valid_batch["events"][0]["prompt_tokens"] = -1
        valid_batch["events"][0]["completion_tokens"] = -1
        valid_batch["events"][0]["latency_ms"] = -10
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        # Should have at least 3 errors (one per invalid field)
        assert len(errors) >= 3
        assert data["detail"] == f"Validation failed with {len(errors)} error(s)"

    def test_invalid_batch_level_and_event_level(self, client, valid_batch):
        valid_batch["sdk_version"] = "bad"
        valid_batch["events"][0]["prompt_tokens"] = -1
        response = client.post("/v1/telemetry", json=valid_batch)
        assert response.status_code == 422
        data = response.json()
        errors = data["errors"]
        # Both errors should be present
        assert len(errors) >= 2
