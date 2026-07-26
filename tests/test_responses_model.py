"""Tests for API response models (IngestionResponse, HealthResponse, ErrorResponse)."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from src.models.responses import (
    ErrorResponse,
    HealthResponse,
    IngestionResponse,
    ValidationErrorDetail,
)


class TestIngestionResponse:
    """Tests for IngestionResponse model (Requirement 1.3)."""

    def test_valid_response_with_required_fields(self):
        batch_id = uuid4()
        resp = IngestionResponse(batch_id=batch_id, event_count=10)
        assert resp.batch_id == batch_id
        assert resp.event_count == 10

    def test_status_defaults_to_accepted(self):
        resp = IngestionResponse(batch_id=uuid4(), event_count=5)
        assert resp.status == "accepted"

    def test_accepted_at_defaults_to_utcnow(self):
        before = datetime.now(timezone.utc)
        resp = IngestionResponse(batch_id=uuid4(), event_count=1)
        after = datetime.now(timezone.utc)
        assert before <= resp.accepted_at <= after

    def test_explicit_status_overrides_default(self):
        resp = IngestionResponse(batch_id=uuid4(), event_count=3, status="processing")
        assert resp.status == "processing"

    def test_explicit_accepted_at_used(self):
        custom_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        resp = IngestionResponse(batch_id=uuid4(), event_count=1, accepted_at=custom_time)
        assert resp.accepted_at == custom_time

    def test_batch_id_is_uuid(self):
        resp = IngestionResponse(batch_id=uuid4(), event_count=1)
        assert isinstance(resp.batch_id, UUID)

    def test_missing_batch_id_raises_error(self):
        with pytest.raises(ValidationError):
            IngestionResponse(event_count=5)

    def test_missing_event_count_raises_error(self):
        with pytest.raises(ValidationError):
            IngestionResponse(batch_id=uuid4())

    def test_serializes_to_json(self):
        batch_id = uuid4()
        resp = IngestionResponse(batch_id=batch_id, event_count=7)
        json_str = resp.model_dump_json()
        assert str(batch_id) in json_str
        assert "accepted" in json_str

    def test_round_trip_serialization(self):
        resp = IngestionResponse(batch_id=uuid4(), event_count=42)
        json_str = resp.model_dump_json()
        restored = IngestionResponse.model_validate_json(json_str)
        assert restored.batch_id == resp.batch_id
        assert restored.event_count == resp.event_count
        assert restored.status == resp.status


class TestValidationErrorDetail:
    """Tests for ValidationErrorDetail model."""

    def test_valid_creation(self):
        detail = ValidationErrorDetail(
            field="prompt_tokens", message="must be non-negative", type="value_error"
        )
        assert detail.field == "prompt_tokens"
        assert detail.message == "must be non-negative"
        assert detail.type == "value_error"

    def test_missing_field_raises_error(self):
        with pytest.raises(ValidationError):
            ValidationErrorDetail(message="error", type="value_error")

    def test_missing_message_raises_error(self):
        with pytest.raises(ValidationError):
            ValidationErrorDetail(field="x", type="value_error")

    def test_missing_type_raises_error(self):
        with pytest.raises(ValidationError):
            ValidationErrorDetail(field="x", message="error")

    def test_serializes_to_json(self):
        detail = ValidationErrorDetail(
            field="total_cost", message="too many decimal places", type="value_error"
        )
        json_str = detail.model_dump_json()
        assert "total_cost" in json_str
        assert "value_error" in json_str


class TestErrorResponse:
    """Tests for ErrorResponse model (Requirement 1.4)."""

    def test_valid_with_detail_only(self):
        err = ErrorResponse(detail="Validation failed")
        assert err.detail == "Validation failed"
        assert err.errors == []
        assert err.request_id is None

    def test_valid_with_errors_list(self):
        errors = [
            ValidationErrorDetail(field="prompt_tokens", message="must be >= 0", type="value_error"),
            ValidationErrorDetail(field="timestamp", message="too far in future", type="value_error"),
        ]
        err = ErrorResponse(detail="Validation failed", errors=errors)
        assert len(err.errors) == 2
        assert err.errors[0].field == "prompt_tokens"
        assert err.errors[1].field == "timestamp"

    def test_request_id_optional(self):
        err = ErrorResponse(detail="Error")
        assert err.request_id is None

    def test_request_id_explicit(self):
        req_id = uuid4()
        err = ErrorResponse(detail="Error", request_id=req_id)
        assert err.request_id == req_id

    def test_missing_detail_raises_error(self):
        with pytest.raises(ValidationError):
            ErrorResponse(errors=[])

    def test_errors_defaults_to_empty_list(self):
        err = ErrorResponse(detail="Something broke")
        assert err.errors == []

    def test_serializes_to_json(self):
        err = ErrorResponse(
            detail="Invalid payload",
            errors=[
                ValidationErrorDetail(field="sdk_version", message="invalid semver", type="value_error")
            ],
            request_id=uuid4(),
        )
        json_str = err.model_dump_json()
        assert "Invalid payload" in json_str
        assert "sdk_version" in json_str

    def test_round_trip_serialization(self):
        req_id = uuid4()
        err = ErrorResponse(
            detail="Validation failed",
            errors=[
                ValidationErrorDetail(field="cost", message="negative", type="value_error")
            ],
            request_id=req_id,
        )
        json_str = err.model_dump_json()
        restored = ErrorResponse.model_validate_json(json_str)
        assert restored.detail == err.detail
        assert len(restored.errors) == 1
        assert restored.errors[0].field == "cost"
        assert restored.request_id == req_id


class TestHealthResponse:
    """Tests for HealthResponse model."""

    def test_valid_all_connected(self):
        health = HealthResponse(
            kafka_connected=True, redis_connected=True, db_connected=True, version="0.1.0"
        )
        assert health.status == "healthy"
        assert health.kafka_connected is True
        assert health.redis_connected is True
        assert health.db_connected is True
        assert health.version == "0.1.0"

    def test_status_defaults_to_healthy(self):
        health = HealthResponse(
            kafka_connected=True, redis_connected=True, db_connected=True, version="1.0.0"
        )
        assert health.status == "healthy"

    def test_status_override(self):
        health = HealthResponse(
            kafka_connected=False, redis_connected=True, db_connected=True,
            version="0.1.0", status="degraded"
        )
        assert health.status == "degraded"

    def test_missing_kafka_connected_raises_error(self):
        with pytest.raises(ValidationError):
            HealthResponse(redis_connected=True, db_connected=True, version="0.1.0")

    def test_missing_redis_connected_raises_error(self):
        with pytest.raises(ValidationError):
            HealthResponse(kafka_connected=True, db_connected=True, version="0.1.0")

    def test_missing_db_connected_raises_error(self):
        with pytest.raises(ValidationError):
            HealthResponse(kafka_connected=True, redis_connected=True, version="0.1.0")

    def test_missing_version_raises_error(self):
        with pytest.raises(ValidationError):
            HealthResponse(kafka_connected=True, redis_connected=True, db_connected=True)

    def test_serializes_to_json(self):
        health = HealthResponse(
            kafka_connected=True, redis_connected=False, db_connected=True, version="0.2.0"
        )
        json_str = health.model_dump_json()
        assert "healthy" in json_str
        assert "0.2.0" in json_str

    def test_round_trip_serialization(self):
        health = HealthResponse(
            kafka_connected=False, redis_connected=True, db_connected=False, version="1.0.0"
        )
        json_str = health.model_dump_json()
        restored = HealthResponse.model_validate_json(json_str)
        assert restored.kafka_connected is False
        assert restored.redis_connected is True
        assert restored.db_connected is False
        assert restored.version == "1.0.0"
