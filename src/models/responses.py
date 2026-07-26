"""API response models for Project Sentinel.

Defines response schemas for the ingestion gateway, health probes,
and error responses per Requirement 1 (Acceptance Criteria 1.3, 1.4).
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class IngestionResponse(BaseModel):
    """Response returned on successful telemetry ingestion (HTTP 202 Accepted).

    Contains a batch_id for tracking the ingested batch through the pipeline.
    Validates: Requirement 1.3 - Valid batches return HTTP 202 Accepted with a batch_id.
    """

    batch_id: UUID = Field(..., description="Unique identifier for tracking the ingested batch")
    status: str = Field(default="accepted", description="Ingestion status indicator")
    event_count: int = Field(..., description="Number of events accepted in the batch")
    accepted_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the batch was accepted for processing",
    )


class ValidationErrorDetail(BaseModel):
    """Structured detail for a single field-level validation error.

    Used within ErrorResponse to provide actionable feedback on which
    fields failed validation and why.
    """

    field: str = Field(..., description="Name or path of the field that failed validation")
    message: str = Field(..., description="Human-readable description of the validation failure")
    type: str = Field(..., description="Error type classifier (e.g., 'value_error', 'type_error')")


class ErrorResponse(BaseModel):
    """Error response returned for validation failures (HTTP 422) and other errors.

    Provides a top-level detail message along with structured field-level errors
    for client-side error handling.
    Validates: Requirement 1.4 - Invalid payloads return HTTP 422 with descriptive validation errors.
    """

    detail: str = Field(..., description="Top-level human-readable error description")
    errors: list[ValidationErrorDetail] = Field(
        default_factory=list,
        description="List of field-level validation errors with structured details",
    )
    request_id: Optional[UUID] = Field(
        default=None,
        description="Request identifier for correlating errors with logs",
    )


class HealthResponse(BaseModel):
    """Response for liveness and readiness health check probes.

    Reports connectivity status of downstream dependencies
    (Kafka, Redis, TimescaleDB) and the service version.
    """

    status: str = Field(default="healthy", description="Overall service health status")
    kafka_connected: bool = Field(..., description="Whether the Kafka producer is connected")
    redis_connected: bool = Field(..., description="Whether the Redis cache is reachable")
    db_connected: bool = Field(..., description="Whether TimescaleDB is reachable")
    version: str = Field(..., description="Service version identifier")
