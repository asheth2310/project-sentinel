"""
FastAPI route definitions for the ingestion gateway.

Implements POST /v1/telemetry endpoint with bearer token authentication,
circuit breaker enforcement at ingestion (Requirement 3), rate limiting
per-agent and per-organization (Requirement 14.5), agent identity
verification, and atomic batch production to Kafka (all-or-nothing
semantics per Requirement 1.6).
"""

import json
import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse

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
from src.gateway.kafka_producer import (
    KafkaProducerError,
    KafkaProducerService,
    KafkaUnavailableError,
)
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter, RateLimitResult
from src.models.responses import ErrorResponse, IngestionResponse
from src.models.telemetry import TelemetryBatch

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1",
    tags=["telemetry"],
)


@router.post(
    "/telemetry",
    response_model=IngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid authentication"},
        403: {"model": ErrorResponse, "description": "Agent ID mismatch with token"},
        422: {"model": ErrorResponse, "description": "Validation error in request body"},
        429: {"model": ErrorResponse, "description": "Circuit breaker active for agent"},
        503: {"description": "Service unavailable - Kafka broker down or buffer full"},
    },
    summary="Ingest telemetry batch",
    description=(
        "Accept a batch of telemetry events from an AI agent SDK. "
        "Requires bearer token authentication. The agent_id in the batch "
        "must match the agent identity associated with the API key. "
        "Rejects requests with 429 if the agent's circuit breaker is active. "
        "Batch processing is atomic: either all events are produced to Kafka or none are."
    ),
)
async def ingest_telemetry(
    batch: TelemetryBatch,
    identity: AgentIdentity = Depends(authenticate_agent),
    cb_middleware: CircuitBreakerMiddleware = Depends(get_circuit_breaker_middleware),
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
    dedup_service: DeduplicationService = Depends(get_deduplication_service),
    kafka_producer: KafkaProducerService = Depends(get_kafka_producer),
    event_buffer: EventBuffer = Depends(get_event_buffer),
    kafka_settings: KafkaSettings = Depends(get_kafka_settings),
) -> Response:
    """Accept a telemetry batch from an authenticated agent and produce to Kafka atomically.

    Authentication is handled by the authenticate_agent dependency which
    validates the bearer token and resolves the agent identity.

    Circuit breaker enforcement (Requirement 3): Before processing the batch,
    checks Redis for the agent's circuit breaker state. Returns 429 if active.
    Fails open if Redis is unavailable.

    Idempotency (Requirement 1.7): Before producing, checks if the batch_id
    has already been processed. If so, returns the same 202 response without
    re-producing to Kafka. Fails open if Redis is unavailable.

    Authorization check: the agent_id in the submitted batch must match
    the agent_id associated with the authenticated API key.

    Kafka production is atomic (all-or-nothing):
    - On success: all events produced to Kafka, returns 202 Accepted
    - On KafkaUnavailableError: buffers events, returns 202 (buffered)
    - On buffer full: returns 503 with Retry-After header
    - On KafkaProducerError: returns 503 (no partial writes)

    Validates: Requirement 1.6 - Batch processing is atomic.
    Validates: Requirement 1.7 - Idempotent batch ingestion.
    Validates: Requirement 3.1, 3.2, 3.4 - Circuit breaker enforcement.
    """
    # Circuit breaker check (Requirement 3.1, 3.2, 3.4)
    # Raises HTTPException 429 if breaker is active for this agent.
    # Fails open if Redis is unavailable (accepts telemetry).
    await cb_middleware.check(batch.agent_id)

    # Rate limit check (Requirement 14.5)
    # Enforced per-agent and per-organization using Redis sliding windows.
    # Fails open if Redis is unavailable.
    rate_limit_result = await rate_limiter.check_rate_limit(
        identity.agent_id, identity.org_id
    )
    if not rate_limit_result.allowed:
        reset_timestamp = int(rate_limit_result.reset_at.timestamp())
        seconds_until_reset = max(
            0, reset_timestamp - int(time.time())
        )
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": "Rate limit exceeded"},
            headers={
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(reset_timestamp),
                "Retry-After": str(seconds_until_reset),
            },
        )

    # Authorization: verify agent_id in batch matches authenticated identity
    if batch.agent_id != identity.agent_id:
        logger.warning(
            "Agent ID mismatch: batch has %s but token authenticates %s",
            batch.agent_id,
            identity.agent_id,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Forbidden: batch agent_id ({batch.agent_id}) does not match "
                f"authenticated agent ({identity.agent_id})"
            ),
        )

    topic = kafka_settings.topic_telemetry_raw
    agent_id_key = str(batch.agent_id)

    # Idempotency check (Requirement 1.7)
    # If this batch_id was already processed, return same 202 response without re-producing.
    # Fails open if Redis is unavailable (allows the request through).
    if await dedup_service.is_duplicate(batch.batch_id):
        logger.info(
            "Duplicate batch %s ignored (idempotent, agent=%s)",
            batch.batch_id,
            batch.agent_id,
        )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=IngestionResponse(
                batch_id=batch.batch_id,
                status="accepted",
                event_count=len(batch.events),
            ).model_dump(mode="json"),
        )

    # Serialize each event to JSON bytes for Kafka production
    serialized_events: list[tuple[str, str, bytes]] = []
    for event in batch.events:
        event_bytes = json.dumps(
            event.model_dump(mode="json"),
            default=str,
        ).encode("utf-8")
        serialized_events.append((topic, agent_id_key, event_bytes))

    try:
        # Atomic batch produce: all-or-nothing delivery guaranteed by produce_batch
        await kafka_producer.produce_batch(serialized_events)

        # Mark batch as processed for idempotency (Requirement 1.7)
        await dedup_service.mark_processed(batch.batch_id)

        logger.info(
            "Batch %s produced to Kafka (%d events, agent=%s)",
            batch.batch_id,
            len(batch.events),
            batch.agent_id,
        )

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content=IngestionResponse(
                batch_id=batch.batch_id,
                status="accepted",
                event_count=len(batch.events),
            ).model_dump(mode="json"),
            headers={
                "X-RateLimit-Remaining": str(rate_limit_result.remaining),
                "X-RateLimit-Reset": str(
                    int(rate_limit_result.reset_at.timestamp())
                ),
            },
        )

    except KafkaUnavailableError as exc:
        # Kafka is down - try to buffer events for later flush
        logger.warning(
            "Kafka unavailable for batch %s (agent=%s): %s. Attempting to buffer.",
            batch.batch_id,
            batch.agent_id,
            exc,
        )

        buffered_event = {
            "batch_id": str(batch.batch_id),
            "agent_id": agent_id_key,
            "topic": topic,
            "events": serialized_events,
        }

        accepted = await event_buffer.add(buffered_event)

        if accepted:
            logger.info(
                "Batch %s buffered (%d events). Will flush when Kafka recovers.",
                batch.batch_id,
                len(batch.events),
            )
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=IngestionResponse(
                    batch_id=batch.batch_id,
                    status="buffered",
                    event_count=len(batch.events),
                ).model_dump(mode="json"),
            )
        else:
            # Buffer is full - reject with 503
            logger.error(
                "Event buffer full. Rejecting batch %s (agent=%s).",
                batch.batch_id,
                batch.agent_id,
            )
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "detail": "Service temporarily unavailable. Kafka broker is down and event buffer is full.",
                    "retry_after": 30,
                },
                headers={"Retry-After": "30"},
            )

    except KafkaProducerError as exc:
        # Partial failure or unrecoverable produce error.
        # produce_batch guarantees no partial writes, so we return 503.
        logger.error(
            "Kafka producer error for batch %s (agent=%s): %s",
            batch.batch_id,
            batch.agent_id,
            exc,
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "detail": "Failed to produce batch to Kafka. No events were written (atomic guarantee).",
                "retry_after": 10,
            },
            headers={"Retry-After": "10"},
        )
