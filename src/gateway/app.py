"""
FastAPI application instance for the Project Sentinel ingestion gateway.

Creates and configures the FastAPI app with:
- OpenAPI documentation (title, version, tags)
- Lifespan management for service startup/shutdown (Kafka, Redis, EventBuffer)
- Custom validation error handler
- Route registration for telemetry ingestion endpoints
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config.settings import get_settings
from src.gateway.circuit_breaker import CircuitBreakerService
from src.gateway.dependencies import (
    set_circuit_breaker_middleware,
    set_event_buffer,
    set_kafka_producer,
    set_rate_limiter,
    set_redis_service,
)
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter
from src.gateway.redis_service import RedisService
from src.gateway.routes import router as telemetry_router
from src.gateway.health import router as health_router
from src.gateway.validation import register_validation_handler

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle: start/stop Kafka, Redis, and buffer on app startup/shutdown.

    Services that fail to connect will log warnings but won't prevent startup.
    This allows the gateway to run in degraded mode when dependencies are unavailable.
    """
    settings = get_settings()
    kafka_producer = None
    redis_service = None

    # Initialize Kafka producer (non-fatal if unavailable)
    try:
        kafka_producer = KafkaProducerService(settings.kafka)
        await kafka_producer.start()
        set_kafka_producer(kafka_producer)
        logger.info("Kafka producer started")
    except Exception as exc:
        logger.warning("Kafka unavailable, running in degraded mode: %s", exc)

    # Initialize Redis service (non-fatal if unavailable)
    try:
        redis_service = RedisService(settings.redis)
        await redis_service.start()
        set_redis_service(redis_service)
        logger.info("Redis service started")
    except Exception as exc:
        logger.warning("Redis unavailable, running in degraded mode: %s", exc)

    # Initialize event buffer for Kafka failure fallback
    event_buffer = EventBuffer(max_size=settings.kafka.buffer_max_events)
    set_event_buffer(event_buffer)

    # Initialize circuit breaker middleware (uses Redis, graceful if unavailable)
    if redis_service:
        cb_service = CircuitBreakerService(redis_service)
        cb_middleware = CircuitBreakerMiddleware(cb_service)
        set_circuit_breaker_middleware(cb_middleware)

        # Initialize rate limiter (per-agent and per-organization sliding windows)
        rate_limiter = RateLimiter(redis_service)
        set_rate_limiter(rate_limiter)

    logger.info("Ingestion gateway services started")

    yield

    # Shutdown services
    if kafka_producer:
        await kafka_producer.stop()
    if redis_service:
        await redis_service.stop()
    logger.info("Ingestion gateway services stopped")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns a fully configured FastAPI instance with routes registered,
    lifespan management for services, and OpenAPI documentation.
    """
    settings = get_settings()

    app = FastAPI(
        title=settings.app.title,
        version=settings.app.version,
        description=(
            "Enterprise-grade observability and governance platform for "
            "multi-agent AI deployments. Provides real-time telemetry ingestion, "
            "anomaly detection, and automated circuit breakers."
        ),
        lifespan=lifespan,
        openapi_tags=[
            {
                "name": "telemetry",
                "description": "Telemetry ingestion endpoints for AI agent SDKs",
            },
            {
                "name": "health",
                "description": "Service health and readiness probes",
            },
        ],
    )

    # Register custom validation error handler (Requirement 1.4)
    register_validation_handler(app)

    # Register routers
    app.include_router(telemetry_router)
    app.include_router(health_router)

    return app


# Application instance for uvicorn
app = create_app()
