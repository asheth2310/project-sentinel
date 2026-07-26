"""
Shared FastAPI dependency functions for the ingestion gateway.

Provides dependency injection for services (Kafka, Redis) and
authentication via bearer token extraction and validation.
"""

import logging
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from src.config.settings import KafkaSettings, Settings, get_settings
from src.gateway.auth import AgentIdentity, APIKeyRegistry, get_api_key_registry
from src.gateway.deduplication import DeduplicationService
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter
from src.gateway.redis_service import RedisService

logger = logging.getLogger(__name__)

# Module-level service singletons (initialized during app startup)
_kafka_producer: KafkaProducerService | None = None
_redis_service: RedisService | None = None
_circuit_breaker_middleware: CircuitBreakerMiddleware | None = None
_event_buffer: EventBuffer | None = None
_rate_limiter: RateLimiter | None = None
_deduplication_service: DeduplicationService | None = None


def set_kafka_producer(producer: KafkaProducerService) -> None:
    """Set the Kafka producer instance (called during app startup)."""
    global _kafka_producer
    _kafka_producer = producer


def set_redis_service(redis_svc: RedisService) -> None:
    """Set the Redis service instance (called during app startup)."""
    global _redis_service
    _redis_service = redis_svc


def set_circuit_breaker_middleware(middleware: CircuitBreakerMiddleware) -> None:
    """Set the circuit breaker middleware instance (called during app startup)."""
    global _circuit_breaker_middleware
    _circuit_breaker_middleware = middleware


def set_event_buffer(buffer: EventBuffer) -> None:
    """Set the event buffer instance (called during app startup)."""
    global _event_buffer
    _event_buffer = buffer


def set_rate_limiter(limiter: RateLimiter) -> None:
    """Set the rate limiter instance (called during app startup)."""
    global _rate_limiter
    _rate_limiter = limiter


def set_deduplication_service(dedup_svc: DeduplicationService) -> None:
    """Set the deduplication service instance (called during app startup)."""
    global _deduplication_service
    _deduplication_service = dedup_svc


def get_kafka_producer() -> KafkaProducerService:
    """FastAPI dependency: get the Kafka producer service."""
    if _kafka_producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer not initialized",
        )
    return _kafka_producer


def get_redis_service() -> RedisService:
    """FastAPI dependency: get the Redis service."""
    if _redis_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis service not initialized",
        )
    return _redis_service


def get_circuit_breaker_middleware() -> CircuitBreakerMiddleware:
    """FastAPI dependency: get the circuit breaker middleware."""
    if _circuit_breaker_middleware is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Circuit breaker middleware not initialized",
        )
    return _circuit_breaker_middleware


def get_event_buffer() -> EventBuffer:
    """FastAPI dependency: get the event buffer for Kafka failure scenarios."""
    if _event_buffer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event buffer not initialized",
        )
    return _event_buffer


def get_rate_limiter() -> RateLimiter:
    """FastAPI dependency: get the rate limiter."""
    if _rate_limiter is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Rate limiter not initialized",
        )
    return _rate_limiter


def get_deduplication_service() -> DeduplicationService:
    """FastAPI dependency: get the deduplication service."""
    if _deduplication_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Deduplication service not initialized",
        )
    return _deduplication_service


def get_kafka_settings() -> KafkaSettings:
    """FastAPI dependency: get Kafka settings."""
    return get_settings().kafka


def get_settings_dep() -> Settings:
    """FastAPI dependency: get application settings."""
    return get_settings()


async def authenticate_agent(
    authorization: Annotated[str | None, Header()] = None,
    registry: APIKeyRegistry = Depends(get_api_key_registry),
) -> AgentIdentity:
    """FastAPI dependency: authenticate an agent via bearer token.

    Extracts the bearer token from the Authorization header, validates it
    against the API key registry, and returns the resolved agent identity.

    Raises:
        HTTPException 401: If Authorization header is missing or token is invalid.
    """
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Extract bearer token
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header format. Expected: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = parts[1].strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Empty bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Resolve token to agent identity
    identity = registry.resolve(token)
    if identity is None:
        logger.warning("Authentication failed: invalid API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return identity
