"""
Redis service with connection pooling, health checks, and fail-open behavior.

Provides async Redis operations for circuit breaker state and rate limiting.
When Redis is unavailable, all operations fail open (return graceful defaults)
to ensure telemetry ingestion continues uninterrupted.
"""

import logging
from typing import Any

import redis.asyncio as redis
from redis.asyncio import ConnectionPool, Redis

from src.config.settings import RedisSettings

logger = logging.getLogger(__name__)


class RedisService:
    """Async Redis client with connection pooling and fail-open semantics.

    All read/write operations catch connection and timeout errors,
    log a warning, and return safe defaults instead of raising.
    This ensures the ingestion gateway remains available even when
    Redis is down (Error Scenario 2 from design).
    """

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._pool: ConnectionPool | None = None
        self._client: Redis | None = None
        self._available: bool = False

    @property
    def is_available(self) -> bool:
        """Whether the Redis connection is currently healthy."""
        return self._available

    async def start(self) -> None:
        """Create the connection pool and establish initial connection."""
        try:
            self._pool = ConnectionPool(
                host=self._settings.host,
                port=self._settings.port,
                db=self._settings.db,
                password=self._settings.password,
                max_connections=self._settings.connection_pool_size,
                socket_timeout=self._settings.socket_timeout,
                socket_connect_timeout=self._settings.socket_connect_timeout,
                decode_responses=True,
            )
            self._client = Redis(connection_pool=self._pool)
            # Verify connectivity with a ping
            await self._client.ping()
            self._available = True
            logger.info(
                "Redis connection established: %s:%d db=%d",
                self._settings.host,
                self._settings.port,
                self._settings.db,
            )
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            self._available = False
            logger.warning(
                "Redis unavailable during startup, operating in fail-open mode: %s",
                exc,
            )

    async def stop(self) -> None:
        """Close the connection pool and release resources."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
        if self._pool is not None:
            try:
                await self._pool.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._pool = None
        self._available = False
        logger.info("Redis connection closed")

    async def health_check(self) -> bool:
        """Ping Redis and update availability status.

        Returns True if Redis responds to PING, False otherwise.
        """
        if self._client is None:
            self._available = False
            return False
        try:
            await self._client.ping()
            self._available = True
            return True
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            self._available = False
            logger.warning("Redis health check failed: %s", exc)
            return False

    async def get(self, key: str) -> str | None:
        """Get a value from Redis by key.

        Fail-open behavior: returns None on connection error so that
        callers (e.g., circuit breaker check) treat missing data as
        'no restriction' and accept telemetry.
        """
        if self._client is None:
            logger.warning(
                "Redis GET failed (no client): key=%s, failing open", key
            )
            return None
        try:
            value = await self._client.get(key)
            self._available = True
            return value
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            self._available = False
            logger.warning(
                "Redis GET failed for key=%s, failing open: %s", key, exc
            )
            return None

    async def set(
        self, key: str, value: Any, ttl: int | None = None
    ) -> bool:
        """Set a key-value pair in Redis with optional TTL.

        Fail-open behavior: returns False on connection error.
        The caller should treat a False return as a non-fatal issue.
        """
        if self._client is None:
            logger.warning(
                "Redis SET failed (no client): key=%s, failing open", key
            )
            return False
        try:
            if ttl is not None:
                await self._client.set(key, value, ex=ttl)
            else:
                await self._client.set(key, value)
            self._available = True
            return True
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            self._available = False
            logger.warning(
                "Redis SET failed for key=%s, failing open: %s", key, exc
            )
            return False

    async def delete(self, key: str) -> bool:
        """Delete a key from Redis.

        Fail-open behavior: returns False on connection error.
        """
        if self._client is None:
            logger.warning(
                "Redis DELETE failed (no client): key=%s, failing open", key
            )
            return False
        try:
            await self._client.delete(key)
            self._available = True
            return True
        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            self._available = False
            logger.warning(
                "Redis DELETE failed for key=%s, failing open: %s", key, exc
            )
            return False
