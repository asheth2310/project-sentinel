"""Database connection pool management for Project Sentinel.

Manages separate connection pools for ingestion (writes) and query API (reads)
to prevent read queries from impacting ingestion performance (Task 54).

Uses asyncpg connection pools with configurable min/max sizes.
"""

import logging
from typing import Any, Protocol

from src.config.settings import DatabaseSettings

logger = logging.getLogger(__name__)


class ConnectionPool(Protocol):
    """Protocol for an async database connection pool."""

    async def acquire(self) -> Any: ...
    async def release(self, connection: Any) -> None: ...
    async def close(self) -> None: ...


class DatabaseManager:
    """Manages separate connection pools for ingestion and query.

    Maintains two independent pools:
    - write_pool: Used by ingestion consumers for batch inserts (higher throughput)
    - read_pool: Used by query API endpoints (isolated from write load)

    This separation ensures that heavy read queries (dashboard, anomaly history)
    do not compete with real-time ingestion writes for connections.

    Args:
        settings: Database connection configuration.
    """

    def __init__(self, settings: DatabaseSettings) -> None:
        self._settings = settings
        self._write_pool: Any | None = None
        self._read_pool: Any | None = None
        self._started = False

    async def start(self) -> None:
        """Initialize both connection pools.

        Creates separate asyncpg pools for read and write operations.
        Write pool gets slightly more connections since ingestion is
        the primary workload.

        Raises:
            Exception: If pool creation fails (e.g., database unreachable).
        """
        try:
            import asyncpg

            # Write pool: optimized for ingestion throughput
            # Gets 60% of max connections
            write_max = max(2, int(self._settings.max_pool_size * 0.6))
            write_min = max(1, int(self._settings.min_pool_size * 0.6))

            self._write_pool = await asyncpg.create_pool(
                dsn=self._settings.dsn,
                min_size=write_min,
                max_size=write_max,
                command_timeout=30,
            )

            # Read pool: optimized for query API
            # Gets 40% of max connections
            read_max = max(2, int(self._settings.max_pool_size * 0.4))
            read_min = max(1, int(self._settings.min_pool_size * 0.4))

            self._read_pool = await asyncpg.create_pool(
                dsn=self._settings.dsn,
                min_size=read_min,
                max_size=read_max,
                command_timeout=60,  # Longer timeout for complex queries
            )

            self._started = True
            logger.info(
                "Database pools started: write(min=%d, max=%d), read(min=%d, max=%d)",
                write_min,
                write_max,
                read_min,
                read_max,
            )
        except ImportError:
            logger.warning(
                "asyncpg not available, database pools not created"
            )
        except Exception as exc:
            logger.error("Failed to create database pools: %s", exc)
            raise

    async def stop(self) -> None:
        """Close both connection pools gracefully."""
        if self._write_pool is not None:
            await self._write_pool.close()
            self._write_pool = None
            logger.info("Write connection pool closed")

        if self._read_pool is not None:
            await self._read_pool.close()
            self._read_pool = None
            logger.info("Read connection pool closed")

        self._started = False
        logger.info("Database manager stopped")

    @property
    def write_pool(self) -> Any:
        """Get the write connection pool for ingestion operations.

        Returns:
            The asyncpg connection pool configured for writes.

        Raises:
            RuntimeError: If pools haven't been started yet.
        """
        if self._write_pool is None:
            raise RuntimeError(
                "Write pool not initialized. Call start() first."
            )
        return self._write_pool

    @property
    def read_pool(self) -> Any:
        """Get the read connection pool for query API operations.

        Returns:
            The asyncpg connection pool configured for reads.

        Raises:
            RuntimeError: If pools haven't been started yet.
        """
        if self._read_pool is None:
            raise RuntimeError(
                "Read pool not initialized. Call start() first."
            )
        return self._read_pool

    @property
    def is_started(self) -> bool:
        """Whether the database manager has been started."""
        return self._started
