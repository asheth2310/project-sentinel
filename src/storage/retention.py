"""Data retention management for Project Sentinel.

Implements configurable data retention policies with automated purge
for TimescaleDB time-series data (Task 50).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


class RetentionManager:
    """Manages data retention policies for TimescaleDB.

    Provides automated purge of telemetry data older than a configurable
    retention period. Supports different retention policies for different
    data types (telemetry logs vs anomaly events).

    Args:
        db_pool: asyncpg connection pool for executing purge queries.
        default_retention_days: Default retention period in days (default 90).
    """

    # Tables and their time columns for retention management
    RETENTION_TARGETS = {
        "telemetry_logs": "timestamp",
        "anomaly_events": "detected_at",
        "audit_log": "created_at",
    }

    def __init__(
        self,
        db_pool: Any,
        default_retention_days: int = 90,
    ) -> None:
        self._db_pool = db_pool
        self._default_retention_days = default_retention_days

    async def purge_old_data(self, retention_days: int | None = None) -> int:
        """Delete telemetry data older than the retention period.

        Uses TimescaleDB's drop_chunks for hypertables when available,
        falling back to standard DELETE for regular tables.

        Args:
            retention_days: Number of days to retain. Uses default if None.

        Returns:
            Total number of rows/chunks deleted across all tables.
        """
        days = retention_days if retention_days is not None else self._default_retention_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        total_deleted = 0

        logger.info(
            "Starting retention purge: removing data older than %d days (cutoff: %s)",
            days,
            cutoff.isoformat(),
        )

        for table, time_column in self.RETENTION_TARGETS.items():
            try:
                deleted = await self._purge_table(table, time_column, cutoff)
                total_deleted += deleted
                logger.info(
                    "Purged %d rows from '%s' (cutoff: %s)",
                    deleted,
                    table,
                    cutoff.isoformat(),
                )
            except Exception as exc:
                logger.error(
                    "Failed to purge table '%s': %s",
                    table,
                    exc,
                )

        logger.info(
            "Retention purge complete: %d total rows deleted", total_deleted
        )
        return total_deleted

    async def _purge_table(
        self, table: str, time_column: str, cutoff: datetime
    ) -> int:
        """Purge old data from a specific table.

        Attempts to use TimescaleDB drop_chunks first (more efficient for
        hypertables), falls back to DELETE for regular tables.

        Args:
            table: Table name to purge.
            time_column: Name of the timestamp column.
            cutoff: Delete data with timestamps before this datetime.

        Returns:
            Number of rows deleted.
        """
        async with self._db_pool.acquire() as conn:
            # Try TimescaleDB drop_chunks for hypertables (more efficient)
            try:
                result = await conn.fetchval(
                    "SELECT drop_chunks($1, older_than => $2::timestamptz)",
                    table,
                    cutoff,
                )
                # drop_chunks returns number of chunks dropped
                if result is not None:
                    return int(result) if isinstance(result, int) else 0
            except Exception:
                # Not a hypertable or drop_chunks not available, use DELETE
                pass

            # Fallback: standard DELETE with RETURNING count
            result = await conn.execute(
                f"DELETE FROM {table} WHERE {time_column} < $1",  # noqa: S608
                cutoff,
            )
            # asyncpg execute returns a status string like "DELETE 42"
            if result and result.startswith("DELETE"):
                parts = result.split()
                if len(parts) == 2:
                    return int(parts[1])
            return 0

    async def purge_table(
        self, table: str, retention_days: int | None = None
    ) -> int:
        """Purge old data from a specific table only.

        Args:
            table: Table name to purge (must be in RETENTION_TARGETS).
            retention_days: Number of days to retain. Uses default if None.

        Returns:
            Number of rows deleted.

        Raises:
            ValueError: If the table is not a known retention target.
        """
        if table not in self.RETENTION_TARGETS:
            raise ValueError(
                f"Unknown retention target: '{table}'. "
                f"Valid targets: {list(self.RETENTION_TARGETS.keys())}"
            )

        days = retention_days if retention_days is not None else self._default_retention_days
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        time_column = self.RETENTION_TARGETS[table]

        return await self._purge_table(table, time_column, cutoff)

    @property
    def default_retention_days(self) -> int:
        """The default retention period in days."""
        return self._default_retention_days

    @default_retention_days.setter
    def default_retention_days(self, value: int) -> None:
        """Set the default retention period.

        Args:
            value: Retention period in days (must be positive).

        Raises:
            ValueError: If value is not positive.
        """
        if value <= 0:
            raise ValueError("Retention days must be positive")
        self._default_retention_days = value
