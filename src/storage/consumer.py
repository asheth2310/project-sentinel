"""Storage writer consumer for Project Sentinel.

Consumes telemetry-enriched events from Kafka and writes them to
TimescaleDB in batches for efficient time-series storage (Task 49).
"""

import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from src.config.settings import KafkaSettings
from src.gateway.kafka_consumer import KafkaConsumerService

logger = logging.getLogger(__name__)


class StorageWriterConsumer(KafkaConsumerService):
    """Consumes telemetry-enriched events and writes to TimescaleDB.

    Buffers incoming messages and flushes them in configurable batch sizes
    using multi-row INSERT statements for efficient write throughput.

    Args:
        settings: Kafka connection configuration.
        db_pool: asyncpg connection pool for write operations.
        batch_size: Number of messages to buffer before flushing (default 100).
        flush_interval_seconds: Maximum seconds between flushes (default 5).
    """

    # SQL for batch insert into telemetry_logs hypertable
    INSERT_SQL = """
        INSERT INTO telemetry_logs (
            timestamp, log_id, agent_id, org_id, prompt_tokens,
            completion_tokens, total_cost, latency_ms, tool_name,
            prompt_hash, session_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    """

    def __init__(
        self,
        settings: KafkaSettings,
        db_pool: Any,
        batch_size: int = 100,
        flush_interval_seconds: float = 5.0,
    ) -> None:
        super().__init__(
            settings,
            settings.topic_telemetry_enriched,
            settings.consumer_group_storage,
        )
        self._db_pool = db_pool
        self._batch: list[dict[str, Any]] = []
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._last_flush_time: float = time.monotonic()
        self._total_written: int = 0
        self._total_errors: int = 0

    async def process_message(self, message: dict[str, Any]) -> None:
        """Buffer a telemetry message and flush when batch is full.

        Messages are accumulated in memory until batch_size is reached
        or the flush interval expires, at which point the batch is
        written to TimescaleDB.

        Args:
            message: Deserialized telemetry event from Kafka.
        """
        self._batch.append(message)

        # Flush if batch is full or interval expired
        should_flush = (
            len(self._batch) >= self._batch_size
            or (time.monotonic() - self._last_flush_time) >= self._flush_interval
        )

        if should_flush:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Write accumulated batch to TimescaleDB using executemany.

        Uses asyncpg's executemany for efficient multi-row insertion.
        On failure, logs the error and discards the batch to avoid
        blocking the consumer (events can be replayed from Kafka).
        """
        if not self._batch:
            return

        batch_to_write = self._batch[:]
        self._batch = []
        self._last_flush_time = time.monotonic()

        try:
            rows = [self._parse_row(msg) for msg in batch_to_write]
            # Filter out rows that failed to parse
            valid_rows = [r for r in rows if r is not None]

            if not valid_rows:
                logger.warning("No valid rows in batch of %d messages", len(batch_to_write))
                return

            async with self._db_pool.acquire() as conn:
                await conn.executemany(self.INSERT_SQL, valid_rows)

            self._total_written += len(valid_rows)
            logger.debug(
                "Flushed %d rows to TimescaleDB (total: %d)",
                len(valid_rows),
                self._total_written,
            )
        except Exception as exc:
            self._total_errors += len(batch_to_write)
            logger.error(
                "Failed to flush batch of %d messages to TimescaleDB: %s",
                len(batch_to_write),
                exc,
            )

    def _parse_row(self, message: dict[str, Any]) -> tuple | None:
        """Parse a message dict into a tuple of values for insertion.

        Args:
            message: Raw telemetry message dict.

        Returns:
            Tuple of values matching INSERT_SQL columns, or None if parsing fails.
        """
        try:
            timestamp = message.get("timestamp")
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now(timezone.utc)

            log_id = message.get("log_id")
            if isinstance(log_id, str):
                log_id = UUID(log_id)

            agent_id = message.get("agent_id")
            if isinstance(agent_id, str):
                agent_id = UUID(agent_id)

            org_id = message.get("org_id")
            if isinstance(org_id, str):
                org_id = UUID(org_id)

            session_id = message.get("session_id")
            if isinstance(session_id, str):
                session_id = UUID(session_id)

            total_cost = message.get("total_cost", 0)
            if not isinstance(total_cost, Decimal):
                total_cost = Decimal(str(total_cost))

            return (
                timestamp,
                log_id,
                agent_id,
                org_id,
                int(message.get("prompt_tokens", 0)),
                int(message.get("completion_tokens", 0)),
                total_cost,
                int(message.get("latency_ms", 0)),
                message.get("tool_name"),
                message.get("prompt_hash"),
                session_id,
            )
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Failed to parse telemetry message: %s", exc)
            return None

    @property
    def total_written(self) -> int:
        """Total number of rows successfully written to TimescaleDB."""
        return self._total_written

    @property
    def total_errors(self) -> int:
        """Total number of rows that failed to write."""
        return self._total_errors

    @property
    def pending_batch_size(self) -> int:
        """Number of messages currently buffered awaiting flush."""
        return len(self._batch)
