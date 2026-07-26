"""
Batch deduplication service for idempotent telemetry ingestion.

Implements Requirement 1.7: Duplicate batch submissions (same batch_id)
are idempotent and do not create duplicate events.

Uses Redis to track processed batch_ids with a configurable TTL.
Fails open if Redis is unavailable (allows the request through).
"""

import logging
from uuid import UUID

from src.gateway.redis_service import RedisService

logger = logging.getLogger(__name__)

# Default TTL for deduplication keys: 24 hours
DEFAULT_DEDUP_TTL_SECONDS = 86400

# Redis key prefix for batch deduplication
BATCH_PROCESSED_KEY_PREFIX = "batch_processed"


class DeduplicationService:
    """Service for batch_id deduplication using Redis.

    Ensures that processing the same batch_id multiple times produces the
    same result without re-producing events to Kafka.

    Fail-open behavior: If Redis is unavailable, the request is allowed
    through (may create duplicates in edge cases, but ingestion is not blocked).
    """

    def __init__(self, redis_service: RedisService) -> None:
        self._redis = redis_service

    def _make_key(self, batch_id: UUID) -> str:
        """Build the Redis key for a given batch_id."""
        return f"{BATCH_PROCESSED_KEY_PREFIX}:{batch_id}"

    async def is_duplicate(self, batch_id: UUID) -> bool:
        """Check if a batch_id has already been processed.

        Returns True if the batch_id exists in Redis (duplicate submission).
        Returns False if the key does not exist or Redis is unavailable (fail-open).
        """
        key = self._make_key(batch_id)
        value = await self._redis.get(key)

        if value is not None:
            logger.info(
                "Duplicate batch detected: batch_id=%s (already processed)",
                batch_id,
            )
            return True

        return False

    async def mark_processed(
        self, batch_id: UUID, ttl_seconds: int = DEFAULT_DEDUP_TTL_SECONDS
    ) -> None:
        """Mark a batch_id as processed in Redis with a TTL.

        Sets the key with a TTL so that deduplication state is automatically
        cleaned up after the configured period. If Redis is unavailable,
        logs a warning and continues (fail-open).
        """
        key = self._make_key(batch_id)
        success = await self._redis.set(key, "1", ttl=ttl_seconds)

        if success:
            logger.debug(
                "Marked batch as processed: batch_id=%s (TTL=%ds)",
                batch_id,
                ttl_seconds,
            )
        else:
            logger.warning(
                "Failed to mark batch as processed (Redis unavailable): batch_id=%s. "
                "Duplicate detection may miss this batch on retry.",
                batch_id,
            )
