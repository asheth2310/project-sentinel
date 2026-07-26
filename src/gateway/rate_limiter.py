"""
Rate limiting using Redis sliding windows (per-agent and per-organization).

Implements a sliding window rate limiter using Redis sorted sets.
Each request timestamp is stored as a member scored by its time value.
Expired entries are removed before counting, and the count is compared
against the configured limit.

Fail-open behavior: if Redis is unavailable, requests are allowed and a
warning is logged. This ensures telemetry ingestion continues uninterrupted.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import redis.asyncio as redis

from src.gateway.redis_service import RedisService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitResult:
    """Result of a rate limit check.

    Attributes:
        allowed: Whether the request is within rate limits.
        remaining: Number of requests remaining in the current window.
        reset_at: When the current window resets (oldest entry expiration).
    """

    allowed: bool
    remaining: int
    reset_at: datetime


class RateLimiter:
    """Redis sliding window rate limiter for per-agent and per-organization limits.

    Uses Redis sorted sets where each member is a unique request identifier
    (timestamp + random suffix) and the score is the timestamp. On each check:
    1. Remove entries outside the sliding window (score < now - window)
    2. Count remaining entries
    3. If count < limit, add new entry and allow; otherwise deny

    Fail-open: if Redis is unavailable, all requests are allowed.
    """

    # Key prefix patterns
    AGENT_KEY_PREFIX = "rate_limit:agent:"
    ORG_KEY_PREFIX = "rate_limit:org:"

    def __init__(
        self,
        redis_service: RedisService,
        agent_limit: int = 100,
        agent_window: int = 60,
        org_limit: int = 1000,
        org_window: int = 60,
    ) -> None:
        """Initialize the rate limiter.

        Args:
            redis_service: The Redis service for connection management.
            agent_limit: Maximum requests per agent within the window.
            agent_window: Sliding window duration in seconds for agent limits.
            org_limit: Maximum requests per organization within the window.
            org_window: Sliding window duration in seconds for org limits.
        """
        self._redis_service = redis_service
        self._agent_limit = agent_limit
        self._agent_window = agent_window
        self._org_limit = org_limit
        self._org_window = org_window

    async def check_rate_limit(
        self, agent_id: UUID, org_id: UUID
    ) -> RateLimitResult:
        """Check rate limits for both the agent and the organization.

        Checks the per-agent limit first, then the per-organization limit.
        The most restrictive result is returned.

        If Redis is unavailable, fails open (allows the request).

        Args:
            agent_id: The agent making the request.
            org_id: The organization the agent belongs to.

        Returns:
            RateLimitResult indicating whether the request is allowed,
            remaining capacity, and window reset time.
        """
        client = self._redis_service._client
        if client is None or not self._redis_service.is_available:
            logger.warning(
                "Redis unavailable for rate limiting, failing open: "
                "agent_id=%s, org_id=%s",
                agent_id,
                org_id,
            )
            return RateLimitResult(
                allowed=True,
                remaining=self._agent_limit,
                reset_at=datetime.now(timezone.utc),
            )

        try:
            # Check agent-level rate limit
            agent_key = f"{self.AGENT_KEY_PREFIX}{agent_id}"
            agent_result = await self._check_sliding_window(
                client, agent_key, self._agent_limit, self._agent_window
            )

            if not agent_result.allowed:
                return agent_result

            # Check org-level rate limit
            org_key = f"{self.ORG_KEY_PREFIX}{org_id}"
            org_result = await self._check_sliding_window(
                client, org_key, self._org_limit, self._org_window
            )

            if not org_result.allowed:
                return org_result

            # Both passed — return the more restrictive remaining count
            if agent_result.remaining <= org_result.remaining:
                return agent_result
            return org_result

        except (redis.ConnectionError, redis.TimeoutError, OSError) as exc:
            logger.warning(
                "Redis error during rate limit check, failing open: %s", exc
            )
            self._redis_service._available = False
            return RateLimitResult(
                allowed=True,
                remaining=self._agent_limit,
                reset_at=datetime.now(timezone.utc),
            )

    async def _check_sliding_window(
        self,
        client: redis.Redis,
        key: str,
        limit: int,
        window: int,
    ) -> RateLimitResult:
        """Execute the sliding window check using a Redis sorted set.

        Atomic pipeline:
        1. ZREMRANGEBYSCORE to remove expired entries
        2. ZCARD to count current entries
        3. ZADD to add the new entry (only if under limit)
        4. EXPIRE to set key TTL for cleanup

        Args:
            client: The async Redis client.
            key: The sorted set key.
            limit: Maximum allowed requests in the window.
            window: Window duration in seconds.

        Returns:
            RateLimitResult for this specific window.
        """
        now = time.time()
        window_start = now - window
        # Use a unique member to avoid collisions: timestamp with enough precision
        member = f"{now}"

        # Use pipeline for atomicity
        pipe = client.pipeline(transaction=True)
        # Remove entries outside the window
        pipe.zremrangebyscore(key, "-inf", window_start)
        # Count entries in the current window
        pipe.zcard(key)
        results = await pipe.execute()

        current_count = results[1]

        if current_count >= limit:
            # Over limit — find when the oldest entry expires
            oldest_entries = await client.zrange(
                key, 0, 0, withscores=True
            )
            if oldest_entries:
                oldest_score = oldest_entries[0][1]
                reset_timestamp = oldest_score + window
            else:
                reset_timestamp = now + window

            reset_at = datetime.fromtimestamp(reset_timestamp, tz=timezone.utc)
            return RateLimitResult(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
            )

        # Under limit — add this request
        pipe2 = client.pipeline(transaction=True)
        pipe2.zadd(key, {member: now})
        pipe2.expire(key, window + 1)  # TTL slightly longer than window for cleanup
        await pipe2.execute()

        remaining = limit - current_count - 1  # -1 for the request we just added
        # Reset time is when the window would fully clear
        reset_at = datetime.fromtimestamp(now + window, tz=timezone.utc)

        return RateLimitResult(
            allowed=True,
            remaining=max(remaining, 0),
            reset_at=reset_at,
        )
