"""
Circuit breaker service for managing agent kill-switches via Redis.

Provides atomic read/write operations for circuit breaker state with
optional TTL for automatic deactivation. Follows fail-open semantics:
if Redis is unavailable, agents are assumed active (not circuit-broken).

Implements Requirements 10.2, 10.3, 10.6.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import UUID

from src.gateway.redis_service import RedisService
from src.models.governance import CircuitBreakerState

logger = logging.getLogger(__name__)

# Redis key prefix for circuit breaker state
_KEY_PREFIX = "circuit_breaker"


def _make_key(agent_id: UUID) -> str:
    """Build the Redis key for a given agent's circuit breaker state."""
    return f"{_KEY_PREFIX}:{agent_id}"


class CircuitBreakerService:
    """Manages circuit breaker (kill-switch) state in Redis.

    All state changes are atomic: the full CircuitBreakerState is serialized
    to JSON and written in a single Redis SET operation (Requirement 10.6).

    Reads fail open: if Redis is unavailable, check_agent_status returns
    False (agent is NOT circuit-broken) so telemetry continues flowing.
    """

    def __init__(self, redis_service: RedisService) -> None:
        self._redis = redis_service

    async def check_agent_status(self, agent_id: UUID) -> bool:
        """Check whether the circuit breaker is active for the given agent.

        Returns True if the circuit breaker IS active (agent is killed).
        Returns False if the circuit breaker is inactive or if Redis is
        unavailable (fail-open behavior per Requirement 3.4).
        """
        key = _make_key(agent_id)
        raw = await self._redis.get(key)
        if raw is None:
            return False
        try:
            data = json.loads(raw)
            return data.get("is_active", False)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Malformed circuit breaker state for agent %s, failing open",
                agent_id,
            )
            return False

    async def activate(
        self,
        agent_id: UUID,
        reason: str,
        activated_by: str,
        ttl_seconds: int | None = None,
    ) -> CircuitBreakerState:
        """Activate the circuit breaker for an agent.

        Stores the full state atomically in a single Redis SET (Requirement 10.6).
        If ttl_seconds is provided, the Redis key will auto-expire after that
        duration, effectively deactivating the breaker (Requirement 10.3).

        Args:
            agent_id: The agent to circuit-break.
            reason: Human-readable reason for activation.
            activated_by: Actor identity ('system' for automated, user ID for manual).
            ttl_seconds: Optional TTL in seconds for auto-deactivation.

        Returns:
            The persisted CircuitBreakerState.
        """
        now = datetime.now(timezone.utc)
        state = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=now,
            activated_by=activated_by,
            reason=reason,
            ttl_seconds=ttl_seconds,
        )

        key = _make_key(agent_id)
        payload = self._serialize_state(state)

        # Atomic write: single SET with optional TTL (Requirement 10.6)
        success = await self._redis.set(key, payload, ttl=ttl_seconds)
        if not success:
            logger.warning(
                "Failed to persist circuit breaker activation for agent %s",
                agent_id,
            )

        return state

    async def deactivate(
        self,
        agent_id: UUID,
        authorized_by: str,
    ) -> CircuitBreakerState:
        """Deactivate the circuit breaker for an agent.

        Requires explicit authorization (Requirement 10.4). Removes the
        Redis key entirely so subsequent checks return False.

        Args:
            agent_id: The agent to un-circuit-break.
            authorized_by: Identity of the user authorizing deactivation.

        Returns:
            A CircuitBreakerState representing the deactivated state.
        """
        state = CircuitBreakerState(
            agent_id=agent_id,
            is_active=False,
            activated_at=None,
            activated_by=authorized_by,
            reason="deactivated",
            ttl_seconds=None,
        )

        key = _make_key(agent_id)
        success = await self._redis.delete(key)
        if not success:
            logger.warning(
                "Failed to remove circuit breaker state for agent %s",
                agent_id,
            )

        return state

    async def get_state(self, agent_id: UUID) -> CircuitBreakerState | None:
        """Retrieve the current circuit breaker state for an agent.

        Returns None if no state exists (agent was never circuit-broken
        or the TTL expired) or if Redis is unavailable.
        """
        key = _make_key(agent_id)
        raw = await self._redis.get(key)
        if raw is None:
            return None
        try:
            return self._deserialize_state(raw)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning(
                "Failed to deserialize circuit breaker state for agent %s: %s",
                agent_id,
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_state(state: CircuitBreakerState) -> str:
        """Serialize CircuitBreakerState to a JSON string for Redis storage.

        Uses Pydantic's model serialization which handles UUID and datetime
        conversion. The entire state is written as one value to ensure
        atomicity (Requirement 10.6).
        """
        return state.model_dump_json()

    @staticmethod
    def _deserialize_state(raw: str) -> CircuitBreakerState:
        """Deserialize a JSON string from Redis into CircuitBreakerState."""
        return CircuitBreakerState.model_validate_json(raw)
