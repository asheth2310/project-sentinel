"""
Circuit breaker check middleware for the ingestion gateway.

Implements Requirement 3: Circuit Breaker Enforcement at Ingestion.
Before processing any batch, the gateway checks Redis for the agent's
circuit breaker state. If active, returns HTTP 429 Too Many Requests.
If Redis is unavailable, fails open (accepts telemetry) and logs warning.

This module provides a FastAPI dependency that can be injected into
route handlers to enforce circuit breaker state before processing.
"""

import logging
from uuid import UUID

from fastapi import HTTPException, Request

from src.gateway.circuit_breaker import CircuitBreakerService

logger = logging.getLogger(__name__)

# Default Retry-After header value in seconds
_DEFAULT_RETRY_AFTER_SECONDS = 60


class CircuitBreakerMiddleware:
    """FastAPI dependency that enforces circuit breaker state at ingestion.

    Before the telemetry route handler processes a batch, this dependency
    extracts the agent_id from the request body and checks the circuit
    breaker state in Redis.

    Behavior:
        - If circuit breaker is active: raises HTTPException 429 with
          Retry-After header (Requirement 3.2)
        - If circuit breaker is inactive: allows the request to proceed
        - If Redis is unavailable: fails open, accepts telemetry,
          and logs a warning (Requirement 3.4)
    """

    def __init__(self, circuit_breaker_service: CircuitBreakerService) -> None:
        self._circuit_breaker_service = circuit_breaker_service

    async def check(self, agent_id: UUID) -> None:
        """Check circuit breaker state for the given agent.

        This is the main dependency method to be called from route handlers.
        It checks Redis for the agent's circuit breaker state and raises
        HTTP 429 if the breaker is active.

        Args:
            agent_id: The agent whose circuit breaker state to check.

        Raises:
            HTTPException: 429 Too Many Requests if circuit breaker is active.
        """
        is_active = await self._circuit_breaker_service.check_agent_status(agent_id)

        if is_active:
            logger.info(
                "Circuit breaker active for agent %s, rejecting telemetry with 429",
                agent_id,
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "circuit_breaker_active",
                    "message": (
                        f"Agent {agent_id} has an active circuit breaker. "
                        "Telemetry submission is temporarily blocked."
                    ),
                    "agent_id": str(agent_id),
                },
                headers={"Retry-After": str(_DEFAULT_RETRY_AFTER_SECONDS)},
            )

        # Circuit breaker is not active — request proceeds
        logger.debug(
            "Circuit breaker check passed for agent %s", agent_id
        )

    async def __call__(self, request: Request) -> None:
        """Middleware-style invocation that extracts agent_id from the request body.

        This can be used as a generic middleware that reads the request body,
        extracts agent_id, and performs the circuit breaker check. Useful when
        agent_id is not readily available from route parameters.

        Note: This caches the parsed body on request.state so the route
        handler can access it without re-reading the stream.

        Args:
            request: The incoming FastAPI request.

        Raises:
            HTTPException: 429 if circuit breaker is active for the agent.
        """
        try:
            body = await request.json()
            agent_id_str = body.get("agent_id")

            if agent_id_str is None:
                # No agent_id in body — skip circuit breaker check
                # The route handler's own validation will catch missing fields
                logger.debug(
                    "No agent_id in request body, skipping circuit breaker check"
                )
                return

            agent_id = UUID(agent_id_str)
        except (ValueError, TypeError, KeyError) as exc:
            # Malformed body or invalid UUID — skip circuit breaker check
            # and let the route's payload validation handle the error
            logger.debug(
                "Could not extract agent_id for circuit breaker check: %s", exc
            )
            return

        # Store parsed body on request state to avoid re-reading the stream
        request.state.parsed_body = body

        await self.check(agent_id)
