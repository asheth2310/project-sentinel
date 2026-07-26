"""Circuit breaker management API for Project Sentinel.

Provides endpoints for manually activating, deactivating, and querying
circuit breaker state for agents. Includes audit logging for all
state changes (Requirement 10.4).
"""

from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.gateway.circuit_breaker import CircuitBreakerService
from src.governance.audit import AuditLogger

router = APIRouter(prefix="/v1/circuit-breakers", tags=["circuit-breakers"])

# Module-level service references (set during app startup)
_circuit_breaker_service: CircuitBreakerService | None = None
_audit_logger: AuditLogger | None = None


def set_circuit_breaker_service(service: CircuitBreakerService) -> None:
    """Set the circuit breaker service instance (called during app startup)."""
    global _circuit_breaker_service
    _circuit_breaker_service = service


def set_audit_logger(logger: AuditLogger) -> None:
    """Set the audit logger instance (called during app startup)."""
    global _audit_logger
    _audit_logger = logger


def _get_circuit_breaker_service() -> CircuitBreakerService:
    """Get circuit breaker service, raising 503 if unavailable."""
    if _circuit_breaker_service is None:
        raise HTTPException(
            status_code=503,
            detail="Circuit breaker service unavailable",
        )
    return _circuit_breaker_service


def _get_audit_logger() -> AuditLogger:
    """Get audit logger, raising 503 if unavailable."""
    if _audit_logger is None:
        raise HTTPException(
            status_code=503,
            detail="Audit logger unavailable",
        )
    return _audit_logger


# --- Request/Response Models ---


class ActivateRequest(BaseModel):
    """Request body for circuit breaker activation."""

    reason: str = Field(..., min_length=1, description="Reason for activation")
    authorized_by: str = Field(..., min_length=1, description="User/actor authorizing activation")
    ttl_seconds: int | None = Field(
        default=None,
        gt=0,
        description="Optional TTL for auto-deactivation",
    )


class DeactivateRequest(BaseModel):
    """Request body for circuit breaker deactivation."""

    authorized_by: str = Field(..., min_length=1, description="User/actor authorizing deactivation")


class CircuitBreakerResponse(BaseModel):
    """Response model for circuit breaker state."""

    agent_id: UUID
    is_active: bool
    activated_by: str | None = None
    reason: str | None = None
    ttl_seconds: int | None = None


# --- Endpoints ---


@router.post("/{agent_id}/activate", status_code=200, response_model=CircuitBreakerResponse)
async def activate_circuit_breaker(agent_id: UUID, request: ActivateRequest) -> CircuitBreakerResponse:
    """Manually activate circuit breaker for an agent.

    Requires authorization. Records the activation in the audit log.

    Args:
        agent_id: The agent to circuit-break.
        request: Activation details including reason and authorization.

    Returns:
        The resulting circuit breaker state.
    """
    cb_service = _get_circuit_breaker_service()
    audit = _get_audit_logger()

    state = await cb_service.activate(
        agent_id=agent_id,
        reason=request.reason,
        activated_by=request.authorized_by,
        ttl_seconds=request.ttl_seconds,
    )

    await audit.log(
        agent_id=agent_id,
        action_type="circuit_breaker_activated",
        actor=request.authorized_by,
        reason=request.reason,
        metadata={
            "ttl_seconds": request.ttl_seconds,
            "source": "manual",
        },
    )

    return CircuitBreakerResponse(
        agent_id=state.agent_id,
        is_active=state.is_active,
        activated_by=state.activated_by,
        reason=state.reason,
        ttl_seconds=state.ttl_seconds,
    )


@router.post("/{agent_id}/deactivate", status_code=200, response_model=CircuitBreakerResponse)
async def deactivate_circuit_breaker(agent_id: UUID, request: DeactivateRequest) -> CircuitBreakerResponse:
    """Manually deactivate circuit breaker for an agent.

    Requires authorization. Records the deactivation in the audit log.

    Args:
        agent_id: The agent to un-circuit-break.
        request: Deactivation details including authorization.

    Returns:
        The resulting circuit breaker state.
    """
    cb_service = _get_circuit_breaker_service()
    audit = _get_audit_logger()

    state = await cb_service.deactivate(
        agent_id=agent_id,
        authorized_by=request.authorized_by,
    )

    await audit.log(
        agent_id=agent_id,
        action_type="circuit_breaker_deactivated",
        actor=request.authorized_by,
        reason="Manual deactivation",
        metadata={"source": "manual"},
    )

    return CircuitBreakerResponse(
        agent_id=state.agent_id,
        is_active=state.is_active,
        activated_by=state.activated_by,
        reason=state.reason,
        ttl_seconds=state.ttl_seconds,
    )


@router.get("/{agent_id}", status_code=200, response_model=CircuitBreakerResponse)
async def get_circuit_breaker_status(agent_id: UUID) -> CircuitBreakerResponse:
    """Get current circuit breaker state for an agent.

    Args:
        agent_id: The agent to query.

    Returns:
        The current circuit breaker state. If no state exists,
        returns inactive status.
    """
    cb_service = _get_circuit_breaker_service()

    state = await cb_service.get_state(agent_id)

    if state is None:
        return CircuitBreakerResponse(
            agent_id=agent_id,
            is_active=False,
        )

    return CircuitBreakerResponse(
        agent_id=state.agent_id,
        is_active=state.is_active,
        activated_by=state.activated_by,
        reason=state.reason,
        ttl_seconds=state.ttl_seconds,
    )
