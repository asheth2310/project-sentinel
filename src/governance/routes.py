"""Governance policy CRUD API for Project Sentinel.

Provides FastAPI endpoints for creating, reading, and updating
governance policies per organization. Uses an in-memory store
until TimescaleDB integration is added in later tasks.
"""

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException

from src.models.governance import GovernancePolicy

router = APIRouter(prefix="/v1/policies", tags=["governance"])

# In-memory policy store keyed by org_id.
# Will be replaced by TimescaleDB persistence in a later task.
_policy_store: dict[UUID, GovernancePolicy] = {}


def get_policy_store() -> dict[UUID, GovernancePolicy]:
    """Get the in-memory policy store (useful for testing)."""
    return _policy_store


def clear_policy_store() -> None:
    """Clear the in-memory policy store (useful for testing)."""
    _policy_store.clear()


@router.post("/", status_code=201, response_model=GovernancePolicy)
async def create_policy(policy: GovernancePolicy) -> GovernancePolicy:
    """Create a governance policy for an organization.

    Each organization can have at most one policy. If a policy already
    exists for the given org_id, returns 409 Conflict.

    Args:
        policy: The GovernancePolicy to create.

    Returns:
        The created GovernancePolicy with timestamps set.

    Raises:
        HTTPException 409: If a policy already exists for the org_id.
    """
    if policy.org_id in _policy_store:
        raise HTTPException(
            status_code=409,
            detail=f"Policy already exists for org_id={policy.org_id}. Use PUT to update.",
        )

    now = datetime.now(timezone.utc)
    # Create a new instance with timestamps set
    stored_policy = policy.model_copy(
        update={"created_at": now, "updated_at": now}
    )
    _policy_store[stored_policy.org_id] = stored_policy
    return stored_policy


@router.get("/{org_id}", response_model=GovernancePolicy)
async def get_policy(org_id: UUID) -> GovernancePolicy:
    """Get the governance policy for an organization.

    Args:
        org_id: The organization UUID.

    Returns:
        The GovernancePolicy for the organization.

    Raises:
        HTTPException 404: If no policy exists for the org_id.
    """
    policy = _policy_store.get(org_id)
    if policy is None:
        raise HTTPException(
            status_code=404,
            detail=f"No policy found for org_id={org_id}",
        )
    return policy


@router.put("/{org_id}", response_model=GovernancePolicy)
async def update_policy(org_id: UUID, policy: GovernancePolicy) -> GovernancePolicy:
    """Update the governance policy for an organization.

    The org_id in the URL must match the org_id in the policy body.
    The policy must already exist (use POST to create).

    Args:
        org_id: The organization UUID (from URL path).
        policy: The updated GovernancePolicy.

    Returns:
        The updated GovernancePolicy with updated_at timestamp refreshed.

    Raises:
        HTTPException 404: If no policy exists for the org_id.
        HTTPException 400: If org_id in URL doesn't match policy body.
    """
    if policy.org_id != org_id:
        raise HTTPException(
            status_code=400,
            detail=f"org_id in URL ({org_id}) does not match policy body ({policy.org_id})",
        )

    existing = _policy_store.get(org_id)
    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"No policy found for org_id={org_id}. Use POST to create.",
        )

    now = datetime.now(timezone.utc)
    updated_policy = policy.model_copy(
        update={
            "created_at": existing.created_at,
            "updated_at": now,
        }
    )
    _policy_store[org_id] = updated_policy
    return updated_policy
