"""Audit logging service for governance actions.

Records circuit breaker activations, deactivations, and other governance
actions for accountability and compliance (Requirement 10).

Uses an in-memory store until TimescaleDB integration is added.
"""

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AuditEntry(BaseModel):
    """Represents a single audit log entry."""

    audit_id: UUID = Field(default_factory=uuid4)
    agent_id: UUID
    org_id: UUID | None = None
    action_type: str
    actor: str
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AuditLogger:
    """In-memory audit logger for governance actions.

    Stores audit entries for circuit breaker activations/deactivations
    and other governance policy evaluations.

    Will be replaced by TimescaleDB persistence in a later task.
    """

    def __init__(self) -> None:
        self._entries: list[AuditEntry] = []

    async def log(
        self,
        agent_id: UUID,
        action_type: str,
        actor: str,
        reason: str | None = None,
        org_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Record an audit entry.

        Args:
            agent_id: The agent the action applies to.
            action_type: Type of action (e.g., 'circuit_breaker_activated').
            actor: Who performed the action ('system' or user ID).
            reason: Human-readable reason for the action.
            org_id: Optional organization ID.
            metadata: Optional additional context.

        Returns:
            The created AuditEntry.
        """
        entry = AuditEntry(
            agent_id=agent_id,
            org_id=org_id,
            action_type=action_type,
            actor=actor,
            reason=reason,
            metadata=metadata or {},
        )
        self._entries.append(entry)
        logger.info(
            "Audit log: action=%s, agent_id=%s, actor=%s, reason=%s",
            action_type,
            agent_id,
            actor,
            reason,
        )
        return entry

    def get_entries(
        self,
        agent_id: UUID | None = None,
        action_type: str | None = None,
    ) -> list[AuditEntry]:
        """Retrieve audit entries with optional filtering.

        Args:
            agent_id: Filter by agent ID (optional).
            action_type: Filter by action type (optional).

        Returns:
            List of matching AuditEntry objects.
        """
        entries = self._entries
        if agent_id is not None:
            entries = [e for e in entries if e.agent_id == agent_id]
        if action_type is not None:
            entries = [e for e in entries if e.action_type == action_type]
        return entries

    def clear(self) -> None:
        """Clear all audit entries (useful for testing)."""
        self._entries.clear()
