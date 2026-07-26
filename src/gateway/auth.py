"""
Bearer token authentication for Project Sentinel ingestion gateway.

Implements API key validation and agent identity resolution.
Agent SDKs authenticate using bearer tokens (API keys) in the Authorization header.
Each API key maps to an (agent_id, org_id) pair identifying the agent and its organization.

For now, uses a simple in-memory registry of valid API keys.
This can be replaced with a database lookup in production.
"""

import logging
from dataclasses import dataclass
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentIdentity:
    """Resolved identity from a valid API key."""

    agent_id: UUID
    org_id: UUID


class APIKeyRegistry:
    """In-memory registry of valid API keys mapped to agent identities.

    This is a placeholder implementation for development and testing.
    In production, this would be backed by a database lookup with caching.
    """

    def __init__(self) -> None:
        self._keys: dict[str, AgentIdentity] = {}

    def register(self, api_key: str, agent_id: UUID, org_id: UUID) -> None:
        """Register an API key for an agent.

        Args:
            api_key: The bearer token string.
            agent_id: The agent UUID this key authenticates.
            org_id: The organization UUID this agent belongs to.
        """
        self._keys[api_key] = AgentIdentity(agent_id=agent_id, org_id=org_id)

    def resolve(self, api_key: str) -> AgentIdentity | None:
        """Resolve an API key to an agent identity.

        Args:
            api_key: The bearer token string from Authorization header.

        Returns:
            AgentIdentity if key is valid, None if invalid or unknown.
        """
        return self._keys.get(api_key)

    def revoke(self, api_key: str) -> bool:
        """Revoke an API key.

        Returns True if the key existed and was removed, False otherwise.
        """
        if api_key in self._keys:
            del self._keys[api_key]
            return True
        return False

    @property
    def key_count(self) -> int:
        """Number of registered API keys."""
        return len(self._keys)


# Module-level singleton for the API key registry
_registry = APIKeyRegistry()


def get_api_key_registry() -> APIKeyRegistry:
    """Get the global API key registry instance."""
    return _registry
