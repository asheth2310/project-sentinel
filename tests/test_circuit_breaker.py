"""Tests for CircuitBreakerService.

Validates Requirements 10.2, 10.3, 10.6:
- Activation stores state in Redis with agent_id, activation time, actor, and reason
- Circuit breakers support optional TTL for automatic deactivation
- Circuit breaker state changes are atomic (no partial state)
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from src.gateway.circuit_breaker import CircuitBreakerService, _make_key
from src.models.governance import CircuitBreakerState


@pytest.fixture
def mock_redis():
    """Mock RedisService for unit testing."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=True)
    return redis


@pytest.fixture
def service(mock_redis):
    """CircuitBreakerService with a mocked Redis backend."""
    return CircuitBreakerService(mock_redis)


@pytest.fixture
def agent_id():
    return uuid4()


class TestMakeKey:
    def test_key_format(self):
        agent_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert _make_key(agent_id) == "circuit_breaker:550e8400-e29b-41d4-a716-446655440000"


class TestCheckAgentStatus:
    @pytest.mark.asyncio
    async def test_returns_false_when_no_state(self, service, agent_id):
        """No state in Redis means agent is not circuit-broken."""
        result = await service.check_agent_status(agent_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_when_active(self, service, mock_redis, agent_id):
        """Active circuit breaker returns True."""
        state = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="system",
            reason="token spike",
            ttl_seconds=None,
        )
        mock_redis.get = AsyncMock(return_value=state.model_dump_json())

        result = await service.check_agent_status(agent_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_inactive(self, service, mock_redis, agent_id):
        """Inactive circuit breaker state returns False."""
        data = json.dumps({"is_active": False, "agent_id": str(agent_id)})
        mock_redis.get = AsyncMock(return_value=data)

        result = await service.check_agent_status(agent_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_fails_open_on_malformed_json(self, service, mock_redis, agent_id):
        """Malformed JSON in Redis fails open (returns False)."""
        mock_redis.get = AsyncMock(return_value="not valid json{{{")

        result = await service.check_agent_status(agent_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_fails_open_on_redis_unavailable(self, service, mock_redis, agent_id):
        """Redis returning None (unavailable) fails open."""
        mock_redis.get = AsyncMock(return_value=None)

        result = await service.check_agent_status(agent_id)
        assert result is False


class TestActivate:
    @pytest.mark.asyncio
    async def test_stores_state_with_correct_fields(self, service, mock_redis, agent_id):
        """Activation stores agent_id, activation time, actor, and reason (Req 10.2)."""
        state = await service.activate(
            agent_id=agent_id,
            reason="cost overrun",
            activated_by="governance_engine",
        )

        assert state.agent_id == agent_id
        assert state.is_active is True
        assert state.activated_at is not None
        assert state.activated_by == "governance_engine"
        assert state.reason == "cost overrun"
        assert state.ttl_seconds is None

        # Verify Redis SET was called with the serialized state
        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        key = call_args[0][0]
        payload = call_args[0][1]
        ttl = call_args[1].get("ttl") if call_args[1] else call_args[0][2] if len(call_args[0]) > 2 else None

        assert key == _make_key(agent_id)
        # Payload is a complete JSON string — atomic single value (Req 10.6)
        parsed = json.loads(payload)
        assert parsed["agent_id"] == str(agent_id)
        assert parsed["is_active"] is True
        assert parsed["activated_by"] == "governance_engine"
        assert parsed["reason"] == "cost overrun"
        assert ttl is None

    @pytest.mark.asyncio
    async def test_stores_state_with_ttl(self, service, mock_redis, agent_id):
        """Activation with TTL sets Redis key expiration (Req 10.3)."""
        state = await service.activate(
            agent_id=agent_id,
            reason="temporary suspension",
            activated_by="admin-user-42",
            ttl_seconds=600,
        )

        assert state.ttl_seconds == 600
        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        # ttl is passed as keyword arg
        assert call_args[1]["ttl"] == 600

    @pytest.mark.asyncio
    async def test_activation_time_is_utc_now(self, service, mock_redis, agent_id):
        """activated_at is set to current UTC time."""
        before = datetime.now(timezone.utc)
        state = await service.activate(
            agent_id=agent_id,
            reason="test",
            activated_by="system",
        )
        after = datetime.now(timezone.utc)

        assert before <= state.activated_at <= after

    @pytest.mark.asyncio
    async def test_returns_state_even_when_redis_fails(self, service, mock_redis, agent_id):
        """Returns the state object even if Redis write fails (logged warning)."""
        mock_redis.set = AsyncMock(return_value=False)

        state = await service.activate(
            agent_id=agent_id,
            reason="test",
            activated_by="system",
        )

        assert state.is_active is True


class TestDeactivate:
    @pytest.mark.asyncio
    async def test_deactivate_deletes_key(self, service, mock_redis, agent_id):
        """Deactivation removes the Redis key."""
        state = await service.deactivate(
            agent_id=agent_id,
            authorized_by="admin-user-1",
        )

        mock_redis.delete.assert_called_once_with(_make_key(agent_id))
        assert state.is_active is False
        assert state.agent_id == agent_id

    @pytest.mark.asyncio
    async def test_deactivate_returns_state_with_authorized_by(self, service, mock_redis, agent_id):
        """Deactivation captures who authorized it (Req 10.4)."""
        state = await service.deactivate(
            agent_id=agent_id,
            authorized_by="ops-admin",
        )

        assert state.activated_by == "ops-admin"
        assert state.reason == "deactivated"

    @pytest.mark.asyncio
    async def test_deactivate_returns_state_even_when_redis_fails(self, service, mock_redis, agent_id):
        """Returns deactivated state even if Redis delete fails."""
        mock_redis.delete = AsyncMock(return_value=False)

        state = await service.deactivate(
            agent_id=agent_id,
            authorized_by="admin",
        )

        assert state.is_active is False


class TestGetState:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_state(self, service, agent_id):
        """Returns None if no circuit breaker state exists."""
        result = await service.get_state(agent_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_deserialized_state(self, service, mock_redis, agent_id):
        """Returns a valid CircuitBreakerState from Redis data."""
        original = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="system",
            reason="infinite loop detected",
            ttl_seconds=300,
        )
        mock_redis.get = AsyncMock(return_value=original.model_dump_json())

        result = await service.get_state(agent_id)

        assert result is not None
        assert result.agent_id == agent_id
        assert result.is_active is True
        assert result.activated_by == "system"
        assert result.reason == "infinite loop detected"
        assert result.ttl_seconds == 300

    @pytest.mark.asyncio
    async def test_returns_none_on_malformed_data(self, service, mock_redis, agent_id):
        """Returns None if stored data is corrupted."""
        mock_redis.get = AsyncMock(return_value="{{invalid json")

        result = await service.get_state(agent_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_invalid_state(self, service, mock_redis, agent_id):
        """Returns None if JSON is valid but doesn't match the model schema."""
        mock_redis.get = AsyncMock(return_value=json.dumps({"bad": "data"}))

        result = await service.get_state(agent_id)
        assert result is None


class TestAtomicity:
    """Verify that state changes are atomic (Requirement 10.6)."""

    @pytest.mark.asyncio
    async def test_activate_writes_single_key(self, service, mock_redis, agent_id):
        """Activation is a single SET operation, not multiple field writes."""
        await service.activate(
            agent_id=agent_id,
            reason="test atomicity",
            activated_by="system",
        )

        # Only one Redis SET call
        assert mock_redis.set.call_count == 1
        # The value is a complete JSON object with all fields
        payload = mock_redis.set.call_args[0][1]
        data = json.loads(payload)
        assert "agent_id" in data
        assert "is_active" in data
        assert "activated_at" in data
        assert "activated_by" in data
        assert "reason" in data

    @pytest.mark.asyncio
    async def test_full_round_trip(self, service, mock_redis, agent_id):
        """State survives serialize -> store -> retrieve -> deserialize."""
        # Set up mock to capture what's written and return it on read
        stored_value = None

        async def capture_set(key, value, ttl=None):
            nonlocal stored_value
            stored_value = value
            return True

        async def return_stored(key):
            return stored_value

        mock_redis.set = AsyncMock(side_effect=capture_set)
        mock_redis.get = AsyncMock(side_effect=return_stored)

        # Activate
        original = await service.activate(
            agent_id=agent_id,
            reason="round trip test",
            activated_by="tester",
            ttl_seconds=120,
        )

        # Retrieve
        retrieved = await service.get_state(agent_id)

        assert retrieved is not None
        assert retrieved.agent_id == original.agent_id
        assert retrieved.is_active == original.is_active
        assert retrieved.activated_by == original.activated_by
        assert retrieved.reason == original.reason
        assert retrieved.ttl_seconds == original.ttl_seconds
