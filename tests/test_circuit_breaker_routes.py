"""Tests for circuit breaker management API (Task 43).

Verifies the activate, deactivate, and get status endpoints with audit logging.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.governance.audit import AuditLogger
from src.governance.circuit_breaker_routes import (
    router,
    set_circuit_breaker_service,
    set_audit_logger,
)
from src.models.governance import CircuitBreakerState


@pytest.fixture
def mock_cb_service():
    """Create a mock circuit breaker service."""
    service = AsyncMock()
    return service


@pytest.fixture
def audit_logger():
    """Create a real audit logger instance."""
    return AuditLogger()


@pytest.fixture
def app(mock_cb_service, audit_logger):
    """Create a FastAPI app with circuit breaker routes."""
    app = FastAPI()
    app.include_router(router)

    # Set the service instances
    set_circuit_breaker_service(mock_cb_service)
    set_audit_logger(audit_logger)

    yield app

    # Cleanup
    set_circuit_breaker_service(None)
    set_audit_logger(None)


@pytest.fixture
def client(app):
    """Create test client."""
    return TestClient(app)


class TestActivateEndpoint:
    """Tests for POST /{agent_id}/activate."""

    def test_activate_success(self, client, mock_cb_service):
        """Activating a circuit breaker returns 200 with state."""
        agent_id = uuid4()
        mock_cb_service.activate.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="admin-user",
            reason="Suspicious activity",
            ttl_seconds=3600,
        )

        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/activate",
            json={
                "reason": "Suspicious activity",
                "authorized_by": "admin-user",
                "ttl_seconds": 3600,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["agent_id"] == str(agent_id)
        assert data["is_active"] is True
        assert data["activated_by"] == "admin-user"
        assert data["reason"] == "Suspicious activity"
        assert data["ttl_seconds"] == 3600

    def test_activate_writes_audit_log(self, client, mock_cb_service, audit_logger):
        """Activating records an audit entry."""
        agent_id = uuid4()
        mock_cb_service.activate.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="admin-user",
            reason="Testing",
        )

        client.post(
            f"/v1/circuit-breakers/{agent_id}/activate",
            json={
                "reason": "Testing",
                "authorized_by": "admin-user",
            },
        )

        entries = audit_logger.get_entries(agent_id=agent_id)
        assert len(entries) == 1
        assert entries[0].action_type == "circuit_breaker_activated"
        assert entries[0].actor == "admin-user"

    def test_activate_without_ttl(self, client, mock_cb_service):
        """Activation without TTL works (no auto-deactivation)."""
        agent_id = uuid4()
        mock_cb_service.activate.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="admin",
            reason="Manual intervention",
        )

        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/activate",
            json={
                "reason": "Manual intervention",
                "authorized_by": "admin",
            },
        )

        assert response.status_code == 200
        assert response.json()["ttl_seconds"] is None

    def test_activate_missing_reason_returns_422(self, client):
        """Missing required field returns validation error."""
        agent_id = uuid4()
        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/activate",
            json={"authorized_by": "admin"},
        )
        assert response.status_code == 422


class TestDeactivateEndpoint:
    """Tests for POST /{agent_id}/deactivate."""

    def test_deactivate_success(self, client, mock_cb_service):
        """Deactivating a circuit breaker returns 200."""
        agent_id = uuid4()
        mock_cb_service.deactivate.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=False,
            activated_by="admin-user",
            reason="deactivated",
        )

        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/deactivate",
            json={"authorized_by": "admin-user"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["agent_id"] == str(agent_id)
        assert data["is_active"] is False

    def test_deactivate_writes_audit_log(self, client, mock_cb_service, audit_logger):
        """Deactivation records an audit entry."""
        agent_id = uuid4()
        mock_cb_service.deactivate.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=False,
            activated_by="admin-user",
            reason="deactivated",
        )

        client.post(
            f"/v1/circuit-breakers/{agent_id}/deactivate",
            json={"authorized_by": "admin-user"},
        )

        entries = audit_logger.get_entries(agent_id=agent_id)
        assert len(entries) == 1
        assert entries[0].action_type == "circuit_breaker_deactivated"
        assert entries[0].actor == "admin-user"

    def test_deactivate_missing_authorized_by_returns_422(self, client):
        """Missing authorized_by returns validation error."""
        agent_id = uuid4()
        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/deactivate",
            json={},
        )
        assert response.status_code == 422


class TestGetStatusEndpoint:
    """Tests for GET /{agent_id}."""

    def test_get_active_status(self, client, mock_cb_service):
        """Getting active circuit breaker returns its state."""
        agent_id = uuid4()
        mock_cb_service.get_state.return_value = CircuitBreakerState(
            agent_id=agent_id,
            is_active=True,
            activated_at=datetime.now(timezone.utc),
            activated_by="system",
            reason="Hard limit breach",
            ttl_seconds=1800,
        )

        response = client.get(f"/v1/circuit-breakers/{agent_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["agent_id"] == str(agent_id)
        assert data["is_active"] is True
        assert data["activated_by"] == "system"

    def test_get_inactive_status(self, client, mock_cb_service):
        """Getting non-existent circuit breaker returns inactive state."""
        agent_id = uuid4()
        mock_cb_service.get_state.return_value = None

        response = client.get(f"/v1/circuit-breakers/{agent_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["agent_id"] == str(agent_id)
        assert data["is_active"] is False


class TestServiceUnavailable:
    """Tests for service unavailability scenarios."""

    def test_activate_service_unavailable(self):
        """Returns 503 when circuit breaker service is not set."""
        app = FastAPI()
        app.include_router(router)
        set_circuit_breaker_service(None)
        set_audit_logger(AuditLogger())
        client = TestClient(app)

        agent_id = uuid4()
        response = client.post(
            f"/v1/circuit-breakers/{agent_id}/activate",
            json={"reason": "test", "authorized_by": "admin"},
        )
        assert response.status_code == 503
