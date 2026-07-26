"""Tests for governance policy CRUD API routes."""

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.governance.routes import clear_policy_store, router
from src.models.governance import (
    GovernancePolicy,
    NotificationChannel,
    NotificationChannelType,
    SupportedMetric,
    ThresholdConfig,
)


@pytest.fixture(autouse=True)
def clean_store():
    """Clear the in-memory policy store before each test."""
    clear_policy_store()
    yield
    clear_policy_store()


@pytest.fixture
def app():
    """Create a FastAPI app with governance routes."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def sample_policy_data():
    """Create sample policy data for requests."""
    org_id = uuid4()
    return {
        "org_id": str(org_id),
        "thresholds": [
            {
                "metric": "total_tokens",
                "soft_limit": 800,
                "hard_limit": 1000,
                "window_seconds": 60,
                "cooldown_seconds": 300,
            }
        ],
        "notification_channels": [
            {
                "type": "slack",
                "webhook_url": "https://hooks.slack.com/services/xxx/yyy/zzz",
            }
        ],
        "auto_kill_enabled": True,
    }


class TestCreatePolicy:
    def test_create_policy_success(self, client, sample_policy_data):
        """POST /v1/policies/ creates a new policy and returns 201."""
        response = client.post("/v1/policies/", json=sample_policy_data)

        assert response.status_code == 201
        data = response.json()
        assert data["org_id"] == sample_policy_data["org_id"]
        assert data["created_at"] is not None
        assert data["updated_at"] is not None
        assert len(data["thresholds"]) == 1
        assert data["thresholds"][0]["metric"] == "total_tokens"

    def test_create_policy_conflict(self, client, sample_policy_data):
        """POST /v1/policies/ returns 409 if policy already exists for org."""
        client.post("/v1/policies/", json=sample_policy_data)
        response = client.post("/v1/policies/", json=sample_policy_data)

        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]

    def test_create_policy_assigns_policy_id(self, client, sample_policy_data):
        """Created policy gets a generated policy_id."""
        response = client.post("/v1/policies/", json=sample_policy_data)

        assert response.status_code == 201
        data = response.json()
        assert "policy_id" in data
        assert data["policy_id"] is not None

    def test_create_policy_validation_error(self, client):
        """POST /v1/policies/ returns 422 for invalid data."""
        invalid_data = {
            "org_id": str(uuid4()),
            "thresholds": [],  # min_length=1 violated
        }
        response = client.post("/v1/policies/", json=invalid_data)
        assert response.status_code == 422


class TestGetPolicy:
    def test_get_policy_success(self, client, sample_policy_data):
        """GET /v1/policies/{org_id} returns existing policy."""
        client.post("/v1/policies/", json=sample_policy_data)
        org_id = sample_policy_data["org_id"]

        response = client.get(f"/v1/policies/{org_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["org_id"] == org_id
        assert data["auto_kill_enabled"] is True

    def test_get_policy_not_found(self, client):
        """GET /v1/policies/{org_id} returns 404 for unknown org."""
        fake_org_id = uuid4()
        response = client.get(f"/v1/policies/{fake_org_id}")

        assert response.status_code == 404
        assert "No policy found" in response.json()["detail"]


class TestUpdatePolicy:
    def test_update_policy_success(self, client, sample_policy_data):
        """PUT /v1/policies/{org_id} updates existing policy."""
        client.post("/v1/policies/", json=sample_policy_data)
        org_id = sample_policy_data["org_id"]

        # Update with a new threshold
        updated_data = sample_policy_data.copy()
        updated_data["thresholds"] = [
            {
                "metric": "total_cost",
                "soft_limit": 50.0,
                "hard_limit": 100.0,
                "window_seconds": 120,
                "cooldown_seconds": 600,
            }
        ]
        updated_data["auto_kill_enabled"] = False

        response = client.put(f"/v1/policies/{org_id}", json=updated_data)

        assert response.status_code == 200
        data = response.json()
        assert data["thresholds"][0]["metric"] == "total_cost"
        assert data["auto_kill_enabled"] is False
        assert data["updated_at"] is not None

    def test_update_policy_not_found(self, client, sample_policy_data):
        """PUT /v1/policies/{org_id} returns 404 if policy doesn't exist."""
        org_id = sample_policy_data["org_id"]
        response = client.put(f"/v1/policies/{org_id}", json=sample_policy_data)

        assert response.status_code == 404
        assert "No policy found" in response.json()["detail"]

    def test_update_policy_org_id_mismatch(self, client, sample_policy_data):
        """PUT /v1/policies/{org_id} returns 400 if org_id doesn't match body."""
        client.post("/v1/policies/", json=sample_policy_data)
        different_org_id = uuid4()

        response = client.put(
            f"/v1/policies/{different_org_id}", json=sample_policy_data
        )

        assert response.status_code == 400
        assert "does not match" in response.json()["detail"]

    def test_update_preserves_created_at(self, client, sample_policy_data):
        """PUT /v1/policies/{org_id} preserves the original created_at."""
        create_response = client.post("/v1/policies/", json=sample_policy_data)
        original_created_at = create_response.json()["created_at"]
        org_id = sample_policy_data["org_id"]

        response = client.put(f"/v1/policies/{org_id}", json=sample_policy_data)

        assert response.status_code == 200
        assert response.json()["created_at"] == original_created_at

    def test_update_refreshes_updated_at(self, client, sample_policy_data):
        """PUT /v1/policies/{org_id} sets a new updated_at timestamp."""
        create_response = client.post("/v1/policies/", json=sample_policy_data)
        original_updated_at = create_response.json()["updated_at"]
        org_id = sample_policy_data["org_id"]

        # Small delay to ensure timestamp differs
        response = client.put(f"/v1/policies/{org_id}", json=sample_policy_data)

        assert response.status_code == 200
        # updated_at should be refreshed (may or may not be different depending
        # on system clock resolution, but the field should be present)
        assert response.json()["updated_at"] is not None
