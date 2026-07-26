"""Tests for the Query API routes (Tasks 51-53).

Tests anomaly listing with filtering and pagination, agent status,
and aggregated metrics endpoints.
"""

import pytest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.models.anomaly import AnomalyEvent, AnomalyType, Severity
from src.storage.query_routes import (
    add_anomaly,
    add_telemetry,
    clear_anomaly_store,
    clear_agent_circuit_breaker_store,
    clear_telemetry_store,
    router,
    set_agent_circuit_breaker,
    _encode_cursor,
    _decode_cursor,
)


@pytest.fixture
def app():
    """Create a FastAPI test app with query routes."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def clean_stores():
    """Clear all in-memory stores before each test."""
    clear_anomaly_store()
    clear_telemetry_store()
    clear_agent_circuit_breaker_store()
    yield
    clear_anomaly_store()
    clear_telemetry_store()
    clear_agent_circuit_breaker_store()


def _make_anomaly(
    agent_id=None,
    severity=Severity.HIGH,
    anomaly_type=AnomalyType.TOKEN_SPIKE,
    detected_at=None,
) -> AnomalyEvent:
    """Create a test anomaly event."""
    return AnomalyEvent(
        anomaly_id=uuid4(),
        agent_id=agent_id or uuid4(),
        org_id=uuid4(),
        anomaly_type=anomaly_type,
        severity=severity,
        detected_at=detected_at or datetime.now(timezone.utc),
        window_start=datetime.now(timezone.utc) - timedelta(seconds=60),
        window_end=datetime.now(timezone.utc),
        metric_value=5000.0,
        threshold_value=3000.0,
        description="Test anomaly",
        metadata={"test": True},
    )


# ============================================================================
# Task 51: GET /v1/anomalies tests
# ============================================================================


class TestGetAnomalies:
    """Tests for GET /v1/anomalies endpoint."""

    def test_empty_store_returns_empty_list(self, client):
        """Returns empty list when no anomalies exist."""
        resp = client.get("/v1/anomalies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["anomalies"] == []
        assert data["next_cursor"] is None
        assert data["total_count"] == 0

    def test_returns_all_anomalies(self, client):
        """Returns all anomalies when no filters applied."""
        for _ in range(3):
            add_anomaly(_make_anomaly())

        resp = client.get("/v1/anomalies")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["anomalies"]) == 3
        assert data["total_count"] == 3

    def test_filter_by_agent_id(self, client):
        """Filters anomalies by agent_id."""
        target_agent = uuid4()
        add_anomaly(_make_anomaly(agent_id=target_agent))
        add_anomaly(_make_anomaly())  # Different agent
        add_anomaly(_make_anomaly(agent_id=target_agent))

        resp = client.get(f"/v1/anomalies?agent_id={target_agent}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_count"] == 2
        for a in data["anomalies"]:
            assert a["agent_id"] == str(target_agent)

    def test_filter_by_severity(self, client):
        """Filters anomalies by severity level."""
        add_anomaly(_make_anomaly(severity=Severity.HIGH))
        add_anomaly(_make_anomaly(severity=Severity.LOW))
        add_anomaly(_make_anomaly(severity=Severity.HIGH))

        resp = client.get("/v1/anomalies?severity=high")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_count"] == 2

    def test_filter_by_time_range(self, client):
        """Filters anomalies by time range."""
        now = datetime.now(timezone.utc)
        old = now - timedelta(hours=2)
        recent = now - timedelta(minutes=10)

        add_anomaly(_make_anomaly(detected_at=old))
        add_anomaly(_make_anomaly(detected_at=recent))
        add_anomaly(_make_anomaly(detected_at=now))

        # Only last hour - use format without colon in timezone
        start = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        resp = client.get("/v1/anomalies", params={"start_time": start})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_count"] == 2

    def test_invalid_severity_returns_400(self, client):
        """Invalid severity filter returns 400."""
        resp = client.get("/v1/anomalies?severity=invalid")
        assert resp.status_code == 400

    def test_pagination_limit(self, client):
        """Respects limit parameter for page size."""
        for _ in range(5):
            add_anomaly(_make_anomaly())

        resp = client.get("/v1/anomalies?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["anomalies"]) == 2
        assert data["next_cursor"] is not None
        assert data["total_count"] == 5

    def test_pagination_cursor_navigation(self, client):
        """Cursor allows navigating through pages."""
        for _ in range(5):
            add_anomaly(_make_anomaly())

        # First page
        resp = client.get("/v1/anomalies?limit=2")
        data = resp.json()
        assert len(data["anomalies"]) == 2
        cursor = data["next_cursor"]
        assert cursor is not None

        # Second page
        resp = client.get(f"/v1/anomalies?limit=2&cursor={cursor}")
        data = resp.json()
        assert len(data["anomalies"]) == 2
        cursor = data["next_cursor"]
        assert cursor is not None

        # Third page (last, only 1 remaining)
        resp = client.get(f"/v1/anomalies?limit=2&cursor={cursor}")
        data = resp.json()
        assert len(data["anomalies"]) == 1
        assert data["next_cursor"] is None

    def test_invalid_cursor_returns_400(self, client):
        """Malformed cursor returns 400."""
        resp = client.get("/v1/anomalies?cursor=not-valid-base64!!!")
        assert resp.status_code == 400

    def test_results_sorted_by_detected_at_desc(self, client):
        """Results are sorted by detected_at descending (newest first)."""
        now = datetime.now(timezone.utc)
        add_anomaly(_make_anomaly(detected_at=now - timedelta(minutes=30)))
        add_anomaly(_make_anomaly(detected_at=now))
        add_anomaly(_make_anomaly(detected_at=now - timedelta(minutes=15)))

        resp = client.get("/v1/anomalies")
        data = resp.json()
        timestamps = [a["detected_at"] for a in data["anomalies"]]
        assert timestamps == sorted(timestamps, reverse=True)


class TestCursorEncoding:
    """Tests for cursor encode/decode utilities."""

    def test_roundtrip(self):
        """Encoding then decoding returns original index."""
        for idx in [0, 1, 50, 100, 999]:
            cursor = _encode_cursor(idx)
            assert _decode_cursor(cursor) == idx

    def test_cursor_is_url_safe(self):
        """Encoded cursor uses URL-safe characters."""
        cursor = _encode_cursor(42)
        # Should not contain +, /, or =
        assert "+" not in cursor
        assert "/" not in cursor


# ============================================================================
# Task 52: GET /v1/agents/{agent_id}/status tests
# ============================================================================


class TestGetAgentStatus:
    """Tests for GET /v1/agents/{agent_id}/status endpoint."""

    def test_agent_not_found_returns_inactive(self, client):
        """Unknown agent returns inactive circuit breaker state."""
        agent_id = uuid4()
        resp = client.get(f"/v1/agents/{agent_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["agent_id"] == str(agent_id)
        assert data["circuit_breaker"]["state"] == "inactive"
        assert data["recent_metrics"]["event_count"] == 0

    def test_agent_with_active_circuit_breaker(self, client):
        """Returns active circuit breaker state when set."""
        agent_id = uuid4()
        set_agent_circuit_breaker(agent_id, {
            "state": "active",
            "activated_at": "2024-01-01T00:00:00Z",
            "reason": "Token spike detected",
            "activated_by": "governance_engine",
        })

        resp = client.get(f"/v1/agents/{agent_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["circuit_breaker"]["state"] == "active"
        assert data["circuit_breaker"]["reason"] == "Token spike detected"

    def test_agent_with_telemetry_metrics(self, client):
        """Returns computed metrics from telemetry store."""
        agent_id = uuid4()
        now = datetime.now(timezone.utc)

        for i in range(3):
            add_telemetry({
                "agent_id": str(agent_id),
                "timestamp": (now - timedelta(minutes=i)).isoformat(),
                "prompt_tokens": 100,
                "completion_tokens": 200,
                "total_cost": "0.003",
                "latency_ms": 150 + i * 10,
            })

        resp = client.get(f"/v1/agents/{agent_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        metrics = data["recent_metrics"]
        assert metrics["event_count"] == 3
        assert metrics["total_tokens"] == 900  # 3 * (100 + 200)
        assert float(metrics["total_cost"]) == pytest.approx(0.009, abs=0.001)
        assert metrics["avg_latency_ms"] > 0
        assert metrics["last_seen"] is not None

    def test_invalid_agent_id_returns_422(self, client):
        """Non-UUID agent_id returns 422."""
        resp = client.get("/v1/agents/not-a-uuid/status")
        assert resp.status_code == 422


# ============================================================================
# Task 53: GET /v1/metrics tests
# ============================================================================


class TestGetMetrics:
    """Tests for GET /v1/metrics endpoint."""

    def test_empty_stores_returns_zeros(self, client):
        """Returns zero metrics when stores are empty."""
        resp = client.get("/v1/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_events"] == 0
        assert data["total_tokens"] == 0
        assert float(data["total_cost"]) == 0.0
        assert data["active_agents"] == 0
        assert data["total_anomalies"] == 0

    def test_aggregated_telemetry_metrics(self, client):
        """Returns correct aggregated telemetry metrics."""
        agent1 = uuid4()
        agent2 = uuid4()
        now = datetime.now(timezone.utc)

        add_telemetry({
            "agent_id": str(agent1),
            "timestamp": now.isoformat(),
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_cost": "0.002",
            "latency_ms": 100,
        })
        add_telemetry({
            "agent_id": str(agent2),
            "timestamp": now.isoformat(),
            "prompt_tokens": 200,
            "completion_tokens": 100,
            "total_cost": "0.005",
            "latency_ms": 200,
        })

        resp = client.get("/v1/metrics")
        data = resp.json()
        assert data["total_events"] == 2
        assert data["total_tokens"] == 450  # 150 + 300
        assert float(data["total_cost"]) == pytest.approx(0.007, abs=0.001)
        assert data["active_agents"] == 2
        assert data["avg_latency_ms"] == 150.0

    def test_anomaly_breakdown(self, client):
        """Returns anomaly counts broken down by type and severity."""
        add_anomaly(_make_anomaly(
            anomaly_type=AnomalyType.TOKEN_SPIKE, severity=Severity.HIGH
        ))
        add_anomaly(_make_anomaly(
            anomaly_type=AnomalyType.TOKEN_SPIKE, severity=Severity.CRITICAL
        ))
        add_anomaly(_make_anomaly(
            anomaly_type=AnomalyType.INFINITE_LOOP, severity=Severity.HIGH
        ))

        resp = client.get("/v1/metrics")
        data = resp.json()
        assert data["total_anomalies"] == 3
        assert data["anomalies_by_type"]["token_spike"] == 2
        assert data["anomalies_by_type"]["infinite_loop"] == 1
        assert data["anomalies_by_severity"]["high"] == 2
        assert data["anomalies_by_severity"]["critical"] == 1

    def test_time_range_filter(self, client):
        """Time range filter restricts returned metrics."""
        now = datetime.now(timezone.utc)
        old = now - timedelta(hours=3)

        add_telemetry({
            "agent_id": str(uuid4()),
            "timestamp": old.isoformat(),
            "prompt_tokens": 500,
            "completion_tokens": 500,
            "total_cost": "0.010",
            "latency_ms": 100,
        })
        add_telemetry({
            "agent_id": str(uuid4()),
            "timestamp": now.isoformat(),
            "prompt_tokens": 100,
            "completion_tokens": 100,
            "total_cost": "0.002",
            "latency_ms": 50,
        })

        # Only recent data - use params dict to avoid URL encoding issues
        start = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        resp = client.get("/v1/metrics", params={"start_time": start})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_events"] == 1
        assert data["total_tokens"] == 200
