"""Query API routes for Project Sentinel.

Provides REST endpoints for querying telemetry data, anomaly events,
and agent status (Tasks 51-53). Uses in-memory data stores for
development and testing, with database integration wired in later.
"""

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query

from src.models.anomaly import AnomalyEvent, Severity

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["query"])

# ============================================================================
# In-Memory Data Stores
# ============================================================================
# These simulate database storage for development/testing.
# Will be replaced by TimescaleDB queries once DB integration is wired up.

_anomaly_store: list[AnomalyEvent] = []
_telemetry_store: list[dict[str, Any]] = []
_agent_circuit_breaker_store: dict[UUID, dict[str, Any]] = {}


def get_anomaly_store() -> list[AnomalyEvent]:
    """Get the in-memory anomaly store (useful for testing)."""
    return _anomaly_store


def add_anomaly(anomaly: AnomalyEvent) -> None:
    """Add an anomaly event to the in-memory store."""
    _anomaly_store.append(anomaly)


def clear_anomaly_store() -> None:
    """Clear the in-memory anomaly store (useful for testing)."""
    _anomaly_store.clear()


def get_telemetry_store() -> list[dict[str, Any]]:
    """Get the in-memory telemetry store (useful for testing)."""
    return _telemetry_store


def add_telemetry(event: dict[str, Any]) -> None:
    """Add a telemetry event to the in-memory store."""
    _telemetry_store.append(event)


def clear_telemetry_store() -> None:
    """Clear the in-memory telemetry store (useful for testing)."""
    _telemetry_store.clear()


def get_agent_circuit_breaker_store() -> dict[UUID, dict[str, Any]]:
    """Get the in-memory circuit breaker store (useful for testing)."""
    return _agent_circuit_breaker_store


def set_agent_circuit_breaker(agent_id: UUID, state: dict[str, Any]) -> None:
    """Set circuit breaker state for an agent."""
    _agent_circuit_breaker_store[agent_id] = state


def clear_agent_circuit_breaker_store() -> None:
    """Clear the in-memory circuit breaker store."""
    _agent_circuit_breaker_store.clear()


# ============================================================================
# Cursor Encoding/Decoding
# ============================================================================


def _encode_cursor(index: int) -> str:
    """Encode an index position into an opaque cursor string."""
    payload = json.dumps({"idx": index})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_cursor(cursor: str) -> int:
    """Decode an opaque cursor string back to an index position.

    Args:
        cursor: Base64-encoded cursor string.

    Returns:
        The decoded index position.

    Raises:
        HTTPException: If the cursor is malformed.
    """
    try:
        payload = base64.urlsafe_b64decode(cursor.encode()).decode()
        data = json.loads(payload)
        return int(data["idx"])
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cursor: {exc}",
        ) from exc


# ============================================================================
# GET /v1/anomalies - Task 51
# ============================================================================


@router.get("/anomalies")
async def get_anomalies(
    agent_id: UUID | None = Query(default=None, description="Filter by agent ID"),
    start_time: datetime | None = Query(default=None, description="Start of time range (inclusive)"),
    end_time: datetime | None = Query(default=None, description="End of time range (inclusive)"),
    severity: str | None = Query(default=None, description="Filter by severity (low, medium, high, critical)"),
    cursor: str | None = Query(default=None, description="Cursor for pagination"),
    limit: int = Query(default=50, ge=1, le=200, description="Maximum results per page"),
) -> dict[str, Any]:
    """Get paginated anomaly events with optional filtering.

    Supports filtering by agent_id, time range, and severity level.
    Uses cursor-based pagination for stable iteration over results.

    Returns:
        Dictionary with 'anomalies' list, 'next_cursor' (null if no more),
        and 'total_count' of matching results.
    """
    # Validate severity if provided
    if severity is not None:
        valid_severities = [s.value for s in Severity]
        if severity.lower() not in valid_severities:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid severity '{severity}'. Must be one of: {valid_severities}",
            )

    # Apply filters
    filtered = _filter_anomalies(
        agent_id=agent_id,
        start_time=start_time,
        end_time=end_time,
        severity=severity,
    )

    # Sort by detected_at descending (most recent first)
    filtered.sort(key=lambda a: a.detected_at, reverse=True)

    total_count = len(filtered)

    # Apply cursor-based pagination
    start_idx = 0
    if cursor is not None:
        start_idx = _decode_cursor(cursor)

    # Slice results
    page = filtered[start_idx : start_idx + limit]

    # Generate next cursor if more results exist
    next_cursor = None
    if start_idx + limit < total_count:
        next_cursor = _encode_cursor(start_idx + limit)

    return {
        "anomalies": [_serialize_anomaly(a) for a in page],
        "next_cursor": next_cursor,
        "total_count": total_count,
    }


def _filter_anomalies(
    agent_id: UUID | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    severity: str | None = None,
) -> list[AnomalyEvent]:
    """Filter the anomaly store by the given criteria."""
    results = list(_anomaly_store)

    if agent_id is not None:
        results = [a for a in results if a.agent_id == agent_id]

    if start_time is not None:
        # Ensure timezone-aware comparison
        st = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
        results = [a for a in results if a.detected_at >= st]

    if end_time is not None:
        et = end_time if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc)
        results = [a for a in results if a.detected_at <= et]

    if severity is not None:
        results = [a for a in results if a.severity.value == severity.lower()]

    return results


def _serialize_anomaly(anomaly: AnomalyEvent) -> dict[str, Any]:
    """Serialize an AnomalyEvent for JSON response."""
    return {
        "anomaly_id": str(anomaly.anomaly_id),
        "agent_id": str(anomaly.agent_id),
        "org_id": str(anomaly.org_id),
        "anomaly_type": anomaly.anomaly_type.value,
        "severity": anomaly.severity.value,
        "detected_at": anomaly.detected_at.isoformat(),
        "window_start": anomaly.window_start.isoformat(),
        "window_end": anomaly.window_end.isoformat(),
        "metric_value": anomaly.metric_value,
        "threshold_value": anomaly.threshold_value,
        "description": anomaly.description,
        "metadata": anomaly.metadata,
    }


# ============================================================================
# GET /v1/agents/{agent_id}/status - Task 52
# ============================================================================


@router.get("/agents/{agent_id}/status")
async def get_agent_status(agent_id: UUID) -> dict[str, Any]:
    """Get agent status including circuit breaker state and recent metrics.

    Returns the current circuit breaker state (active/inactive) and
    recent telemetry metrics aggregated from the in-memory store.

    Args:
        agent_id: The agent UUID.

    Returns:
        Dictionary with 'agent_id', 'circuit_breaker' state, and 'recent_metrics'.
    """
    # Get circuit breaker state
    cb_state = _agent_circuit_breaker_store.get(agent_id)
    circuit_breaker = {
        "state": "inactive",
        "activated_at": None,
        "reason": None,
        "activated_by": None,
    }
    if cb_state is not None:
        circuit_breaker = {
            "state": cb_state.get("state", "inactive"),
            "activated_at": cb_state.get("activated_at"),
            "reason": cb_state.get("reason"),
            "activated_by": cb_state.get("activated_by"),
        }

    # Compute recent metrics from telemetry store
    recent_metrics = _compute_agent_metrics(agent_id)

    return {
        "agent_id": str(agent_id),
        "circuit_breaker": circuit_breaker,
        "recent_metrics": recent_metrics,
    }


def _compute_agent_metrics(agent_id: UUID) -> dict[str, Any]:
    """Compute recent telemetry metrics for a specific agent.

    Aggregates the most recent events for the agent from the
    in-memory telemetry store.
    """
    agent_events = [
        e for e in _telemetry_store
        if _match_agent_id(e.get("agent_id"), agent_id)
    ]

    if not agent_events:
        return {
            "event_count": 0,
            "total_tokens": 0,
            "total_cost": "0",
            "avg_latency_ms": 0.0,
            "last_seen": None,
        }

    total_tokens = sum(
        int(e.get("prompt_tokens", 0)) + int(e.get("completion_tokens", 0))
        for e in agent_events
    )
    total_cost = sum(float(e.get("total_cost", 0)) for e in agent_events)
    latencies = [int(e.get("latency_ms", 0)) for e in agent_events]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0

    # Find most recent timestamp
    timestamps = []
    for e in agent_events:
        ts = e.get("timestamp")
        if isinstance(ts, str):
            try:
                timestamps.append(datetime.fromisoformat(ts))
            except ValueError:
                pass
        elif isinstance(ts, datetime):
            timestamps.append(ts)

    last_seen = max(timestamps).isoformat() if timestamps else None

    return {
        "event_count": len(agent_events),
        "total_tokens": total_tokens,
        "total_cost": f"{total_cost:.6f}",
        "avg_latency_ms": round(avg_latency, 2),
        "last_seen": last_seen,
    }


def _match_agent_id(stored_id: Any, target_id: UUID) -> bool:
    """Compare a stored agent_id (string or UUID) against a target UUID."""
    if stored_id is None:
        return False
    if isinstance(stored_id, UUID):
        return stored_id == target_id
    try:
        return UUID(str(stored_id)) == target_id
    except (ValueError, TypeError):
        return False


# ============================================================================
# GET /v1/metrics - Task 53
# ============================================================================


@router.get("/metrics")
async def get_metrics(
    start_time: datetime | None = Query(default=None, description="Start of time range"),
    end_time: datetime | None = Query(default=None, description="End of time range"),
) -> dict[str, Any]:
    """Get aggregated telemetry metrics for dashboard visualization.

    Returns high-level aggregated statistics across all agents,
    suitable for rendering in the governance dashboard.

    Args:
        start_time: Optional start of time range filter.
        end_time: Optional end of time range filter.

    Returns:
        Dictionary with aggregated metrics including total events,
        tokens, costs, active agents, and anomaly counts.
    """
    # Filter telemetry by time range
    filtered_telemetry = _filter_telemetry_by_time(start_time, end_time)

    # Filter anomalies by time range
    filtered_anomalies = _filter_anomalies(
        start_time=start_time, end_time=end_time
    )

    # Compute aggregated metrics
    total_events = len(filtered_telemetry)
    total_tokens = sum(
        int(e.get("prompt_tokens", 0)) + int(e.get("completion_tokens", 0))
        for e in filtered_telemetry
    )
    total_cost = sum(float(e.get("total_cost", 0)) for e in filtered_telemetry)

    # Unique agents
    active_agents: set[str] = set()
    for e in filtered_telemetry:
        aid = e.get("agent_id")
        if aid is not None:
            active_agents.add(str(aid))

    # Average latency
    latencies = [int(e.get("latency_ms", 0)) for e in filtered_telemetry]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0

    # Anomaly breakdown by type
    anomaly_by_type: dict[str, int] = {}
    anomaly_by_severity: dict[str, int] = {}
    for anomaly in filtered_anomalies:
        atype = anomaly.anomaly_type.value
        anomaly_by_type[atype] = anomaly_by_type.get(atype, 0) + 1
        sev = anomaly.severity.value
        anomaly_by_severity[sev] = anomaly_by_severity.get(sev, 0) + 1

    return {
        "total_events": total_events,
        "total_tokens": total_tokens,
        "total_cost": f"{total_cost:.6f}",
        "active_agents": len(active_agents),
        "avg_latency_ms": round(avg_latency, 2),
        "total_anomalies": len(filtered_anomalies),
        "anomalies_by_type": anomaly_by_type,
        "anomalies_by_severity": anomaly_by_severity,
    }


def _filter_telemetry_by_time(
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> list[dict[str, Any]]:
    """Filter telemetry store by time range."""
    results = list(_telemetry_store)

    if start_time is None and end_time is None:
        return results

    filtered = []
    for event in results:
        ts = event.get("timestamp")
        if ts is None:
            continue

        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                continue

        if not isinstance(ts, datetime):
            continue

        # Ensure timezone-aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        if start_time is not None:
            st = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
            if ts < st:
                continue

        if end_time is not None:
            et = end_time if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc)
            if ts > et:
                continue

        filtered.append(event)

    return filtered
