"""Integration tests for end-to-end flow and circuit breaker propagation.

Tests the full pipeline interactions between components using mocked
external services (Redis, Kafka) but real component logic.

Validates: Requirements 1, 3, 9 (ingestion flow, circuit breaker, governance).
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.anomaly.detectors import AnomalyDetector
from src.anomaly.producer import AnomalyEventProducer
from src.anomaly.window_manager import WindowManager
from src.config.settings import KafkaSettings
from src.gateway.auth import AgentIdentity
from src.gateway.circuit_breaker import CircuitBreakerService
from src.gateway.deduplication import DeduplicationService
from src.gateway.dependencies import (
    authenticate_agent,
    get_circuit_breaker_middleware,
    get_deduplication_service,
    get_event_buffer,
    get_kafka_producer,
    get_kafka_settings,
    get_rate_limiter,
)
from src.gateway.event_buffer import EventBuffer
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.middleware import CircuitBreakerMiddleware
from src.gateway.rate_limiter import RateLimiter, RateLimitResult
from src.gateway.routes import router
from src.governance.engine import ActionType, GovernanceEngine
from src.models.anomaly import AnomalyEvent, AnomalyType, Severity
from src.models.governance import (
    GovernancePolicy,
    SupportedMetric,
    ThresholdConfig,
)
from src.models.telemetry import TelemetryEvent


AGENT_ID = UUID("550e8400-e29b-41d4-a716-446655440000")
ORG_ID = UUID("7c9e6679-7425-40de-944b-e07fc1f90ae7")
API_KEY = "test-api-key-integration"


def _make_telemetry_event(
    agent_id: UUID = AGENT_ID,
    org_id: UUID = ORG_ID,
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
    tool_name: str | None = "web_search",
) -> dict:
    """Create a valid telemetry event payload dict."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent_id": str(agent_id),
        "org_id": str(org_id),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_cost": "0.001500",
        "latency_ms": 200,
        "tool_name": tool_name,
    }


def _make_batch(
    agent_id: UUID = AGENT_ID,
    org_id: UUID = ORG_ID,
    event_count: int = 1,
    batch_id: UUID | None = None,
    tool_name: str | None = "web_search",
) -> dict:
    """Create a valid telemetry batch payload dict."""
    return {
        "agent_id": str(agent_id),
        "org_id": str(org_id),
        "sdk_version": "1.0.0",
        "batch_id": str(batch_id or uuid4()),
        "events": [
            _make_telemetry_event(agent_id, org_id, tool_name=tool_name)
            for _ in range(event_count)
        ],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def kafka_settings():
    return KafkaSettings()


@pytest.fixture
def mock_kafka_producer():
    producer = AsyncMock(spec=KafkaProducerService)
    producer.produce_batch = AsyncMock(return_value=None)
    producer.produce = AsyncMock(return_value=None)
    return producer


@pytest.fixture
def event_buffer():
    return EventBuffer(max_size=100)


@pytest.fixture
def mock_redis_service():
    """Mock RedisService that simulates a working Redis."""
    redis_svc = AsyncMock()
    redis_svc.get = AsyncMock(return_value=None)
    redis_svc.set = AsyncMock(return_value=True)
    redis_svc.delete = AsyncMock(return_value=True)
    redis_svc.is_available = True
    redis_svc._client = MagicMock()
    return redis_svc


@pytest.fixture
def circuit_breaker_service(mock_redis_service):
    return CircuitBreakerService(mock_redis_service)


@pytest.fixture
def cb_middleware(circuit_breaker_service):
    return CircuitBreakerMiddleware(circuit_breaker_service)


@pytest.fixture
def mock_dedup_service(mock_redis_service):
    return DeduplicationService(mock_redis_service)


@pytest.fixture
def mock_rate_limiter():
    """Mock RateLimiter that always allows requests."""
    limiter = AsyncMock(spec=RateLimiter)
    limiter.check_rate_limit = AsyncMock(
        return_value=RateLimitResult(
            allowed=True, remaining=99, reset_at=datetime.now(timezone.utc)
        )
    )
    return limiter


@pytest.fixture
def mock_identity():
    return AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)


@pytest.fixture
def app(
    mock_kafka_producer,
    event_buffer,
    kafka_settings,
    mock_identity,
    cb_middleware,
    mock_dedup_service,
    mock_rate_limiter,
):
    """Create FastAPI app with real middleware logic and mocked external services."""
    test_app = FastAPI()
    test_app.include_router(router)

    test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
    test_app.dependency_overrides[get_event_buffer] = lambda: event_buffer
    test_app.dependency_overrides[get_kafka_settings] = lambda: kafka_settings
    test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
    test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: cb_middleware
    test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup_service
    test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_limiter

    return test_app


@pytest.fixture
def client(app):
    return TestClient(app)


# ---------------------------------------------------------------------------
# Test 1: End-to-end telemetry ingestion flow
# ---------------------------------------------------------------------------


class TestEndToEndIngestionFlow:
    """Verify the full ingestion pipeline: validation → circuit breaker → Kafka."""

    def test_valid_batch_passes_validation_and_produces_to_kafka(
        self, client, mock_kafka_producer, mock_redis_service
    ):
        """Submit a valid batch → passes validation → CB check → Kafka produce."""
        batch = _make_batch(event_count=3)
        response = client.post("/v1/telemetry", json=batch)

        # 1. Passes validation and returns 202
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["event_count"] == 3
        assert data["batch_id"] == batch["batch_id"]

        # 2. Circuit breaker check was called (Redis get for CB state)
        mock_redis_service.get.assert_called()

        # 3. Kafka produce_batch was called with all events
        mock_kafka_producer.produce_batch.assert_called_once()
        produced_events = mock_kafka_producer.produce_batch.call_args[0][0]
        assert len(produced_events) == 3

        # 4. Each event is properly serialized
        for topic, key, value_bytes in produced_events:
            assert topic == "telemetry-raw"
            assert key == str(AGENT_ID)
            parsed = json.loads(value_bytes)
            assert parsed["prompt_tokens"] == 100
            assert parsed["agent_id"] == str(AGENT_ID)

    def test_dedup_mark_processed_called_on_success(
        self, client, mock_kafka_producer, mock_redis_service
    ):
        """After successful produce, batch_id is marked as processed for idempotency."""
        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        assert response.status_code == 202

        # Redis.set should be called to mark batch as processed
        # (one call for dedup mark_processed)
        set_calls = mock_redis_service.set.call_args_list
        dedup_key = f"batch_processed:{batch['batch_id']}"
        dedup_calls = [c for c in set_calls if c[0][0] == dedup_key]
        assert len(dedup_calls) == 1


# ---------------------------------------------------------------------------
# Test 2: Circuit breaker blocks ingestion
# ---------------------------------------------------------------------------


class TestCircuitBreakerBlocksIngestion:
    """Verify that an active circuit breaker blocks telemetry ingestion."""

    def test_active_circuit_breaker_returns_429(self, mock_redis_service):
        """When CB is active for an agent, ingestion returns 429."""
        # Simulate active circuit breaker in Redis
        cb_state = json.dumps({"is_active": True, "agent_id": str(AGENT_ID)})
        mock_redis_service.get = AsyncMock(return_value=cb_state)

        # Build app with real CB middleware that reads from mocked Redis
        cb_service = CircuitBreakerService(mock_redis_service)
        cb_mid = CircuitBreakerMiddleware(cb_service)

        mock_kafka_producer = AsyncMock(spec=KafkaProducerService)
        mock_kafka_producer.produce_batch = AsyncMock(return_value=None)
        mock_identity = AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)

        mock_dedup = DeduplicationService(mock_redis_service)
        mock_rate_lim = AsyncMock(spec=RateLimiter)
        mock_rate_lim.check_rate_limit = AsyncMock(
            return_value=RateLimitResult(
                allowed=True, remaining=99, reset_at=datetime.now(timezone.utc)
            )
        )

        test_app = FastAPI()
        test_app.include_router(router)
        test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka_producer
        test_app.dependency_overrides[get_event_buffer] = lambda: EventBuffer(max_size=100)
        test_app.dependency_overrides[get_kafka_settings] = lambda: KafkaSettings()
        test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
        test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: cb_mid
        test_app.dependency_overrides[get_deduplication_service] = lambda: mock_dedup
        test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_lim

        client = TestClient(test_app)

        batch = _make_batch()
        response = client.post("/v1/telemetry", json=batch)

        # Should be rejected with 429
        assert response.status_code == 429
        assert "Retry-After" in response.headers

        # Kafka should NOT have been called
        mock_kafka_producer.produce_batch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: Anomaly detection pipeline
# ---------------------------------------------------------------------------


class TestAnomalyDetectionPipeline:
    """Verify the anomaly detection pipeline: event → window → detector → produce."""

    def test_infinite_loop_detection_produces_anomaly(self):
        """Feed repeated tool calls → window updates → detector triggers anomaly."""
        window_manager = WindowManager(window_duration_seconds=60)
        detector = AnomalyDetector(loop_threshold=5)
        mock_producer = AsyncMock(spec=AnomalyEventProducer)
        mock_producer.produce_anomalies = AsyncMock(return_value=None)

        # Feed 5 identical tool calls to trigger infinite loop detection
        for i in range(5):
            event = TelemetryEvent(
                timestamp=datetime.now(timezone.utc),
                agent_id=AGENT_ID,
                org_id=ORG_ID,
                prompt_tokens=100,
                completion_tokens=50,
                total_cost=Decimal("0.001500"),
                latency_ms=200,
                tool_name="web_search",
            )
            window = window_manager.add_event(event)

        # Detector should find an infinite loop anomaly
        anomalies = detector.detect_anomalies(event, window)

        assert len(anomalies) >= 1
        loop_anomalies = [
            a for a in anomalies if a.anomaly_type == AnomalyType.INFINITE_LOOP
        ]
        assert len(loop_anomalies) == 1
        assert loop_anomalies[0].metric_value == 5.0
        assert loop_anomalies[0].agent_id == AGENT_ID

    def test_token_spike_detection_with_window(self):
        """Feed normal events + spike → window detects TOKEN_SPIKE anomaly."""
        window_manager = WindowManager(window_duration_seconds=60)
        detector = AnomalyDetector(spike_z_threshold=2.0, min_events_for_spike=2)

        # Feed several normal events to build baseline
        for _ in range(5):
            normal_event = TelemetryEvent(
                timestamp=datetime.now(timezone.utc),
                agent_id=AGENT_ID,
                org_id=ORG_ID,
                prompt_tokens=100,
                completion_tokens=50,
                total_cost=Decimal("0.001500"),
                latency_ms=200,
                tool_name="search",
            )
            window = window_manager.add_event(normal_event)

        # Feed a spike event (10x normal tokens)
        spike_event = TelemetryEvent(
            timestamp=datetime.now(timezone.utc),
            agent_id=AGENT_ID,
            org_id=ORG_ID,
            prompt_tokens=5000,
            completion_tokens=5000,
            total_cost=Decimal("0.100000"),
            latency_ms=200,
            tool_name="generate",
        )
        window = window_manager.add_event(spike_event)

        anomalies = detector.detect_anomalies(spike_event, window)

        spike_anomalies = [
            a for a in anomalies if a.anomaly_type == AnomalyType.TOKEN_SPIKE
        ]
        assert len(spike_anomalies) == 1
        assert spike_anomalies[0].severity in (
            Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL
        )

    @pytest.mark.asyncio
    async def test_full_anomaly_pipeline_produces_events(self):
        """Full pipeline: event → WindowManager → Detector → Producer.produce_anomalies."""
        window_manager = WindowManager(window_duration_seconds=60)
        detector = AnomalyDetector(loop_threshold=3)
        mock_kafka = AsyncMock(spec=KafkaProducerService)
        mock_kafka.produce_batch = AsyncMock(return_value=None)
        producer = AnomalyEventProducer(mock_kafka, topic="anomaly-events")

        # Feed 3 identical calls to trigger loop detection
        for _ in range(3):
            event = TelemetryEvent(
                timestamp=datetime.now(timezone.utc),
                agent_id=AGENT_ID,
                org_id=ORG_ID,
                prompt_tokens=100,
                completion_tokens=50,
                total_cost=Decimal("0.001500"),
                latency_ms=200,
                tool_name="repeated_tool",
            )
            window = window_manager.add_event(event)

        anomalies = detector.detect_anomalies(event, window)
        assert len(anomalies) >= 1

        # Produce anomalies through the real producer (mocked Kafka)
        await producer.produce_anomalies(anomalies)

        # Verify Kafka produce_batch was called with serialized anomalies
        mock_kafka.produce_batch.assert_called_once()
        produced = mock_kafka.produce_batch.call_args[0][0]
        assert len(produced) == len(anomalies)
        for topic, key, value_bytes in produced:
            assert topic == "anomaly-events"
            assert key == str(AGENT_ID)
            parsed = json.loads(value_bytes)
            assert parsed["anomaly_type"] == "infinite_loop"


# ---------------------------------------------------------------------------
# Test 4: Governance evaluation and circuit breaker activation
# ---------------------------------------------------------------------------


class TestGovernanceEvaluationAndCircuitBreaker:
    """Verify governance policy evaluation triggers KILL_SWITCH and activates CB."""

    @pytest.mark.asyncio
    async def test_hard_limit_breach_activates_circuit_breaker(
        self, mock_redis_service
    ):
        """Anomaly breaching hard limit → KILL_SWITCH → CB activation in Redis."""
        # Set up governance policy with threshold
        policy = GovernancePolicy(
            org_id=ORG_ID,
            thresholds=[
                ThresholdConfig(
                    metric=SupportedMetric.CONSECUTIVE_IDENTICAL_CALLS,
                    soft_limit=5.0,
                    hard_limit=10.0,
                    window_seconds=60,
                    cooldown_seconds=300,
                )
            ],
            auto_kill_enabled=True,
        )

        # Set up circuit breaker service with mocked Redis
        cb_service = CircuitBreakerService(mock_redis_service)

        # Create governance engine with real logic
        policy_store = {ORG_ID: policy}
        engine = GovernanceEngine(
            policy_store=policy_store,
            circuit_breaker_service=cb_service,
        )

        # Create an anomaly event that breaches the hard limit (value=12 > hard=10)
        anomaly = AnomalyEvent(
            agent_id=AGENT_ID,
            org_id=ORG_ID,
            anomaly_type=AnomalyType.INFINITE_LOOP,
            severity=Severity.HIGH,
            detected_at=datetime.now(timezone.utc),
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            metric_value=12.0,
            threshold_value=10.0,
            description="Test anomaly breaching hard limit",
            metadata={"consecutive_count": 12},
        )

        # Evaluate the anomaly against the policy
        actions = await engine.evaluate_anomaly(anomaly)

        # Should produce WARNING + KILL_SWITCH (monotonicity guarantee)
        action_types = [a.action_type for a in actions]
        assert ActionType.WARNING in action_types
        assert ActionType.KILL_SWITCH in action_types

        # Circuit breaker should have been activated in Redis
        # (cb_service.activate calls redis.set)
        set_calls = mock_redis_service.set.call_args_list
        cb_key = f"circuit_breaker:{AGENT_ID}"
        cb_writes = [c for c in set_calls if c[0][0] == cb_key]
        assert len(cb_writes) == 1

        # Verify the persisted state is correct
        persisted_json = cb_writes[0][0][1]
        persisted_state = json.loads(persisted_json)
        assert persisted_state["is_active"] is True
        assert persisted_state["activated_by"] == "system"

    @pytest.mark.asyncio
    async def test_auto_kill_disabled_produces_critical_warning(
        self, mock_redis_service
    ):
        """When auto_kill is disabled, hard breach produces CRITICAL warning, not kill-switch."""
        policy = GovernancePolicy(
            org_id=ORG_ID,
            thresholds=[
                ThresholdConfig(
                    metric=SupportedMetric.TOTAL_TOKENS,
                    soft_limit=800.0,
                    hard_limit=1000.0,
                    window_seconds=3600,
                    cooldown_seconds=300,
                )
            ],
            auto_kill_enabled=False,
        )

        cb_service = CircuitBreakerService(mock_redis_service)
        engine = GovernanceEngine(
            policy_store={ORG_ID: policy},
            circuit_breaker_service=cb_service,
        )

        anomaly = AnomalyEvent(
            agent_id=AGENT_ID,
            org_id=ORG_ID,
            anomaly_type=AnomalyType.TOKEN_SPIKE,
            severity=Severity.CRITICAL,
            detected_at=datetime.now(timezone.utc),
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            metric_value=1200.0,
            threshold_value=1000.0,
            description="Token spike exceeding hard limit",
            metadata={},
        )

        actions = await engine.evaluate_anomaly(anomaly)

        action_types = [a.action_type for a in actions]
        # Should NOT produce KILL_SWITCH when auto_kill is disabled
        assert ActionType.KILL_SWITCH not in action_types
        # Should produce WARNING actions (including CRITICAL-severity warning)
        assert ActionType.WARNING in action_types
        critical_warnings = [
            a for a in actions if a.severity == Severity.CRITICAL
        ]
        assert len(critical_warnings) >= 1


# ---------------------------------------------------------------------------
# Test 5: Deduplication prevents reprocessing
# ---------------------------------------------------------------------------


class TestDeduplicationPreventsReprocessing:
    """Verify that duplicate batch_ids are handled idempotently."""

    def test_second_submission_with_same_batch_id_returns_202_no_kafka(
        self, mock_redis_service
    ):
        """Submit same batch_id twice → second returns 202 without Kafka produce."""
        batch_id = uuid4()

        # Track how many times the specific dedup key has been queried
        dedup_key = f"batch_processed:{batch_id}"
        dedup_get_count = {"n": 0}

        async def smart_get(key):
            if key == dedup_key:
                dedup_get_count["n"] += 1
                # First dedup check: not processed yet
                if dedup_get_count["n"] <= 1:
                    return None
                # Subsequent checks: already processed
                return "1"
            # Circuit breaker check - not active
            return None

        mock_redis_service.get = AsyncMock(side_effect=smart_get)

        cb_service = CircuitBreakerService(mock_redis_service)
        cb_mid = CircuitBreakerMiddleware(cb_service)
        dedup_service = DeduplicationService(mock_redis_service)
        mock_kafka = AsyncMock(spec=KafkaProducerService)
        mock_kafka.produce_batch = AsyncMock(return_value=None)
        mock_identity = AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)
        mock_rate_lim = AsyncMock(spec=RateLimiter)
        mock_rate_lim.check_rate_limit = AsyncMock(
            return_value=RateLimitResult(
                allowed=True, remaining=99, reset_at=datetime.now(timezone.utc)
            )
        )

        test_app = FastAPI()
        test_app.include_router(router)
        test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka
        test_app.dependency_overrides[get_event_buffer] = lambda: EventBuffer(max_size=100)
        test_app.dependency_overrides[get_kafka_settings] = lambda: KafkaSettings()
        test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
        test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: cb_mid
        test_app.dependency_overrides[get_deduplication_service] = lambda: dedup_service
        test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_lim

        client = TestClient(test_app)

        batch = _make_batch(batch_id=batch_id)

        # First submission: produces to Kafka
        resp1 = client.post("/v1/telemetry", json=batch)
        assert resp1.status_code == 202
        assert mock_kafka.produce_batch.call_count == 1

        # Second submission: same batch_id, should be idempotent
        resp2 = client.post("/v1/telemetry", json=batch)
        assert resp2.status_code == 202
        data2 = resp2.json()
        assert data2["batch_id"] == str(batch_id)
        assert data2["status"] == "accepted"

        # Kafka should NOT be called a second time
        assert mock_kafka.produce_batch.call_count == 1


# ---------------------------------------------------------------------------
# Test 6: Rate limiting enforcement
# ---------------------------------------------------------------------------


class TestRateLimitingEnforcement:
    """Verify that rate limiting returns 429 when limit is exceeded."""

    def test_rate_limit_exceeded_returns_429(self):
        """Submit more requests than rate limit → 429 returned."""
        # First N requests allowed, then rate limit kicks in
        call_count = {"n": 0}
        limit = 3

        async def check_rate_limit(agent_id, org_id):
            call_count["n"] += 1
            if call_count["n"] > limit:
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    reset_at=datetime.now(timezone.utc),
                )
            return RateLimitResult(
                allowed=True,
                remaining=limit - call_count["n"],
                reset_at=datetime.now(timezone.utc),
            )

        mock_rate_lim = AsyncMock(spec=RateLimiter)
        mock_rate_lim.check_rate_limit = AsyncMock(side_effect=check_rate_limit)

        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock(return_value=True)

        cb_service = CircuitBreakerService(mock_redis)
        cb_mid = CircuitBreakerMiddleware(cb_service)
        dedup_service = DeduplicationService(mock_redis)
        mock_kafka = AsyncMock(spec=KafkaProducerService)
        mock_kafka.produce_batch = AsyncMock(return_value=None)
        mock_identity = AgentIdentity(agent_id=AGENT_ID, org_id=ORG_ID)

        test_app = FastAPI()
        test_app.include_router(router)
        test_app.dependency_overrides[get_kafka_producer] = lambda: mock_kafka
        test_app.dependency_overrides[get_event_buffer] = lambda: EventBuffer(max_size=100)
        test_app.dependency_overrides[get_kafka_settings] = lambda: KafkaSettings()
        test_app.dependency_overrides[authenticate_agent] = lambda: mock_identity
        test_app.dependency_overrides[get_circuit_breaker_middleware] = lambda: cb_mid
        test_app.dependency_overrides[get_deduplication_service] = lambda: dedup_service
        test_app.dependency_overrides[get_rate_limiter] = lambda: mock_rate_lim

        client = TestClient(test_app)

        # First 3 requests succeed
        for i in range(limit):
            batch = _make_batch()
            resp = client.post("/v1/telemetry", json=batch)
            assert resp.status_code == 202, f"Request {i+1} should succeed"

        # 4th request should be rate-limited
        batch = _make_batch()
        resp = client.post("/v1/telemetry", json=batch)
        assert resp.status_code == 429
        assert "Rate limit" in resp.json().get("detail", "")
