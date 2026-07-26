"""Tests for health check endpoints.

Validates Requirement 1 - health check interface providing liveness,
readiness, and combined health status probes.
"""

from unittest.mock import AsyncMock, PropertyMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config.settings import AppSettings
from src.gateway.health import (
    HealthService,
    get_health_service,
    init_health_service,
    router,
)
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.redis_service import RedisService
from src.models.responses import HealthResponse


@pytest.fixture
def mock_kafka_producer():
    """Mock KafkaProducerService."""
    producer = AsyncMock(spec=KafkaProducerService)
    type(producer).is_running = PropertyMock(return_value=True)
    return producer


@pytest.fixture
def mock_redis_service():
    """Mock RedisService."""
    service = AsyncMock(spec=RedisService)
    service.health_check = AsyncMock(return_value=True)
    return service


@pytest.fixture
def app_settings():
    """AppSettings with test version."""
    return AppSettings(version="1.0.0-test")


@pytest.fixture
def health_service(mock_kafka_producer, mock_redis_service, app_settings):
    """HealthService with mocked dependencies."""
    return HealthService(mock_kafka_producer, mock_redis_service, app_settings)


@pytest.fixture
def app(health_service):
    """FastAPI test app with health routes registered."""
    test_app = FastAPI()
    test_app.include_router(router)

    # Override the dependency to use our test health service
    test_app.dependency_overrides[get_health_service] = lambda: health_service

    return test_app


@pytest.fixture
def client(app):
    """Test client for making HTTP requests."""
    return TestClient(app)


class TestHealthService:
    """Unit tests for the HealthService logic."""

    def test_version_from_settings(self, health_service, app_settings):
        """Version comes from app settings."""
        assert health_service.version == "1.0.0-test"

    def test_check_kafka_returns_is_running(self, health_service, mock_kafka_producer):
        """Kafka check delegates to producer.is_running."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=True)
        assert health_service.check_kafka() is True

        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)
        assert health_service.check_kafka() is False

    @pytest.mark.asyncio
    async def test_check_redis_delegates_to_health_check(
        self, health_service, mock_redis_service
    ):
        """Redis check calls redis_service.health_check()."""
        mock_redis_service.health_check = AsyncMock(return_value=True)
        assert await health_service.check_redis() is True

        mock_redis_service.health_check = AsyncMock(return_value=False)
        assert await health_service.check_redis() is False

    @pytest.mark.asyncio
    async def test_check_db_returns_true(self, health_service):
        """DB check returns True (placeholder until DB service exists)."""
        assert await health_service.check_db() is True

    @pytest.mark.asyncio
    async def test_status_healthy_when_all_connected(
        self, health_service, mock_kafka_producer, mock_redis_service
    ):
        """Status is 'healthy' when all dependencies are connected."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=True)
        mock_redis_service.health_check = AsyncMock(return_value=True)

        result = await health_service.get_health_status()

        assert result.status == "healthy"
        assert result.kafka_connected is True
        assert result.redis_connected is True
        assert result.db_connected is True
        assert result.version == "1.0.0-test"

    @pytest.mark.asyncio
    async def test_status_degraded_when_kafka_down(
        self, health_service, mock_kafka_producer, mock_redis_service
    ):
        """Status is 'degraded' when Kafka is down but others are up."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)
        mock_redis_service.health_check = AsyncMock(return_value=True)

        result = await health_service.get_health_status()

        assert result.status == "degraded"
        assert result.kafka_connected is False
        assert result.redis_connected is True

    @pytest.mark.asyncio
    async def test_status_degraded_when_redis_down(
        self, health_service, mock_kafka_producer, mock_redis_service
    ):
        """Status is 'degraded' when Redis is down but others are up."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=True)
        mock_redis_service.health_check = AsyncMock(return_value=False)

        result = await health_service.get_health_status()

        assert result.status == "degraded"
        assert result.kafka_connected is True
        assert result.redis_connected is False

    @pytest.mark.asyncio
    async def test_status_unhealthy_when_all_down(
        self, health_service, mock_kafka_producer, mock_redis_service
    ):
        """Status is 'unhealthy' when all dependencies are down."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)
        mock_redis_service.health_check = AsyncMock(return_value=False)

        # Patch check_db to return False for this test
        health_service.check_db = AsyncMock(return_value=False)

        result = await health_service.get_health_status()

        assert result.status == "unhealthy"
        assert result.kafka_connected is False
        assert result.redis_connected is False
        assert result.db_connected is False


class TestLivenessEndpoint:
    """Tests for GET /health/live."""

    def test_returns_200_always(self, client):
        """Liveness probe always returns 200 if process is up."""
        response = client.get("/health/live")
        assert response.status_code == 200

    def test_returns_alive_status(self, client):
        """Liveness probe returns alive status."""
        response = client.get("/health/live")
        assert response.json() == {"status": "alive"}


class TestReadinessEndpoint:
    """Tests for GET /health/ready."""

    def test_returns_200_when_all_ready(self, client):
        """Readiness probe returns 200 when all dependencies are connected."""
        response = client.get("/health/ready")
        assert response.status_code == 200
        body = response.json()
        assert body["ready"] is True
        assert body["kafka_connected"] is True
        assert body["redis_connected"] is True
        assert body["db_connected"] is True

    def test_returns_503_when_kafka_down(
        self, client, mock_kafka_producer
    ):
        """Readiness probe returns 503 when Kafka is down."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)

        response = client.get("/health/ready")
        assert response.status_code == 503
        body = response.json()
        assert body["ready"] is False
        assert body["kafka_connected"] is False

    def test_returns_503_when_redis_down(
        self, client, mock_redis_service
    ):
        """Readiness probe returns 503 when Redis is down."""
        mock_redis_service.health_check = AsyncMock(return_value=False)

        response = client.get("/health/ready")
        assert response.status_code == 503
        body = response.json()
        assert body["ready"] is False
        assert body["redis_connected"] is False


class TestCombinedHealthEndpoint:
    """Tests for GET /health."""

    def test_returns_200_when_healthy(self, client):
        """Combined health returns 200 with healthy status when all connected."""
        response = client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "healthy"
        assert body["kafka_connected"] is True
        assert body["redis_connected"] is True
        assert body["db_connected"] is True
        assert body["version"] == "1.0.0-test"

    def test_returns_200_when_degraded(self, client, mock_kafka_producer):
        """Combined health returns 200 with degraded status."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)

        response = client.get("/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "degraded"

    def test_returns_503_when_unhealthy(
        self, client, health_service, mock_kafka_producer, mock_redis_service
    ):
        """Combined health returns 503 when unhealthy (all dependencies down)."""
        type(mock_kafka_producer).is_running = PropertyMock(return_value=False)
        mock_redis_service.health_check = AsyncMock(return_value=False)
        health_service.check_db = AsyncMock(return_value=False)

        response = client.get("/health")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "unhealthy"

    def test_includes_version(self, client):
        """Combined health response includes the service version."""
        response = client.get("/health")
        body = response.json()
        assert body["version"] == "1.0.0-test"


class TestInitHealthService:
    """Tests for the init_health_service function."""

    def test_initializes_and_returns_service(
        self, mock_kafka_producer, mock_redis_service, app_settings
    ):
        """init_health_service creates and returns a HealthService."""
        service = init_health_service(
            mock_kafka_producer, mock_redis_service, app_settings
        )
        assert isinstance(service, HealthService)
        assert service.version == app_settings.version
