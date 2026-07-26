"""
Health check endpoints for the ingestion gateway.

Provides liveness, readiness, and combined health probes for
container orchestration and monitoring.

- GET /health/live  — Simple liveness probe (always 200 if process is up)
- GET /health/ready — Readiness probe checking Kafka, Redis, and DB connectivity
- GET /health       — Combined health status using HealthResponse model

Implements Requirement 1 (Telemetry Ingestion API) health check interface.
"""

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from src.config.settings import AppSettings, get_settings, Settings
from src.gateway.kafka_producer import KafkaProducerService
from src.gateway.redis_service import RedisService
from src.models.responses import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])


class HealthService:
    """Aggregates health status from downstream dependencies.

    Checks Kafka producer state, Redis connectivity, and DB connectivity
    to determine overall service health.
    """

    def __init__(
        self,
        kafka_producer: KafkaProducerService,
        redis_service: RedisService,
        app_settings: AppSettings,
    ) -> None:
        self._kafka_producer = kafka_producer
        self._redis_service = redis_service
        self._app_settings = app_settings

    @property
    def version(self) -> str:
        """Application version from settings."""
        return self._app_settings.version

    def check_kafka(self) -> bool:
        """Check if the Kafka producer is connected and running."""
        return self._kafka_producer.is_running

    async def check_redis(self) -> bool:
        """Check if Redis is reachable via health check ping."""
        return await self._redis_service.health_check()

    async def check_db(self) -> bool:
        """Check if the database is reachable.

        Returns True for now until the DB service is fully implemented.
        """
        # TODO: Implement actual DB connection test when DB service exists
        return True

    async def get_health_status(self) -> HealthResponse:
        """Compute overall health status from dependency checks.

        Status logic:
        - "healthy": all dependencies connected
        - "degraded": some dependencies down
        - "unhealthy": all dependencies down
        """
        kafka_connected = self.check_kafka()
        redis_connected = await self.check_redis()
        db_connected = await self.check_db()

        connected_count = sum([kafka_connected, redis_connected, db_connected])

        if connected_count == 3:
            status = "healthy"
        elif connected_count == 0:
            status = "unhealthy"
        else:
            status = "degraded"

        return HealthResponse(
            status=status,
            kafka_connected=kafka_connected,
            redis_connected=redis_connected,
            db_connected=db_connected,
            version=self.version,
        )


# Module-level health service instance, set during app startup
_health_service: HealthService | None = None


def init_health_service(
    kafka_producer: KafkaProducerService,
    redis_service: RedisService,
    app_settings: AppSettings,
) -> HealthService:
    """Initialize and register the health service singleton.

    Called during FastAPI app startup to wire dependencies.
    """
    global _health_service
    _health_service = HealthService(kafka_producer, redis_service, app_settings)
    return _health_service


def get_health_service() -> HealthService:
    """Dependency for retrieving the health service instance."""
    if _health_service is None:
        raise RuntimeError("HealthService not initialized. Call init_health_service() during startup.")
    return _health_service


@router.get("/live", status_code=200)
async def liveness_probe() -> dict:
    """Liveness probe — always returns 200 if the process is up.

    Used by container orchestrators (e.g., Kubernetes) to determine
    if the process needs to be restarted.
    """
    return {"status": "alive"}


@router.get("/ready")
async def readiness_probe(
    health_service: HealthService = Depends(get_health_service),
) -> JSONResponse:
    """Readiness probe — checks if the service can handle traffic.

    Verifies Kafka, Redis, and DB connectivity. Returns 200 if ready,
    503 if not ready to accept traffic.
    """
    kafka_ok = health_service.check_kafka()
    redis_ok = await health_service.check_redis()
    db_ok = await health_service.check_db()

    all_ready = kafka_ok and redis_ok and db_ok

    body = {
        "ready": all_ready,
        "kafka_connected": kafka_ok,
        "redis_connected": redis_ok,
        "db_connected": db_ok,
    }

    status_code = 200 if all_ready else 503
    return JSONResponse(content=body, status_code=status_code)


@router.get("")
async def health_check(
    health_service: HealthService = Depends(get_health_service),
) -> JSONResponse:
    """Combined health status using the HealthResponse model.

    Returns full dependency status with overall health classification:
    - "healthy": all dependencies connected
    - "degraded": some dependencies down
    - "unhealthy": all dependencies down

    Returns 200 for healthy/degraded, 503 for unhealthy.
    """
    health = await health_service.get_health_status()

    status_code = 503 if health.status == "unhealthy" else 200
    return JSONResponse(content=health.model_dump(), status_code=status_code)
