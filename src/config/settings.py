"""
Environment variables and settings management for Project Sentinel.

Uses pydantic-settings to load configuration from environment variables
and .env files with sensible defaults for local development.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka connection and topic configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    topic_telemetry_raw: str = Field(
        default="telemetry-raw",
        description="Topic for raw telemetry events from ingestion gateway",
    )
    topic_telemetry_enriched: str = Field(
        default="telemetry-enriched",
        description="Topic for enriched telemetry events after processing",
    )
    topic_anomaly_events: str = Field(
        default="anomaly-events",
        description="Topic for detected anomaly events",
    )
    acks: str = Field(
        default="1",
        description="Producer acknowledgment setting (0, 1, or 'all')",
    )
    buffer_max_events: int = Field(
        default=10000,
        description="Maximum in-memory buffer size during transient Kafka failures",
    )
    producer_linger_ms: int = Field(
        default=5,
        description="Time to wait before sending a batch of messages",
    )
    producer_batch_size: int = Field(
        default=16384,
        description="Maximum batch size in bytes for producer batching",
    )


class RedisSettings(BaseSettings):
    """Redis connection configuration for circuit breaker state and rate limiting."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")

    host: str = Field(
        default="localhost",
        description="Redis server hostname",
    )
    port: int = Field(
        default=6379,
        description="Redis server port",
    )
    db: int = Field(
        default=0,
        description="Redis database number",
    )
    password: str | None = Field(
        default=None,
        description="Redis authentication password",
    )
    connection_pool_size: int = Field(
        default=20,
        description="Maximum number of connections in the pool",
    )
    socket_timeout: float = Field(
        default=2.0,
        description="Socket timeout in seconds for Redis operations",
    )
    socket_connect_timeout: float = Field(
        default=2.0,
        description="Socket connect timeout in seconds",
    )


class DatabaseSettings(BaseSettings):
    """TimescaleDB connection configuration for time-series storage."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    host: str = Field(
        default="localhost",
        description="TimescaleDB server hostname",
    )
    port: int = Field(
        default=5432,
        description="TimescaleDB server port",
    )
    name: str = Field(
        default="sentinel",
        description="Database name",
    )
    user: str = Field(
        default="sentinel",
        description="Database user",
    )
    password: str = Field(
        default="sentinel",
        description="Database password",
    )
    ingestion_pool_size: int = Field(
        default=10,
        description="Connection pool size for the ingestion write path",
    )
    ingestion_pool_max_overflow: int = Field(
        default=5,
        description="Max overflow connections for ingestion pool",
    )
    query_pool_size: int = Field(
        default=5,
        description="Connection pool size for the query/read path",
    )
    query_pool_max_overflow: int = Field(
        default=3,
        description="Max overflow connections for query pool",
    )

    @property
    def dsn(self) -> str:
        """Build the PostgreSQL DSN connection string."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def async_dsn(self) -> str:
        """Build the async PostgreSQL DSN connection string."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class AppSettings(BaseSettings):
    """FastAPI application server configuration."""

    model_config = SettingsConfigDict(env_prefix="APP_")

    host: str = Field(
        default="0.0.0.0",
        description="Server bind host",
    )
    port: int = Field(
        default=8000,
        description="Server bind port",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode (auto-reload, verbose logging)",
    )
    log_level: str = Field(
        default="info",
        description="Logging level (debug, info, warning, error, critical)",
    )
    title: str = Field(
        default="Project Sentinel",
        description="API title for OpenAPI documentation",
    )
    version: str = Field(
        default="0.1.0",
        description="API version",
    )


class AnomalySettings(BaseSettings):
    """Anomaly detection thresholds and configuration."""

    model_config = SettingsConfigDict(env_prefix="ANOMALY_")

    window_duration_seconds: int = Field(
        default=60,
        description="Sliding window duration in seconds for anomaly detection",
    )
    loop_threshold: int = Field(
        default=10,
        description="Number of consecutive identical tool calls to trigger INFINITE_LOOP anomaly",
    )
    loop_warning_ratio: float = Field(
        default=0.5,
        description="Ratio of loop_threshold at which a soft warning is issued",
    )
    cascade_rate_threshold: float = Field(
        default=1000.0,
        description="Token growth rate (tokens/second) threshold for PROMPT_CASCADE detection",
    )
    spike_z_threshold: float = Field(
        default=3.0,
        description="Z-score threshold for TOKEN_SPIKE detection",
    )
    min_events_for_spike: int = Field(
        default=2,
        description="Minimum prior events required for meaningful Z-score computation",
    )


class GovernanceSettings(BaseSettings):
    """Governance policy defaults and circuit breaker configuration."""

    model_config = SettingsConfigDict(env_prefix="GOVERNANCE_")

    default_soft_limit_ratio: float = Field(
        default=0.8,
        description="Default soft limit as ratio of hard limit (80%)",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum notification delivery retry attempts",
    )
    cooldown_seconds: int = Field(
        default=300,
        description="Default cooldown period in seconds before re-triggering a threshold",
    )
    circuit_breaker_default_ttl: int | None = Field(
        default=None,
        description="Default TTL in seconds for circuit breakers (None = no auto-deactivation)",
    )
    retry_backoff_base: float = Field(
        default=1.0,
        description="Base delay in seconds for exponential backoff on notification retries",
    )


class Settings(BaseSettings):
    """
    Root settings aggregating all subsystem configurations.

    Loads from environment variables and .env files.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    app: AppSettings = Field(default_factory=AppSettings)
    anomaly: AnomalySettings = Field(default_factory=AnomalySettings)
    governance: GovernanceSettings = Field(default_factory=GovernanceSettings)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings instance.

    Uses lru_cache to ensure settings are loaded once and reused
    across the application lifecycle.
    """
    return Settings()
