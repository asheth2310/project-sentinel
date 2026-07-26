"""
Environment variables and settings management for Project Sentinel.

Uses pydantic-settings to load configuration from environment variables
and .env files with sensible defaults for local development.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    consumer_group_anomaly: str = Field(
        default="sentinel-anomaly-engine",
        description="Consumer group for the anomaly detection engine",
    )
    consumer_group_governance: str = Field(
        default="sentinel-governance-engine",
        description="Consumer group for the governance engine",
    )
    consumer_group_storage: str = Field(
        default="sentinel-storage-writer",
        description="Consumer group for the TimescaleDB storage writer",
    )
    acks: str = Field(
        default="1",
        description="Producer acknowledgment setting (0, 1, or 'all')",
    )
    request_timeout_ms: int = Field(
        default=30000,
        description="Kafka request timeout in milliseconds",
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
    password: str | None = Field(
        default=None,
        description="Redis authentication password (optional)",
    )
    db: int = Field(
        default=0,
        description="Redis database number",
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
        default="sentinel_db",
        description="Database name",
    )
    user: str = Field(
        default="sentinel",
        description="Database user",
    )
    password: str = Field(
        default="sentinel_dev",
        description="Database password",
    )
    min_pool_size: int = Field(
        default=5,
        description="Minimum number of connections in the pool",
    )
    max_pool_size: int = Field(
        default=20,
        description="Maximum number of connections in the pool",
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

    soft_limit_percent: int = Field(
        default=80,
        description="Default soft limit as percentage of hard limit (e.g. 80 means 80%)",
    )
    cooldown_seconds: int = Field(
        default=300,
        description="Default cooldown period in seconds before re-triggering a threshold",
    )
    circuit_breaker_default_ttl: int | None = Field(
        default=None,
        description="Default TTL in seconds for circuit breakers (None = no auto-deactivation)",
    )


class NotificationSettings(BaseSettings):
    """Notification delivery configuration."""

    model_config = SettingsConfigDict(env_prefix="NOTIFICATION_")

    max_retries: int = Field(
        default=3,
        description="Maximum number of retry attempts for failed webhook deliveries",
    )
    base_retry_delay_seconds: float = Field(
        default=1.0,
        description="Base delay in seconds for exponential backoff (1s, 2s, 4s...)",
    )
    webhook_timeout_seconds: float = Field(
        default=10.0,
        description="Timeout in seconds for outbound webhook HTTP requests",
    )


class SecuritySettings(BaseSettings):
    """Security, authentication, and rate limiting configuration."""

    model_config = SettingsConfigDict(env_prefix="SECURITY_")

    api_key_header: str = Field(
        default="X-API-Key",
        description="HTTP header name used for API key authentication",
    )
    rate_limit_per_agent: int = Field(
        default=100,
        description="Maximum requests per minute per agent",
    )
    rate_limit_per_org: int = Field(
        default=1000,
        description="Maximum requests per minute per organization",
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

    app: AppSettings = Field(default_factory=AppSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    anomaly: AnomalySettings = Field(default_factory=AnomalySettings)
    governance: GovernanceSettings = Field(default_factory=GovernanceSettings)
    notification: NotificationSettings = Field(default_factory=NotificationSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings instance.

    Uses lru_cache to ensure settings are loaded once and reused
    across the application lifecycle.
    """
    return Settings()
