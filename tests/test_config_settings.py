"""Tests for configuration and settings management."""

import os
from unittest.mock import patch

from src.config.settings import (
    AnomalySettings,
    AppSettings,
    DatabaseSettings,
    GovernanceSettings,
    KafkaSettings,
    NotificationSettings,
    RedisSettings,
    SecuritySettings,
    Settings,
)


class TestAppSettings:
    def test_defaults(self):
        settings = AppSettings()
        assert settings.host == "0.0.0.0"
        assert settings.port == 8000
        assert settings.debug is False
        assert settings.log_level == "info"
        assert settings.title == "Project Sentinel"
        assert settings.version == "0.1.0"

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "APP_PORT": "9000",
                "APP_DEBUG": "true",
                "APP_LOG_LEVEL": "debug",
            },
        ):
            settings = AppSettings()
            assert settings.port == 9000
            assert settings.debug is True
            assert settings.log_level == "debug"


class TestKafkaSettings:
    def test_defaults(self):
        settings = KafkaSettings()
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.topic_telemetry_raw == "telemetry-raw"
        assert settings.topic_telemetry_enriched == "telemetry-enriched"
        assert settings.topic_anomaly_events == "anomaly-events"
        assert settings.consumer_group_anomaly == "sentinel-anomaly-engine"
        assert settings.consumer_group_governance == "sentinel-governance-engine"
        assert settings.consumer_group_storage == "sentinel-storage-writer"
        assert settings.acks == "1"
        assert settings.request_timeout_ms == 30000
        assert settings.buffer_max_events == 10000
        assert settings.producer_linger_ms == 5
        assert settings.producer_batch_size == 16384

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092,kafka2:9092",
                "KAFKA_ACKS": "all",
                "KAFKA_BUFFER_MAX_EVENTS": "5000",
                "KAFKA_REQUEST_TIMEOUT_MS": "60000",
                "KAFKA_CONSUMER_GROUP_ANOMALY": "custom-anomaly-group",
            },
        ):
            settings = KafkaSettings()
            assert settings.bootstrap_servers == "kafka1:9092,kafka2:9092"
            assert settings.acks == "all"
            assert settings.buffer_max_events == 5000
            assert settings.request_timeout_ms == 60000
            assert settings.consumer_group_anomaly == "custom-anomaly-group"


class TestRedisSettings:
    def test_defaults(self):
        settings = RedisSettings()
        assert settings.host == "localhost"
        assert settings.port == 6379
        assert settings.password is None
        assert settings.db == 0
        assert settings.connection_pool_size == 20
        assert settings.socket_timeout == 2.0
        assert settings.socket_connect_timeout == 2.0

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "REDIS_HOST": "redis-prod",
                "REDIS_PORT": "6380",
                "REDIS_PASSWORD": "secret123",
                "REDIS_DB": "2",
                "REDIS_CONNECTION_POOL_SIZE": "50",
            },
        ):
            settings = RedisSettings()
            assert settings.host == "redis-prod"
            assert settings.port == 6380
            assert settings.password == "secret123"
            assert settings.db == 2
            assert settings.connection_pool_size == 50


class TestDatabaseSettings:
    def test_defaults(self):
        settings = DatabaseSettings()
        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.name == "sentinel_db"
        assert settings.user == "sentinel"
        assert settings.password == "sentinel_dev"
        assert settings.min_pool_size == 5
        assert settings.max_pool_size == 20

    def test_dsn_property(self):
        settings = DatabaseSettings()
        assert (
            settings.dsn
            == "postgresql://sentinel:sentinel_dev@localhost:5432/sentinel_db"
        )

    def test_async_dsn_property(self):
        settings = DatabaseSettings()
        assert (
            settings.async_dsn
            == "postgresql+asyncpg://sentinel:sentinel_dev@localhost:5432/sentinel_db"
        )

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "DB_HOST": "timescale-prod",
                "DB_PORT": "5433",
                "DB_NAME": "sentinel_prod",
                "DB_USER": "admin",
                "DB_PASSWORD": "prodpass",
                "DB_MIN_POOL_SIZE": "10",
                "DB_MAX_POOL_SIZE": "50",
            },
        ):
            settings = DatabaseSettings()
            assert (
                settings.dsn
                == "postgresql://admin:prodpass@timescale-prod:5433/sentinel_prod"
            )
            assert settings.min_pool_size == 10
            assert settings.max_pool_size == 50


class TestAnomalySettings:
    def test_defaults(self):
        settings = AnomalySettings()
        assert settings.window_duration_seconds == 60
        assert settings.loop_threshold == 10
        assert settings.cascade_rate_threshold == 1000.0
        assert settings.spike_z_threshold == 3.0
        assert settings.min_events_for_spike == 2

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "ANOMALY_WINDOW_DURATION_SECONDS": "120",
                "ANOMALY_LOOP_THRESHOLD": "15",
                "ANOMALY_SPIKE_Z_THRESHOLD": "2.5",
                "ANOMALY_CASCADE_RATE_THRESHOLD": "500.0",
            },
        ):
            settings = AnomalySettings()
            assert settings.window_duration_seconds == 120
            assert settings.loop_threshold == 15
            assert settings.spike_z_threshold == 2.5
            assert settings.cascade_rate_threshold == 500.0


class TestGovernanceSettings:
    def test_defaults(self):
        settings = GovernanceSettings()
        assert settings.soft_limit_percent == 80
        assert settings.cooldown_seconds == 300
        assert settings.circuit_breaker_default_ttl is None

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "GOVERNANCE_SOFT_LIMIT_PERCENT": "70",
                "GOVERNANCE_COOLDOWN_SECONDS": "600",
                "GOVERNANCE_CIRCUIT_BREAKER_DEFAULT_TTL": "3600",
            },
        ):
            settings = GovernanceSettings()
            assert settings.soft_limit_percent == 70
            assert settings.cooldown_seconds == 600
            assert settings.circuit_breaker_default_ttl == 3600


class TestNotificationSettings:
    def test_defaults(self):
        settings = NotificationSettings()
        assert settings.max_retries == 3
        assert settings.base_retry_delay_seconds == 1.0
        assert settings.webhook_timeout_seconds == 10.0

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "NOTIFICATION_MAX_RETRIES": "5",
                "NOTIFICATION_BASE_RETRY_DELAY_SECONDS": "2.0",
                "NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS": "30.0",
            },
        ):
            settings = NotificationSettings()
            assert settings.max_retries == 5
            assert settings.base_retry_delay_seconds == 2.0
            assert settings.webhook_timeout_seconds == 30.0


class TestSecuritySettings:
    def test_defaults(self):
        settings = SecuritySettings()
        assert settings.api_key_header == "X-API-Key"
        assert settings.rate_limit_per_agent == 100
        assert settings.rate_limit_per_org == 1000

    def test_env_override(self):
        with patch.dict(
            os.environ,
            {
                "SECURITY_API_KEY_HEADER": "Authorization",
                "SECURITY_RATE_LIMIT_PER_AGENT": "200",
                "SECURITY_RATE_LIMIT_PER_ORG": "5000",
            },
        ):
            settings = SecuritySettings()
            assert settings.api_key_header == "Authorization"
            assert settings.rate_limit_per_agent == 200
            assert settings.rate_limit_per_org == 5000


class TestRootSettings:
    def test_creates_all_subsettings(self):
        settings = Settings()
        assert isinstance(settings.app, AppSettings)
        assert isinstance(settings.kafka, KafkaSettings)
        assert isinstance(settings.redis, RedisSettings)
        assert isinstance(settings.database, DatabaseSettings)
        assert isinstance(settings.anomaly, AnomalySettings)
        assert isinstance(settings.governance, GovernanceSettings)
        assert isinstance(settings.notification, NotificationSettings)
        assert isinstance(settings.security, SecuritySettings)

    def test_subsettings_have_correct_defaults(self):
        settings = Settings()
        assert settings.app.port == 8000
        assert settings.kafka.bootstrap_servers == "localhost:9092"
        assert settings.redis.port == 6379
        assert settings.database.name == "sentinel_db"
        assert settings.anomaly.loop_threshold == 10
        assert settings.governance.soft_limit_percent == 80
        assert settings.notification.max_retries == 3
        assert settings.security.api_key_header == "X-API-Key"
