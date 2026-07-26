"""Tests for AnomalyEngineConsumer."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from src.anomaly.detectors import AnomalyDetector
from src.anomaly.engine import AnomalyEngineConsumer
from src.anomaly.producer import AnomalyEventProducer
from src.anomaly.window_manager import WindowManager
from src.config.settings import KafkaSettings
from src.models.anomaly import AnomalyEvent, AnomalyType, Severity


@pytest.fixture
def kafka_settings():
    return KafkaSettings()


@pytest.fixture
def window_manager():
    return WindowManager(window_duration_seconds=60)


@pytest.fixture
def detector():
    return AnomalyDetector(
        loop_threshold=10,
        cascade_rate_threshold=1000.0,
        spike_z_threshold=3.0,
    )


@pytest.fixture
def mock_producer():
    producer = MagicMock(spec=AnomalyEventProducer)
    producer.produce_anomalies = AsyncMock()
    producer.produce_anomaly = AsyncMock()
    return producer


@pytest.fixture
def engine(kafka_settings, window_manager, detector, mock_producer):
    return AnomalyEngineConsumer(
        settings=kafka_settings,
        window_manager=window_manager,
        detector=detector,
        producer=mock_producer,
    )


@pytest.fixture
def sample_telemetry_message():
    """Create a sample telemetry message dict (as it would arrive from Kafka)."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent_id": str(uuid4()),
        "org_id": str(uuid4()),
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "total_cost": "0.001",
        "latency_ms": 200,
        "tool_name": "search_web",
    }


class TestAnomalyEngineConsumerInit:
    def test_subscribes_to_telemetry_enriched_topic(self, engine, kafka_settings):
        """Engine subscribes to the telemetry-enriched topic."""
        assert engine._topic == kafka_settings.topic_telemetry_enriched

    def test_uses_anomaly_consumer_group(self, engine, kafka_settings):
        """Engine uses the anomaly consumer group."""
        assert engine._group_id == kafka_settings.consumer_group_anomaly


class TestProcessMessage:
    @pytest.mark.asyncio
    async def test_process_valid_message_updates_window(
        self, engine, window_manager, sample_telemetry_message
    ):
        """Processing a valid message updates the agent's sliding window."""
        agent_id_str = sample_telemetry_message["agent_id"]

        await engine.process_message(sample_telemetry_message)

        from uuid import UUID

        agent_id = UUID(agent_id_str)
        window = window_manager.get_or_create_window(agent_id)
        assert window.event_count >= 1

    @pytest.mark.asyncio
    async def test_process_invalid_message_skips(self, engine, mock_producer):
        """Processing an invalid message doesn't produce anomalies or crash."""
        invalid_message = {"not_a_valid": "telemetry_event"}

        await engine.process_message(invalid_message)

        mock_producer.produce_anomalies.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_message_no_anomaly_no_produce(
        self, engine, mock_producer, sample_telemetry_message
    ):
        """When no anomalies are detected, nothing is produced."""
        await engine.process_message(sample_telemetry_message)

        mock_producer.produce_anomalies.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_message_detects_anomaly_and_produces(
        self, kafka_settings, window_manager, mock_producer
    ):
        """When anomalies are detected, they are produced to Kafka."""
        # Use a very low spike threshold so a spike is easily triggered
        detector = AnomalyDetector(
            loop_threshold=10,
            cascade_rate_threshold=1000.0,
            spike_z_threshold=0.5,
            min_events_for_spike=2,
        )
        engine = AnomalyEngineConsumer(
            settings=kafka_settings,
            window_manager=window_manager,
            detector=detector,
            producer=mock_producer,
        )

        agent_id = str(uuid4())
        org_id = str(uuid4())
        base_time = datetime.now(timezone.utc)

        # First event: establish baseline
        msg1 = {
            "timestamp": base_time.isoformat(),
            "agent_id": agent_id,
            "org_id": org_id,
            "prompt_tokens": 10,
            "completion_tokens": 10,
            "total_cost": "0.001",
            "latency_ms": 100,
        }
        await engine.process_message(msg1)

        # Second event: normal
        msg2 = {
            "timestamp": base_time.isoformat(),
            "agent_id": agent_id,
            "org_id": org_id,
            "prompt_tokens": 10,
            "completion_tokens": 10,
            "total_cost": "0.001",
            "latency_ms": 100,
        }
        await engine.process_message(msg2)

        # Third event: massive spike
        msg3 = {
            "timestamp": base_time.isoformat(),
            "agent_id": agent_id,
            "org_id": org_id,
            "prompt_tokens": 50000,
            "completion_tokens": 50000,
            "total_cost": "10.0",
            "latency_ms": 5000,
        }
        await engine.process_message(msg3)

        # Anomaly should have been produced
        mock_producer.produce_anomalies.assert_called()

    @pytest.mark.asyncio
    async def test_process_message_multiple_events_same_agent(
        self, engine, sample_telemetry_message
    ):
        """Multiple events from same agent accumulate in the window."""
        # Process same agent message multiple times
        for _ in range(5):
            await engine.process_message(sample_telemetry_message)

        from uuid import UUID

        agent_id = UUID(sample_telemetry_message["agent_id"])
        window = engine._window_manager.get_or_create_window(agent_id)
        assert window.event_count == 5
