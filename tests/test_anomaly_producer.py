"""Tests for AnomalyEventProducer."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.anomaly.producer import AnomalyEventProducer
from src.models.anomaly import AnomalyEvent, AnomalyType, Severity


@pytest.fixture
def mock_kafka_producer():
    """Create a mock KafkaProducerService."""
    producer = MagicMock()
    producer.produce = AsyncMock()
    producer.produce_batch = AsyncMock()
    return producer


@pytest.fixture
def anomaly_producer(mock_kafka_producer):
    """Create an AnomalyEventProducer with mock dependencies."""
    return AnomalyEventProducer(mock_kafka_producer, topic="anomaly-events")


@pytest.fixture
def sample_anomaly():
    """Create a sample AnomalyEvent for testing."""
    return AnomalyEvent(
        agent_id=uuid4(),
        org_id=uuid4(),
        anomaly_type=AnomalyType.TOKEN_SPIKE,
        severity=Severity.HIGH,
        detected_at=datetime.now(timezone.utc),
        window_start=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        window_end=datetime(2024, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
        metric_value=4.5,
        threshold_value=3.0,
        description="Token spike detected: Z-score 4.5 exceeds threshold 3.0",
        metadata={"z_score": 4.5, "event_tokens": 5000},
    )


class TestAnomalyEventProducerInit:
    def test_default_topic(self, mock_kafka_producer):
        """Default topic is 'anomaly-events'."""
        producer = AnomalyEventProducer(mock_kafka_producer)
        assert producer.topic == "anomaly-events"

    def test_custom_topic(self, mock_kafka_producer):
        """Custom topic can be provided."""
        producer = AnomalyEventProducer(mock_kafka_producer, topic="custom-topic")
        assert producer.topic == "custom-topic"


class TestProduceAnomaly:
    @pytest.mark.asyncio
    async def test_produce_anomaly_calls_kafka_produce(
        self, anomaly_producer, mock_kafka_producer, sample_anomaly
    ):
        """produce_anomaly sends the event to Kafka with correct topic and key."""
        await anomaly_producer.produce_anomaly(sample_anomaly)

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "anomaly-events"
        assert call_kwargs["key"] == str(sample_anomaly.agent_id)

    @pytest.mark.asyncio
    async def test_produce_anomaly_serializes_to_json(
        self, anomaly_producer, mock_kafka_producer, sample_anomaly
    ):
        """produce_anomaly serializes the event as JSON bytes."""
        await anomaly_producer.produce_anomaly(sample_anomaly)

        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        value = call_kwargs["value"]
        assert isinstance(value, bytes)

        # Verify it's valid JSON
        data = json.loads(value)
        assert data["anomaly_type"] == "token_spike"
        assert data["severity"] == "high"
        assert data["agent_id"] == str(sample_anomaly.agent_id)
        assert data["metric_value"] == 4.5
        assert data["threshold_value"] == 3.0

    @pytest.mark.asyncio
    async def test_produce_anomaly_uses_agent_id_as_key(
        self, anomaly_producer, mock_kafka_producer, sample_anomaly
    ):
        """Message key is the agent_id (ensures per-agent partition ordering)."""
        await anomaly_producer.produce_anomaly(sample_anomaly)

        call_kwargs = mock_kafka_producer.produce.call_args.kwargs
        assert call_kwargs["key"] == str(sample_anomaly.agent_id)


class TestProduceAnomalies:
    @pytest.mark.asyncio
    async def test_produce_anomalies_empty_list(
        self, anomaly_producer, mock_kafka_producer
    ):
        """produce_anomalies with empty list is a no-op."""
        await anomaly_producer.produce_anomalies([])
        mock_kafka_producer.produce_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_produce_anomalies_batch(
        self, anomaly_producer, mock_kafka_producer, sample_anomaly
    ):
        """produce_anomalies uses batch production for multiple events."""
        anomaly2 = AnomalyEvent(
            agent_id=sample_anomaly.agent_id,
            org_id=sample_anomaly.org_id,
            anomaly_type=AnomalyType.INFINITE_LOOP,
            severity=Severity.CRITICAL,
            detected_at=datetime.now(timezone.utc),
            window_start=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            window_end=datetime(2024, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
            metric_value=15.0,
            threshold_value=10.0,
            description="Infinite loop detected",
        )

        await anomaly_producer.produce_anomalies([sample_anomaly, anomaly2])

        mock_kafka_producer.produce_batch.assert_called_once()
        batch_events = mock_kafka_producer.produce_batch.call_args[0][0]
        assert len(batch_events) == 2

        # Each event is a (topic, key, value) tuple
        for topic, key, value in batch_events:
            assert topic == "anomaly-events"
            assert key == str(sample_anomaly.agent_id)
            assert isinstance(value, bytes)
            json.loads(value)  # Should be valid JSON


class TestSerialization:
    def test_serialize_anomaly_includes_all_fields(
        self, anomaly_producer, sample_anomaly
    ):
        """Serialization includes all AnomalyEvent fields."""
        value = anomaly_producer._serialize_anomaly(sample_anomaly)
        data = json.loads(value)

        assert "anomaly_id" in data
        assert "agent_id" in data
        assert "org_id" in data
        assert "anomaly_type" in data
        assert "severity" in data
        assert "detected_at" in data
        assert "window_start" in data
        assert "window_end" in data
        assert "metric_value" in data
        assert "threshold_value" in data
        assert "description" in data
        assert "metadata" in data

    def test_serialize_anomaly_metadata_preserved(
        self, anomaly_producer, sample_anomaly
    ):
        """Metadata dict is preserved in serialization."""
        value = anomaly_producer._serialize_anomaly(sample_anomaly)
        data = json.loads(value)
        assert data["metadata"] == {"z_score": 4.5, "event_tokens": 5000}
