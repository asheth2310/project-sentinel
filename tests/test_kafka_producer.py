"""Tests for KafkaProducerService."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config.settings import KafkaSettings
from src.gateway.kafka_producer import (
    KafkaProducerError,
    KafkaProducerService,
    KafkaUnavailableError,
)


@pytest.fixture
def kafka_settings():
    return KafkaSettings()


@pytest.fixture
def producer_service(kafka_settings):
    return KafkaProducerService(kafka_settings)


class TestKafkaProducerServiceInit:
    def test_initial_state(self, producer_service):
        """Producer starts in stopped state."""
        assert producer_service.is_running is False

    def test_takes_kafka_settings(self, kafka_settings):
        """Service accepts KafkaSettings configuration."""
        service = KafkaProducerService(kafka_settings)
        assert service._settings is kafka_settings


class TestKafkaProducerServiceStart:
    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_start_creates_producer(self, mock_producer_class, kafka_settings):
        """start() creates a confluent-kafka Producer with correct config."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer.poll.return_value = 0
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()

        mock_producer_class.assert_called_once_with({
            "bootstrap.servers": "localhost:9092",
            "acks": "1",
            "linger.ms": 5,
            "batch.size": 16384,
            "error_cb": service._on_error,
        })
        assert service.is_running is True
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_start_is_idempotent(self, mock_producer_class, kafka_settings):
        """Calling start() twice does not recreate the producer."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer.poll.return_value = 0
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        await service.start()

        mock_producer_class.assert_called_once()
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_start_raises_unavailable_on_kafka_exception(
        self, mock_producer_class, kafka_settings
    ):
        """start() raises KafkaUnavailableError if Producer fails to create."""
        from confluent_kafka import KafkaException

        mock_producer_class.side_effect = KafkaException(
            MagicMock(str=lambda self: "Connection refused")
        )
        service = KafkaProducerService(kafka_settings)

        with pytest.raises(KafkaUnavailableError):
            await service.start()

        assert service.is_running is False


class TestKafkaProducerServiceStop:
    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_stop_flushes_and_cleans_up(self, mock_producer_class, kafka_settings):
        """stop() flushes the producer and sets running to False."""
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        mock_producer.poll.return_value = 0
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        await service.stop()

        mock_producer.flush.assert_called_once_with(timeout=10.0)
        assert service.is_running is False

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, producer_service):
        """stop() is a no-op when producer is not running."""
        await producer_service.stop()  # Should not raise
        assert producer_service.is_running is False


class TestKafkaProducerServiceProduce:
    @pytest.mark.asyncio
    async def test_produce_raises_when_not_running(self, producer_service):
        """produce() raises KafkaUnavailableError if producer not started."""
        with pytest.raises(KafkaUnavailableError, match="not running"):
            await producer_service.produce("telemetry-raw", "agent-123", b"payload")

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_calls_confluent_produce(
        self, mock_producer_class, kafka_settings
    ):
        """produce() calls the underlying confluent-kafka produce method."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0

        # Simulate successful delivery via callback
        def mock_produce(topic, key, value, callback):
            callback(None, MagicMock())

        mock_producer.produce.side_effect = mock_produce
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        await service.produce("telemetry-raw", "agent-id-123", b'{"data": "test"}')

        mock_producer.produce.assert_called_once_with(
            topic="telemetry-raw",
            key=b"agent-id-123",
            value=b'{"data": "test"}',
            callback=mock_producer.produce.call_args.kwargs["callback"],
        )
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_raises_on_buffer_error(
        self, mock_producer_class, kafka_settings
    ):
        """produce() raises KafkaUnavailableError on BufferError."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0
        mock_producer.produce.side_effect = BufferError("Local queue full")
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        with pytest.raises(KafkaUnavailableError, match="buffer full"):
            await service.produce("telemetry-raw", "agent-123", b"payload")
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_raises_on_delivery_failure(
        self, mock_producer_class, kafka_settings
    ):
        """produce() raises KafkaProducerError when delivery callback reports error."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0

        # Simulate delivery failure via callback
        def mock_produce(topic, key, value, callback):
            callback(MagicMock(str=lambda self: "Broker not available"), None)

        mock_producer.produce.side_effect = mock_produce
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        with pytest.raises(KafkaProducerError, match="delivery failed"):
            await service.produce("telemetry-raw", "agent-123", b"payload")
        await service.stop()


class TestKafkaProducerServiceProduceBatch:
    @pytest.mark.asyncio
    async def test_produce_batch_raises_when_not_running(self, producer_service):
        """produce_batch() raises KafkaUnavailableError if producer not started."""
        with pytest.raises(KafkaUnavailableError, match="not running"):
            await producer_service.produce_batch([
                ("telemetry-raw", "agent-1", b"payload")
            ])

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_batch_empty_list(self, mock_producer_class, kafka_settings):
        """produce_batch() with empty list returns immediately."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        await service.produce_batch([])  # Should not raise
        mock_producer.produce.assert_not_called()
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_batch_all_succeed(self, mock_producer_class, kafka_settings):
        """produce_batch() succeeds when all deliveries are acknowledged."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0

        # Track callbacks and trigger them all as success
        callbacks = []

        def mock_produce(topic, key, value, callback):
            callbacks.append(callback)

        mock_producer.produce.side_effect = mock_produce
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()

        events = [
            ("telemetry-raw", "agent-1", b"event1"),
            ("telemetry-raw", "agent-1", b"event2"),
            ("telemetry-raw", "agent-1", b"event3"),
        ]

        # Start the batch produce in a task
        task = asyncio.create_task(service.produce_batch(events))
        await asyncio.sleep(0.05)

        # Trigger all delivery callbacks as success
        for cb in callbacks:
            cb(None, MagicMock())

        await task  # Should complete without error
        assert mock_producer.produce.call_count == 3
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_batch_partial_failure_raises(
        self, mock_producer_class, kafka_settings
    ):
        """produce_batch() raises KafkaProducerError if any delivery fails."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0

        callbacks = []

        def mock_produce(topic, key, value, callback):
            callbacks.append(callback)

        mock_producer.produce.side_effect = mock_produce
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()

        events = [
            ("telemetry-raw", "agent-1", b"event1"),
            ("telemetry-raw", "agent-1", b"event2"),
        ]

        task = asyncio.create_task(service.produce_batch(events))
        await asyncio.sleep(0.05)

        # First succeeds, second fails
        callbacks[0](None, MagicMock())
        callbacks[1](MagicMock(str=lambda self: "Broker down"), None)

        with pytest.raises(KafkaProducerError, match="Batch production failed"):
            await task
        await service.stop()

    @pytest.mark.asyncio
    @patch("src.gateway.kafka_producer.Producer")
    async def test_produce_batch_buffer_error_raises_unavailable(
        self, mock_producer_class, kafka_settings
    ):
        """produce_batch() raises KafkaUnavailableError on BufferError."""
        mock_producer = MagicMock()
        mock_producer.poll.return_value = 0
        mock_producer.flush.return_value = 0
        mock_producer.produce.side_effect = BufferError("Queue full")
        mock_producer_class.return_value = mock_producer
        service = KafkaProducerService(kafka_settings)

        await service.start()
        with pytest.raises(KafkaUnavailableError, match="buffer full"):
            await service.produce_batch([
                ("telemetry-raw", "agent-1", b"event1"),
            ])
        await service.stop()


class TestCustomExceptions:
    def test_kafka_producer_error_stores_original(self):
        """KafkaProducerError stores the original exception."""
        original = ValueError("original error")
        error = KafkaProducerError("wrapper message", original_error=original)
        assert str(error) == "wrapper message"
        assert error.original_error is original

    def test_kafka_producer_error_without_original(self):
        """KafkaProducerError works without original_error."""
        error = KafkaProducerError("simple message")
        assert str(error) == "simple message"
        assert error.original_error is None

    def test_kafka_unavailable_error_default_message(self):
        """KafkaUnavailableError has sensible default message."""
        error = KafkaUnavailableError()
        assert "unavailable" in str(error).lower()

    def test_kafka_unavailable_error_custom_message(self):
        """KafkaUnavailableError accepts custom message."""
        error = KafkaUnavailableError("Custom broker error")
        assert str(error) == "Custom broker error"
