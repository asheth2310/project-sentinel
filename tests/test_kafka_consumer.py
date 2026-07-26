"""Unit tests for KafkaConsumerService base class."""

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.config.settings import KafkaSettings
from src.gateway.kafka_consumer import KafkaConsumerService


class ConcreteConsumer(KafkaConsumerService):
    """Concrete implementation for testing the abstract base class."""

    def __init__(self, settings: KafkaSettings, topic: str, group_id: str):
        super().__init__(settings, topic, group_id)
        self.processed_messages: list[dict[str, Any]] = []

    async def process_message(self, message: dict[str, Any]) -> None:
        self.processed_messages.append(message)


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings(bootstrap_servers="localhost:9092")


@pytest.fixture
def consumer(kafka_settings: KafkaSettings) -> ConcreteConsumer:
    return ConcreteConsumer(
        settings=kafka_settings,
        topic="telemetry-enriched",
        group_id="test-group",
    )


class TestKafkaConsumerServiceInit:
    """Test initialization and configuration."""

    def test_init_stores_settings(self, consumer: ConcreteConsumer) -> None:
        assert consumer._settings.bootstrap_servers == "localhost:9092"
        assert consumer._topic == "telemetry-enriched"
        assert consumer._group_id == "test-group"

    def test_init_not_running(self, consumer: ConcreteConsumer) -> None:
        assert consumer.is_running is False

    def test_build_consumer_config(self, consumer: ConcreteConsumer) -> None:
        config = consumer._build_consumer_config()
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "test-group"
        assert config["auto.offset.reset"] == "earliest"
        assert config["enable.auto.commit"] is False


class TestDeserialization:
    """Test message deserialization behavior."""

    def test_deserialize_valid_json(self, consumer: ConcreteConsumer) -> None:
        payload = {"agent_id": "abc-123", "tokens": 100}
        raw = json.dumps(payload).encode("utf-8")
        result = consumer._deserialize_message(raw)
        assert result == payload

    def test_deserialize_invalid_json_returns_none(
        self, consumer: ConcreteConsumer
    ) -> None:
        raw = b"not valid json {{"
        result = consumer._deserialize_message(raw)
        assert result is None

    def test_deserialize_invalid_utf8_returns_none(
        self, consumer: ConcreteConsumer
    ) -> None:
        raw = b"\xff\xfe invalid bytes"
        result = consumer._deserialize_message(raw)
        assert result is None

    def test_deserialize_empty_object(self, consumer: ConcreteConsumer) -> None:
        raw = b"{}"
        result = consumer._deserialize_message(raw)
        assert result == {}


class TestLifecycle:
    """Test start/stop lifecycle methods."""

    @pytest.mark.asyncio
    async def test_start_sets_running(self, consumer: ConcreteConsumer) -> None:
        with patch("src.gateway.kafka_consumer.Consumer"):
            await consumer.start()
            assert consumer.is_running is True
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_running(self, consumer: ConcreteConsumer) -> None:
        with patch("src.gateway.kafka_consumer.Consumer"):
            await consumer.start()
            await consumer.stop()
            assert consumer.is_running is False

    @pytest.mark.asyncio
    async def test_start_when_already_running_is_noop(
        self, consumer: ConcreteConsumer
    ) -> None:
        with patch("src.gateway.kafka_consumer.Consumer"):
            await consumer.start()
            # Second start should not create a new thread
            thread_before = consumer._thread
            await consumer.start()
            assert consumer._thread is thread_before
            await consumer.stop()

    @pytest.mark.asyncio
    async def test_stop_when_not_running_is_noop(
        self, consumer: ConcreteConsumer
    ) -> None:
        # Should not raise
        await consumer.stop()
        assert consumer.is_running is False


class TestRebalanceCallbacks:
    """Test partition rebalance callbacks."""

    def test_on_assign_logs_partitions(
        self, consumer: ConcreteConsumer
    ) -> None:
        from confluent_kafka import TopicPartition

        mock_consumer = MagicMock()
        partitions = [
            TopicPartition("telemetry-enriched", 0),
            TopicPartition("telemetry-enriched", 1),
        ]
        # Should not raise
        consumer._on_assign(mock_consumer, partitions)

    def test_on_revoke_commits_offsets(
        self, consumer: ConcreteConsumer
    ) -> None:
        from confluent_kafka import TopicPartition

        mock_consumer = MagicMock()
        partitions = [TopicPartition("telemetry-enriched", 0)]
        consumer._on_revoke(mock_consumer, partitions)
        mock_consumer.commit.assert_called_once_with(asynchronous=False)

    def test_on_revoke_handles_commit_failure(
        self, consumer: ConcreteConsumer
    ) -> None:
        from confluent_kafka import KafkaException, TopicPartition

        mock_consumer = MagicMock()
        mock_consumer.commit.side_effect = KafkaException(
            MagicMock(code=MagicMock(return_value=-1))
        )
        partitions = [TopicPartition("telemetry-enriched", 0)]
        # Should not raise, just log warning
        consumer._on_revoke(mock_consumer, partitions)


class TestAbstractMethod:
    """Test that the abstract class cannot be instantiated directly."""

    def test_cannot_instantiate_abstract_class(
        self, kafka_settings: KafkaSettings
    ) -> None:
        with pytest.raises(TypeError):
            KafkaConsumerService(  # type: ignore[abstract]
                settings=kafka_settings,
                topic="test",
                group_id="test-group",
            )
