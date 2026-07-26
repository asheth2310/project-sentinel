"""
KafkaConsumerService - Abstract base class for downstream Kafka consumers.

Provides consumer group support, graceful rebalance handling,
offset management, and deserialization error handling.
Uses confluent-kafka Consumer wrapped in asyncio via a dedicated thread.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from threading import Event, Thread
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.config.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaConsumerService(ABC):
    """
    Abstract base class for Kafka consumers with consumer group support.

    Subclasses implement `process_message` to handle deserialized messages.
    The consumer loop runs in a dedicated thread, integrated with asyncio
    for cooperative lifecycle management.

    Args:
        settings: Kafka connection configuration.
        topic: The Kafka topic to subscribe to.
        group_id: Consumer group ID for independent processing.
    """

    def __init__(self, settings: KafkaSettings, topic: str, group_id: str) -> None:
        self._settings = settings
        self._topic = topic
        self._group_id = group_id
        self._consumer: Consumer | None = None
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._running = False

    @abstractmethod
    async def process_message(self, message: dict[str, Any]) -> None:
        """
        Process a single deserialized Kafka message.

        Subclasses must implement this method to handle the business logic
        for each consumed message.

        Args:
            message: The deserialized JSON message payload.

        Raises:
            Exception: If processing fails, the offset will not be committed
                       and the message may be reprocessed.
        """
        ...

    def _build_consumer_config(self) -> dict[str, Any]:
        """Build confluent-kafka Consumer configuration dict."""
        return {
            "bootstrap.servers": self._settings.bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,
        }

    def _on_assign(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        """Callback invoked when partitions are assigned after a rebalance."""
        partition_list = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(
            "Consumer group '%s' assigned partitions: %s",
            self._group_id,
            partition_list,
        )

    def _on_revoke(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        """Callback invoked when partitions are revoked during a rebalance."""
        partition_list = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(
            "Consumer group '%s' partitions revoked: %s",
            self._group_id,
            partition_list,
        )
        # Commit offsets for revoked partitions to avoid reprocessing
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.warning(
                "Failed to commit offsets during rebalance revoke: %s", e
            )

    def _deserialize_message(self, raw_value: bytes) -> dict[str, Any] | None:
        """
        Deserialize a raw message value from JSON bytes.

        Returns None and logs a warning if deserialization fails,
        allowing the consumer to skip malformed messages.
        """
        try:
            return json.loads(raw_value)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(
                "Deserialization failed for message on topic '%s' "
                "(group '%s'): %s. Skipping message.",
                self._topic,
                self._group_id,
                e,
            )
            return None

    def _consume_loop(self) -> None:
        """
        Main consumer loop that polls Kafka, deserializes messages,
        and delegates to process_message. Commits offsets after
        successful processing.

        Runs in a dedicated thread to avoid blocking the asyncio event loop.
        """
        config = self._build_consumer_config()
        self._consumer = Consumer(config)
        self._consumer.subscribe(
            [self._topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        logger.info(
            "KafkaConsumerService started: topic='%s', group_id='%s'",
            self._topic,
            self._group_id,
        )

        try:
            while not self._stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                error = msg.error()
                if error:
                    if error.code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Reached end of partition %s[%d] at offset %d",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                        continue
                    else:
                        logger.error(
                            "Consumer error on topic '%s' (group '%s'): %s",
                            self._topic,
                            self._group_id,
                            error,
                        )
                        continue

                # Deserialize the message value
                raw_value = msg.value()
                if raw_value is None:
                    continue

                payload = self._deserialize_message(raw_value)
                if payload is None:
                    # Deserialization failed - commit offset to skip
                    self._consumer.commit(message=msg, asynchronous=False)
                    continue

                # Process the message via the subclass implementation
                try:
                    # Run the async process_message in a new event loop context
                    # since we're in a thread
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(self.process_message(payload))
                    finally:
                        loop.close()

                    # Commit offset after successful processing
                    self._consumer.commit(message=msg, asynchronous=False)
                except Exception:
                    logger.exception(
                        "Error processing message from topic '%s' "
                        "(group '%s') at partition %d offset %d. "
                        "Offset not committed; message may be reprocessed.",
                        self._topic,
                        self._group_id,
                        msg.partition(),
                        msg.offset(),
                    )
        except KafkaException as e:
            logger.error(
                "Fatal Kafka error in consumer loop (topic='%s', "
                "group='%s'): %s",
                self._topic,
                self._group_id,
                e,
            )
        finally:
            logger.info(
                "Closing consumer: topic='%s', group_id='%s'",
                self._topic,
                self._group_id,
            )
            self._consumer.close()
            self._consumer = None

    async def start(self) -> None:
        """
        Start the consumer loop in a dedicated background thread.

        The consumer subscribes to the configured topic with the
        specified consumer group and begins polling for messages.
        """
        if self._running:
            logger.warning(
                "Consumer already running: topic='%s', group_id='%s'",
                self._topic,
                self._group_id,
            )
            return

        self._stop_event.clear()
        self._running = True
        self._thread = Thread(
            target=self._consume_loop,
            name=f"kafka-consumer-{self._group_id}-{self._topic}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Consumer thread started: topic='%s', group_id='%s'",
            self._topic,
            self._group_id,
        )

    async def stop(self) -> None:
        """
        Gracefully stop the consumer loop and wait for the thread to exit.

        Signals the consumer to stop polling, waits for the current
        poll/process cycle to complete, and joins the thread.
        """
        if not self._running:
            return

        logger.info(
            "Stopping consumer: topic='%s', group_id='%s'",
            self._topic,
            self._group_id,
        )
        self._stop_event.set()

        if self._thread is not None:
            # Wait for the thread to finish in a non-blocking way
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._thread.join, 10.0)
            if self._thread.is_alive():
                logger.warning(
                    "Consumer thread did not stop within timeout: "
                    "topic='%s', group_id='%s'",
                    self._topic,
                    self._group_id,
                )
            self._thread = None

        self._running = False
        logger.info(
            "Consumer stopped: topic='%s', group_id='%s'",
            self._topic,
            self._group_id,
        )

    @property
    def is_running(self) -> bool:
        """Return whether the consumer loop is currently active."""
        return self._running
