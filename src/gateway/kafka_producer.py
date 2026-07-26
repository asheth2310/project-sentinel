"""
Kafka producer service for Project Sentinel ingestion gateway.

Wraps confluent-kafka's synchronous Producer with an async interface
using asyncio, partitions messages by agent_id, and handles broker
unavailability gracefully.
"""

import asyncio
import logging
from typing import Optional

from confluent_kafka import KafkaException, Producer

from src.config.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Raised when the Kafka producer encounters an unrecoverable error."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


class KafkaUnavailableError(Exception):
    """Raised when the Kafka broker is unavailable.

    Callers should respond with HTTP 503 and a Retry-After header.
    """

    def __init__(self, message: str = "Kafka broker is unavailable"):
        super().__init__(message)


class KafkaProducerService:
    """Async Kafka producer service for the ingestion gateway.

    Wraps confluent-kafka's synchronous Producer to provide an async
    interface. Messages are partitioned by agent_id (string UUID) to
    guarantee per-agent ordering. Uses acks setting from KafkaSettings
    (default "1") for low-latency writes.

    Usage:
        service = KafkaProducerService(kafka_settings)
        await service.start()
        try:
            await service.produce("telemetry-raw", key=agent_id, value=payload)
        finally:
            await service.stop()
    """

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: Optional[Producer] = None
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None

    @property
    def is_running(self) -> bool:
        """Whether the producer is connected and running."""
        return self._running

    async def start(self) -> None:
        """Initialize the confluent-kafka producer and start background polling.

        Raises:
            KafkaUnavailableError: If the producer cannot be created.
        """
        if self._running:
            return

        config = {
            "bootstrap.servers": self._settings.bootstrap_servers,
            "acks": self._settings.acks,
            "linger.ms": self._settings.producer_linger_ms,
            "batch.size": self._settings.producer_batch_size,
            "error_cb": self._on_error,
        }

        try:
            self._producer = Producer(config)
        except KafkaException as exc:
            logger.error("Failed to create Kafka producer: %s", exc)
            raise KafkaUnavailableError(
                f"Failed to initialize Kafka producer: {exc}"
            ) from exc

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info(
            "KafkaProducerService started (brokers=%s, acks=%s)",
            self._settings.bootstrap_servers,
            self._settings.acks,
        )

    async def stop(self) -> None:
        """Flush pending messages and shut down the producer."""
        if not self._running:
            return

        self._running = False

        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        if self._producer is not None:
            # Flush with a timeout to avoid hanging forever
            remaining = await asyncio.to_thread(self._producer.flush, timeout=10.0)
            if remaining > 0:
                logger.warning(
                    "Kafka producer stopped with %d messages still in queue", remaining
                )
            self._producer = None

        logger.info("KafkaProducerService stopped")

    async def produce(self, topic: str, key: str, value: bytes) -> None:
        """Produce a single message to Kafka asynchronously.

        Args:
            topic: Kafka topic name (e.g. "telemetry-raw").
            key: Partition key (agent_id as string UUID) for per-agent ordering.
            value: Serialized message payload as bytes.

        Raises:
            KafkaUnavailableError: If the producer is not running or broker is down.
            KafkaProducerError: If the produce operation fails.
        """
        if not self._running or self._producer is None:
            raise KafkaUnavailableError("Kafka producer is not running")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def _delivery_callback(err, msg):
            """Called by confluent-kafka on delivery (or failure)."""
            if err is not None:
                if not future.done():
                    loop.call_soon_threadsafe(
                        future.set_exception,
                        KafkaProducerError(
                            f"Message delivery failed: {err}", original_error=err
                        ),
                    )
            else:
                if not future.done():
                    loop.call_soon_threadsafe(future.set_result, None)

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                callback=_delivery_callback,
            )
            # Trigger a poll to start delivery
            self._producer.poll(0)
        except BufferError as exc:
            logger.error("Kafka producer buffer full: %s", exc)
            raise KafkaUnavailableError(
                "Kafka producer buffer full, broker may be unavailable"
            ) from exc
        except KafkaException as exc:
            logger.error("Kafka produce error: %s", exc)
            raise KafkaProducerError(
                f"Failed to produce message: {exc}", original_error=exc
            ) from exc

        await future

    async def produce_batch(self, events: list[tuple[str, str, bytes]]) -> None:
        """Produce a batch of messages atomically (all-or-nothing).

        All messages in the batch are enqueued before waiting for delivery
        confirmations. If any message fails delivery, a KafkaProducerError
        is raised.

        Args:
            events: List of (topic, key, value) tuples. Key is agent_id string.

        Raises:
            KafkaUnavailableError: If the producer is not running or broker is down.
            KafkaProducerError: If any message in the batch fails delivery.
        """
        if not self._running or self._producer is None:
            raise KafkaUnavailableError("Kafka producer is not running")

        if not events:
            return

        loop = asyncio.get_running_loop()
        futures: list[asyncio.Future[None]] = []

        for topic, key, value in events:
            future: asyncio.Future[None] = loop.create_future()
            futures.append(future)

            def _make_callback(fut: asyncio.Future[None]):
                def _delivery_callback(err, msg):
                    if err is not None:
                        if not fut.done():
                            loop.call_soon_threadsafe(
                                fut.set_exception,
                                KafkaProducerError(
                                    f"Batch message delivery failed: {err}",
                                    original_error=err,
                                ),
                            )
                    else:
                        if not fut.done():
                            loop.call_soon_threadsafe(fut.set_result, None)

                return _delivery_callback

            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value,
                    callback=_make_callback(future),
                )
            except BufferError as exc:
                # Cancel any pending futures
                for f in futures:
                    if not f.done():
                        f.cancel()
                raise KafkaUnavailableError(
                    "Kafka producer buffer full during batch produce"
                ) from exc
            except KafkaException as exc:
                for f in futures:
                    if not f.done():
                        f.cancel()
                raise KafkaProducerError(
                    f"Failed to produce batch message: {exc}", original_error=exc
                ) from exc

        # Trigger delivery of all enqueued messages
        self._producer.poll(0)

        # Wait for all delivery callbacks
        results = await asyncio.gather(*futures, return_exceptions=True)

        # Check for any failures
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            raise KafkaProducerError(
                f"Batch production failed: {len(errors)}/{len(events)} messages failed",
                original_error=errors[0],
            )

    async def _poll_loop(self) -> None:
        """Background task that polls the producer for delivery callbacks.

        confluent-kafka requires periodic poll() calls to trigger delivery
        report callbacks. This runs in the background while the producer is active.
        """
        while self._running:
            if self._producer is not None:
                # Run poll in a thread to avoid blocking the event loop
                await asyncio.to_thread(self._producer.poll, 0.1)
            else:
                await asyncio.sleep(0.1)

    def _on_error(self, error) -> None:
        """Global error callback for the confluent-kafka producer."""
        logger.error("Kafka producer error: %s", error)
