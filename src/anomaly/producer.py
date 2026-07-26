"""Anomaly event producer for Project Sentinel.

Produces AnomalyEvent instances to the anomaly-events Kafka topic
using the existing KafkaProducerService.
"""

import json
import logging

from src.gateway.kafka_producer import KafkaProducerService
from src.models.anomaly import AnomalyEvent

logger = logging.getLogger(__name__)


class AnomalyEventProducer:
    """Produces anomaly events to the anomaly-events Kafka topic.

    Serializes AnomalyEvent instances to JSON and produces them keyed
    by agent_id for per-agent partition ordering.

    Args:
        kafka_producer: The shared KafkaProducerService instance.
        topic: Kafka topic name for anomaly events (default: "anomaly-events").
    """

    def __init__(
        self, kafka_producer: KafkaProducerService, topic: str = "anomaly-events"
    ) -> None:
        self._kafka_producer = kafka_producer
        self._topic = topic

    @property
    def topic(self) -> str:
        """The Kafka topic this producer writes to."""
        return self._topic

    def _serialize_anomaly(self, anomaly: AnomalyEvent) -> bytes:
        """Serialize an AnomalyEvent to JSON bytes.

        Uses Pydantic's model_dump with mode='json' for proper
        serialization of UUID, datetime, and Enum fields.

        Args:
            anomaly: The anomaly event to serialize.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        data = anomaly.model_dump(mode="json")
        return json.dumps(data).encode("utf-8")

    async def produce_anomaly(self, anomaly: AnomalyEvent) -> None:
        """Serialize and produce an anomaly event to Kafka.

        The message is keyed by agent_id to ensure all anomalies for
        a given agent land on the same partition (ordering guarantee).

        Args:
            anomaly: The AnomalyEvent to produce.

        Raises:
            KafkaUnavailableError: If the Kafka broker is unavailable.
            KafkaProducerError: If the produce operation fails.
        """
        key = str(anomaly.agent_id)
        value = self._serialize_anomaly(anomaly)

        await self._kafka_producer.produce(
            topic=self._topic,
            key=key,
            value=value,
        )

        logger.info(
            "Produced anomaly event: anomaly_id=%s, agent_id=%s, type=%s, severity=%s",
            anomaly.anomaly_id,
            anomaly.agent_id,
            anomaly.anomaly_type.value,
            anomaly.severity.value,
        )

    async def produce_anomalies(self, anomalies: list[AnomalyEvent]) -> None:
        """Produce multiple anomaly events to Kafka.

        Uses batch production for efficiency when multiple anomalies
        are detected in a single detection cycle.

        Args:
            anomalies: List of AnomalyEvent instances to produce.

        Raises:
            KafkaUnavailableError: If the Kafka broker is unavailable.
            KafkaProducerError: If the produce operation fails.
        """
        if not anomalies:
            return

        events = [
            (self._topic, str(a.agent_id), self._serialize_anomaly(a))
            for a in anomalies
        ]

        await self._kafka_producer.produce_batch(events)

        logger.info(
            "Produced %d anomaly events to topic '%s'",
            len(anomalies),
            self._topic,
        )
