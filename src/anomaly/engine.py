"""Anomaly engine consumer for Project Sentinel.

Consumes telemetry-enriched events from Kafka, maintains per-agent
sliding windows, runs anomaly detection, and produces anomaly events.
"""

import logging
from typing import Any

from src.anomaly.detectors import AnomalyDetector
from src.anomaly.producer import AnomalyEventProducer
from src.anomaly.window_manager import WindowManager
from src.config.settings import KafkaSettings
from src.gateway.kafka_consumer import KafkaConsumerService
from src.models.telemetry import TelemetryEvent

logger = logging.getLogger(__name__)


class AnomalyEngineConsumer(KafkaConsumerService):
    """Consumes telemetry-enriched events, maintains per-agent windows, runs detection.

    Ties together the Kafka consumer, WindowManager, AnomalyDetector, and
    AnomalyEventProducer into a single processing pipeline:

    1. Consume telemetry event from telemetry-enriched topic
    2. Parse into TelemetryEvent model
    3. Update per-agent sliding window via WindowManager
    4. Run anomaly detection on the updated window
    5. Produce any detected anomalies to anomaly-events topic

    Args:
        settings: Kafka connection configuration.
        window_manager: Per-agent sliding window manager.
        detector: Anomaly detection engine with configured thresholds.
        producer: Anomaly event producer for writing to Kafka.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        window_manager: WindowManager,
        detector: AnomalyDetector,
        producer: AnomalyEventProducer,
    ) -> None:
        super().__init__(
            settings,
            settings.topic_telemetry_enriched,
            settings.consumer_group_anomaly,
        )
        self._window_manager = window_manager
        self._detector = detector
        self._producer = producer

    async def process_message(self, message: dict[str, Any]) -> None:
        """Process a telemetry event: update window, run detection, produce anomalies.

        Steps:
        1. Deserialize message dict into TelemetryEvent
        2. Add event to the agent's sliding window
        3. Run all detection rules against the updated window
        4. If anomalies found, produce them to the anomaly-events topic

        Args:
            message: Deserialized JSON payload from the telemetry-enriched topic.

        Raises:
            Exception: If anomaly production fails (offset won't be committed).
        """
        # Step 1: Parse the message into a TelemetryEvent
        try:
            event = TelemetryEvent(**message)
        except Exception as e:
            logger.warning(
                "Failed to parse telemetry event: %s. Skipping message.",
                e,
            )
            return

        # Step 2: Update the agent's sliding window
        window = self._window_manager.add_event(event)

        # Step 3: Run anomaly detection
        anomalies = self._detector.detect_anomalies(event, window)

        # Step 4: Produce any detected anomalies
        if anomalies:
            logger.info(
                "Detected %d anomalies for agent_id=%s",
                len(anomalies),
                event.agent_id,
            )
            await self._producer.produce_anomalies(anomalies)
