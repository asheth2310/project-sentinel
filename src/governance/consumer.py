"""Kafka consumer for the governance engine.

Consumes anomaly-events from Kafka and evaluates them against
governance policies to determine appropriate actions.
"""

import logging
from typing import Any

from src.config.settings import KafkaSettings
from src.gateway.kafka_consumer import KafkaConsumerService
from src.governance.engine import GovernanceEngine

logger = logging.getLogger(__name__)


class GovernanceEngineConsumer(KafkaConsumerService):
    """Consumes anomaly-events from Kafka and evaluates governance policies.

    Subscribes to the anomaly-events topic and passes each event through
    the GovernanceEngine for policy evaluation and action determination.

    Args:
        settings: Kafka connection configuration.
        governance_engine: The governance engine instance for policy evaluation.
    """

    def __init__(
        self, settings: KafkaSettings, governance_engine: GovernanceEngine
    ) -> None:
        super().__init__(
            settings,
            settings.topic_anomaly_events,
            settings.consumer_group_governance,
        )
        self._governance_engine = governance_engine

    async def process_message(self, message: dict[str, Any]) -> None:
        """Evaluate an anomaly event against governance policies.

        Deserializes the message into an AnomalyEvent and evaluates it
        through the governance engine. Logs resulting actions.

        Args:
            message: The deserialized JSON message from Kafka.

        Raises:
            ValueError: If the message cannot be parsed as an AnomalyEvent.
        """
        from src.models.anomaly import AnomalyEvent

        try:
            anomaly = AnomalyEvent(**message)
        except Exception as e:
            logger.error(
                "Failed to parse anomaly event from message: %s. Error: %s",
                message,
                e,
            )
            raise ValueError(f"Invalid anomaly event message: {e}") from e

        logger.info(
            "Evaluating anomaly event: anomaly_id=%s, agent_id=%s, type=%s",
            anomaly.anomaly_id,
            anomaly.agent_id,
            anomaly.anomaly_type,
        )

        actions = await self._governance_engine.evaluate_anomaly(anomaly)

        for action in actions:
            logger.info(
                "Governance action determined: type=%s, agent_id=%s, "
                "severity=%s, metric=%s, reason=%s",
                action.action_type.value,
                action.agent_id,
                action.severity.value,
                action.threshold_metric,
                action.reason,
            )

        if not actions:
            logger.debug(
                "No governance actions for anomaly_id=%s (no thresholds breached or in cooldown)",
                anomaly.anomaly_id,
            )
