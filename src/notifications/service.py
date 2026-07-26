"""Notification service for Project Sentinel.

Dispatches alerts to configured notification channels (Slack, PagerDuty)
with retry logic, exponential backoff, and dead-letter queue for failed
deliveries.

Implements:
- Multi-channel delivery to all configured channels (Requirement 8.6)
- Slack webhook formatting with anomaly details
- PagerDuty event formatting with severity mapping
- Exponential backoff retry (1s, 2s, 4s) with max 3 retries
- Dead-letter queue for exhausted retries
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import httpx

from src.config.settings import NotificationSettings
from src.models.anomaly import Severity
from src.models.governance import NotificationChannel, NotificationChannelType

logger = logging.getLogger(__name__)


# PagerDuty severity mapping: Sentinel severity -> PagerDuty severity
_PAGERDUTY_SEVERITY_MAP: dict[str, str] = {
    Severity.LOW.value: "info",
    Severity.MEDIUM.value: "warning",
    Severity.HIGH.value: "error",
    Severity.CRITICAL.value: "critical",
}


class NotificationService:
    """Dispatches alerts to configured notification channels with retry.

    Supports Slack webhooks and PagerDuty event API. Uses exponential
    backoff for retries and maintains a dead-letter queue for failed
    deliveries.

    Args:
        settings: Notification delivery configuration.
        http_client: Optional httpx async client (injected for testing).
    """

    def __init__(
        self,
        settings: NotificationSettings,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient(
            timeout=settings.webhook_timeout_seconds
        )
        self._dead_letter_queue: list[dict[str, Any]] = []

    async def send_alert(
        self,
        alert: dict[str, Any],
        channels: list[NotificationChannel],
    ) -> list[dict[str, Any]]:
        """Send alert to all configured channels with retry.

        Dispatches the alert to each channel concurrently. Results include
        delivery status for each channel.

        Args:
            alert: Alert data containing anomaly_type, severity, agent_id,
                   metric_value, threshold_value, and description.
            channels: List of notification channels to deliver to.

        Returns:
            List of delivery result dicts with keys: channel_type, success, error.
        """
        results: list[dict[str, Any]] = []

        for channel in channels:
            if channel.type == NotificationChannelType.SLACK:
                success = await self.send_slack_message(
                    webhook_url=channel.webhook_url,
                    message=alert,
                )
                results.append({
                    "channel_type": "slack",
                    "success": success,
                    "error": None if success else "delivery_failed",
                })
            elif channel.type == NotificationChannelType.PAGERDUTY:
                success = await self.send_pagerduty_event(
                    routing_key=channel.routing_key,
                    event=alert,
                )
                results.append({
                    "channel_type": "pagerduty",
                    "success": success,
                    "error": None if success else "delivery_failed",
                })

        return results

    async def send_slack_message(self, webhook_url: str, message: dict[str, Any]) -> bool:
        """Send alert to Slack with formatted payload.

        Formats the alert as Slack blocks containing anomaly_type, severity,
        agent_id, metric_value, and threshold_value.

        Args:
            webhook_url: Slack incoming webhook URL.
            message: Alert data to format.

        Returns:
            True if delivery succeeded, False if all retries exhausted.
        """
        payload = self._format_slack_payload(message)
        return await self._deliver_with_retry(
            url=webhook_url,
            payload=payload,
            context={"channel": "slack", "alert": message},
        )

    async def send_pagerduty_event(self, routing_key: str, event: dict[str, Any]) -> bool:
        """Send alert to PagerDuty with severity mapping.

        Maps Sentinel severity levels to PagerDuty severity:
        LOW/MEDIUM -> info/warning, HIGH -> error, CRITICAL -> critical.

        Args:
            routing_key: PagerDuty integration routing key.
            event: Alert data to format.

        Returns:
            True if delivery succeeded, False if all retries exhausted.
        """
        payload = self._format_pagerduty_payload(routing_key, event)
        return await self._deliver_with_retry(
            url="https://events.pagerduty.com/v2/enqueue",
            payload=payload,
            context={"channel": "pagerduty", "alert": event},
        )

    async def _deliver_with_retry(
        self,
        url: str,
        payload: dict[str, Any],
        max_retries: int | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        """Deliver webhook with exponential backoff (1s, 2s, 4s).

        Retries on HTTP errors or network failures. After all retries
        are exhausted, the failed delivery is added to the dead-letter queue.

        Args:
            url: Target webhook URL.
            payload: JSON payload to send.
            max_retries: Maximum retry attempts (defaults to settings).
            context: Optional context for dead-letter queue entries.

        Returns:
            True if delivery succeeded within retry budget, False otherwise.
        """
        retries = max_retries if max_retries is not None else self._settings.max_retries
        base_delay = self._settings.base_retry_delay_seconds

        last_error: str | None = None

        for attempt in range(retries):
            try:
                response = await self._http_client.post(url, json=payload)
                if response.status_code < 300:
                    return True
                last_error = f"HTTP {response.status_code}: {response.text}"
                logger.warning(
                    "Webhook delivery attempt %d/%d failed: %s",
                    attempt + 1,
                    retries,
                    last_error,
                )
            except (httpx.HTTPError, Exception) as exc:
                last_error = str(exc)
                logger.warning(
                    "Webhook delivery attempt %d/%d error: %s",
                    attempt + 1,
                    retries,
                    last_error,
                )

            # Exponential backoff: delay = base_delay * 2^attempt
            # Attempts 0,1,2 -> delays 1s, 2s, 4s
            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        # All retries exhausted - add to dead-letter queue
        self._add_to_dead_letter_queue(
            url=url,
            payload=payload,
            error=last_error,
            attempts=retries,
            context=context,
        )
        return False

    @property
    def dead_letter_queue(self) -> list[dict[str, Any]]:
        """Get all failed deliveries in the dead-letter queue.

        Returns:
            List of failed delivery entries with url, payload, error,
            attempts, and timestamp.
        """
        return list(self._dead_letter_queue)

    def clear_dead_letter_queue(self) -> None:
        """Clear the dead-letter queue (useful after manual review)."""
        self._dead_letter_queue.clear()

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_slack_payload(alert: dict[str, Any]) -> dict[str, Any]:
        """Format alert as Slack blocks with anomaly details.

        Includes: anomaly_type, severity, agent_id, metric_value,
        threshold_value.

        Args:
            alert: Alert data dictionary.

        Returns:
            Slack-formatted payload with blocks.
        """
        anomaly_type = alert.get("anomaly_type", "unknown")
        severity = alert.get("severity", "unknown")
        agent_id = str(alert.get("agent_id", "unknown"))
        metric_value = alert.get("metric_value", "N/A")
        threshold_value = alert.get("threshold_value", "N/A")
        description = alert.get("description", "")

        severity_emoji = {
            "low": "ℹ️",
            "medium": "⚠️",
            "high": "🔴",
            "critical": "🚨",
        }.get(severity, "❓")

        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{severity_emoji} Sentinel Alert: {anomaly_type}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Anomaly Type:*\n{anomaly_type}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:*\n{severity.upper()}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Agent ID:*\n`{agent_id}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Metric Value:*\n{metric_value}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Threshold:*\n{threshold_value}",
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Description:*\n{description}",
                    },
                },
            ],
        }

    @staticmethod
    def _format_pagerduty_payload(
        routing_key: str, event: dict[str, Any]
    ) -> dict[str, Any]:
        """Format alert as PagerDuty Events API v2 payload.

        Maps severity: LOW->info, MEDIUM->warning, HIGH->error, CRITICAL->critical.

        Args:
            routing_key: PagerDuty integration routing key.
            event: Alert data dictionary.

        Returns:
            PagerDuty Events API v2 formatted payload.
        """
        severity = event.get("severity", "medium")
        pd_severity = _PAGERDUTY_SEVERITY_MAP.get(severity, "warning")
        anomaly_type = event.get("anomaly_type", "unknown")
        agent_id = str(event.get("agent_id", "unknown"))
        metric_value = event.get("metric_value", "N/A")
        threshold_value = event.get("threshold_value", "N/A")
        description = event.get("description", f"Anomaly detected: {anomaly_type}")

        return {
            "routing_key": routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"[Sentinel] {anomaly_type} - Agent {agent_id}",
                "severity": pd_severity,
                "source": f"sentinel-agent-{agent_id}",
                "component": "project-sentinel",
                "group": "anomaly-detection",
                "class": anomaly_type,
                "custom_details": {
                    "agent_id": agent_id,
                    "anomaly_type": anomaly_type,
                    "metric_value": metric_value,
                    "threshold_value": threshold_value,
                    "description": description,
                },
            },
        }

    def _add_to_dead_letter_queue(
        self,
        url: str,
        payload: dict[str, Any],
        error: str | None,
        attempts: int,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Add a failed delivery to the dead-letter queue.

        Args:
            url: The target URL that failed.
            payload: The payload that couldn't be delivered.
            error: The last error message.
            attempts: Number of attempts made.
            context: Optional context about the delivery.
        """
        entry = {
            "url": url,
            "payload": payload,
            "error": error,
            "attempts": attempts,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "context": context or {},
        }
        self._dead_letter_queue.append(entry)
        logger.error(
            "Delivery exhausted after %d attempts, added to dead-letter queue: url=%s, error=%s",
            attempts,
            url,
            error,
        )
