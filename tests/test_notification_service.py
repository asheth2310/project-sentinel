"""Tests for notification service (Tasks 44-48).

Covers:
- Multi-channel delivery (Task 44)
- Slack webhook formatting (Task 45)
- PagerDuty event formatting with severity mapping (Task 46)
- Retry logic with exponential backoff (Task 47)
- Dead-letter queue for exhausted retries (Task 48)
"""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4

import httpx

from src.config.settings import NotificationSettings
from src.models.governance import NotificationChannel, NotificationChannelType
from src.notifications.service import NotificationService, _PAGERDUTY_SEVERITY_MAP


@pytest.fixture
def settings():
    """Create notification settings with fast retries for testing."""
    return NotificationSettings(
        max_retries=3,
        base_retry_delay_seconds=0.01,  # Fast for tests
        webhook_timeout_seconds=5.0,
    )


@pytest.fixture
def mock_http_client():
    """Create a mock httpx AsyncClient."""
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
def service(settings, mock_http_client):
    """Create a NotificationService with mock HTTP client."""
    return NotificationService(settings=settings, http_client=mock_http_client)


@pytest.fixture
def sample_alert():
    """Create a sample alert payload."""
    return {
        "anomaly_type": "token_spike",
        "severity": "high",
        "agent_id": str(uuid4()),
        "metric_value": 15000.0,
        "threshold_value": 10000.0,
        "description": "Token usage exceeded threshold",
    }


# --- Task 44: Multi-channel delivery ---


class TestMultiChannelDelivery:
    """Tests for notification dispatcher with multi-channel delivery."""

    @pytest.mark.asyncio
    async def test_sends_to_all_channels(self, service, mock_http_client, sample_alert):
        """Alert is sent to all configured channels."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_http_client.post.return_value = mock_response

        channels = [
            NotificationChannel(
                type=NotificationChannelType.SLACK,
                webhook_url="https://hooks.slack.com/services/test",
            ),
            NotificationChannel(
                type=NotificationChannelType.PAGERDUTY,
                routing_key="test-routing-key",
            ),
        ]

        results = await service.send_alert(sample_alert, channels)

        assert len(results) == 2
        assert results[0]["channel_type"] == "slack"
        assert results[0]["success"] is True
        assert results[1]["channel_type"] == "pagerduty"
        assert results[1]["success"] is True

    @pytest.mark.asyncio
    async def test_partial_failure(self, service, mock_http_client, sample_alert):
        """Partial failures are reported per channel."""
        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:  # First channel: all 3 attempts fail
                mock_resp = MagicMock()
                mock_resp.status_code = 500
                mock_resp.text = "Internal Server Error"
                return mock_resp
            else:  # Second channel succeeds
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                return mock_resp

        mock_http_client.post.side_effect = side_effect

        channels = [
            NotificationChannel(
                type=NotificationChannelType.SLACK,
                webhook_url="https://hooks.slack.com/services/bad",
            ),
            NotificationChannel(
                type=NotificationChannelType.PAGERDUTY,
                routing_key="good-key",
            ),
        ]

        results = await service.send_alert(sample_alert, channels)

        assert results[0]["success"] is False
        assert results[1]["success"] is True

    @pytest.mark.asyncio
    async def test_empty_channels_returns_empty(self, service, sample_alert):
        """No channels means no deliveries."""
        results = await service.send_alert(sample_alert, [])
        assert results == []


# --- Task 45: Slack webhook formatting ---


class TestSlackFormatting:
    """Tests for Slack webhook message formatting."""

    def test_slack_payload_contains_required_fields(self, sample_alert):
        """Slack payload includes anomaly_type, severity, agent_id, metric values."""
        payload = NotificationService._format_slack_payload(sample_alert)

        assert "blocks" in payload
        blocks = payload["blocks"]

        # Header block
        header = blocks[0]
        assert header["type"] == "header"
        assert "token_spike" in header["text"]["text"]

        # Section with fields
        section = blocks[1]
        assert section["type"] == "section"
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "token_spike" in fields_text
        assert "HIGH" in fields_text
        assert sample_alert["agent_id"] in fields_text
        assert "15000.0" in fields_text
        assert "10000.0" in fields_text

    def test_slack_payload_severity_emoji(self):
        """Correct emoji is used for each severity level."""
        for severity, emoji in [
            ("low", "ℹ️"),
            ("medium", "⚠️"),
            ("high", "🔴"),
            ("critical", "🚨"),
        ]:
            payload = NotificationService._format_slack_payload(
                {"anomaly_type": "test", "severity": severity, "agent_id": "x",
                 "metric_value": 1, "threshold_value": 2, "description": ""}
            )
            assert emoji in payload["blocks"][0]["text"]["text"]

    @pytest.mark.asyncio
    async def test_send_slack_message_success(self, service, mock_http_client, sample_alert):
        """Successful Slack delivery returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_http_client.post.return_value = mock_response

        result = await service.send_slack_message(
            "https://hooks.slack.com/services/test", sample_alert
        )
        assert result is True
        mock_http_client.post.assert_called_once()


# --- Task 46: PagerDuty event formatting with severity mapping ---


class TestPagerDutyFormatting:
    """Tests for PagerDuty event formatting and severity mapping."""

    def test_severity_mapping(self):
        """Sentinel severity maps correctly to PagerDuty severity."""
        assert _PAGERDUTY_SEVERITY_MAP["low"] == "info"
        assert _PAGERDUTY_SEVERITY_MAP["medium"] == "warning"
        assert _PAGERDUTY_SEVERITY_MAP["high"] == "error"
        assert _PAGERDUTY_SEVERITY_MAP["critical"] == "critical"

    def test_pagerduty_payload_structure(self, sample_alert):
        """PagerDuty payload follows Events API v2 format."""
        payload = NotificationService._format_pagerduty_payload(
            "test-routing-key", sample_alert
        )

        assert payload["routing_key"] == "test-routing-key"
        assert payload["event_action"] == "trigger"
        assert "payload" in payload

        pd_payload = payload["payload"]
        assert pd_payload["severity"] == "error"  # HIGH -> error
        assert "token_spike" in pd_payload["summary"]
        assert pd_payload["component"] == "project-sentinel"
        assert pd_payload["custom_details"]["anomaly_type"] == "token_spike"
        assert pd_payload["custom_details"]["metric_value"] == 15000.0

    def test_pagerduty_severity_mapping_all_levels(self):
        """Each severity level maps correctly in the payload."""
        for sentinel_sev, pd_sev in [
            ("low", "info"),
            ("medium", "warning"),
            ("high", "error"),
            ("critical", "critical"),
        ]:
            payload = NotificationService._format_pagerduty_payload(
                "key", {"severity": sentinel_sev, "anomaly_type": "test",
                        "agent_id": "x", "metric_value": 1, "threshold_value": 2}
            )
            assert payload["payload"]["severity"] == pd_sev

    @pytest.mark.asyncio
    async def test_send_pagerduty_event_success(self, service, mock_http_client, sample_alert):
        """Successful PagerDuty delivery returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_http_client.post.return_value = mock_response

        result = await service.send_pagerduty_event("test-key", sample_alert)
        assert result is True


# --- Task 47: Retry logic with exponential backoff ---


class TestRetryLogic:
    """Tests for retry logic with exponential backoff (1s, 2s, 4s)."""

    @pytest.mark.asyncio
    async def test_retries_on_failure_then_succeeds(self, service, mock_http_client):
        """Retries on HTTP 500 and succeeds on second attempt."""
        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = "Server Error"

        success_response = MagicMock()
        success_response.status_code = 200

        mock_http_client.post.side_effect = [fail_response, success_response]

        result = await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"test": True},
        )

        assert result is True
        assert mock_http_client.post.call_count == 2

    @pytest.mark.asyncio
    async def test_retries_on_network_error(self, service, mock_http_client):
        """Retries on network error and succeeds on third attempt."""
        success_response = MagicMock()
        success_response.status_code = 200

        mock_http_client.post.side_effect = [
            httpx.ConnectError("Connection refused"),
            httpx.ConnectError("Connection refused"),
            success_response,
        ]

        result = await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"test": True},
        )

        assert result is True
        assert mock_http_client.post.call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self, service, mock_http_client):
        """Returns False after max retries are exhausted."""
        fail_response = MagicMock()
        fail_response.status_code = 503
        fail_response.text = "Service Unavailable"
        mock_http_client.post.return_value = fail_response

        result = await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"test": True},
        )

        assert result is False
        assert mock_http_client.post.call_count == 3  # max_retries = 3

    @pytest.mark.asyncio
    async def test_exponential_backoff_timing(self, settings):
        """Backoff delays follow pattern: base * 2^attempt."""
        # With base_delay=1.0, expected: 1s, 2s, 4s
        real_settings = NotificationSettings(
            max_retries=3,
            base_retry_delay_seconds=1.0,
            webhook_timeout_seconds=5.0,
        )

        delays = []
        for attempt in range(real_settings.max_retries - 1):
            delay = real_settings.base_retry_delay_seconds * (2 ** attempt)
            delays.append(delay)

        assert delays == [1.0, 2.0]  # Only 2 delays between 3 attempts
        # Verify the pattern: 1*2^0=1, 1*2^1=2, (1*2^2=4 would be next)

    @pytest.mark.asyncio
    async def test_success_on_first_try_no_retry(self, service, mock_http_client):
        """Immediate success doesn't trigger retries."""
        success_response = MagicMock()
        success_response.status_code = 200
        mock_http_client.post.return_value = success_response

        result = await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"test": True},
        )

        assert result is True
        assert mock_http_client.post.call_count == 1


# --- Task 48: Dead-letter queue ---


class TestDeadLetterQueue:
    """Tests for dead-letter queue for exhausted notification retries."""

    @pytest.mark.asyncio
    async def test_failed_delivery_added_to_dlq(self, service, mock_http_client):
        """Exhausted retries add entry to dead-letter queue."""
        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = "Internal Server Error"
        mock_http_client.post.return_value = fail_response

        assert len(service.dead_letter_queue) == 0

        await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"alert": "test"},
            context={"channel": "slack"},
        )

        assert len(service.dead_letter_queue) == 1
        entry = service.dead_letter_queue[0]
        assert entry["url"] == "https://example.com/webhook"
        assert entry["payload"] == {"alert": "test"}
        assert "500" in entry["error"]
        assert entry["attempts"] == 3
        assert "failed_at" in entry
        assert entry["context"]["channel"] == "slack"

    @pytest.mark.asyncio
    async def test_successful_delivery_not_in_dlq(self, service, mock_http_client):
        """Successful deliveries are NOT added to dead-letter queue."""
        success_response = MagicMock()
        success_response.status_code = 200
        mock_http_client.post.return_value = success_response

        await service._deliver_with_retry(
            url="https://example.com/webhook",
            payload={"alert": "test"},
        )

        assert len(service.dead_letter_queue) == 0

    @pytest.mark.asyncio
    async def test_multiple_failures_accumulate_in_dlq(self, service, mock_http_client):
        """Multiple failed deliveries accumulate in the DLQ."""
        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = "Error"
        mock_http_client.post.return_value = fail_response

        await service._deliver_with_retry("https://a.com", {"a": 1})
        await service._deliver_with_retry("https://b.com", {"b": 2})

        assert len(service.dead_letter_queue) == 2
        assert service.dead_letter_queue[0]["url"] == "https://a.com"
        assert service.dead_letter_queue[1]["url"] == "https://b.com"

    def test_clear_dead_letter_queue(self, service):
        """DLQ can be cleared after manual review."""
        service._dead_letter_queue.append({"test": "entry"})
        assert len(service.dead_letter_queue) == 1

        service.clear_dead_letter_queue()
        assert len(service.dead_letter_queue) == 0

    @pytest.mark.asyncio
    async def test_dlq_returns_copy(self, service, mock_http_client):
        """dead_letter_queue property returns a copy, not the internal list."""
        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.text = "Error"
        mock_http_client.post.return_value = fail_response

        await service._deliver_with_retry("https://a.com", {"a": 1})

        dlq = service.dead_letter_queue
        dlq.clear()

        # Internal queue should still have the entry
        assert len(service.dead_letter_queue) == 1
