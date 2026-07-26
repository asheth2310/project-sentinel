"""Unit tests for the Kafka topic creation script."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# We import from the script module path
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.create_topics import (
    TOPICS,
    create_topics,
    get_bootstrap_servers,
    get_existing_topics,
    get_num_partitions,
    get_replication_factor,
    verify_topics,
    wait_for_kafka,
)


class TestTopicConstants:
    """Test that topic constants are correctly defined."""

    def test_topics_list_has_three_topics(self):
        assert len(TOPICS) == 3

    def test_topics_include_telemetry_raw(self):
        assert "telemetry-raw" in TOPICS

    def test_topics_include_telemetry_enriched(self):
        assert "telemetry-enriched" in TOPICS

    def test_topics_include_anomaly_events(self):
        assert "anomaly-events" in TOPICS


class TestGetBootstrapServers:
    """Test bootstrap server configuration loading."""

    def test_default_value(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert get_bootstrap_servers() == "localhost:9092"

    def test_from_environment(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
        assert get_bootstrap_servers() == "broker1:9092,broker2:9092"


class TestGetNumPartitions:
    """Test partition configuration loading."""

    def test_default_value(self, monkeypatch):
        monkeypatch.delenv("KAFKA_NUM_PARTITIONS", raising=False)
        assert get_num_partitions() == 6

    def test_from_environment(self, monkeypatch):
        monkeypatch.setenv("KAFKA_NUM_PARTITIONS", "12")
        assert get_num_partitions() == 12


class TestGetReplicationFactor:
    """Test replication factor configuration loading."""

    def test_default_value(self, monkeypatch):
        monkeypatch.delenv("KAFKA_REPLICATION_FACTOR", raising=False)
        assert get_replication_factor() == 1

    def test_from_environment(self, monkeypatch):
        monkeypatch.setenv("KAFKA_REPLICATION_FACTOR", "3")
        assert get_replication_factor() == 3


class TestGetExistingTopics:
    """Test retrieval of existing topics from the cluster."""

    def test_returns_topic_names(self):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            "telemetry-raw": MagicMock(),
            "other-topic": MagicMock(),
        }
        mock_admin.list_topics.return_value = mock_metadata

        result = get_existing_topics(mock_admin)
        assert result == {"telemetry-raw", "other-topic"}

    def test_returns_empty_set_when_no_topics(self):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {}
        mock_admin.list_topics.return_value = mock_metadata

        result = get_existing_topics(mock_admin)
        assert result == set()


class TestCreateTopics:
    """Test topic creation logic."""

    def test_skips_existing_topics(self, capsys):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            "telemetry-raw": MagicMock(),
            "telemetry-enriched": MagicMock(),
            "anomaly-events": MagicMock(),
        }
        mock_admin.list_topics.return_value = mock_metadata

        create_topics(mock_admin, TOPICS, 6, 1)

        captured = capsys.readouterr()
        assert "[SKIP]" in captured.out
        assert "telemetry-raw" in captured.out
        # create_topics should not be called since all exist
        mock_admin.create_topics.assert_not_called()

    def test_creates_missing_topics(self, capsys):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {"telemetry-raw": MagicMock()}
        mock_admin.list_topics.return_value = mock_metadata

        # Mock futures for created topics
        mock_future_enriched = MagicMock()
        mock_future_enriched.result.return_value = None
        mock_future_anomaly = MagicMock()
        mock_future_anomaly.result.return_value = None

        mock_admin.create_topics.return_value = {
            "telemetry-enriched": mock_future_enriched,
            "anomaly-events": mock_future_anomaly,
        }

        create_topics(mock_admin, TOPICS, 6, 1)

        captured = capsys.readouterr()
        assert "[SKIP]" in captured.out
        assert "[CREATED]" in captured.out
        mock_admin.create_topics.assert_called_once()

    def test_handles_race_condition_topic_exists(self, capsys):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {}
        mock_admin.list_topics.return_value = mock_metadata

        # Simulate TOPIC_ALREADY_EXISTS error
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("TOPIC_ALREADY_EXISTS")

        mock_admin.create_topics.return_value = {
            "telemetry-raw": mock_future,
        }

        # Should not raise since it handles the race condition
        create_topics(mock_admin, ["telemetry-raw"], 6, 1)

        captured = capsys.readouterr()
        assert "race condition" in captured.out

    def test_raises_on_unexpected_error(self):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {}
        mock_admin.list_topics.return_value = mock_metadata

        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Broker not available")

        mock_admin.create_topics.return_value = {
            "telemetry-raw": mock_future,
        }

        with pytest.raises(Exception, match="Broker not available"):
            create_topics(mock_admin, ["telemetry-raw"], 6, 1)


class TestVerifyTopics:
    """Test topic verification logic."""

    def test_all_topics_present(self, capsys):
        mock_admin = MagicMock()

        # For get_existing_topics call
        mock_metadata_all = MagicMock()
        mock_metadata_all.topics = {
            "telemetry-raw": MagicMock(),
            "telemetry-enriched": MagicMock(),
            "anomaly-events": MagicMock(),
        }

        # For individual topic metadata calls
        def list_topics_side_effect(topic=None, timeout=10):
            if topic is None:
                return mock_metadata_all
            mock_topic_meta = MagicMock()
            mock_topic_meta.partitions = {0: None, 1: None, 2: None, 3: None, 4: None, 5: None}
            mock_single = MagicMock()
            mock_single.topics = {topic: mock_topic_meta}
            return mock_single

        mock_admin.list_topics.side_effect = list_topics_side_effect

        result = verify_topics(mock_admin, TOPICS)
        assert result is True

        captured = capsys.readouterr()
        assert "[OK]" in captured.out

    def test_missing_topic(self, capsys):
        mock_admin = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topics = {
            "telemetry-raw": MagicMock(),
        }
        mock_admin.list_topics.return_value = mock_metadata

        result = verify_topics(mock_admin, TOPICS)
        assert result is False

        captured = capsys.readouterr()
        assert "[MISSING]" in captured.out


class TestWaitForKafka:
    """Test Kafka connectivity wait logic."""

    def test_succeeds_immediately(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.return_value = MagicMock()

        result = wait_for_kafka(mock_admin, max_retries=3, delay=0.01)
        assert result is True

    def test_succeeds_after_retry(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.side_effect = [
            Exception("Connection refused"),
            MagicMock(),
        ]

        result = wait_for_kafka(mock_admin, max_retries=3, delay=0.01)
        assert result is True

    def test_fails_after_max_retries(self):
        mock_admin = MagicMock()
        mock_admin.list_topics.side_effect = Exception("Connection refused")

        result = wait_for_kafka(mock_admin, max_retries=2, delay=0.01)
        assert result is False
