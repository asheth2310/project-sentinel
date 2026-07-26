#!/usr/bin/env python3
"""
Kafka topic configuration script for Project Sentinel.

Creates and verifies the required Kafka topics:
  - telemetry-raw: Raw telemetry events from the ingestion gateway
  - telemetry-enriched: Enriched telemetry events after processing
  - anomaly-events: Detected anomaly events from the anomaly engine

Topics are partitioned by agent_id (6 partitions for dev) with
replication factor 1 for local development.

Usage:
    python scripts/create_topics.py

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
    KAFKA_NUM_PARTITIONS: Number of partitions per topic (default: 6)
    KAFKA_REPLICATION_FACTOR: Replication factor (default: 1)
"""

from __future__ import annotations

import os
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic


# Topic definitions for Project Sentinel
TOPICS = [
    "telemetry-raw",
    "telemetry-enriched",
    "anomaly-events",
]


def get_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers from environment or default."""
    # Try the settings-style env var first, then fall back to a direct one
    return os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )


def get_num_partitions() -> int:
    """Get number of partitions from environment or default (6 for dev)."""
    return int(os.environ.get("KAFKA_NUM_PARTITIONS", "6"))


def get_replication_factor() -> int:
    """Get replication factor from environment or default (1 for local dev)."""
    return int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1"))


def create_admin_client(bootstrap_servers: str) -> AdminClient:
    """Create a confluent-kafka AdminClient."""
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def get_existing_topics(admin: AdminClient) -> set[str]:
    """Retrieve the set of existing topic names from the cluster."""
    metadata = admin.list_topics(timeout=10)
    return set(metadata.topics.keys())


def create_topics(
    admin: AdminClient,
    topics: list[str],
    num_partitions: int,
    replication_factor: int,
) -> None:
    """
    Create Kafka topics idempotently.

    Skips topics that already exist and reports status for each topic.
    """
    existing = get_existing_topics(admin)

    topics_to_create: list[NewTopic] = []
    for topic_name in topics:
        if topic_name in existing:
            print(f"  [SKIP] Topic '{topic_name}' already exists.")
        else:
            topics_to_create.append(
                NewTopic(
                    topic=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
            )

    if not topics_to_create:
        print("\nAll topics already exist. Nothing to create.")
        return

    # Request topic creation
    futures = admin.create_topics(topics_to_create)

    # Wait for each topic creation to complete
    for topic_name, future in futures.items():
        try:
            future.result()  # Block until topic is created or fails
            print(f"  [CREATED] Topic '{topic_name}' created successfully "
                  f"(partitions={num_partitions}, replication_factor={replication_factor}).")
        except Exception as e:
            # Handle case where topic was created between our check and create call
            error_str = str(e)
            if "TOPIC_ALREADY_EXISTS" in error_str:
                print(f"  [SKIP] Topic '{topic_name}' already exists (race condition).")
            else:
                print(f"  [ERROR] Failed to create topic '{topic_name}': {e}")
                raise


def verify_topics(admin: AdminClient, topics: list[str]) -> bool:
    """Verify all required topics exist and print their configuration."""
    existing = get_existing_topics(admin)
    all_present = True

    print("\nTopic verification:")
    for topic_name in topics:
        if topic_name in existing:
            metadata = admin.list_topics(topic=topic_name, timeout=10)
            topic_meta = metadata.topics[topic_name]
            partition_count = len(topic_meta.partitions)
            print(f"  [OK] '{topic_name}' - {partition_count} partitions")
        else:
            print(f"  [MISSING] '{topic_name}' - topic not found!")
            all_present = False

    return all_present


def wait_for_kafka(admin: AdminClient, max_retries: int = 5, delay: float = 2.0) -> bool:
    """Wait for Kafka to become available."""
    for attempt in range(1, max_retries + 1):
        try:
            admin.list_topics(timeout=5)
            return True
        except Exception:
            if attempt < max_retries:
                print(f"  Kafka not ready, retrying in {delay}s... "
                      f"(attempt {attempt}/{max_retries})")
                time.sleep(delay)
            else:
                return False


def main() -> int:
    """Main entry point for the topic creation script."""
    bootstrap_servers = get_bootstrap_servers()
    num_partitions = get_num_partitions()
    replication_factor = get_replication_factor()

    print("=" * 60)
    print("Project Sentinel - Kafka Topic Configuration")
    print("=" * 60)
    print(f"\nBootstrap servers: {bootstrap_servers}")
    print(f"Partitions per topic: {num_partitions}")
    print(f"Replication factor: {replication_factor}")
    print(f"Topics to configure: {', '.join(TOPICS)}")
    print()

    # Create admin client
    admin = create_admin_client(bootstrap_servers)

    # Wait for Kafka to be available
    print("Connecting to Kafka...")
    if not wait_for_kafka(admin):
        print("\n[FATAL] Could not connect to Kafka. Ensure the broker is running.")
        return 1

    print("  Connected successfully.\n")

    # Create topics
    print("Creating topics:")
    try:
        create_topics(admin, TOPICS, num_partitions, replication_factor)
    except Exception as e:
        print(f"\n[FATAL] Topic creation failed: {e}")
        return 1

    # Verify all topics exist
    if not verify_topics(admin, TOPICS):
        print("\n[FATAL] Not all required topics are present.")
        return 1

    print("\n" + "=" * 60)
    print("All Kafka topics configured successfully!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
