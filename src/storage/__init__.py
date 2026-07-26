"""Storage Layer - Database and TimescaleDB interaction.

Provides:
- DatabaseManager: Separate connection pools for reads and writes
- StorageWriterConsumer: Kafka consumer for batch-writing to TimescaleDB
- RetentionManager: Configurable data retention policies
- Query API routes: Anomalies, agent status, and metrics endpoints
"""

from src.storage.consumer import StorageWriterConsumer
from src.storage.database import DatabaseManager
from src.storage.retention import RetentionManager

__all__ = [
    "DatabaseManager",
    "RetentionManager",
    "StorageWriterConsumer",
]
