"""
Simple migration runner for Project Sentinel TimescaleDB schema.

Usage:
    python migrations/run_migrations.py

Reads database connection from environment variables or falls back to defaults
matching the docker-compose.yml development configuration.
"""

import os
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def get_connection_params() -> dict:
    """Get database connection parameters from environment or defaults."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "sentinel_db"),
        "user": os.getenv("POSTGRES_USER", "sentinel"),
        "password": os.getenv("POSTGRES_PASSWORD", "sentinel_dev"),
    }


def get_migration_files() -> list[Path]:
    """Get all SQL migration files in order."""
    migrations_dir = Path(__file__).parent
    files = sorted(migrations_dir.glob("*.sql"))
    return files


def run_migrations():
    """Execute all migration files in order."""
    params = get_connection_params()
    migration_files = get_migration_files()

    if not migration_files:
        print("No migration files found.")
        return

    print(f"Connecting to {params['host']}:{params['port']}/{params['dbname']}...")

    try:
        conn = psycopg2.connect(**params)
        conn.autocommit = True
    except psycopg2.OperationalError as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

    cursor = conn.cursor()

    for migration_file in migration_files:
        print(f"Running {migration_file.name}...")
        try:
            sql = migration_file.read_text(encoding="utf-8")
            cursor.execute(sql)
            print(f"  ✓ {migration_file.name} applied successfully")
        except psycopg2.Error as e:
            print(f"  ✗ {migration_file.name} failed: {e}")
            cursor.close()
            conn.close()
            sys.exit(1)

    cursor.close()
    conn.close()
    print("\nAll migrations applied successfully.")


if __name__ == "__main__":
    run_migrations()
