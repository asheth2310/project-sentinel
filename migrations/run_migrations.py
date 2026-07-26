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


def ensure_migrations_table(cursor) -> None:
    """Create the schema_migrations tracking table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)


def get_applied_migrations(cursor) -> set[str]:
    """Get the set of already-applied migration filenames."""
    cursor.execute("SELECT filename FROM schema_migrations ORDER BY filename;")
    return {row[0] for row in cursor.fetchall()}


def run_migrations():
    """Execute all pending migration files in order."""
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

    # Ensure tracking table exists
    ensure_migrations_table(cursor)
    applied = get_applied_migrations(cursor)

    pending = [f for f in migration_files if f.name not in applied]

    if not pending:
        print("All migrations already applied.")
        cursor.close()
        conn.close()
        return

    print(f"Found {len(pending)} pending migration(s).")

    for migration_file in pending:
        print(f"Running {migration_file.name}...")
        try:
            sql = migration_file.read_text(encoding="utf-8")
            cursor.execute(sql)
            # Record successful migration
            cursor.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s);",
                (migration_file.name,),
            )
            print(f"  ✓ {migration_file.name} applied successfully")
        except psycopg2.Error as e:
            print(f"  ✗ {migration_file.name} failed: {e}")
            cursor.close()
            conn.close()
            sys.exit(1)

    cursor.close()
    conn.close()
    print(f"\nAll {len(pending)} migration(s) applied successfully.")


if __name__ == "__main__":
    run_migrations()
