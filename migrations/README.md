# Database Migrations

SQL migration scripts for Project Sentinel's TimescaleDB schema.

## Prerequisites

- TimescaleDB 2.13+ (PostgreSQL 16)
- Database created: `sentinel_db`
- User: `sentinel` (as configured in docker-compose.yml)

## Running Migrations

Migrations are numbered and should be executed in order:

```bash
# Connect to TimescaleDB and run migrations in order
psql -h localhost -U sentinel -d sentinel_db -f migrations/001_extensions.sql
psql -h localhost -U sentinel -d sentinel_db -f migrations/002_telemetry_logs.sql
psql -h localhost -U sentinel -d sentinel_db -f migrations/003_governance_policies.sql
psql -h localhost -U sentinel -d sentinel_db -f migrations/004_audit_log.sql
psql -h localhost -U sentinel -d sentinel_db -f migrations/005_anomaly_events.sql
```

Or use the migration runner script:

```bash
python migrations/run_migrations.py
```

## Migration Descriptions

| File | Description |
|------|-------------|
| 001_extensions.sql | Enables TimescaleDB and uuid-ossp extensions |
| 002_telemetry_logs.sql | Creates telemetry_logs hypertable (1-hour chunks, 90-day retention) |
| 003_governance_policies.sql | Creates governance_policies table for org-level policy config |
| 004_audit_log.sql | Creates audit_log table for circuit breaker accountability |
| 005_anomaly_events.sql | Creates anomaly_events table for detected anomalies |

## Schema Notes

- `telemetry_logs` is a TimescaleDB hypertable partitioned by timestamp
- Chunk interval: 1 hour (optimized for recent-data queries)
- Retention policy: 90 days (configurable via `remove_retention_policy` / `add_retention_policy`)
- All tables use UUID primary keys via `uuid_generate_v4()`
