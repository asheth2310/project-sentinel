-- Migration 001: Enable required PostgreSQL extensions
-- Project Sentinel - TimescaleDB Schema

-- Enable TimescaleDB extension for hypertable support
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable uuid-ossp for uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
