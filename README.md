# Project Sentinel

**Enterprise-grade observability and governance platform for multi-agent AI deployments.**

Project Sentinel provides real-time token tracking, latency auditing, and anomaly detection to prevent runaway costs and recursive prompt loops across fleets of AI agents.

---

## Architecture Overview

```
AI Agent SDKs ──► Ingestion Gateway (FastAPI) ──► Kafka Event Bus
                                                       │
                                    ┌──────────────────┼──────────────────┐
                                    ▼                                      ▼
                           Anomaly Engine (Flink)                  TimescaleDB Storage
                                    │                                      │
                                    ▼                                      ▼
                           Governance Engine                      Governance Dashboard
                                    │
                          ┌─────────┼─────────┐
                          ▼                   ▼
                   Circuit Breaker     Notifications
                     (Redis)         (Slack / PagerDuty)
```

### Data Flow

1. **Agent SDKs** submit telemetry batches via `POST /v1/telemetry`
2. **Ingestion Gateway** authenticates, validates, rate-limits, and produces events to Kafka
3. **Anomaly Engine** consumes telemetry, maintains sliding windows, and detects anomalies
4. **Governance Engine** evaluates anomalies against policies and triggers circuit breakers
5. **Notification Service** delivers alerts to Slack and PagerDuty with retry guarantees

---

## Features

| Feature | Description |
|---------|-------------|
| **Telemetry Ingestion** | Async REST API with p99 < 15ms, atomic batch production, idempotent submissions |
| **Anomaly Detection** | Infinite loop detection, prompt cascade detection, token spike detection (Z-score) |
| **Circuit Breakers** | Automated kill-switches with Redis state, TTL auto-deactivation, fail-open behavior |
| **Governance Policies** | Configurable soft (80%) and hard (100%) thresholds per organization |
| **Rate Limiting** | Per-agent and per-organization sliding window limits via Redis sorted sets |
| **Notifications** | Multi-channel delivery (Slack, PagerDuty) with exponential backoff retry |
| **Observability** | Health endpoints, structured logging, audit trails |

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| API Framework | FastAPI + Uvicorn |
| Event Streaming | Apache Kafka |
| State Store | Redis 7.2 |
| Time-Series DB | TimescaleDB (PostgreSQL) |
| Stream Processing | Apache Flink |
| Validation | Pydantic v2 |
| Testing | pytest + Hypothesis (property-based) |
| Containerization | Docker Compose |

---

## Project Structure

```
project-sentinel/
├── src/
│   ├── gateway/          # Ingestion Gateway (FastAPI)
│   │   ├── app.py           # Application factory + lifespan
│   │   ├── routes.py        # POST /v1/telemetry endpoint
│   │   ├── auth.py          # Bearer token authentication
│   │   ├── middleware.py    # Circuit breaker enforcement
│   │   ├── validation.py    # Payload validation pipeline (422 responses)
│   │   ├── kafka_producer.py    # Async Kafka producer (acks=1)
│   │   ├── kafka_consumer.py    # Abstract base consumer class
│   │   ├── redis_service.py     # Redis client with fail-open behavior
│   │   ├── circuit_breaker.py   # Circuit breaker state management
│   │   ├── rate_limiter.py      # Sliding window rate limiting
│   │   ├── deduplication.py     # Batch idempotency (batch_id dedup)
│   │   ├── event_buffer.py      # In-memory buffer for Kafka failures
│   │   ├── health.py            # Health check endpoints
│   │   └── dependencies.py      # FastAPI dependency injection
│   ├── anomaly/          # Anomaly Detection Engine
│   ├── governance/       # Policy Evaluation & Circuit Breakers
│   ├── notifications/    # Webhook Notification Service
│   ├── models/           # Shared Pydantic Models
│   │   ├── telemetry.py      # TelemetryEvent, TelemetryBatch
│   │   ├── anomaly.py        # AnomalyEvent, AnomalyType, Severity
│   │   ├── governance.py     # GovernancePolicy, CircuitBreakerState
│   │   ├── responses.py      # API response models
│   │   └── window.py         # WindowState dataclass
│   └── config/           # Configuration & Settings
│       └── settings.py       # Pydantic-settings (env vars)
├── tests/                # 349+ unit tests
├── migrations/           # TimescaleDB SQL migrations
├── scripts/              # Kafka topic creation script
├── docker-compose.yml    # Local dev infrastructure
├── pyproject.toml        # Python project config
└── .env.example          # Environment variable template
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/asheth2310/project-sentinel.git
cd project-sentinel
```

### 2. Set Up Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate it
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# Install dependencies (including dev tools)
pip install -e ".[dev]"
```

### 3. Start Infrastructure

```bash
# Start Kafka, Redis, TimescaleDB, and Zookeeper
docker compose up -d

# Wait for services to be healthy
docker compose ps
```

### 4. Create Kafka Topics

```bash
python scripts/create_topics.py
```

### 5. Run Database Migrations

```bash
python migrations/run_migrations.py
```

### 6. Configure Environment

```bash
# Copy the example env file
cp .env.example .env

# Edit .env if needed (defaults work for local Docker setup)
```

### 7. Start the Gateway

```bash
uvicorn src.gateway.app:app --reload --host 0.0.0.0 --port 8000
```

The API is now available at `http://localhost:8000`.

- **OpenAPI docs:** http://localhost:8000/docs
- **Health check:** http://localhost:8000/health

---

## API Reference

### Telemetry Ingestion

```http
POST /v1/telemetry
Authorization: Bearer <api_key>
Content-Type: application/json
```

**Request body:**
```json
{
  "agent_id": "550e8400-e29b-41d4-a716-446655440000",
  "org_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "sdk_version": "1.2.0",
  "batch_id": "unique-batch-uuid",
  "events": [
    {
      "timestamp": "2024-01-15T10:30:00Z",
      "agent_id": "550e8400-e29b-41d4-a716-446655440000",
      "org_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "prompt_tokens": 1500,
      "completion_tokens": 800,
      "total_cost": "0.003400",
      "latency_ms": 245,
      "tool_name": "web_search"
    }
  ]
}
```

**Responses:**

| Status | Meaning |
|--------|---------|
| `202 Accepted` | Batch queued for processing |
| `401 Unauthorized` | Missing or invalid API key |
| `403 Forbidden` | Agent ID mismatch with token |
| `422 Unprocessable Entity` | Validation errors (structured) |
| `429 Too Many Requests` | Rate limit exceeded or circuit breaker active |
| `503 Service Unavailable` | Kafka broker down and buffer full |

### Health Checks

```http
GET /health          # Combined status (healthy/degraded/unhealthy)
GET /health/live     # Liveness probe (always 200 if process up)
GET /health/ready    # Readiness probe (checks Kafka, Redis, DB)
```

---

## Governance Policies

Configure per-organization thresholds:

```json
{
  "org_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "thresholds": [
    {
      "metric": "total_tokens",
      "soft_limit": 800000,
      "hard_limit": 1000000,
      "window_seconds": 3600,
      "cooldown_seconds": 300
    },
    {
      "metric": "consecutive_identical_calls",
      "soft_limit": 5,
      "hard_limit": 10,
      "window_seconds": 60,
      "cooldown_seconds": 60
    }
  ],
  "notification_channels": [
    {"type": "slack", "webhook_url": "https://hooks.slack.com/..."},
    {"type": "pagerduty", "routing_key": "abc123..."}
  ],
  "auto_kill_enabled": true
}
```

**Threshold behavior:**
- **Soft limit (80%)** → Warning notification sent
- **Hard limit (100%)** → Circuit breaker activated (kill-switch)
- **auto_kill_enabled: false** → Hard breaches produce CRITICAL warnings instead

---

## Anomaly Detection

### Infinite Loop Detection
Detects agents stuck calling the same tool repeatedly.
- Tracks consecutive identical tool calls per sliding window
- Triggers at configurable threshold (default: 10 calls)
- Soft warning at 50% of threshold

### Prompt Cascade Detection
Detects exponential token growth from recursive prompts.
- Computes token growth rate (tokens/second) within window
- Triggers when growth rate exceeds configured threshold

### Token Spike Detection
Detects sudden statistical spikes using Z-score analysis.
- Requires 2+ prior events for meaningful detection
- Triggers when Z-score exceeds threshold (default: 3.0)
- Severity scales with Z-score magnitude

---

## Circuit Breakers

Circuit breakers prevent runaway agents from consuming resources:

```
Agent active → Anomaly detected → Threshold breached → Circuit breaker activated
                                                              │
                                                              ▼
                                         Agent blocked (429 on all requests)
                                                              │
                                              ┌───────────────┼───────────────┐
                                              ▼                               ▼
                                     TTL auto-expires                  Manual deactivation
                                                                      (authorized user)
```

- **Atomic state**: Single Redis key per agent (no partial state)
- **Fail-open**: If Redis is down, agents continue operating
- **Audit trail**: All activations/deactivations logged
- **TTL support**: Auto-deactivation after configured period

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Run specific test file
pytest tests/test_telemetry_model.py

# Run property-based tests only
pytest tests/test_property_* -v
```

**Test coverage areas:**
- Payload validation (all field constraints)
- Anomaly detection algorithms
- Circuit breaker state machine
- Rate limiting sliding windows
- Kafka producer/consumer behavior
- Redis fail-open semantics
- API endpoint integration tests
- Batch deduplication (idempotency)

---

## Configuration

All settings are configurable via environment variables. See `.env.example` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_PORT` | `8000` | Server port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REDIS_HOST` | `localhost` | Redis host |
| `DB_HOST` | `localhost` | TimescaleDB host |
| `ANOMALY_LOOP_THRESHOLD` | `10` | Consecutive calls to trigger loop detection |
| `ANOMALY_SPIKE_Z_THRESHOLD` | `3.0` | Z-score threshold for spike detection |
| `ANOMALY_WINDOW_DURATION_SECONDS` | `60` | Sliding window size |
| `GOVERNANCE_COOLDOWN_SECONDS` | `300` | Cooldown between threshold triggers |

---

## Docker Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| Kafka | confluentinc/cp-kafka:7.6.0 | 9092 | Event streaming |
| Zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 | Kafka coordination |
| Redis | redis:7.2.4-alpine | 6379 | Circuit breaker state, rate limiting |
| TimescaleDB | timescale/timescaledb:2.13.1-pg16 | 5432 | Time-series storage |

---

## Correctness Properties

The system guarantees these properties:

1. **Ingestion Atomicity** — Either all events in a batch are produced to Kafka or none are
2. **Circuit Breaker Consistency** — Active breaker → all requests from that agent rejected
3. **Threshold Monotonicity** — Kill-switch never activated without warning first
4. **Window Temporal Bounds** — All events in a window are within the configured duration
5. **Notification Delivery Guarantee** — Critical anomalies always trigger delivery attempts
6. **Idempotent Ingestion** — Same batch_id processed multiple times produces same result
7. **Kill-Switch Reversibility** — Every breaker can be deactivated via user action or TTL

---

## Roadmap

- [x] Ingestion Gateway (auth, validation, Kafka production)
- [x] Circuit Breaker enforcement at ingestion
- [x] Rate limiting (per-agent, per-organization)
- [x] Batch deduplication (idempotency)
- [x] Health check endpoints
- [x] Data models and database schema
- [ ] Sliding window aggregation engine
- [ ] Anomaly detection algorithms (loop, cascade, spike)
- [ ] Governance policy evaluation
- [ ] Notification service (Slack, PagerDuty)
- [ ] Query API for dashboard
- [ ] Property-based tests (Hypothesis)
- [ ] Integration test suite

---

## License

MIT

---

## Author

**Aagam Kalpesh Sheth**
