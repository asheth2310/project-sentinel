# Project Sentinel - Deployment Guide

## Architecture

```
┌─────────────────┐         ┌──────────────────────────┐
│  Vercel          │  HTTP   │  Railway                  │
│  (Dashboard)     │────────►│  (Python Backend)         │
│  React + Vite    │         │  FastAPI + Consumers      │
└─────────────────┘         └──────────┬───────────────┘
                                       │
                            ┌──────────┼───────────────┐
                            │          │               │
                       ┌────▼───┐ ┌────▼───┐ ┌────────▼──┐
                       │ Kafka  │ │ Redis  │ │TimescaleDB│
                       │(Railway│ │(Railway│ │ (Railway  │
                       │add-on) │ │add-on) │ │  add-on)  │
                       └────────┘ └────────┘ └───────────┘
```

## Step 1: Deploy Backend on Railway

1. Push your repo to GitHub
2. Go to [railway.app](https://railway.app) and create a new project
3. Connect your GitHub repo
4. Railway will detect the `Dockerfile` and `railway.toml` automatically
5. Add managed services:
   - **Redis** → click "New Service" → "Redis"
   - **PostgreSQL** → click "New Service" → "PostgreSQL" (TimescaleDB not native, use the Dockerfile-based TimescaleDB template or standard PostgreSQL)
   - **Kafka** → Use Upstash Kafka (Railway plugin) or set `KAFKA_BOOTSTRAP_SERVERS` to an external Kafka/Redpanda cluster

6. Set environment variables on the backend service (Railway will auto-inject database/redis URLs):
   ```
   KAFKA_BOOTSTRAP_SERVERS=<your-kafka-broker>
   REDIS_HOST=<auto-injected by Railway Redis>
   REDIS_PORT=6379
   DB_HOST=<auto-injected by Railway PostgreSQL>
   DB_PORT=5432
   DB_NAME=railway
   DB_USER=postgres
   DB_PASSWORD=<auto-injected>
   APP_DEBUG=false
   APP_LOG_LEVEL=info
   ```

7. After deploy, note your backend URL: `https://project-sentinel-production.up.railway.app`

## Step 2: Deploy Dashboard on Vercel

1. Go to [vercel.com](https://vercel.com) and import your GitHub repo
2. Configure the project:
   - **Root Directory**: `dashboard`
   - **Framework Preset**: Vite
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`

3. Add environment variable:
   ```
   VITE_API_BASE_URL=https://project-sentinel-production.up.railway.app
   ```

4. Update `dashboard/vercel.json` — replace `your-railway-backend.up.railway.app` with your actual Railway URL

5. Deploy!

## Step 3: Run Database Migrations

After Railway PostgreSQL is up:

```bash
# Set connection string
export POSTGRES_HOST=<railway-host>
export POSTGRES_PORT=<railway-port>
export POSTGRES_DB=railway
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=<railway-password>

# Run migrations
python migrations/run_migrations.py
```

## Step 4: Create Kafka Topics

```bash
export KAFKA_BOOTSTRAP_SERVERS=<your-kafka-broker>
python scripts/create_topics.py
```

## Local Development

```bash
# Start infrastructure
docker compose up -d

# Run migrations
python migrations/run_migrations.py

# Create Kafka topics
python scripts/create_topics.py

# Start backend
uvicorn src.gateway.app:app --reload --port 8000

# Start dashboard (in another terminal)
cd dashboard
npm install
npm run dev
```

Dashboard available at http://localhost:3000, API at http://localhost:8000.

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_API_BASE_URL` | (empty) | Backend URL for production dashboard |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka broker addresses |
| `REDIS_HOST` | localhost | Redis hostname |
| `REDIS_PORT` | 6379 | Redis port |
| `DB_HOST` | localhost | TimescaleDB hostname |
| `DB_PORT` | 5432 | TimescaleDB port |
| `DB_NAME` | sentinel_db | Database name |
| `DB_USER` | sentinel | Database user |
| `DB_PASSWORD` | sentinel_dev | Database password |
| `APP_DEBUG` | false | Enable debug mode |
| `APP_LOG_LEVEL` | info | Logging level |
