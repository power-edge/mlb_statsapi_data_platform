# MLB Data Platform - UI Tools Guide

## Overview

The MLB Data Platform includes several UI tools for data exploration, visualization, and monitoring.

## Available Tools

### 1. pgAdmin - PostgreSQL Database Administration
**Purpose**: Explore database schema, run SQL queries, manage tables

- **URL**: http://localhost:5050
- **Credentials**:
  - Email: `admin@example.com`
  - Password: `admin`

**First Time Setup**:
1. Open http://localhost:5050
2. Login with the credentials above
3. Right-click "Servers" → "Register" → "Server"
4. General tab: Name = `MLB Data Platform`
5. Connection tab:
   - Host: `postgres` (or `localhost` if accessing from host machine)
   - Port: `5432`
   - Maintenance database: `mlb_games`
   - Username: `mlb_admin`
   - Password: `mlb_dev_password`
6. Click "Save"

**What to Explore**:
- `metadata` schema - Track schema versions and job executions
- `season` schema - MLB season data
- `schedule` schema - Game schedules
- `game` schema - Live game data (raw + 22 normalized tables)

---

### 2. Metabase - Data Visualization & Business Intelligence
**Purpose**: Create dashboards, visualize data, build reports

- **URL**: http://localhost:3000
- **First Time Setup Required**: Yes (will prompt on first visit)

**Initial Setup Steps**:
1. Open http://localhost:3000 (may take 1-2 minutes to initialize)
2. Create your admin account
3. Choose "I'll add my data later" OR:
4. Add database connection:
   - Database type: `PostgreSQL`
   - Name: `MLB Games`
   - Host: `postgres`
   - Port: `5432`
   - Database name: `mlb_games`
   - Username: `mlb_admin`
   - Password: `mlb_dev_password`

**Dashboard Ideas**:
- Games by season/date
- Team performance metrics
- Pitch type distribution
- Live game scoreboard
- Player statistics

---

### 3. RedisInsight - Redis Cache Monitoring
**Purpose**: Monitor cache performance, view cached data

- **URL**: http://localhost:8001
- **No login required**

**First Time Setup**:
1. Open http://localhost:8001
2. Click "Add Redis Database"
3. Choose "Connect to a Redis Database"
4. Connection details:
   - Host: `redis`
   - Port: `6379`
   - Database Alias: `MLB Cache`
   - Password: `mlb_dev_password`
5. Click "Add Redis Database"

**What to Monitor**:
- Cache hit/miss rates
- Cached game data
- Key expiration times
- Memory usage

---

### 4. MinIO Console - Object Storage (S3-compatible)
**Purpose**: Browse archived data, manage buckets

- **URL**: http://localhost:9001
- **Credentials**:
  - Username: `mlb_admin`
  - Password: `mlb_dev_password`

**Pre-created Buckets**:
- `raw-data` - Raw API responses in Avro format
- `processed-data` - Transformed data
- `archived-data` - Historical game archives

---

## Quick Commands

### Start all services (including UI tools):
```bash
docker compose --profile tools up -d
```

### Start only core services (no UI tools):
```bash
docker compose up -d
```

### Stop all services:
```bash
docker compose --profile tools down
```

### Check service status:
```bash
docker compose --profile tools ps
```

### View logs:
```bash
docker compose logs -f pgadmin    # pgAdmin logs
docker compose logs -f metabase   # Metabase logs
```

---

## Data Exploration Workflow

### 1. Ingest Data
```bash
# Ingest season data
uv run mlb-etl ingest \
  --job config/jobs/season_daily.yaml \
  --stub-mode replay \
  --save \
  --db-password mlb_dev_password

# Check ingested data
docker compose exec postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT id, captured_at, data->'seasons'->0->>'seasonId' as season FROM season.seasons;"
```

### 2. Explore in pgAdmin
- Navigate to `mlb_games` database
- Browse schemas: `metadata`, `season`, `schedule`, `game`
- Run queries using the Query Tool

### 3. Visualize in Metabase
- Create a new question
- Choose your database and table
- Build visualizations
- Save to dashboard

### 4. Monitor Cache in RedisInsight
- View real-time cache keys
- Monitor memory usage
- Inspect cached values

---

## Sample Queries

### Count of ingested data by schema:
```sql
SELECT
    'season.seasons' as table_name,
    COUNT(*) as count
FROM season.seasons
UNION ALL
SELECT 'schedule.schedule', COUNT(*) FROM schedule.schedule
UNION ALL
SELECT 'game.live_game_v1', COUNT(*) FROM game.live_game_v1;
```

### Latest captured season data:
```sql
SELECT
    id,
    captured_at,
    data->'seasons'->0->>'seasonId' as season_id,
    data->'seasons'->0->>'regularSeasonStartDate' as start_date,
    data->'seasons'->0->>'regularSeasonEndDate' as end_date
FROM season.seasons
ORDER BY captured_at DESC
LIMIT 1;
```

### Schema version tracking:
```sql
SELECT
    endpoint,
    method,
    version,
    effective_date,
    notes
FROM metadata.schema_versions
ORDER BY effective_date DESC;
```

---

## Troubleshooting

### pgAdmin won't connect to database:
- Use `postgres` as hostname if connecting from within Docker network
- Use `localhost` if pgAdmin is installed locally
- Verify credentials: `mlb_admin` / `mlb_dev_password`

### Metabase is slow to start:
- Metabase takes 1-2 minutes to initialize on first run
- Check logs: `docker compose logs -f metabase`
- Wait for "Metabase Initialization Complete" message

### RedisInsight connection issues:
- Verify Redis is running: `docker compose ps redis`
- Check password is correct: `mlb_dev_password`
- Try connecting with redis-cli: `docker compose exec redis redis-cli -a mlb_dev_password`

---

## Next Steps

1. **Ingest more data**: Run schedule and game ingestion jobs
2. **Create dashboards**: Build Metabase dashboards for key metrics
3. **Set up alerts**: Configure Metabase alerts for data quality
4. **Explore schema**: Use pgAdmin to understand the data model

For more information, see:
- [README.md](./README.md) - Project overview
- [CLAUDE.md](./CLAUDE.md) - Development guide
- [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md) - Schema documentation
