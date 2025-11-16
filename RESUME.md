# Quick Resume Guide

**Last Session**: 2025-11-15
**Status**: ✅ Phase 1 Complete - Raw Ingestion Layer Working

---

## What We Built

A complete **raw data ingestion layer** for MLB Stats API data:

1. ✅ PostgreSQL raw tables (6 endpoints)
2. ✅ SQLModel ORM models
3. ✅ RawStorageClient for ingestion
4. ✅ Working example with stub data
5. ✅ Full replay capability

**Architecture**: PostgreSQL as single source of truth (no MinIO/S3), append-only versioned storage

---

## Quick Start (30 seconds)

```bash
cd ~/github.com/power-edge/mlb_statsapi_data_platform

# 1. Start infrastructure
docker compose up -d

# 2. Run example
uv run python examples/raw_ingestion_example.py

# 3. Query data
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT game_pk, captured_at, status_code FROM game.live_game_v1_raw ORDER BY captured_at DESC LIMIT 5;"
```

**Expected Output**:
```
✓ Loaded game_pk=747175
✓ Saved to game.live_game_v1_raw
✓ Found 2 version(s) of game 747175
✓ Extracted from JSONB: Arizona Diamondbacks vs Toronto Blue Jays (Final)
```

---

## Files Modified/Created

### Created (NEW)
- `sql/migrations/V3__raw_tables.sql` - Raw table DDL
- `src/mlb_data_platform/models/raw.py` - ORM models
- `src/mlb_data_platform/ingestion/raw_storage.py` - Storage client
- `examples/raw_ingestion_example.py` - Working example
- `PROGRESS.md` - Detailed progress doc
- `RESUME.md` - This file

### Modified
- `src/mlb_data_platform/models/__init__.py` - Export raw models
- `src/mlb_data_platform/ingestion/__init__.py` - Export RawStorageClient

---

## Database Schema

### Raw Tables Created

```sql
-- All with composite PK: (entity_id, captured_at)
game.live_game_v1_raw       -- Live game feeds
game.live_game_diff_raw     -- Diff patches
schedule.schedule_raw       -- Schedule data
season.seasons_raw          -- Season metadata
person.person_raw           -- Player data
team.team_raw               -- Team data

-- Metadata
meta.transformation_checkpoints  -- ETL tracking
```

### Table Structure Example

```sql
CREATE TABLE game.live_game_v1_raw (
    game_pk INTEGER NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL,          -- Full API response
    endpoint TEXT NOT NULL,        -- "game"
    method TEXT NOT NULL,          -- "liveGameV1"
    params JSONB,                  -- {"game_pk": 747175}
    url TEXT NOT NULL,             -- API URL
    status_code INTEGER NOT NULL,  -- HTTP status
    PRIMARY KEY (game_pk, captured_at)
);
```

---

## API Usage

### Store Data

```python
from mlb_data_platform.ingestion import RawStorageClient
from mlb_data_platform.database import get_session

# Response from pymlb_statsapi (includes metadata)
response = {
    "data": {...},  # Full API response
    "metadata": {
        "endpoint": "game",
        "method": "liveGameV1",
        "params": {"game_pk": 747175},
        "url": "https://...",
        "status_code": 200,
        "captured_at": "2024-11-15T20:30:00Z"
    }
}

storage = RawStorageClient()
with get_session() as session:
    raw_game = storage.save_live_game(session, response)
    print(f"Saved: {raw_game.game_pk} at {raw_game.captured_at}")
```

### Query Data

```python
from mlb_data_platform.models import RawLiveGameV1
from mlb_data_platform.database import get_session
from sqlmodel import select

with get_session() as session:
    # Get latest version
    latest = storage.get_latest_live_game(session, game_pk=747175)

    # Get all versions (history)
    history = storage.get_game_history(session, game_pk=747175, limit=10)

    # Query by date range
    from datetime import datetime, timezone
    cutoff = datetime(2024, 11, 1, tzinfo=timezone.utc)
    stmt = select(RawLiveGameV1).where(RawLiveGameV1.captured_at >= cutoff)
    recent = session.exec(stmt).all()
```

### JSONB Queries

```python
from sqlalchemy import text

with get_session() as session:
    stmt = text("""
        SELECT
            game_pk,
            data->'gameData'->'status'->>'abstractGameState' as game_state,
            data->'gameData'->'teams'->'home'->>'name' as home_team
        FROM game.live_game_v1_raw
        WHERE game_pk = :game_pk
        ORDER BY captured_at DESC
        LIMIT 1
    """)
    result = session.exec(stmt.bindparams(game_pk=747175))
    row = result.first()
```

---

## Next Steps (Prioritized)

### 🔴 High Priority

1. **Add upsert logic** to RawStorageClient
   - Use `ON CONFLICT DO UPDATE` for idempotency
   - Prevent PK errors on re-runs

### 🟡 Medium Priority

2. **Expand endpoint coverage**
   - Schedule, Seasons, Person, Team endpoints
   - Create example scripts for each

3. **Real ingestion jobs**
   - Daily schedule job
   - Live game polling
   - Historical backfill

4. **Transformation layer**
   - Read from raw → transform → normalized tables
   - Use transformation checkpoints

### 🟢 Low Priority

5. **Monitoring/observability**
6. **Performance optimization**

---

## Common Commands

```bash
# Development
uv sync                              # Install dependencies
uv run pytest                        # Run tests
ruff check --fix . && ruff format .  # Lint + format

# Infrastructure
docker compose up -d                 # Start services
docker compose down                  # Stop services
docker compose logs -f postgres      # View logs

# Database
docker compose exec -T postgres psql -U mlb_admin -d mlb_games

# Query examples
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT COUNT(*) FROM game.live_game_v1_raw;"

docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT game_pk, captured_at, endpoint, method FROM game.live_game_v1_raw ORDER BY captured_at DESC LIMIT 5;"

# Clear data (for testing)
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "TRUNCATE TABLE game.live_game_v1_raw CASCADE;"

# Run example
uv run python examples/raw_ingestion_example.py
```

---

## Verification Checklist

After resuming on new machine:

- [ ] Repository cloned
- [ ] `uv sync` completed
- [ ] Docker Compose running (`docker compose ps`)
- [ ] Migrations applied (check `\dt *.*_raw`)
- [ ] Models import: `uv run python -c "from mlb_data_platform.models import RawLiveGameV1; print('✓')"`
- [ ] Client imports: `uv run python -c "from mlb_data_platform.ingestion import RawStorageClient; print('✓')"`
- [ ] Example runs: `uv run python examples/raw_ingestion_example.py`

---

## Key Decisions Made

1. **PostgreSQL as single source of truth** - No MinIO/S3 for raw data
2. **Append-only versioning** - Composite PK with timestamp for full history
3. **JSONB storage** - Complete API responses, queryable with PostgreSQL operators
4. **Stub-based testing** - Use pymlb_statsapi stubs for deterministic tests
5. **Modern Python tooling** - `uv` for package management, `ruff` for linting

---

## Resources

- **Full Progress**: See `PROGRESS.md` for detailed work log
- **Architecture**: See `CLAUDE.md` for complete system design
- **Schema Mapping**: See `SCHEMA_MAPPING.md` for endpoint → table mapping
- **pymlb_statsapi**: https://github.com/power-edge/pymlb_statsapi

---

## Contact

**Developer**: Nikolaus Schuetz (@nikolauspschuetz)
**Project**: MLB Stats API Data Platform
**Workspace**: `~/github.com/power-edge/mlb_statsapi_data_platform`

---

**Status**: 🎉 Phase 1 Complete - Ready for Phase 2 (Transformations)
