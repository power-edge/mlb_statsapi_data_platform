# Build System Documentation

**Complete guide to the one-step build/generate process**

## Overview

We have a **Make-based build system** that automates:
- Code generation (DDL + ORM from YAML)
- Docker Compose local development environment
- Database initialization and migrations
- Testing and code quality checks

## Quick Start

```bash
# Complete setup for new developers (one command!)
make quickstart

# This will:
# 1. Install all dependencies (uv sync)
# 2. Generate DDL migrations from YAML
# 3. Generate ORM models from YAML
# 4. Start Docker Compose (PostgreSQL, Redis, MinIO)
# 5. Initialize database schemas (V1 migration)
# 6. Run DDL migrations (V2 migration with 19 tables)
```

After `make quickstart` completes, you'll have:
- ✅ PostgreSQL running with all 19 tables
- ✅ Redis cache running
- ✅ MinIO S3-compatible storage running
- ✅ All ORM models generated and importable

---

## Architecture

### Code Generation Flow

```
config/schemas/mappings/game/live_game_v1.yaml
                    ↓
            [YAML Schema Mapping]
         (Single Source of Truth)
                    ↓
        ┌───────────┴───────────┐
        ↓                       ↓
[DDL Generator]         [ORM Generator]
        ↓                       ↓
sql/migrations/         src/mlb_data_platform/
V2__game_live_          models/game_live.py
tables.sql
        ↓                       ↓
[PostgreSQL]            [SQLModel Classes]
 19 Tables               20 Classes
```

### Database Migration Flow

```
V1__initial_schema.sql
  ↓ (creates schemas + raw tables)
PostgreSQL Database
  ↓
V2__game_live_tables.sql
  ↓ (creates 19 normalized tables)
Complete Database
```

---

## Makefile Targets

### 🚀 Quick Start

| Command | Description |
|---------|-------------|
| `make quickstart` | Complete setup: install + generate + docker up + init |
| `make help` | Show all available commands |

### 📦 Code Generation

| Command | Description | Output |
|---------|-------------|--------|
| `make generate-all` | Generate DDL + ORM from YAML | Both files below |
| `make generate-ddl` | Generate PostgreSQL migrations | `sql/migrations/V2__game_live_tables.sql` |
| `make generate-orm` | Generate SQLModel ORM classes | `src/mlb_data_platform/models/game_live.py` |

**When to regenerate:**
- After editing `config/schemas/mappings/game/live_game_v1.yaml`
- After adding new fields to tables
- After adding new tables

### 🐳 Docker Compose (Local Development)

| Command | Description |
|---------|-------------|
| `make docker-up` | Start PostgreSQL, Redis, MinIO |
| `make docker-down` | Stop all services |
| `make docker-init` | Initialize database schemas (V1 migration) |
| `make docker-migrate` | Run DDL migrations (V2 migration) |
| `make docker-reset` | **⚠️ DESTROYS DATA** - Reset database |
| `make docker-shell` | Open PostgreSQL shell |
| `make docker-logs` | Show database logs |

**Service URLs:**
- PostgreSQL: `postgresql://mlb_admin:mlb_password@localhost:5432/mlb_games`
- Redis: `redis://localhost:6379`
- MinIO: `http://localhost:9000` (username: `minio`, password: `minio123`)
- PgAdmin: `http://localhost:5050` (optional, run with `--profile tools`)

### 🧪 Testing

| Command | Description |
|---------|-------------|
| `make test` | Run unit tests (pytest) |
| `make test-integration` | Run integration tests (requires database) |
| `make lint` | Run linters (ruff) |
| `make format` | Format code (ruff) |

### 🛠️ Development

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies (`uv sync`) |
| `make clean` | Clean build artifacts |

---

## Generated Files

### 1. DDL Migration (`sql/migrations/V2__game_live_tables.sql`)

**Generated from:** `config/schemas/mappings/game/live_game_v1.yaml`
**Generator:** `src/mlb_data_platform/schema/generator.py`

**Contents:**
- 19 CREATE TABLE statements
- Indexes on primary keys, foreign keys, partition columns
- Comments on tables and columns
- Partitioning support (with validation)

**Example:**
```sql
CREATE TABLE IF NOT EXISTS game.live_game_metadata (
    game_pk BIGINT NOT NULL,
    game_date DATE NOT NULL,
    home_team_name VARCHAR(100),
    ...
    PRIMARY KEY (game_pk)
) PARTITION BY RANGE (game_date);

CREATE INDEX IF NOT EXISTS idx_live_game_metadata_game_date
    ON game.live_game_metadata (game_date);

COMMENT ON TABLE game.live_game_metadata
    IS 'Core game information: teams, venue, status, timing, weather';
```

**Tables created:**
1. `game.live_game_metadata` - Core game info
2. `game.live_game_linescore_innings` - Inning scores
3. `game.live_game_players` - All players
4. `game.live_game_plays` - All at-bats
5. `game.live_game_pitch_events` - Pitches only (`isPitch=true`)
6. `game.live_game_play_actions` - Non-pitch events (`isPitch=false`)
7. `game.live_game_fielding_credits` - Defensive credits (putouts, assists)
8. `game.live_game_runners` - Base runners
9. `game.live_game_scoring_plays` - RBI plays
10-19. Batting orders, pitchers, bench, bullpen, umpires, venue (9 more tables)

### 2. ORM Models (`src/mlb_data_platform/models/game_live.py`)

**Generated from:** `config/schemas/mappings/game/live_game_v1.yaml`
**Generator:** `src/mlb_data_platform/schema/orm_generator.py`

**Contents:**
- 20 SQLModel classes (19 tables + 1 base class)
- Type-safe with Python type hints
- Pydantic validation
- SQLAlchemy 2.0 compatible

**Example:**
```python
class LiveGameMetadata(BaseModel, table=True):
    """Core game information: teams, venue, status, timing, weather"""

    __tablename__ = "live_game_metadata"
    __table_args__ = {"schema": "game"}

    game_pk: int = Field(sa_type=BigInteger, primary_key=True, nullable=False)
    game_date: date = Field(sa_type=Date, nullable=False)
    home_team_name: Optional[str] = Field(sa_type=String(100), default=None)
    ...
```

**Usage:**
```python
from mlb_data_platform.models import LiveGameMetadata

game = LiveGameMetadata(
    game_pk=747175,
    game_date=date(2024, 10, 25),
    home_team_name="Los Angeles Dodgers",
)
```

---

## Workflow Examples

### New Developer Setup

```bash
# Clone repository
git clone <repo-url>
cd mlb_statsapi_data_platform

# One-step setup
make quickstart

# Verify setup
make docker-shell
# Inside PostgreSQL shell:
\dt game.*  # List all game schema tables (should see 19 tables)
```

### Adding a New Field to a Table

```bash
# 1. Edit YAML schema
vim config/schemas/mappings/game/live_game_v1.yaml

# 2. Regenerate DDL and ORM
make generate-all

# 3. Review changes
git diff sql/migrations/V2__game_live_tables.sql
git diff src/mlb_data_platform/models/game_live.py

# 4. Apply migration to local database
make docker-migrate

# 5. Test changes
make test
```

### Resetting Local Database

```bash
# ⚠️ WARNING: This destroys all data!
make docker-reset

# This will:
# 1. Stop Docker Compose
# 2. Delete volumes (destroys data)
# 3. Start Docker Compose
# 4. Run V1 migration (initial schemas)
# 5. Run V2 migration (19 tables)
```

### Daily Development

```bash
# Start database
make docker-up

# ... do development work ...

# Run tests
make test

# Format code
make format

# Stop database
make docker-down
```

---

## Troubleshooting

### "PostgreSQL not ready" Error

```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Check logs
make docker-logs

# Manually test connection
docker compose exec postgres pg_isready -U mlb_admin
```

### "Permission denied" on SQL files

```bash
# Make sure migration files are readable
chmod 644 sql/migrations/*.sql
```

### "Table already exists" Error

```bash
# Reset database completely
make docker-reset
```

### ORM Models Not Importing

```bash
# Regenerate models
make generate-orm

# Verify file exists
ls -la src/mlb_data_platform/models/game_live.py

# Reinstall package
make install
```

---

## Advanced Usage

### Running Optional Services (PgAdmin, RedisInsight)

```bash
# Start with optional GUI tools
docker compose --profile tools up -d

# Access tools:
# - PgAdmin: http://localhost:5050 (admin@mlb.local / admin)
# - RedisInsight: http://localhost:8001
```

### Custom PostgreSQL Queries

```bash
# Open PostgreSQL shell
make docker-shell

# Example queries:
-- List all tables
\dt game.*

-- Count rows in a table
SELECT COUNT(*) FROM game.live_game_metadata;

-- Show table structure
\d game.live_game_metadata
```

### Manual Code Generation

```bash
# Generate DDL only
uv run python -m mlb_data_platform.schema.generator \
    config/schemas/mappings/game/live_game_v1.yaml \
    > sql/migrations/V2__game_live_tables.sql

# Generate ORM only
uv run python -m mlb_data_platform.schema.orm_generator \
    config/schemas/mappings/game/live_game_v1.yaml \
    > src/mlb_data_platform/models/game_live.py
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Build and Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh

      - name: Generate code
        run: make generate-all

      - name: Start services
        run: make docker-up

      - name: Initialize database
        run: make docker-init && make docker-migrate

      - name: Run tests
        run: make test

      - name: Cleanup
        run: make docker-down
```

---

## See Also

- [ORM Usage Examples](./ORM_USAGE_EXAMPLES.md) - How to use generated SQLModel classes
- [TIMESTAMP_STRATEGY.md](./TIMESTAMP_STRATEGY.md) - Defensive upsert strategy
- [ADDITIONAL_TABLES_NEEDED.md](./ADDITIONAL_TABLES_NEEDED.md) - Future table additions
- [DEFENSIVE_UPSERT.md](./DEFENSIVE_UPSERT.md) - Defensive upsert implementation

---

**Made with ❤️ for the baseball data community**
