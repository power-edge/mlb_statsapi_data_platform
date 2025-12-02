# MLB StatsAPI Data Platform

**Kubernetes-native data platform** for ingesting, transforming, and serving MLB Stats API data at scale.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

## 🎯 Overview

A production-grade ETL/data platform for MLB Stats API built with:

- **🐍 Python 3.11+** with modern tooling (`uv`, `hatch`, `ruff`)
- **⚡ PySpark 3.5+** for batch and streaming transformations
- **🐘 PostgreSQL** as the data mart (partitioned, indexed, optimized)
- **🗃️ MinIO** for S3-compatible object storage (raw data layer)
- **📊 Redis** for caching live game data
- **🔄 Argo Workflows** for orchestration (DAG-based pipelines)
- **☸️ Kubernetes** as the deployment target (Helm charts)
- **📝 Avro schemas** for schema evolution and versioning
- **🧪 pytest + behave** for comprehensive testing with stubs

## 🏗️ Architecture

```
MLB Stats API
    ↓
[Ingestion Layer] → pymlb_statsapi client
    ↓
[Raw Storage] → MinIO (S3-compatible) + PostgreSQL (raw JSONB tables)
    ↓
[Transform Layer] → PySpark (batch + streaming)
    ↓
[Data Mart] → PostgreSQL (normalized relational tables)
    ↓
[Cache Layer] → Redis (live game data)
    ↓
[Orchestration] → Argo Workflows (Season → Schedule → Game → Player)
```

### Two-Tier Data Architecture

#### Layer 1: RAW Data Layer
- Complete JSON responses stored in PostgreSQL with JSONB columns
- One raw table per `Endpoint.method()` (e.g., `game.live_game_v1`)
- Metadata: `captured_at`, `schema_version`, `source_url`, `request_params`
- Partitioned by timestamp for performance

#### Layer 2: NORMALIZED Data Layer
- Flattened relational tables extracted from raw JSONB
- Example: `game.live_game_v1` → 17 normalized tables
  - `game.live_game_metadata` - Top-level game info
  - `game.live_game_plays` - All plays (at-bats)
  - `game.live_game_pitch_events` - Every pitch thrown
  - `game.live_game_players` - All players in the game
  - ... and 13 more tables

See [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md) for complete endpoint/method → table mapping.

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **uv** (modern Python package manager): `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **K3d** (local Kubernetes): `brew install k3d` (macOS) or see [k3d.io](https://k3d.io)
- **kubectl** (Kubernetes CLI): `brew install kubectl` (macOS)
- **Docker Desktop** (for K3d)

### Local Development Setup (K3d Cluster)

```bash
# Clone repository
git clone https://github.com/power-edge/mlb_statsapi_data_platform.git
cd mlb_statsapi_data_platform

# Install dependencies
uv sync

# Create local K3d cluster (mlb-data-platform-local)
make cluster-create

# Deploy infrastructure (PostgreSQL, Redis, MinIO)
make cluster-deploy

# Initialize database schemas
make db-init

# Check cluster status
make cluster-status

# OR: Do it all in one command
make cluster-full

# Test ingestion (with stubs)
uv run mlb-etl ingest \
  --job config/jobs/season_daily.yaml \
  --stub-mode replay \
  --save \
  --db-host localhost \
  --db-port 65254

# Query data
kubectl exec -it -n mlb-data-platform deployment/postgres -- \
  psql -U mlb_admin -d mlb_games \
  -c "SELECT * FROM schedule.schedule ORDER BY captured_at DESC LIMIT 5;"

# Or connect directly
make db-connect
```

**Access Services (MLB-specific 652XX ports):**
- PostgreSQL: `localhost:65254` (user: `mlb_admin`, password: `mlb_admin_password`)
- MinIO API: `http://localhost:65290` (user: `minioadmin`, password: `minioadmin`)
- MinIO Console: `http://localhost:65291`
- Redis: `localhost:65263`

> **Note**: Ports use 652XX range (652 = MLB) to avoid conflicts with other local services

**Cluster Management:**
```bash
make cluster-stop    # Stop cluster when not in use
make cluster-start   # Start cluster again
make cluster-delete  # Remove cluster entirely
```

### Kubernetes Deployment (Local k3s)

```bash
# 1. Verify k3s cluster is running
kubectl cluster-info

# 2. Build and import ingestion image
docker build -f docker/Dockerfile.ingestion -t mlb-data-platform/ingestion:latest .
docker save mlb-data-platform/ingestion:latest | sudo k3s ctr images import -

# 3. Submit workflow
argo submit config/workflows/workflow-pipeline-daily.yaml -n mlb-data-platform --watch

# 4. Access Argo UI
kubectl port-forward svc/argo-server 2746:2746 -n argo
# Open http://localhost:2746
```

## 📊 Data Workflows

### Season → Schedule → Game Pipeline

```yaml
# Runs daily at midnight UTC
1. Fetch season data (Season.seasons)
2. Fetch today's schedule (Schedule.schedule)
3. For each scheduled game:
   a. Fetch live game data (Game.liveGameV1)
   b. Fetch player data (Person.person)
   c. Fetch team data (Team.team)
4. Transform raw data → normalized tables
5. Update materialized views
```

### Live Game Streaming

```yaml
# Polls every 30 seconds during active games
1. Detect active games (status="Live")
2. Poll Game.liveGameV1 for each game_pk
3. Cache in Redis (TTL=60s)
4. Stream to MinIO (timestamped snapshots)
5. Incremental merge to PostgreSQL
6. On game completion: Archive to S3
```

## 🗂️ Project Structure

```
mlb_statsapi_data_platform/
├── src/mlb_data_platform/       # Python application
│   ├── ingestion/                # API client + job runner
│   ├── schema/                   # Avro schema management
│   ├── storage/                  # MinIO, PostgreSQL, Redis clients
│   ├── transform/                # PySpark batch/streaming jobs
│   └── orchestration/            # Argo Workflows integration
├── config/
│   ├── jobs/                     # Declarative job definitions (YAML)
│   ├── workflows/                # Argo Workflow definitions
│   └── k8s/                      # Kubernetes manifests
├── sql/                          # PostgreSQL migrations + schemas
├── docker/                       # Dockerfiles (ingestion, PySpark)
├── scripts/                      # Helper scripts (deploy, setup)
├── tests/                        # pytest + behave (BDD with stubs)
└── docs/                         # Documentation
```

## 💾 Database Usage

### Context Manager API

The platform provides a clean, Pythonic API for database operations with automatic resource management:

```python
from mlb_data_platform.database import get_session, DatabaseConfig
from mlb_data_platform.models import LiveGameMetadata
from sqlmodel import select

# Basic usage (uses local development defaults)
with get_session() as session:
    game = LiveGameMetadata(
        game_pk=747175,
        game_date=date(2024, 10, 25),
        home_team_name="Los Angeles Dodgers",
        away_team_name="New York Yankees",
    )
    session.add(game)
    # Automatically commits on exit

# Query with type safety
with get_session() as session:
    games = session.exec(select(LiveGameMetadata)).all()
    for game in games:
        print(f"{game.away_team_name} @ {game.home_team_name}")

# Configuration from environment variables
config = DatabaseConfig.from_env()
with get_session(config) as session:
    games = session.exec(select(LiveGameMetadata)).all()

# Custom configuration
prod_config = DatabaseConfig(
    host="prod-db.example.com",
    port=5432,
    database="mlb_games",
    user="mlb_admin",
    password="***",
    pool_size=20,
)
with get_session(prod_config) as session:
    games = session.exec(select(LiveGameMetadata)).all()
```

**Features:**
- ✅ Automatic session cleanup
- ✅ Automatic commits on success
- ✅ Automatic rollback on errors
- ✅ Connection pooling (engines cached per connection URL)
- ✅ Type-safe queries with SQLModel
- ✅ Environment-aware configuration

See [docs/DATABASE_CONTEXT_MANAGERS.md](./docs/DATABASE_CONTEXT_MANAGERS.md) for complete documentation and examples.

## 🛠️ Development

### Code Quality

```bash
# Linting
ruff check .
ruff format .

# Type checking
mypy src/

# Security scanning
bandit -r src/

# Run all checks
pre-commit run --all-files
```

### Testing

```bash
# Unit tests
pytest

# BDD tests with stubs (fast, deterministic)
behave

# Integration tests (requires Docker)
pytest tests/integration/

# End-to-end pipeline test
./scripts/test_pipeline.sh
```

### Building

```bash
# Build Docker images
docker build -f docker/Dockerfile.ingestion -t mlb-data-platform-ingestion:latest .
docker build -f docker/Dockerfile.spark -t mlb-data-platform-spark:latest .

# Build Python wheel
uv build

# Build Helm chart
helm package helm/mlb-data-platform/
```

## 📖 Documentation

- **[SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md)** - Complete endpoint → table mapping
- **[CLAUDE.md](./CLAUDE.md)** - Architecture and operations guide
- **[docs/deployment.md](./docs/deployment.md)** - Deployment guide
- **[docs/workflows.md](./docs/workflows.md)** - Argo Workflows guide
- **[docs/data_model.md](./docs/data_model.md)** - PostgreSQL schema documentation

## 🔑 Key Features

### Schema-Driven Design
- All table structures generated from pymlb_statsapi JSON schemas
- Automatic schema evolution handling
- Version tracking for API changes

### Configuration-Driven ETL
- Declarative job definitions in YAML
- No code changes for new workflows
- Parameterized job templates

### Production-Ready
- Comprehensive error handling and retry logic
- Observability: structured logging, metrics, traces
- Multi-tenant support with PostgreSQL role templates
- Kubernetes-native secrets management

### Testing Strategy
- **Stub-based testing** using pymlb_statsapi captured responses
- BDD scenarios for all critical workflows
- Integration tests with ephemeral Kubernetes clusters
- End-to-end pipeline validation

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run code quality checks (`pre-commit run --all-files`)
5. Commit with conventional commits (`feat:`, `fix:`, `docs:`, etc.)
6. Push and create a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built on [pymlb_statsapi](https://github.com/power-edge/pymlb_statsapi) - Schema-driven MLB Stats API client
- Powered by [MLB Stats API](https://statsapi.mlb.com/)
- Inspired by modern data platform patterns

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/power-edge/mlb_statsapi_data_platform/issues)
- **Discussions**: [GitHub Discussions](https://github.com/power-edge/mlb_statsapi_data_platform/discussions)

---

**Made with ❤️ by Nikolaus Schuetz**
