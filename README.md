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
- **Docker** (for local development)
- **Kubernetes cluster** (for deployment)
  - Tested with K3s on Hetzner Cloud
  - Works with any Kubernetes 1.25+

### Local Development Setup

```bash
# Clone repository
git clone https://github.com/power-edge/mlb_statsapi_data_platform.git
cd mlb_statsapi_data_platform

# Install dependencies
uv sync

# Start local infrastructure (PostgreSQL, Redis, MinIO)
docker compose up -d

# Initialize database schemas
./scripts/init_database.sh

# Run ingestion job (with stubs for testing)
uv run mlb-etl ingest --job config/jobs/season_daily.yaml --stub-mode replay

# Run PySpark transform
uv run mlb-etl transform --job config/jobs/season_daily.yaml

# Query data
psql -h localhost -U mlb_admin -d mlb_games \
  -c "SELECT * FROM schedule.schedule ORDER BY captured_at DESC LIMIT 5;"
```

### Kubernetes Deployment

```bash
# 1. Bootstrap Terraform (creates AWS S3 state backend + secrets)
./scripts/bootstrap_terraform.sh

# 2. Deploy to Kubernetes
./scripts/deploy.sh dev  # or 'prod'

# 3. Access services
kubectl port-forward svc/postgresql 5432:5432 -n mlb-data-platform
kubectl port-forward svc/minio 9000:9000 -n mlb-data-platform
kubectl port-forward svc/argo-server 2746:2746 -n mlb-data-platform

# 4. Submit workflow
argo submit workflows/season-daily-pipeline.yaml -n mlb-data-platform --watch
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
│   └── schemas/                  # Avro schemas + hierarchical mappings
├── helm/mlb-data-platform/       # Helm chart with sub-charts
├── terraform/                    # AWS state backend + secrets only
├── sql/                          # PostgreSQL migrations + schemas
├── docker/                       # Dockerfiles (ingestion, PySpark)
├── scripts/                      # Helper scripts (deploy, setup)
├── tests/                        # pytest + behave (BDD with stubs)
└── docs/                         # Documentation
```

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
- Secrets management via AWS Secrets Manager + External Secrets Operator

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
