# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working with this repository.

## Project Overview

**MLB StatsAPI Data Platform** is a Kubernetes-native data platform for ingesting, transforming, and serving MLB Stats API data at scale.

### Key Characteristics
- **Kubernetes-first**: Designed to run entirely in K8s (no AWS services except state/secrets)
- **Schema-driven**: Leverages `pymlb_statsapi` for schema-aware API interactions
- **Two-tier architecture**: Raw JSONB layer + normalized relational layer
- **Configuration over code**: Declarative job definitions in YAML
- **Production-ready**: Comprehensive testing, observability, multi-tenancy support

### Technology Stack
- **Python 3.11+**: Modern Python with `uv` package manager
- **PySpark 3.5+**: Batch and streaming transformations
- **PostgreSQL 15+**: Data mart with partitioning and indexing
- **MinIO**: S3-compatible object storage (in-cluster)
- **Redis 7+**: Caching layer for live game data
- **Argo Workflows**: DAG-based orchestration
- **Helm**: Kubernetes packaging and deployment
- **Terraform**: AWS state backend + secrets management only

---

## Quick Start

### Local Development

```bash
# 1. Setup
uv sync                          # Install dependencies
pre-commit install               # Install git hooks

# 2. Start infrastructure
docker compose up -d             # PostgreSQL, Redis, MinIO

# Wait for services to be healthy
docker compose ps

# 3. Initialize database
./scripts/init_database.sh

# 4. Run CLI
uv run mlb-etl version           # Test CLI
uv run mlb-etl schema list       # List available schemas

# 5. Run ingestion (with stubs for testing)
uv run mlb-etl ingest \
  --job config/jobs/season_daily.yaml \
  --stub-mode replay

# 6. Query data
docker compose exec postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT * FROM schedule.schedule ORDER BY captured_at DESC LIMIT 5;"

# 7. Stop infrastructure
docker compose down
```

### Kubernetes Deployment

```bash
# 1. Bootstrap Terraform (creates AWS S3 state backend + secrets)
./scripts/bootstrap_terraform.sh

# 2. Deploy to K8s
./scripts/deploy.sh dev          # or: prod

# 3. Access services
kubectl port-forward svc/postgresql 5432:5432 -n mlb-data-platform
kubectl port-forward svc/minio 9000:9000 -n mlb-data-platform
kubectl port-forward svc/argo-server 2746:2746 -n mlb-data-platform

# 4. Submit workflow
argo submit helm/mlb-data-platform/templates/workflows/workflow-season-pipeline.yaml \
  --namespace mlb-data-platform \
  --watch

# 5. View logs
argo logs season-pipeline-xxxxx -n mlb-data-platform -f
```

---

## Architecture

### Data Flow

```
MLB Stats API (External)
    ↓
Ingestion Layer (Python + pymlb_statsapi)
    ↓ saves to ↓
RAW Storage (MinIO S3 buckets + PostgreSQL raw tables)
    ↓ reads from ↓
Transform Layer (PySpark batch/streaming jobs)
    ↓ writes to ↓
Data Mart (PostgreSQL normalized tables)
    ↓ cached in ↓
Cache Layer (Redis)
```

### Two-Tier Schema Design

#### Layer 1: RAW Data
- **Purpose**: Store complete API responses with minimal transformation
- **Format**: JSONB columns in PostgreSQL
- **Schema**: `{endpoint}.{method_name}` (e.g., `game.live_game_v1`)
- **Metadata**: `captured_at`, `schema_version`, `source_url`, `request_params`
- **Partitioning**: By `captured_at` timestamp (monthly partitions)

**Example Raw Table:**
```sql
CREATE TABLE game.live_game_v1 (
    id BIGSERIAL PRIMARY KEY,
    game_pk BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    schema_version VARCHAR(10) NOT NULL,
    data JSONB NOT NULL,  -- Complete API response
    INDEX idx_game_pk (game_pk),
    INDEX idx_captured_at (captured_at)
) PARTITION BY RANGE (captured_at);
```

#### Layer 2: NORMALIZED Data
- **Purpose**: Flattened relational tables for efficient querying
- **Format**: Standard PostgreSQL tables with foreign keys
- **Schema**: Extracted from raw JSONB using PySpark
- **Example**: `game.live_game_v1` → 17 normalized tables
  - `game.live_game_metadata` - Top-level game info
  - `game.live_game_players` - All players (51+ per game)
  - `game.live_game_plays` - All plays (75+ per game)
  - `game.live_game_pitch_events` - Every pitch (250+ per game)
  - ... and 13 more tables

See [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md) for complete endpoint/method → table mapping.

---

## Project Structure

```
mlb_statsapi_data_platform/
├── src/mlb_data_platform/        # Python application
│   ├── __init__.py
│   ├── cli.py                    # Typer CLI entry point
│   │
│   ├── ingestion/                # API ingestion layer
│   │   ├── __init__.py
│   │   ├── client.py             # pymlb_statsapi wrapper
│   │   ├── rate_limiter.py       # Token bucket rate limiting
│   │   ├── job_runner.py         # Execute job configs
│   │   └── config.py             # Job configuration models (Pydantic)
│   │
│   ├── schema/                   # Schema management
│   │   ├── __init__.py
│   │   ├── registry.py           # Schema version tracking
│   │   ├── converter.py          # JSON → Avro conversion
│   │   ├── mapper.py             # Hierarchical mapping logic
│   │   └── generator.py          # DDL generator from schemas
│   │
│   ├── storage/                  # Storage backends
│   │   ├── __init__.py
│   │   ├── minio_client.py       # MinIO/S3 operations
│   │   ├── postgres.py           # PostgreSQL client
│   │   ├── redis_client.py       # Redis cache operations
│   │   └── models.py             # Database models
│   │
│   ├── transform/                # PySpark jobs
│   │   ├── __init__.py
│   │   ├── base.py               # Base transform class
│   │   ├── batch/
│   │   │   ├── __init__.py
│   │   │   ├── season.py         # Season data transform
│   │   │   ├── schedule.py       # Schedule data transform
│   │   │   ├── game.py           # Game data transform
│   │   │   └── player.py         # Player data transform
│   │   └── streaming/
│   │       ├── __init__.py
│   │       └── game_live.py      # Live game streaming
│   │
│   └── orchestration/            # Workflow helpers
│       ├── __init__.py
│       └── argo_client.py        # Argo Workflows API client
│
├── config/
│   ├── jobs/                     # Declarative job definitions
│   │   ├── season_daily.yaml
│   │   ├── schedule_polling.yaml
│   │   ├── game_live_streaming.yaml
│   │   └── player_roster.yaml
│   │
│   └── schemas/                  # Avro schemas + mappings
│       ├── registry/             # Avro schema definitions
│       │   └── mlb_statsapi/
│       │       └── v1/
│       │           ├── season.avsc
│       │           ├── schedule.avsc
│       │           ├── game.avsc
│       │           └── person.avsc
│       └── mappings/             # Hierarchical table mappings
│           ├── game.yaml         # game → 17 tables
│           ├── schedule.yaml
│           └── person.yaml
│
├── tests/
│   ├── unit/                     # Unit tests (pytest)
│   │   ├── test_ingestion.py
│   │   ├── test_schema.py
│   │   └── test_storage.py
│   │
│   ├── bdd/                      # BDD tests (behave)
│   │   ├── features/
│   │   │   ├── ingestion.feature
│   │   │   ├── transform.feature
│   │   │   └── storage.feature
│   │   ├── steps/
│   │   │   └── common_steps.py
│   │   └── stubs/                # Copied from pymlb_statsapi
│   │
│   └── integration/              # Integration tests
│       └── test_e2e_pipeline.py
│
├── helm/
│   └── mlb-data-platform/        # Main Helm chart
│       ├── Chart.yaml
│       ├── values.yaml           # Default values
│       ├── values-dev.yaml       # Dev overrides
│       ├── values-prod.yaml      # Prod overrides
│       │
│       ├── charts/               # Sub-charts (dependencies)
│       │   ├── postgresql/       # Bitnami PostgreSQL
│       │   ├── redis/            # Bitnami Redis
│       │   ├── minio/            # MinIO chart
│       │   └── argo-workflows/   # Argo Workflows
│       │
│       └── templates/
│           ├── namespace.yaml
│           ├── configmap.yaml    # Job configs, schemas
│           ├── secrets.yaml      # External Secrets manifests
│           │
│           ├── ingestion/
│           │   ├── cronjob-season.yaml
│           │   ├── cronjob-schedule.yaml
│           │   └── deployment-game-streaming.yaml
│           │
│           ├── spark/
│           │   ├── spark-operator.yaml
│           │   ├── spark-app-season.yaml
│           │   └── spark-app-game.yaml
│           │
│           ├── postgres/
│           │   ├── init-job.yaml
│           │   └── migration-job.yaml
│           │
│           ├── workflows/
│           │   ├── workflow-season-pipeline.yaml
│           │   ├── workflow-game-live.yaml
│           │   └── cron-workflow-daily.yaml
│           │
│           └── services/
│               ├── postgres-service.yaml
│               ├── redis-service.yaml
│               └── minio-service.yaml
│
├── sql/
│   ├── migrations/               # Flyway-style migrations
│   │   ├── V1__initial_schema.sql
│   │   ├── V2__add_game_tables.sql
│   │   └── V3__add_indexes.sql
│   │
│   ├── schemas/                  # Schema definitions
│   │   ├── mlb_games.sql
│   │   ├── mlb_teams.sql
│   │   └── mlb_players.sql
│   │
│   └── templates/
│       └── customer_role_template.sql
│
├── docker/
│   ├── Dockerfile.ingestion      # Lightweight Python image
│   ├── Dockerfile.spark          # PySpark + dependencies
│   └── Dockerfile.migration      # Migration runner
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── backend.tf                # S3 backend config
│   │
│   ├── modules/
│   │   ├── s3-state-backend/     # Terraform state bucket
│   │   ├── secrets-manager/      # AWS Secrets Manager
│   │   └── iam/                  # IAM for External Secrets
│   │
│   └── environments/
│       ├── dev/
│       │   ├── main.tf
│       │   ├── terraform.tfvars
│       │   └── backend.hcl
│       └── prod/
│           ├── main.tf
│           ├── terraform.tfvars
│           └── backend.hcl
│
├── scripts/
│   ├── bootstrap_terraform.sh    # Bootstrap TF state backend
│   ├── setup_github.sh           # Setup GitHub secrets/tokens
│   ├── deploy.sh                 # Deploy to K8s
│   ├── init_database.sh          # Initialize PostgreSQL
│   ├── provision_customer.sh     # Create customer user
│   └── test_pipeline.sh          # E2E pipeline test
│
├── docs/
│   ├── architecture.md
│   ├── deployment.md
│   ├── data_model.md
│   └── workflows.md
│
├── pyproject.toml                # Project configuration (uv/hatch)
├── docker-compose.yaml           # Local development stack
├── .pre-commit-config.yaml       # Pre-commit hooks
├── .gitignore
├── LICENSE                       # Apache 2.0
├── README.md
├── CLAUDE.md                     # This file
└── SCHEMA_MAPPING.md             # Complete schema mapping
```

---

## Development Workflow

### Making Changes

1. **Create feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make changes with tests**
   - Add/modify code in `src/mlb_data_platform/`
   - Add tests in `tests/unit/` or `tests/bdd/`
   - Update documentation if needed

3. **Run code quality checks**
   ```bash
   ruff check .                    # Lint
   ruff format .                   # Format
   mypy src/                       # Type check
   bandit -r src/                  # Security scan
   pre-commit run --all-files      # All hooks
   ```

4. **Run tests**
   ```bash
   pytest                          # Unit tests
   behave                          # BDD tests (with stubs)
   pytest tests/integration/       # Integration tests
   ```

5. **Commit with conventional commits**
   ```bash
   git add .
   git commit -m "feat: add game live streaming job"
   # or: fix:, docs:, chore:, refactor:, test:
   ```

6. **Push and create PR**
   ```bash
   git push origin feature/your-feature-name
   # Create PR on GitHub
   ```

### Adding New Endpoints

When MLB adds a new API endpoint or you want to ingest a new endpoint:

1. **Verify in pymlb_statsapi**
   ```bash
   cd ~/github.com/power-edge/pymlb_statsapi
   uv run python -c "from pymlb_statsapi import api; print(api.get_endpoint_names())"
   ```

2. **Create job configuration**
   ```yaml
   # config/jobs/new_endpoint.yaml
   name: new_endpoint_job
   type: batch
   schedule: "0 */6 * * *"  # Every 6 hours

   source:
     endpoint: new_endpoint
     method: method_name
     parameters:
       param1: value1

   storage:
     raw:
       backend: s3
       path: "raw/new_endpoint/date=${date}"
       format: avro

   transform:
     spark_job: flatten_new_endpoint
     output_tables:
       - new_endpoint.method_name
   ```

3. **Generate SQL DDL**
   ```bash
   uv run mlb-etl schema generate --endpoint new_endpoint --method method_name
   # Outputs DDL to sql/migrations/V{next}__add_new_endpoint.sql
   ```

4. **Create PySpark transform job**
   ```python
   # src/mlb_data_platform/transform/batch/new_endpoint.py
   from pyspark.sql import SparkSession
   from ..base import BaseTransform

   class NewEndpointTransform(BaseTransform):
       def run(self):
           # Read from raw table
           df = self.spark.read.table("new_endpoint.method_name")

           # Transform logic
           transformed_df = df.select("...")

           # Write to normalized table
           transformed_df.write.mode("append").saveAsTable("new_endpoint.normalized")
   ```

5. **Add tests**
   ```python
   # tests/unit/test_new_endpoint.py
   def test_new_endpoint_transform():
       # Test transform logic
       pass
   ```

6. **Run and validate**
   ```bash
   uv run mlb-etl ingest --job config/jobs/new_endpoint.yaml
   uv run mlb-etl transform --job config/jobs/new_endpoint.yaml
   ```

---

## Deployment

### AWS Setup (State & Secrets Only)

```bash
# 1. Bootstrap Terraform state backend
./scripts/bootstrap_terraform.sh

# Creates:
# - S3 bucket: power-edge-sports.terraform
# - DynamoDB table: terraform-lock
# - AWS Secrets Manager secrets:
#   - mlb-data-platform/postgres
#   - mlb-data-platform/redis
#   - mlb-data-platform/minio

# 2. Verify secrets
aws secretsmanager list-secrets --query "SecretList[?starts_with(Name, 'mlb-data-platform')].Name"
```

### Kubernetes Deployment

```bash
# 1. Ensure kubeconfig is set
export KUBECONFIG=~/.kube/config

# 2. Deploy to dev environment
./scripts/deploy.sh dev

# 3. Monitor deployment
kubectl get pods -n mlb-data-platform -w

# 4. Check services
kubectl get svc -n mlb-data-platform

# 5. View logs
kubectl logs -f deployment/mlb-ingestion -n mlb-data-platform
```

### Accessing Services

```bash
# PostgreSQL
kubectl port-forward svc/postgresql 5432:5432 -n mlb-data-platform
psql -h localhost -U mlb_admin -d mlb_games

# MinIO Console
kubectl port-forward svc/minio 9000:9000 -n mlb-data-platform
# Open http://localhost:9000

# Redis
kubectl port-forward svc/redis 6379:6379 -n mlb-data-platform
redis-cli -h localhost -a <password>

# Argo Workflows UI
kubectl port-forward svc/argo-server 2746:2746 -n mlb-data-platform
# Open http://localhost:2746
```

---

## Data Workflows

### Season → Schedule → Game Pipeline

**Trigger**: Daily at midnight UTC (CronWorkflow)

```
1. Fetch Season Data
   - Endpoint: Season.seasons(sportId=1)
   - Output: season.seasons raw table
   - Transform: Extract season metadata

2. Fetch Schedule Data
   - Endpoint: Schedule.schedule(date=today, sportId=1)
   - Output: schedule.schedule raw table
   - Transform: Extract games list

3. For Each Game in Schedule
   3a. Fetch Live Game Data
       - Endpoint: Game.liveGameV1(game_pk=X)
       - Output: game.live_game_v1 raw table
       - Transform: Flatten to 17 normalized tables

   3b. Fetch Player Data (for unique players)
       - Endpoint: Person.person(personIds=Y)
       - Output: person.person raw table

   3c. Fetch Team Data (for home/away teams)
       - Endpoint: Team.team(teamId=Z)
       - Output: team.team raw table

4. Update Materialized Views
   - game.latest_live
   - schedule.today
```

### Live Game Streaming

**Trigger**: Event-driven (when games are "Live")

```
1. Detect Active Games
   - Query: SELECT game_pk FROM schedule.games WHERE status_code = 'L'

2. For Each Active Game
   2a. Poll API every 30 seconds
       - Endpoint: Game.liveGameV1(game_pk=X)

   2b. Cache in Redis
       - Key: game:live:{game_pk}
       - TTL: 60 seconds

   2c. Save Timestamped Snapshot to MinIO
       - Path: s3://raw-data/game/live/date=2024-11-09/game_pk=X/timecode=203000.avro

   2d. Incremental Merge to PostgreSQL
       - Upsert to game.live_game_v1
       - Extract new plays/pitches to normalized tables

3. On Game Completion
   3a. Final snapshot to game.live_game_v1
   3b. Archive to S3 cold storage
   3c. Stop polling
```

---

## Testing

### Unit Tests

```bash
# Run all unit tests
pytest

# Run specific test file
pytest tests/unit/test_ingestion.py

# With coverage
pytest --cov=mlb_data_platform --cov-report=html

# Verbose output
pytest -v -s
```

### BDD Tests with Stubs

```bash
# Run all BDD tests (uses stubs from pymlb_statsapi)
behave

# Run specific feature
behave tests/bdd/features/ingestion.feature

# With verbose output
behave -v

# Generate test report
behave --format html --outfile reports/bdd-report.html
```

### Integration Tests

```bash
# Start local infrastructure
docker compose up -d

# Run integration tests
pytest tests/integration/

# Stop infrastructure
docker compose down
```

### End-to-End Pipeline Test

```bash
# Runs full pipeline: ingest → transform → validate
./scripts/test_pipeline.sh
```

---

## Troubleshooting

### Database Connection Issues

```bash
# Check PostgreSQL is running
docker compose ps postgres

# Test connection
docker compose exec postgres psql -U mlb_admin -d mlb_games -c "SELECT version();"

# View logs
docker compose logs postgres -f
```

### MinIO Access Issues

```bash
# Check MinIO is running
docker compose ps minio

# Test connection
docker compose exec minio-init mc alias ls

# View buckets
docker compose exec minio-init mc ls minio/
```

### PySpark Job Failures

```bash
# Check Spark logs in Kubernetes
kubectl logs -l spark-role=driver -n mlb-data-platform

# View Spark UI (port-forward)
kubectl port-forward svc/spark-ui 4040:4040 -n mlb-data-platform

# Check Spark Operator status
kubectl get sparkapplications -n mlb-data-platform
```

### Argo Workflow Issues

```bash
# List workflows
argo list -n mlb-data-platform

# Get workflow details
argo get <workflow-name> -n mlb-data-platform

# View logs
argo logs <workflow-name> -n mlb-data-platform -f

# Resubmit failed workflow
argo resubmit <workflow-name> -n mlb-data-platform
```

---

## Configuration

### Environment Variables

#### Ingestion
- `PYMLB_STATSAPI__BASE_FILE_PATH`: Base path for file storage
- `STUB_MODE`: Test stub mode (`capture`, `replay`, `passthrough`)

#### PostgreSQL
- `POSTGRES_HOST`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_DB`: Database name

#### MinIO
- `MINIO_ENDPOINT`: MinIO endpoint URL
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_SECURE`: Use HTTPS (default: false)

#### Redis
- `REDIS_HOST`: Redis host
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_PASSWORD`: Redis password
- `REDIS_DB`: Redis database number (default: 0)

#### Spark
- `SPARK_MASTER`: Spark master URL (default: `local[*]`)
- `SPARK_APP_NAME`: Spark application name

---

## Best Practices

### Code Style
- Use `ruff` for linting and formatting (line length: 100)
- Follow PEP 8 conventions
- Add type hints where possible (checked by `mypy`)
- Write docstrings for public functions/classes

### Testing
- Aim for >80% code coverage
- Use stubs from `pymlb_statsapi` for deterministic tests
- Write BDD scenarios for critical workflows
- Add integration tests for storage backends

### Git Workflow
- Use conventional commits: `feat:`, `fix:`, `docs:`, `chore:`
- Keep commits atomic and focused
- Write descriptive commit messages
- Squash commits before merging PRs

### Performance
- Use partitioned tables for time-series data
- Index frequently queried columns
- Use materialized views for expensive queries
- Cache frequently accessed data in Redis

### Security
- Never commit secrets or credentials
- Use AWS Secrets Manager for sensitive data
- Rotate credentials regularly
- Use least-privilege IAM policies

---

## Resources

- **pymlb_statsapi**: [GitHub](https://github.com/power-edge/pymlb_statsapi) | [Docs](https://pymlb-statsapi.readthedocs.io/)
- **MLB Stats API**: [Docs](https://statsapi.mlb.com/docs/)
- **Argo Workflows**: [Docs](https://argoproj.github.io/argo-workflows/)
- **PySpark**: [Docs](https://spark.apache.org/docs/latest/api/python/)
- **PostgreSQL**: [Docs](https://www.postgresql.org/docs/15/)

---

## Support

For questions or issues:
- **GitHub Issues**: [Issues](https://github.com/power-edge/mlb_statsapi_data_platform/issues)
- **Documentation**: [docs/](./docs/)
- **Architecture**: [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md)

---

**Made with ❤️ for the baseball data community**
