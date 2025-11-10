# MLB StatsAPI Data Platform - Project Status

**Created**: 2024-11-09
**Status**: ✅ **Foundation Complete - Ready for Development**

---

## 🎉 What Has Been Built

This document summarizes the comprehensive **foundation** that has been created for the MLB Stats API Data Platform. The project is now ready for iterative development.

### ✅ Completed Components

#### 1. **Project Structure & Configuration**
- ✅ Modern Python packaging with `uv` and `hatch`
- ✅ pyproject.toml with all dependencies (PySpark, PostgreSQL, Redis, MinIO)
- ✅ Pre-commit hooks (ruff, mypy, bandit)
- ✅ .gitignore optimized for Python/Docker/K8s/Terraform
- ✅ Apache 2.0 LICENSE

**Files Created**:
- `pyproject.toml`
- `.pre-commit-config.yaml`
- `.gitignore`
- `LICENSE`

---

#### 2. **Documentation (Comprehensive)**
- ✅ **README.md** - Quick start, architecture overview, deployment guide
- ✅ **CLAUDE.md** - Complete operations manual for Claude Code
- ✅ **SCHEMA_MAPPING.md** - Detailed endpoint → table mapping with examples
- ✅ **PROJECT_STATUS.md** - This file (project status and next steps)

**Key Documentation Features**:
- Two-tier architecture explained (raw JSONB + normalized relational)
- Complete mapping: 21 MLB endpoints → PostgreSQL schemas/tables
- Example: `game.live_game_v1` decomposes into 17 normalized tables
- Workflow orchestration patterns (Season → Schedule → Game → Player)
- Local development setup with Docker Compose
- Kubernetes deployment procedures

---

#### 3. **Database Schema (PostgreSQL)**
- ✅ **V1 Migration** - Initial schema with 4 schemas, 5+ tables
  - `metadata` schema - Job tracking, schema versioning
  - `season` schema - Season data (raw table)
  - `schedule` schema - Schedule data (raw table with partitions)
  - `game` schema - Live game data (raw table with partitions)
- ✅ Partitioning strategy (by date for performance)
- ✅ Materialized views (`game.latest_live`, `schedule.today`)
- ✅ Helper functions (`refresh_all_views()`, `extract_jsonpath()`)
- ✅ Triggers (ensure only one "latest" row per game)

**Files Created**:
- `sql/migrations/V1__initial_schema.sql`

**Schema Highlights**:
- **game.live_game_v1**: Stores complete 5000+ line JSON responses
- **Partitioning**: Monthly for raw tables, yearly for game tables
- **GIN indexes**: Fast JSONB querying
- **Materialized views**: Pre-computed aggregations for performance

---

#### 4. **Job Configurations (Declarative YAML)**
- ✅ **season_daily.yaml** - Daily season data ingestion
- ✅ **schedule_polling.yaml** - 15-minute schedule polling
- ✅ **game_live_streaming.yaml** - Live game streaming (30-second polls)

**Configuration Features**:
- Declarative job definitions (no code changes for new workflows)
- Rate limiting, retry logic, timeout configuration
- Multi-backend storage (PostgreSQL, MinIO, Redis)
- Transform job integration (PySpark)
- Post-completion actions (refresh views, trigger workflows)
- Dependency tracking between jobs

**Files Created**:
- `config/jobs/season_daily.yaml`
- `config/jobs/schedule_polling.yaml`
- `config/jobs/game_live_streaming.yaml`

---

#### 5. **CLI Application (Typer + Rich)**
- ✅ `mlb-etl` command-line interface
- ✅ Commands implemented (structure only, ready for logic):
  - `mlb-etl ingest` - Run ingestion jobs
  - `mlb-etl transform` - Run PySpark transforms
  - `mlb-etl schema` - Manage Avro schemas
  - `mlb-etl db` - Database management
  - `mlb-etl workflow` - Argo Workflows integration
  - `mlb-etl version` - Show version info

**Files Created**:
- `src/mlb_data_platform/__init__.py`
- `src/mlb_data_platform/cli.py`

---

#### 6. **Local Development Stack (Docker Compose)**
- ✅ PostgreSQL 15 (data mart)
- ✅ Redis 7 (cache layer)
- ✅ MinIO (S3-compatible storage)
- ✅ MinIO auto-initialization (creates buckets: raw-data, processed-data, archived-data)
- ✅ Optional GUI tools (PgAdmin, RedisInsight via `--profile tools`)

**Files Created**:
- `docker-compose.yaml`

**Services**:
```
PostgreSQL: localhost:5432
Redis:      localhost:6379
MinIO API:  localhost:9000
MinIO UI:   localhost:9001
```

---

#### 7. **Helper Scripts**
- ✅ **init_database.sh** - Initialize PostgreSQL schemas
  - Supports local, dev, prod environments
  - Runs migrations in order
  - Verifies schema integrity
  - Saves connection info

**Files Created**:
- `scripts/init_database.sh` (executable)

---

#### 8. **Directory Structure**
Complete project structure with logical organization:

```
mlb_statsapi_data_platform/
├── src/mlb_data_platform/      # Python application (ready for implementation)
│   ├── ingestion/               # API client layer
│   ├── schema/                  # Avro schema management
│   ├── storage/                 # Backend clients (PostgreSQL, MinIO, Redis)
│   ├── transform/               # PySpark jobs
│   └── orchestration/           # Argo Workflows integration
├── config/
│   ├── jobs/                    # Job configurations (3 samples created)
│   └── schemas/                 # Avro schemas + mappings (structure ready)
├── tests/
│   ├── unit/                    # pytest unit tests
│   ├── bdd/                     # behave BDD tests
│   └── integration/             # Integration tests
├── helm/                        # Kubernetes Helm chart (structure ready)
├── terraform/                   # AWS state/secrets only (structure ready)
├── sql/
│   ├── migrations/              # Database migrations (V1 complete)
│   ├── schemas/                 # Schema definitions
│   └── templates/               # SQL templates
├── docker/                      # Dockerfiles (structure ready)
├── scripts/                     # Helper scripts (init_database.sh complete)
└── docs/                        # Additional documentation
```

---

## 📊 Project Statistics

| Category | Count | Status |
|----------|-------|--------|
| **Documentation Files** | 4 | ✅ Complete |
| **Configuration Files** | 5 | ✅ Complete |
| **Job Configs** | 3 | ✅ Complete |
| **SQL Migrations** | 1 (V1) | ✅ Complete |
| **Helper Scripts** | 1 | ✅ Complete |
| **Source Modules** | 2 | ⚠️ Structure only |
| **Docker Compose Services** | 3 | ✅ Complete |
| **Database Schemas** | 4 | ✅ Complete |
| **Raw Tables** | 5+ | ✅ Complete |
| **Materialized Views** | 2 | ✅ Complete |

---

## 🚀 Ready to Use Right Now

### Quick Start (Local Development)

```bash
# 1. Clone and setup
cd ~/github.com/power-edge/mlb_statsapi_data_platform
uv sync
pre-commit install

# 2. Start infrastructure
docker compose up -d

# 3. Initialize database
./scripts/init_database.sh

# 4. Test CLI
uv run mlb-etl version
uv run mlb-etl schema list

# 5. Query database
docker compose exec postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT * FROM metadata.schema_versions;"
```

---

## 🛠️ Next Steps for Development

### Phase 1: Core Ingestion (Immediate Priority)

#### 1.1 Implement Ingestion Layer
**Goal**: Make `mlb-etl ingest` actually work

**Tasks**:
- [ ] Implement `src/mlb_data_platform/ingestion/client.py`
  - Wrapper around `pymlb_statsapi`
  - Handle stub mode (capture/replay/passthrough)
  - Rate limiting with token bucket algorithm
- [ ] Implement `src/mlb_data_platform/ingestion/config.py`
  - Pydantic models for job configuration
  - Load/validate YAML configs
- [ ] Implement `src/mlb_data_platform/ingestion/job_runner.py`
  - Execute jobs based on configuration
  - Save to PostgreSQL raw tables
  - Save to MinIO (optional archival)
- [ ] Implement `src/mlb_data_platform/storage/postgres.py`
  - Connection pooling
  - Insert/upsert operations
  - Partition management
- [ ] Implement `src/mlb_data_platform/storage/minio_client.py`
  - S3-compatible storage operations
  - Bucket management

**Success Criteria**:
```bash
# This should work end-to-end:
uv run mlb-etl ingest --job config/jobs/season_daily.yaml

# Verify data in database:
docker compose exec postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT COUNT(*) FROM season.seasons;"
```

---

#### 1.2 Implement Storage Backends
**Goal**: Complete PostgreSQL, MinIO, Redis clients

**Tasks**:
- [ ] PostgreSQL client with psycopg
  - Connection pooling
  - JSONB operations
  - Partition creation
- [ ] MinIO client
  - Bucket operations
  - Object upload/download
  - Presigned URLs
- [ ] Redis client
  - Key-value caching
  - TTL management
  - Pipeline operations

---

#### 1.3 Add Testing
**Goal**: Comprehensive test coverage for ingestion

**Tasks**:
- [ ] Copy stubs from pymlb_statsapi to `tests/bdd/stubs/`
- [ ] Write unit tests for ingestion components
- [ ] Write BDD scenarios for end-to-end workflows
- [ ] Add integration tests with Docker Compose

---

### Phase 2: PySpark Transformations

#### 2.1 Implement Base Transform Framework
**Tasks**:
- [ ] Implement `src/mlb_data_platform/transform/base.py`
  - BaseTransform class
  - Spark session management
  - Read from raw tables
  - Write to normalized tables

#### 2.2 Implement Season Transform
**Tasks**:
- [ ] Create `src/mlb_data_platform/transform/batch/season.py`
  - Extract season metadata from JSONB
  - Write to `season.seasons_normalized`

#### 2.3 Implement Schedule Transform
**Tasks**:
- [ ] Create `src/mlb_data_platform/transform/batch/schedule.py`
  - Extract games array from schedule
  - Write to `schedule.games`
  - Write summary to `schedule.daily_summary`

#### 2.4 Implement Game Transform (Complex)
**Tasks**:
- [ ] Design normalized schema (17 tables)
- [ ] Create SQL DDL for normalized tables (V2 migration)
- [ ] Implement `src/mlb_data_platform/transform/batch/game.py`
  - Extract top-level metadata
  - Extract players array
  - Extract plays array
  - Extract pitches from plays
  - Extract runners from plays
  - Write to 17 normalized tables

---

### Phase 3: Schema Management

#### 3.1 Avro Schema Generation
**Tasks**:
- [ ] Implement `src/mlb_data_platform/schema/converter.py`
  - Convert pymlb_statsapi JSON schemas to Avro
  - Save to `config/schemas/registry/mlb_statsapi/v1/`
- [ ] Implement `src/mlb_data_platform/schema/registry.py`
  - Track schema versions
  - Handle schema evolution

#### 3.2 DDL Generator
**Tasks**:
- [ ] Implement `src/mlb_data_platform/schema/generator.py`
  - Generate SQL DDL from Avro schemas
  - Create partitioned tables
  - Create indexes
  - Create foreign keys

#### 3.3 Hierarchical Mapping
**Tasks**:
- [ ] Create mapping YAML files
  - `config/schemas/mappings/game.yaml` - game → 17 tables
  - `config/schemas/mappings/schedule.yaml`
  - `config/schemas/mappings/person.yaml`

---

### Phase 4: Kubernetes Deployment

#### 4.1 Helm Chart
**Tasks**:
- [ ] Create `helm/mlb-data-platform/Chart.yaml`
- [ ] Add sub-chart dependencies (PostgreSQL, Redis, MinIO, Argo Workflows)
- [ ] Create values.yaml with sensible defaults
- [ ] Create templates for:
  - CronJobs (season, schedule)
  - Deployments (game streaming)
  - ConfigMaps (job configs)
  - Secrets (External Secrets manifests)
  - Services (expose PostgreSQL, Redis, MinIO)

#### 4.2 Dockerfiles
**Tasks**:
- [ ] Create `docker/Dockerfile.ingestion`
  - Multi-stage build
  - uv-based dependency installation
  - Minimal runtime image
- [ ] Create `docker/Dockerfile.spark`
  - Based on apache/spark-py
  - Include PySpark dependencies
  - Copy transform jobs
- [ ] Create `docker/Dockerfile.migration`
  - PostgreSQL client
  - Run SQL migrations

#### 4.3 Argo Workflows
**Tasks**:
- [ ] Create WorkflowTemplate for season pipeline
- [ ] Create WorkflowTemplate for game live polling
- [ ] Create CronWorkflow for daily execution

---

### Phase 5: AWS Integration (State & Secrets Only)

#### 5.1 Terraform Modules
**Tasks**:
- [ ] Create `terraform/modules/s3-state-backend/`
  - S3 bucket for Terraform state
  - DynamoDB table for locking
- [ ] Create `terraform/modules/secrets-manager/`
  - AWS Secrets Manager for PostgreSQL, Redis, MinIO credentials
- [ ] Create `terraform/modules/iam/`
  - IAM role for External Secrets Operator

#### 5.2 External Secrets Operator
**Tasks**:
- [ ] Install External Secrets Operator in K8s
- [ ] Create ExternalSecret manifests
  - Sync AWS Secrets Manager → K8s Secrets

---

### Phase 6: Additional Features

#### 6.1 Multi-Tenancy
**Tasks**:
- [ ] Create SQL template for customer roles
- [ ] Implement `scripts/provision_customer.sh`
- [ ] Add role-based access control (RBAC)

#### 6.2 Observability
**Tasks**:
- [ ] Add structured logging (python-json-logger)
- [ ] Add metrics (Prometheus)
- [ ] Add tracing (OpenTelemetry)
- [ ] Create Grafana dashboards

#### 6.3 Data Quality
**Tasks**:
- [ ] Integrate Great Expectations or Deequ
- [ ] Add data quality checks to transforms
- [ ] Create alerting for data quality issues

---

## 📦 Deliverables Summary

### What You Have Now

| Deliverable | Status | Notes |
|-------------|--------|-------|
| **Project Structure** | ✅ Complete | All directories created |
| **Documentation** | ✅ Complete | README, CLAUDE.md, SCHEMA_MAPPING.md, PROJECT_STATUS.md |
| **Database Schema (Raw)** | ✅ Complete | V1 migration with 4 schemas, raw tables |
| **Job Configurations** | ✅ Complete | 3 sample YAML configs |
| **Docker Compose** | ✅ Complete | PostgreSQL, Redis, MinIO |
| **CLI Structure** | ✅ Complete | Commands defined (implementation pending) |
| **Helper Scripts** | ✅ Complete | init_database.sh |
| **Python Package** | ✅ Complete | pyproject.toml with all dependencies |

### What Needs Implementation

| Component | Priority | Estimated Effort |
|-----------|----------|------------------|
| **Ingestion Layer** | 🔴 High | 2-3 days |
| **Storage Backends** | 🔴 High | 1-2 days |
| **Testing Infrastructure** | 🟡 Medium | 2-3 days |
| **PySpark Transforms** | 🟡 Medium | 3-5 days |
| **Normalized Schema** | 🟡 Medium | 2-3 days |
| **Helm Chart** | 🟡 Medium | 2-3 days |
| **Dockerfiles** | 🟡 Medium | 1 day |
| **Argo Workflows** | 🟢 Low | 2-3 days |
| **Terraform Modules** | 🟢 Low | 1-2 days |
| **Observability** | 🟢 Low | 2-3 days |

**Total Estimated Effort**: ~20-30 days of focused development

---

## 🎯 Recommended Development Order

### Week 1: Core Functionality
1. Implement ingestion layer (use stubs for testing)
2. Implement storage backends (PostgreSQL, MinIO)
3. Add unit tests and BDD tests
4. **Milestone**: `mlb-etl ingest` works end-to-end

### Week 2: Transformations
1. Design normalized schema for game data
2. Create V2 migration with normalized tables
3. Implement PySpark base framework
4. Implement season/schedule transforms
5. **Milestone**: Basic transforms working

### Week 3: Kubernetes
1. Create Dockerfiles
2. Build and push Docker images
3. Create Helm chart
4. Deploy to dev K8s cluster
5. **Milestone**: Running in Kubernetes

### Week 4: Orchestration & Polish
1. Create Argo Workflows
2. Implement game live streaming
3. Add observability
4. Performance tuning
5. **Milestone**: Full pipeline operational

---

## 💡 Design Decisions Made

### 1. **Kubernetes-Native Architecture**
- **Decision**: All services run in Kubernetes (no AWS RDS, ElastiCache, S3)
- **Rationale**: Cost control, flexibility, avoid vendor lock-in
- **Trade-off**: More operational complexity

### 2. **Two-Tier Data Architecture**
- **Decision**: Raw JSONB tables + Normalized relational tables
- **Rationale**: Flexibility (schema evolution) + Performance (efficient queries)
- **Trade-off**: Storage overhead, ETL complexity

### 3. **Configuration-Driven Jobs**
- **Decision**: Declarative YAML job definitions
- **Rationale**: No code changes for new workflows, easier maintenance
- **Trade-off**: Initial framework complexity

### 4. **Schema-Driven Design**
- **Decision**: Leverage pymlb_statsapi JSON schemas
- **Rationale**: Single source of truth, automatic updates
- **Trade-off**: Dependency on upstream project

### 5. **Argo Workflows over Airflow**
- **Decision**: Use Argo Workflows for orchestration
- **Rationale**: Kubernetes-native, DAGs as CRDs, better resource management
- **Trade-off**: Less mature ecosystem than Airflow

---

## 📚 Key Resources

### Internal Documentation
- [README.md](./README.md) - Project overview and quick start
- [CLAUDE.md](./CLAUDE.md) - Complete operations manual
- [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md) - Endpoint → table mapping

### External Resources
- [pymlb_statsapi](https://github.com/power-edge/pymlb_statsapi) - Schema-driven MLB API client
- [MLB Stats API](https://statsapi.mlb.com/docs/) - Official API documentation
- [Argo Workflows](https://argoproj.github.io/argo-workflows/) - Workflow orchestration
- [PySpark](https://spark.apache.org/docs/latest/api/python/) - Data transformation

---

## 🤝 Contributing

This project is ready for collaborative development. To contribute:

1. Read [CLAUDE.md](./CLAUDE.md) for detailed development guidelines
2. Follow the recommended development order above
3. Use conventional commits (`feat:`, `fix:`, `docs:`, `chore:`)
4. Add tests for all new functionality
5. Update documentation as you go

---

## ✅ Success Metrics

The project will be considered "production-ready" when:

- [ ] All ingestion jobs run reliably (season, schedule, game)
- [ ] PySpark transforms complete successfully
- [ ] Data quality checks pass
- [ ] Deployed in Kubernetes with Helm
- [ ] Argo Workflows orchestrate the pipeline
- [ ] Test coverage > 80%
- [ ] Documentation up-to-date
- [ ] Performance benchmarks met (< 1 minute latency for live games)

---

## 📞 Support

For questions about this project:
- **Architecture**: See [CLAUDE.md](./CLAUDE.md)
- **Schema Design**: See [SCHEMA_MAPPING.md](./SCHEMA_MAPPING.md)
- **Development Setup**: See [README.md](./README.md)

---

**Project Status**: ✅ **Foundation Complete**
**Next Milestone**: Implement ingestion layer
**Estimated Time to MVP**: 2-3 weeks of focused development

---

*Last Updated: 2024-11-09*
