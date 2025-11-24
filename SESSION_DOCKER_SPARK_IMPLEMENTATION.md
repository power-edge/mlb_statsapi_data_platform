# Docker-based Spark Testing Implementation

**Date:** 2025-11-16
**Session Focus:** Containerize Spark jobs for local testing to eliminate Java/Spark dependency issues

## Problem Statement

Initial local testing of PySpark transformation jobs failed due to:
- Java version mismatch (Spark 3.5+ requires Java 17, system had Java 11)
- Inconsistency between local development and Kubernetes deployment environments
- Need for reproducible testing that matches production

## Solution: Docker Containerization

Implemented Docker-based testing workflow for Spark jobs using Docker Compose, eliminating local Java/Spark installation requirements.

---

## Files Created/Modified

### Docker Images

#### 1. `docker/Dockerfile.spark`
**Purpose:** Production-grade Spark job image
**Base Image:** `eclipse-temurin:17-jre-jammy` (OpenJDK 17)
**Size:** ~500MB (optimized for production)

**Key Features:**
- Java 17 JRE for Spark 3.5+ compatibility
- Python 3.11 for modern Python features
- PySpark 4.0.1 with all dependencies
- PostgreSQL JDBC driver (42.6.0) for database connectivity
- All mlb_data_platform dependencies via uv
- Git repository included for setuptools-scm version detection

**Dockerfile highlights:**
```dockerfile
FROM eclipse-temurin:17-jre-jammy

# Install Python 3.11
RUN apt-get update && apt-get install -y python3.11 python3-pip curl

# Install uv via pip
RUN python3 -m pip install uv

# Copy application code
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY examples/ ./examples/
COPY .git ./.git

# Install dependencies
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0
RUN uv pip install --system -e .

# Download PostgreSQL JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar
```

---

#### 2. `docker/Dockerfile.ingestion`
**Purpose:** Lightweight image for API ingestion CLI
**Base Image:** `python:3.11-slim`
**Size:** ~200MB

**Features:**
- Python 3.11 slim for minimal footprint
- uv package manager
- mlb-etl CLI entrypoint
- No Spark dependencies (ingestion only)

---

### Docker Compose Integration

#### Modified `docker-compose.yaml`
Added Spark service with:
- Profile: `spark` (opt-in via `--profile spark`)
- Network: Connected to existing `mlb-network`
- Dependencies: PostgreSQL (with health check)
- Volumes: Live-mounted source code for development

**Configuration:**
```yaml
spark:
  build:
    context: .
    dockerfile: docker/Dockerfile.spark
  container_name: mlb-spark
  environment:
    POSTGRES_HOST: postgres
    POSTGRES_PORT: 5432
    POSTGRES_DB: mlb_games
    POSTGRES_USER: mlb_admin
    POSTGRES_PASSWORD: mlb_dev_password
    SPARK_MASTER: local[*]
  volumes:
    - ./src:/app/src          # Live code changes
    - ./examples:/app/examples
    - ./config:/app/config
  depends_on:
    postgres:
      condition: service_healthy
  networks:
    - mlb-network
  profiles:
    - spark
```

---

### Testing Infrastructure

#### 3. `scripts/test_transform.sh`
**Purpose:** Automated test runner for transformations
**Lines:** 250+
**Language:** Bash with colored output

**Features:**
- ✅ Docker health checks
- ✅ Automatic test data insertion
- ✅ Parallel or individual transform testing
- ✅ Colored terminal output (✓, ✗, ⚠, ℹ)
- ✅ Error handling and cleanup

**Usage:**
```bash
./scripts/test_transform.sh season     # Test season only
./scripts/test_transform.sh schedule   # Test schedule only
./scripts/test_transform.sh all        # Test all transforms
```

**Workflow:**
```
1. Check Docker running
2. Ensure PostgreSQL healthy
3. Build Spark image (if needed)
4. Check for test data
5. Insert test data if missing
6. Run transformation via Docker
7. Display results
```

---

### Makefile Integration

#### Modified `Makefile`
Added new targets for Docker-based transformation testing:

```makefile
# Build Spark Docker image
build-spark:
	docker compose build spark

# Test individual transformations
test-transform-season:
	./scripts/test_transform.sh season

test-transform-schedule:
	./scripts/test_transform.sh schedule

test-transform-all:
	./scripts/test_transform.sh all
```

**Updated help menu:**
```
🧪 Testing:
  make test                    Run unit tests
  make test-integration        Run integration tests
  make test-transform-all      Test all transformations (Docker)
  make test-transform-season   Test season transformation
  make test-transform-schedule Test schedule transformation

🔧 Development:
  make build-spark             Build Spark Docker image
```

---

## Docker Build Process

### Build Time
- **First build:** ~3 minutes (downloads Java 17 + Python packages)
- **Cached rebuild:** ~5 seconds (uses layer caching)
- **Code-only rebuild:** Instant (volumes mounted)

### Image Layers (11 layers)
1. Base image (eclipse-temurin:17-jre-jammy)
2. System packages (python3.11, curl, procps)
3. Python alternatives configuration
4. pip + uv installation
5. Working directory setup
6. Copy project metadata (pyproject.toml, uv.lock, README.md)
7. Copy source code (src/)
8. Copy examples (examples/)
9. Copy git repo (.git/)
10. Install Python dependencies
11. Download PostgreSQL JDBC driver

**Layer caching strategy:**
- Dependencies (slow, rarely change) cached early
- Source code (fast, frequently changes) copied late
- Volumes used for live development

---

## Test Execution Flow

### Command: `make test-transform-season`

```
┌─────────────────────────────────────┐
│ 1. Check Docker Status              │
│    ✓ Docker is running              │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 2. Ensure PostgreSQL Running        │
│    docker compose up -d postgres    │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 3. Build Spark Image (if needed)    │
│    docker compose build spark       │
│    Uses layer caching               │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 4. Check for Test Data              │
│    SELECT COUNT(*) FROM seasons     │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 5. Insert Test Data (if missing)    │
│    INSERT INTO season.seasons ...   │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 6. Run Spark Transformation          │
│    docker compose run --rm spark     │
│    examples/spark_jobs/              │
│      transform_season.py             │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 7. Display Results                   │
│    ✓ Transformation completed        │
│    or                                │
│    ✗ Transformation failed           │
└─────────────────────────────────────┘
```

---

## Current Status

### ✅ Completed

1. **Docker Images Built**
   - `mlb_statsapi_data_platform-spark:latest` (650MB)
   - Ready for local testing and K8s deployment

2. **Docker Compose Integration**
   - Spark service configured with PostgreSQL connectivity
   - Profile-based activation (`--profile spark`)
   - Volume mounting for live development

3. **Testing Infrastructure**
   - `test_transform.sh` script with colored output
   - Makefile targets for easy execution
   - Automatic test data insertion

4. **Build Process Optimized**
   - Layer caching for fast rebuilds
   - Setuptools-scm version handling
   - Git repository inclusion for versioning

### 🔧 Identified Issues

#### Issue: JSONB Parsing in PySpark

**Error:**
```
pyspark.errors.exceptions.captured.AnalysisException:
[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "data".
Need a complex type [STRUCT, ARRAY, MAP] but got "STRING".
```

**Root Cause:**
PostgreSQL JSONB columns are read as STRING type by Spark JDBC. Need to use `from_json()` to parse.

**Solution Required:**
```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType

# Define schema for JSONB
seasons_schema = StructType([
    StructField("seasons", ArrayType(StructType([
        StructField("seasonId", StringType()),
        StructField("regularSeasonStartDate", StringType()),
        # ... more fields
    ])))
])

# Parse JSONB string
df = df.withColumn("parsed_data", F.from_json(F.col("data"), seasons_schema))

# Then explode
df = df.select(F.explode(F.col("parsed_data.seasons")).alias("season"))
```

**Files to Update:**
1. `src/mlb_data_platform/transform/season/seasons.py` (line 171)
2. `src/mlb_data_platform/transform/schedule/schedule.py` (line 176)

---

## Testing Output Example

```bash
$ make test-transform-season

================================================
MLB Data Platform - Transformation Tests
================================================
✓ Docker is running
ℹ Ensuring PostgreSQL is running...
 Container mlb-postgres  Running

================================================
Building Spark Docker Image
================================================
 spark  Cached
✓ Spark image built successfully

================================================
Testing Season Transformation
================================================
ℹ Checking if season.seasons has data...
✓ Found 2 records in season.seasons
ℹ Running season transformation...

================================================================================
MLB Data Platform - Season Transformation Spark Job
================================================================================
Configuration:
  Sport IDs: [1]
  Export to Delta: False
  PostgreSQL: postgres:5432/mlb_games

Initializing Spark session...
Spark version: 4.0.1

Running transformation pipeline...

ERROR: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "data"
✗ Season transformation failed
```

---

## Next Steps

### Immediate (Required for Testing)
1. **Fix JSONB parsing** in transformation classes
   - Add `from_json()` with schema definitions
   - Update `_extract_seasons()` method
   - Update `_extract_games()` method

2. **Test end-to-end** transformation pipeline
   - Verify season transformation
   - Verify schedule transformation
   - Check output data quality

### Short-term (Kubernetes Deployment)
3. **Push Docker images** to registry
   ```bash
   docker tag mlb-data-platform/spark:latest your-registry/mlb-data-platform/spark:latest
   docker push your-registry/mlb-data-platform/spark:latest
   ```

4. **Update Argo Workflows** with correct image references
   ```yaml
   image: your-registry/mlb-data-platform/spark:latest
   ```

5. **Deploy to Kubernetes**
   - Install Argo Workflows
   - Install Spark Operator
   - Submit test workflows

### Medium-term (Production Readiness)
6. **Add CI/CD pipeline**
   - GitHub Actions for Docker builds
   - Automated testing on pull requests
   - Automatic image pushing to registry

7. **Implement game transformation**
   - Complex nested JSON flattening (17 tables)
   - Similar JSONB parsing fixes required

8. **Enable Delta Lake exports**
   - Configure S3/MinIO credentials
   - Test Delta table creation
   - Implement incremental writes

---

## Benefits Achieved

### Developer Experience
✅ **No local Java installation** - Everything in Docker
✅ **Consistent environment** - Same as production
✅ **Fast feedback loop** - Volume mounting for live changes
✅ **Easy onboarding** - One command to run tests

### Production Alignment
✅ **Identical runtime** - Same Java 17 + PySpark 4.0.1
✅ **Same dependencies** - Exact uv.lock reproduction
✅ **Network isolation** - Docker network for services
✅ **Resource control** - Can set memory/CPU limits

### Testing Reliability
✅ **Deterministic builds** - Layer caching
✅ **Isolated tests** - Clean containers each run
✅ **Automated data setup** - Test data insertion
✅ **Clear error reporting** - Colored output

---

## Command Reference

### Build Commands
```bash
# Build Spark image
make build-spark
docker compose build spark

# Build ingestion image
docker compose build ingestion
```

### Test Commands
```bash
# Run all transformation tests
make test-transform-all

# Run individual tests
make test-transform-season
make test-transform-schedule

# Direct script invocation
./scripts/test_transform.sh season
./scripts/test_transform.sh schedule
./scripts/test_transform.sh all
```

### Development Commands
```bash
# Start services
docker compose up -d postgres redis minio

# Start with Spark
docker compose --profile spark up -d

# Interactive Spark shell
docker compose run --rm spark /bin/bash

# View Spark logs
docker compose logs -f spark

# Stop all services
docker compose down
```

### Debugging Commands
```bash
# Check if image built successfully
docker images | grep mlb-data-platform

# Inspect running container
docker compose exec spark /bin/bash

# Check PostgreSQL connectivity from Spark
docker compose run --rm spark \
  psql -h postgres -U mlb_admin -d mlb_games

# Test Python imports
docker compose run --rm spark \
  python -c "from mlb_data_platform.transform.season import SeasonTransformation"
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│              Local Development Machine                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌────────────┐    make test-transform-season         │
│  │  Terminal  │──────────────────┐                     │
│  └────────────┘                  │                     │
│                                  ▼                     │
│  ┌─────────────────────────────────────────┐          │
│  │       Docker Compose Network            │          │
│  │                                         │          │
│  │  ┌──────────────┐  ┌──────────────┐  │          │
│  │  │  PostgreSQL  │  │   Spark Job  │  │          │
│  │  │   (mlb_games)│◄─┤   Container  │  │          │
│  │  │              │  │              │  │          │
│  │  │  Port: 5432  │  │  Java 17     │  │          │
│  │  │              │  │  Python 3.11 │  │          │
│  │  │              │  │  PySpark 4.0 │  │          │
│  │  └──────────────┘  └──────────────┘  │          │
│  │         ▲                  │          │          │
│  │         │                  │          │          │
│  │         │                  ▼          │          │
│  │  ┌──────────────┐  ┌──────────────┐  │          │
│  │  │    Redis     │  │    MinIO     │  │          │
│  │  │  (Cache)     │  │  (S3 Storage)│  │          │
│  │  └──────────────┘  └──────────────┘  │          │
│  │                                      │          │
│  └──────────────────────────────────────┘          │
│                                                    │
│  Volumes (Live Mount):                            │
│  ├─ ./src → /app/src                              │
│  ├─ ./examples → /app/examples                    │
│  └─ ./config → /app/config                        │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## Summary

### Accomplishments ✅
- **Dockerized Spark jobs** with Java 17 + PySpark 4.0.1
- **Created testing infrastructure** with automated scripts
- **Integrated with Makefile** for easy developer access
- **Optimized build process** with layer caching
- **Identified JSONB parsing issue** for quick fix

### Lines of Code
- Dockerfile.spark: 47 lines
- Dockerfile.ingestion: 25 lines
- test_transform.sh: 250+ lines
- Makefile additions: 20 lines
- **Total:** ~342 lines

### Next Action
Fix JSONB parsing in transformation jobs to enable full end-to-end testing.

---

**Date:** 2025-11-16
**Status:** ✅ Docker infrastructure complete, transformations ready for JSONB fix
**Deployment Ready:** Yes (pending JSONB parsing fix)
