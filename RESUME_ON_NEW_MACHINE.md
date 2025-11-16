# Resume Work on Another Machine

**Date**: 2025-11-16
**Project**: MLB StatsAPI Data Platform
**Last Commit**: `f91c751` - Phase 3 BDD Testing Framework (82% smoke test coverage)

---

## 🎯 Current Status

### ✅ Completed (Committed Locally)
- **Phase 3 BDD Testing Framework** - Complete with 9/11 smoke tests passing (82%)
- **Raw Ingestion Layer** - 100% smoke test coverage (5/5 scenarios)
- **Transformation Layer** - 67% smoke test coverage (4/6 scenarios)
- **Session Management** - All DetachedInstanceError issues fixed
- **Documentation** - 2,100+ lines of comprehensive docs

### 📦 What's Committed
- 81 files added (new modules, tests, docs, examples)
- 8 files modified (config, scripts, docs)
- ~25,798 insertions
- Commit hash: `f91c751`

### 🔴 Not Yet Done
1. **GitHub Repo Not Created** - Local git only, no remote yet
2. **2 Smoke Tests Still Failing** - Duplicate key violations
3. **Regression Tests** - Step definitions needed
4. **Unit Tests** - Structure needs to be created

---

## 🚀 Setup on New Machine

### 1. Prerequisites
```bash
# Required tools
brew install docker kubectl helm terraform git uv

# Start Docker
docker compose up -d
```

### 2. Clone/Copy Repository
Since there's no GitHub remote yet, you'll need to either:

**Option A: Copy from old machine**
```bash
# On old machine
cd ~/github.com/power-edge
tar -czf mlb_statsapi_data_platform.tar.gz mlb_statsapi_data_platform/

# Transfer to new machine
scp mlb_statsapi_data_platform.tar.gz new-machine:~/

# On new machine
cd ~/github.com/power-edge
tar -xzf ~/mlb_statsapi_data_platform.tar.gz
```

**Option B: Create GitHub repo first** (recommended)
See "Creating GitHub Repository" section below.

### 3. Install Dependencies
```bash
cd ~/github.com/power-edge/mlb_statsapi_data_platform

# Install Python dependencies
uv sync

# Install pre-commit hooks
pre-commit install

# Verify installation
uv run python -c "from mlb_data_platform.models import RawLiveGameV1; print('✓ Imports working')"
```

### 4. Database Setup
```bash
# Start PostgreSQL
docker compose up -d

# Wait for database to be ready
docker compose exec postgres psql -U mlb_admin -d mlb_games -c "SELECT 1;"

# Run migrations
./scripts/init_database.sh
```

### 5. Verify Tests
```bash
# Run smoke tests
uv run behave tests/bdd/features/ --tags=smoke

# Expected: 9/11 passing (2 failing due to duplicate key issues)
```

---

## 📋 Creating GitHub Repository

### Step 1: Create `.github/terraform/` Structure

```bash
cd ~/github.com/power-edge/mlb_statsapi_data_platform
mkdir -p .github/terraform
```

### Step 2: Copy Terraform Files from pymlb_statsapi

You can use pymlb_statsapi as a template:

```bash
# Copy base files
cp ~/github.com/power-edge/pymlb_statsapi/.github/terraform/main.tf .github/terraform/
cp ~/github.com/power-edge/pymlb_statsapi/.github/terraform/variables.tf .github/terraform/
cp ~/github.com/power-edge/pymlb_statsapi/.github/terraform/backend.tf.template .github/terraform/
cp ~/github.com/power-edge/pymlb_statsapi/.github/terraform/.env.template .github/terraform/
cp ~/github.com/power-edge/pymlb_statsapi/.github/terraform/.gitignore .github/terraform/
```

### Step 3: Customize for mlb_statsapi_data_platform

Edit `.github/terraform/variables.tf`:
```hcl
variable "github_repository" {
  type        = string
  description = "GitHub repository name (without owner)"
  default     = "mlb_statsapi_data_platform"  # ← Change this
}
```

Edit `.github/terraform/main.tf`:
- Update repository description
- Update topics/tags
- Adjust branch protection rules if needed

### Step 4: Configure Environment

```bash
cd .github/terraform

# Copy template and configure
cp .env.template .env

# Edit .env with your GitHub token
nano .env  # Add: export GITHUB_TOKEN="your_github_token"

# Source environment
source .env
```

### Step 5: Initialize and Apply Terraform

```bash
cd .github/terraform

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Create GitHub repository
terraform apply
```

### Step 6: Add Remote and Push

```bash
cd ~/github.com/power-edge/mlb_statsapi_data_platform

# Add GitHub remote
git remote add origin git@github.com:power-edge/mlb_statsapi_data_platform.git

# Push commits
git push -u origin main
```

---

## 🔧 Quick Commands Reference

### Testing
```bash
# Run all smoke tests
uv run behave tests/bdd/features/ --tags=smoke

# Run specific scenario
uv run behave tests/bdd/features/transformation.feature:75 --no-capture

# Run examples
uv run python examples/raw_ingestion_example.py
uv run python examples/transform_metadata_example.py
uv run python examples/end_to_end_pipeline.py
```

### Database Operations
```bash
# Clear test data
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "TRUNCATE TABLE game.live_game_v1_raw CASCADE;"

# Query data
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT game_pk, COUNT(*) FROM game.live_game_v1_raw GROUP BY game_pk;"

# Check metadata table
docker compose exec -T postgres psql -U mlb_admin -d mlb_games \
  -c "SELECT * FROM game.live_game_metadata ORDER BY game_pk;"
```

### Code Quality
```bash
# Format and lint
ruff check --fix .
ruff format .

# Run pre-commit hooks
pre-commit run --all-files
```

---

## 📝 Next Steps (Priority Order)

### 1. Fix Remaining 2 Smoke Test Failures ⚠️
**Issue**: Duplicate key violations in transformation scenarios
**Files**: `tests/bdd/steps/transformation_steps.py`
**Scenarios**:
- `Transform multiple games in batch` (line 75)
- `Verify normalization improves query performance` (line 103)

**Problem**: The `step_ingest_games_from_table` function has timestamp collision issues when updating captured_at for duplicate game_pks.

**Next Action**: Debug the timestamp update logic and test isolation markers.

### 2. Create GitHub Repository
Follow "Creating GitHub Repository" section above.

### 3. Implement Regression Test Steps
Many regression scenarios have undefined steps - implement them following the smoke test pattern.

### 4. Add Unit Tests
Create `tests/unit/` structure with pytest tests for:
- Individual functions
- Database operations
- Transformation logic

### 5. Add Coverage Reporting
```bash
pytest --cov=mlb_data_platform --cov-report=html
```

### 6. Design Scheduling Architecture
Plan Airflow/Argo workflows for:
- Daily schedule ingestion
- Live game polling
- Backfill operations

### 7. Plan Backfill Strategy
Design strategy to populate complete data mart.

---

## 📚 Documentation Reference

- **TESTING_GUIDE.md** - Complete testing procedures (400+ lines)
- **TRANSFORMATION_GUIDE.md** - Transformation architecture (300+ lines)
- **PHASE_3_SUMMARY.md** - Phase 3 detailed summary (250+ lines)
- **BDD_TEST_COMPLETION_STATUS.md** - Current test status
- **PROJECT_STATUS.md** - Overall project status

---

## 🐛 Known Issues

### Smoke Tests (2 failing)
Both failures are duplicate key violations on composite PK `(game_pk, captured_at)`:
1. **Transform multiple games in batch** - Test isolation issue
2. **Verify normalization improves query performance** - Timestamp collision

**Workaround**: Tests pass individually but fail when run together.

### Temporary Files (Not Committed)
- `.db_connection_info` - Local DB connection cache
- `COMMIT_MESSAGE.txt` - Temporary commit message

These are gitignored and safe to delete.

---

## 💡 Tips

1. **Always start Docker first**: `docker compose up -d`
2. **Check database health**: `docker compose exec postgres psql -U mlb_admin -d mlb_games -c "SELECT 1;"`
3. **Use stub data**: BDD tests use stubs from pymlb_statsapi
4. **Run smoke tests frequently**: They're fast (~3 seconds)
5. **Read the docs**: TESTING_GUIDE.md has all the patterns

---

## 🔗 Related Projects

- **pymlb_statsapi**: Source of stub data and schemas
  - Location: `~/github.com/power-edge/pymlb_statsapi`
  - Stubs: `~/github.com/power-edge/pymlb_statsapi/tests/bdd/stubs/`

---

**Last Updated**: 2025-11-16
**Commit**: f91c751 (Phase 3 BDD Testing Framework)
**Status**: 82% smoke test coverage, ready for GitHub push
