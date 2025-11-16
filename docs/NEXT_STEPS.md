# Next Steps - MLB Stats API Data Platform

**Date**: 2024-11-15
**Status**: Foundation Complete ✅, Ready for Integration

## 🎯 Current State

### ✅ What's Complete
- [x] Transformation architecture (Kappa, schema-driven, callable pattern)
- [x] Defensive upsert implementation (PostgreSQL + Delta Lake)
- [x] Base classes (`BaseTransformation`, `GameLiveV1Transformation`)
- [x] Testing framework (unit tests + integration tests with stub data)
- [x] Documentation (6 comprehensive guides)
- [x] Modern Python tooling (`uv` with dependency groups)
- [x] Timestamp strategy (captured_at + ingested_at)
- [x] API metadata preservation pattern

### 📊 Coverage Stats
- **Schema mappings**: 5/17 tables defined (29% complete)
- **Tests**: 12 tests passing (defensive upsert + stub data validation)
- **Documentation**: 100% (architecture, patterns, usage)

---

## 🚀 Recommended Next Steps

I recommend we prioritize in this order:

### Phase 1: Complete Schema Mappings (1-2 days)
**Goal**: Define all 17 tables for `game.liveGameV1` transformation

**Priority**: 🔥 High - Needed for end-to-end testing

#### Currently Defined (5/17)
1. ✅ `game.live_game_metadata` - Core game info
2. ✅ `game.live_game_linescore` - Inning-by-inning scores
3. ✅ `game.live_game_players` - All players in game
4. ✅ `game.live_game_plays` - All at-bats
5. ✅ `game.live_game_pitch_events` - Every pitch

#### Need to Define (12/17)
6. ⏳ `game.live_game_runners` - Base runners per play
7. ⏳ `game.live_game_scoring_plays` - RBI plays
8. ⏳ `game.live_game_home_batting_order` - Home lineup
9. ⏳ `game.live_game_away_batting_order` - Away lineup
10. ⏳ `game.live_game_home_pitchers` - Home pitching staff
11. ⏳ `game.live_game_away_pitchers` - Away pitching staff
12. ⏳ `game.live_game_home_bench` - Home bench players
13. ⏳ `game.live_game_away_bench` - Away bench players
14. ⏳ `game.live_game_home_bullpen` - Home bullpen
15. ⏳ `game.live_game_away_bullpen` - Away bullpen
16. ⏳ `game.live_game_umpires` - Umpire crew
17. ⏳ `game.live_game_venue_details` - Venue metadata

**Tasks**:
- [ ] Use stub data to identify JSON paths for each field
- [ ] Define primary keys and relationships
- [ ] Add data quality rules
- [ ] Test extraction with stub data

**Output**: Complete `config/schemas/mappings/game/live_game_v1.yaml`

---

### Phase 2: Database Schema Generation (1 day)
**Goal**: Auto-generate PostgreSQL DDL from YAML mappings

**Priority**: 🔥 High - Needed for database setup

**Tasks**:
- [ ] Implement DDL generator (`src/mlb_data_platform/schema/generator.py`)
- [ ] Generate migration SQL files (`sql/migrations/V2__game_live_tables.sql`)
- [ ] Add indexes on primary keys, foreign keys, and captured_at
- [ ] Create partitioning strategy (by captured_at, monthly partitions)
- [ ] Test DDL generation

**Example Output**:
```sql
-- V2__game_live_tables.sql
CREATE TABLE game.live_game_metadata (
    game_pk BIGINT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ... all fields from YAML
    PRIMARY KEY (game_pk)
) PARTITION BY RANGE (captured_at);

CREATE INDEX idx_live_game_metadata_captured_at ON game.live_game_metadata(captured_at);
```

---

### Phase 3: Ingestion Layer (2-3 days)
**Goal**: Fetch data from MLB API and save to raw tables with metadata

**Priority**: 🔥 High - Connects API to transformations

**Tasks**:
- [ ] Implement API client wrapper (`src/mlb_data_platform/ingestion/client.py`)
  - Set `captured_at` timestamp BEFORE making API call
  - Preserve all metadata (endpoint, method, path_params, query_params, url, status_code)
  - Handle rate limiting (token bucket)
  - Retry logic for transient failures
- [ ] Implement job runner (`src/mlb_data_platform/ingestion/job_runner.py`)
  - Parse job configs (`config/jobs/*.yaml`)
  - Execute API requests with parameters
  - Write to raw PostgreSQL tables
  - Write to MinIO/S3 (optional)
- [ ] Create job configs
  - `config/jobs/game_live_specific.yaml` - Fetch specific game_pks
  - `config/jobs/game_live_date.yaml` - Fetch all games for date
  - `config/jobs/game_live_season.yaml` - Fetch all games for season
- [ ] Add CLI commands
  - `mlb-etl ingest --job config/jobs/game_live_specific.yaml --game-pk 747175`
  - `mlb-etl ingest --job config/jobs/game_live_date.yaml --date 2024-10-25`

**Example Job Config**:
```yaml
# config/jobs/game_live_specific.yaml
name: game_live_specific
description: Fetch specific game(s) from MLB API

source:
  endpoint: game
  method: liveGameV1
  capture_metadata: true  # REQUIRED!

storage:
  raw:
    backend: postgres
    table: game.live_game_v1
    preserve_metadata: true  # Store captured_at, endpoint, etc.

parameters:
  - name: game_pks
    type: list[int]
    required: true
    description: "List of game PKs to fetch"
```

---

### Phase 4: End-to-End Pipeline Test (1 day)
**Goal**: Validate complete flow: API → Raw → Transform → Normalized

**Priority**: 🔥 High - Proves everything works together

**Tasks**:
- [ ] Create E2E test (`tests/integration/test_pipeline_e2e.py`)
  - Start with stub data (no real API calls)
  - Load stub into raw table
  - Run transformation
  - Validate all 17 tables populated correctly
  - Verify defensive upserts work
- [ ] Test with local PostgreSQL (Docker Compose)
- [ ] Test idempotency (run transformation twice, same result)
- [ ] Test defensive upserts (old data doesn't overwrite new)

**Setup**:
```bash
# Start local database
docker compose up -d postgres

# Initialize schema
uv run mlb-etl schema init

# Load stub data to raw table
uv run mlb-etl test load-stub --game-pk 747175

# Run transformation
uv run mlb-etl transform --job config/jobs/game_live_transform.yaml

# Validate results
uv run pytest tests/integration/test_pipeline_e2e.py -v
```

---

### Phase 5: Local Kubernetes Testing (2-3 days)
**Goal**: Deploy to your desktop tower K8s cluster

**Priority**: 🟡 Medium - Validates production deployment

**Tasks**:
- [ ] Create `values.local.yaml` (git-ignored)
  - Local PostgreSQL connection
  - Local MinIO endpoint
  - No secrets (use defaults for local testing)
- [ ] Create Helm chart (`helm/mlb-data-platform/`)
  - PostgreSQL (Bitnami chart)
  - MinIO (for S3-compatible storage)
  - Redis (for caching)
  - Ingestion CronJobs
  - Transformation SparkApplications (Spark Operator)
- [ ] Deploy to K8s
  ```bash
  helm install mlb-data-platform ./helm/mlb-data-platform \
    -f helm/mlb-data-platform/values.local.yaml \
    --namespace mlb-data-platform \
    --create-namespace
  ```
- [ ] Verify all components running
- [ ] Run test ingestion job
- [ ] Run test transformation job

---

### Phase 6: Workflow Orchestration (2-3 days)
**Goal**: Argo Workflows for complex pipelines

**Priority**: 🟡 Medium - Enables complex workflows

**Tasks**:
- [ ] Create Argo workflow templates
  - `workflows/game-live-single.yaml` - Process single game
  - `workflows/game-live-daily.yaml` - Process all games for date
  - `workflows/game-live-season.yaml` - Process entire season (parallel)
- [ ] Create CronWorkflow for daily pipeline
  ```yaml
  # Every day at midnight UTC
  schedule: "0 0 * * *"
  workflow:
    - fetch_schedule (today's games)
    - for each game:
        - fetch_game_data
        - transform_game_data
    - update_materialized_views
  ```
- [ ] Test workflow execution
- [ ] Add workflow monitoring/alerting

---

### Phase 7: Additional Endpoints (Ongoing)
**Goal**: Expand beyond `game.liveGameV1`

**Priority**: 🟢 Low - Nice to have

**Endpoints to Add**:
- [ ] `schedule.schedule()` - Daily game schedules
- [ ] `person.person()` - Player profiles
- [ ] `team.team()` - Team information
- [ ] `venue.venue()` - Venue/stadium details
- [ ] `season.seasons()` - Season metadata

Each endpoint needs:
- Schema mapping YAML
- Transformation class
- Job configs
- Tests

---

## 🎓 My Recommendation: Start Here

I recommend we tackle in this order:

### **Option A: Quick Win - Complete One Endpoint** (Recommended)
**Timeline**: 3-5 days

1. **Day 1-2**: Complete all 17 table mappings for `game.liveGameV1`
2. **Day 2**: Generate PostgreSQL DDL and create migrations
3. **Day 3**: Build ingestion layer with stub data
4. **Day 4**: E2E test with local PostgreSQL
5. **Day 5**: Validate defensive upserts work end-to-end

**Outcome**: One complete, working pipeline from API → Database

---

### **Option B: Foundation First - Database + Ingestion** (Alternative)
**Timeline**: 4-6 days

1. **Day 1**: DDL generator implementation
2. **Day 2**: Database setup with Docker Compose
3. **Day 3-4**: Ingestion layer (API client + job runner)
4. **Day 5**: CLI commands for ingestion
5. **Day 6**: Test with stub data

**Outcome**: Solid ingestion foundation, ready for transformations

---

### **Option C: Full Stack - K8s Deployment** (Ambitious)
**Timeline**: 1-2 weeks

1. **Week 1**: Complete Option A
2. **Week 2**: Deploy to local K8s cluster
3. **Week 2**: Add Argo Workflows
4. **Week 2**: Production-ready monitoring

**Outcome**: Production-grade deployment on your tower

---

## 🤔 Which Would You Prefer?

**Questions to help decide**:

1. **Do you want to see data flowing end-to-end quickly?**
   → Option A (Quick Win)

2. **Do you want to build solid ingestion foundation first?**
   → Option B (Foundation First)

3. **Do you want to deploy to K8s and test at scale?**
   → Option C (Full Stack)

4. **Do you have the 12 missing table definitions already mapped out?**
   → If yes: Option A is very achievable
   → If no: Option B might be safer

5. **Is your desktop tower K8s cluster ready?**
   → If yes: Option C is feasible
   → If no: Start with A or B

---

## 🛠️ What I Can Help With Next

I'm ready to help with any of these:

1. **Complete the 12 missing table mappings** (use stub data to extract JSON paths)
2. **Build the DDL generator** (YAML → SQL migrations)
3. **Implement the ingestion layer** (API client with metadata capture)
4. **Create E2E tests** (stub data → raw table → transformation → normalized tables)
5. **Build Helm chart** (for K8s deployment)
6. **Create Argo workflows** (orchestration templates)

---

## 📋 Next Session Agenda

**When you're ready, just tell me which option you prefer**, and I'll:

1. Create detailed task breakdown
2. Start implementing immediately
3. Show you working code/tests as we go
4. Update documentation

**My suggestion**: Start with **Option A** (complete one endpoint end-to-end). This gives you a working pipeline quickly and validates the entire architecture.

What would you like to tackle next? 🚀
