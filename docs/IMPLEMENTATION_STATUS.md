# Implementation Status - PySpark Transformation Framework

**Date**: 2024-11-15
**Status**: Foundation Complete ✅ - Ready for Implementation

---

## 🎉 What We've Built

### 1. Architecture Documentation
**File**: `docs/TRANSFORMATION_ARCHITECTURE.md`

Complete architectural design covering:
- ✅ Schema-driven transformation philosophy
- ✅ Unified batch + streaming execution model
- ✅ Defensive upsert logic (timestamp-based)
- ✅ Data quality gates (Avro + Great Expectations)
- ✅ S3/Delta Lake export strategy
- ✅ Workflow generation patterns
- ✅ Data lineage tracking approach

###2. Schema Mapping Configuration

**File**: `config/schemas/mappings/game/live_game_v1.yaml`

Comprehensive mapping for `Game.liveGameV1()` → 17 normalized tables:
- ✅ game.live_game_metadata (game info, teams, venue, status)
- ✅ game.live_game_linescore_innings (inning-by-inning scoring)
- ✅ game.live_game_players (all 51+ players in game)
- ✅ game.live_game_plays (all at-bats, ~75 per game)
- ✅ game.live_game_pitch_events (every pitch, ~250 per game)
- ✅ 12 additional tables (abbreviated in YAML for space)

**Features**:
- JSON Path expressions for field extraction
- Array/object explosion patterns
- Foreign key relationships
- Data quality rules
- Index definitions
- Export configurations (S3, Delta Lake)

### 3. Pydantic Configuration Models

**File**: `src/mlb_data_platform/schema/mapping.py`

Type-safe models for parsing YAML mappings:
- ✅ `MappingConfig` - Top-level mapping configuration
- ✅ `SourceConfig` - Source raw table definition
- ✅ `TargetTable` - Target normalized table specification
- ✅ `FieldDefinition` - Field extraction and type casting
- ✅ `Relationship` - Foreign key relationships
- ✅ `IndexDefinition` - Database indexes
- ✅ `DataQualityRule` - Validation rules
- ✅ `ExportConfig` - S3/Delta export settings
- ✅ `MappingRegistry` - YAML loading and caching

### 4. Base Transformation Framework

**File**: `src/mlb_data_platform/transform/base.py`

Abstract base class for all transformations:
- ✅ `BaseTransformation` - Core framework
- ✅ `read_source()` - Batch + streaming reads
- ✅ `transform()` - Main transformation logic
- ✅ `_transform_target()` - Single target transformation
- ✅ `_map_fields_direct()` - Direct field mapping
- ✅ `_explode_array()` - Array explosion (plays, pitches)
- ✅ `_explode_object()` - Object explosion (players as object keys)
- ✅ `_build_field_expression()` - JSON Path extraction
- ✅ `_cast_to_type()` - Type casting
- ✅ `_validate_and_cast_schema()` - Schema validation
- ✅ `write_targets()` - Batch + streaming writes
- ✅ Abstract `run()` method for subclass implementation

---

## 🚀 What to Implement Next

### Phase 1: Core Transformation Engine (Week 1)

#### 1.1 Defensive Upsert Logic
**File**: `src/mlb_data_platform/transform/upsert.py`

```python
def defensive_upsert(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    primary_keys: List[str],
    timestamp_column: str = "source_captured_at"
) -> int:
    """
    Implement MERGE INTO logic:
    - Only update if incoming timestamp >= existing timestamp
    - Insert if not exists
    - Return rows affected
    """
    pass
```

**Tasks**:
- [ ] Implement PostgreSQL MERGE INTO via JDBC
- [ ] Add Delta Lake alternative using `delta.tables.DeltaTable.merge()`
- [ ] Add conflict resolution logging
- [ ] Add metrics (inserts vs updates vs skipped)

#### 1.2 Concrete Game Transformation
**File**: `src/mlb_data_platform/transform/game/live_game_v1.py`

```python
class GameLiveV1Transformation(BaseTransformation):
    """Transform game.live_game_v1 → 17 normalized tables."""

    def __init__(self, spark: SparkSession, mode: TransformMode = TransformMode.BATCH):
        super().__init__(spark, "game", "live_game_v1", mode)

    def run(
        self,
        game_pks: Optional[List[int]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Run transformation:
        1. Read raw data (filtered by game_pks or date range)
        2. Transform to all 17 target tables
        3. Write with defensive upserts
        4. Export to S3/Delta
        5. Return metrics
        """
        pass
```

**Tasks**:
- [ ] Implement `run()` method
- [ ] Add progress logging (Rich progress bars)
- [ ] Add error handling and retry logic
- [ ] Add data quality validation
- [ ] Add S3/Delta export

#### 1.3 Complete Remaining Target Tables
**File**: `config/schemas/mappings/game/live_game_v1.yaml`

Add complete definitions for remaining 12 tables:
- [ ] game.live_game_runners (baserunners during plays)
- [ ] game.live_game_scoring_plays (plays that scored runs)
- [ ] game.live_game_home_batting_order (home team lineup)
- [ ] game.live_game_away_batting_order (away team lineup)
- [ ] game.live_game_home_pitchers (home team pitchers used)
- [ ] game.live_game_away_pitchers (away team pitchers used)
- [ ] game.live_game_home_bench (home team bench players)
- [ ] game.live_game_away_bench (away team bench players)
- [ ] game.live_game_home_bullpen (home team bullpen)
- [ ] game.live_game_away_bullpen (away team bullpen)
- [ ] game.live_game_umpires (umpire crew)
- [ ] game.live_game_venue_details (venue-specific details)

### Phase 2: Data Quality & Validation (Week 2)

#### 2.1 Avro Schema Generation
**File**: `src/mlb_data_platform/schema/avro_generator.py`

```python
def generate_avro_schema(target: TargetTable) -> dict:
    """Generate Avro schema from target table definition."""
    pass

def validate_dataframe_against_avro(
    df: DataFrame,
    avro_schema: dict
) -> Tuple[bool, List[str]]:
    """Validate DataFrame against Avro schema."""
    pass
```

**Tasks**:
- [ ] Map SQL types → Avro types
- [ ] Generate Avro schema JSON
- [ ] Implement validation logic
- [ ] Add schema evolution support (backward/forward compatibility)

#### 2.2 Great Expectations Integration
**File**: `src/mlb_data_platform/quality/expectations.py`

```python
class DataQualityValidator:
    """Validate data using Great Expectations."""

    def validate_target(
        self,
        df: DataFrame,
        target: TargetTable
    ) -> Tuple[bool, List[str]]:
        """Run all data quality rules for target."""
        pass
```

**Tasks**:
- [ ] Convert `DataQualityRule` to GE expectations
- [ ] Run expectations on DataFrame
- [ ] Generate validation report
- [ ] Fail on error-level violations

### Phase 3: Workflow Generation (Week 3)

#### 3.1 Argo Workflow Templates
**File**: `helm/mlb-data-platform/templates/workflows/`

Create Argo Workflow templates:
- [ ] `season-batch-workflow.yaml` - Full season backfill
- [ ] `daily-schedule-workflow.yaml` - Daily schedule → games
- [ ] `game-live-streaming-workflow.yaml` - Live game streaming
- [ ] `game-transform-workflow.yaml` - Post-game batch transform

#### 3.2 Workflow Generator
**File**: `src/mlb_data_platform/orchestration/workflow_generator.py`

```python
class WorkflowGenerator:
    """Generate Argo Workflows from configuration."""

    def generate_season_workflow(
        self,
        season: str,
        start_date: date,
        end_date: date
    ) -> dict:
        """Generate workflow for entire season."""
        pass

    def generate_game_streaming_workflow(
        self,
        game_pk: int,
        start_time: datetime
    ) -> dict:
        """Generate streaming workflow for live game."""
        pass

    def generate_daily_batch_workflow(
        self,
        date: date
    ) -> dict:
        """Generate workflow for single day's games."""
        pass
```

**Tasks**:
- [ ] Implement season workflow generation
- [ ] Implement game streaming workflow
- [ ] Implement daily batch workflow
- [ ] Add workflow submission via Argo client
- [ ] Add workflow monitoring/status checking

### Phase 4: Kubernetes Deployment (Week 4)

#### 4.1 Helm Values for Local Testing
**File**: `helm/mlb-data-platform/values.local.yaml`

```yaml
# Local desktop K8s cluster configuration
global:
  environment: local

spark:
  master: k8s://https://kubernetes.default.svc
  nodeSelector:
    workload: data-processing

postgresql:
  host: postgres.mlb-data-platform.svc.cluster.local
  port: 5432

minio:
  endpoint: minio.mlb-data-platform.svc.cluster.local
  port: 9000

argo:
  enabled: true
  namespace: mlb-data-platform
```

**Tasks**:
- [ ] Create `values.local.yaml` template
- [ ] Add `.local.yaml` to `.gitignore`
- [ ] Document setup process
- [ ] Add node selector configuration
- [ ] Add resource limits for local cluster

#### 4.2 Spark Operator Configuration
**File**: `helm/mlb-data-platform/templates/spark/spark-operator.yaml`

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: game-transform
spec:
  type: Python
  pythonVersion: "3.11"
  mode: cluster
  image: "mlb-data-platform-spark:latest"
  mainApplicationFile: "local:///app/transform/game/live_game_v1.py"
  # ... rest of config
```

**Tasks**:
- [ ] Create SparkApplication templates
- [ ] Add batch job template
- [ ] Add streaming job template
- [ ] Configure Spark executors (memory, cores)
- [ ] Add service account and RBAC

#### 4.3 Database User Provisioning
**File**: `scripts/provision_customer.sh`

```bash
#!/bin/bash
# Provision PostgreSQL role for customer access

CUSTOMER_NAME=$1
TIER=$2  # free, basic, premium, enterprise

# Create role
psql -c "CREATE ROLE ${CUSTOMER_NAME} WITH LOGIN PASSWORD '...';"

# Grant schema permissions based on tier
if [[ "$TIER" == "free" ]]; then
  psql -c "GRANT SELECT ON ALL TABLES IN SCHEMA schedule TO ${CUSTOMER_NAME};"
elif [[ "$TIER" == "premium" ]]; then
  psql -c "GRANT SELECT ON ALL TABLES IN SCHEMA game TO ${CUSTOMER_NAME};"
  psql -c "GRANT SELECT ON ALL TABLES IN SCHEMA schedule TO ${CUSTOMER_NAME};"
fi
```

**Tasks**:
- [ ] Implement customer provisioning script
- [ ] Create tier-based permission templates
- [ ] Add Kubernetes Secret generation
- [ ] Add customer metadata tracking table

### Phase 5: Monitoring & Lineage (Week 5)

#### 5.1 Apache Atlas / DataHub Integration
**File**: `src/mlb_data_platform/lineage/reporter.py`

```python
class LineageReporter:
    """Report data lineage to Apache Atlas or DataHub."""

    def report_transformation(
        self,
        source_table: str,
        target_tables: List[str],
        job_id: str,
        metrics: dict
    ):
        """Report transformation lineage."""
        pass
```

**Tasks**:
- [ ] Setup Apache Atlas OR DataHub (choose one)
- [ ] Implement lineage reporting
- [ ] Add entity registration (tables, columns)
- [ ] Add process lineage (transformations)
- [ ] Create lineage visualization dashboards

#### 5.2 Prometheus Metrics
**File**: `src/mlb_data_platform/metrics/exporter.py`

```python
from prometheus_client import Counter, Histogram, Gauge

# Metrics
ROWS_PROCESSED = Counter('mlb_transform_rows_processed', 'Rows processed')
TRANSFORM_DURATION = Histogram('mlb_transform_duration_seconds', 'Transform duration')
CURRENT_GAMES_STREAMING = Gauge('mlb_games_streaming', 'Games currently streaming')
```

**Tasks**:
- [ ] Add Prometheus client library
- [ ] Define transformation metrics
- [ ] Add metrics to transformation code
- [ ] Create Grafana dashboards

---

## 📋 Testing Strategy

### Unit Tests
**Directory**: `tests/unit/transform/`

```python
def test_game_live_v1_transformation():
    """Test game.live_game_v1 transformation."""
    # Load test data from stubs
    # Run transformation
    # Assert output schema matches
    # Assert row counts correct
    # Assert data quality rules pass
    pass
```

### Integration Tests
**Directory**: `tests/integration/transform/`

```python
def test_game_live_v1_end_to_end():
    """Test complete pipeline: ingest → transform → validate."""
    # Start local PostgreSQL
    # Ingest test data (stub mode)
    # Run transformation
    # Query normalized tables
    # Verify relationships
    pass
```

### BDD Tests
**Directory**: `tests/bdd/transform/`

```gherkin
Feature: Game Live Feed Transformation

  Scenario: Transform completed game to normalized tables
    Given a completed game with game_pk "747175"
    And raw data exists in game.live_game_v1
    When I run GameLiveV1Transformation
    Then 17 normalized tables should be populated
    And all foreign key relationships should be valid
    And data quality checks should pass
```

---

## 🎯 Success Criteria

### Milestone 1: Core Engine Working
- [ ] Can transform `game.live_game_v1` → 5 target tables
- [ ] Defensive upsert prevents data overwrites
- [ ] Schema validation catches errors
- [ ] Unit tests pass

### Milestone 2: Complete Game Transformation
- [ ] All 17 target tables populating correctly
- [ ] Relationships valid across all tables
- [ ] Data quality rules enforcing constraints
- [ ] Integration tests pass

### Milestone 3: Workflow Orchestration
- [ ] Can generate Argo Workflows programmatically
- [ ] Season batch workflow completes successfully
- [ ] Daily workflow processes all games
- [ ] Live game streaming works end-to-end

### Milestone 4: Production Ready
- [ ] Deploys to desktop K8s cluster
- [ ] Spark jobs run via Spark Operator
- [ ] Monitoring dashboards functional
- [ ] Data lineage visible in Atlas/DataHub
- [ ] Customer provisioning automated

---

## 📚 Additional Documentation Needed

- [ ] `docs/DEPLOYMENT_GUIDE.md` - Step-by-step deployment
- [ ] `docs/SPARK_JOBS_GUIDE.md` - Running and monitoring Spark jobs
- [ ] `docs/WORKFLOW_GUIDE.md` - Creating and submitting workflows
- [ ] `docs/CUSTOMER_PROVISIONING.md` - Customer onboarding process
- [ ] `docs/TROUBLESHOOTING.md` - Common issues and solutions

---

## 🤝 Next Session Plan

**Recommended Focus**: Phase 1.2 - Implement `GameLiveV1Transformation.run()`

This will give us a working end-to-end transformation that we can test and iterate on.

**Steps**:
1. Implement the `run()` method in `GameLiveV1Transformation`
2. Test with stub data from `pymlb_statsapi`
3. Verify output against expected schema
4. Add defensive upsert logic
5. Run end-to-end test

This will validate the entire framework and identify any gaps in the architecture.

---

**Status**: Ready to begin implementation! 🚀
