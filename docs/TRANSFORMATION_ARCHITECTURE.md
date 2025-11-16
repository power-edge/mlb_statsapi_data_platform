# PySpark Transformation Architecture

**Version**: 1.0
**Status**: Design Document
**Last Updated**: 2024-11-15

---

## 🎯 Core Principles

1. **Schema-Driven**: All transformations defined by declarative schema mappings (YAML/Avro)
2. **Unified Codebase**: Single transformation logic handles both batch and streaming
3. **Defensive Writes**: Timestamp-based upserts ensure old data never overwrites new
4. **Idempotent**: Re-running transformations produces same result
5. **Data Quality**: Schema validation at read and write boundaries
6. **Lineage Tracking**: Every record tracks its source and transformation path

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Configuration Layer                        │
│  config/schemas/mappings/{endpoint}/{method}.yaml            │
│  - Field mappings (JSON Path → SQL column)                  │
│  - Relationship definitions (foreign keys)                   │
│  - Data quality rules (constraints, ranges)                  │
│  - Partition strategy (time-based, hash)                    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  Schema Registry Layer                       │
│  - Load and validate mapping YAML files                     │
│  - Generate Avro schemas from mappings                      │
│  - Provide schema metadata to transformations               │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              Transformation Execution Layer                  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  BaseTransformation (Abstract)                       │  │
│  │  - read_source() → DataFrame                         │  │
│  │  - validate_schema() → bool                          │  │
│  │  - transform() → Dict[str, DataFrame]                │  │
│  │  - write_targets() → metrics                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                              ↓                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Specialized Transformations                         │  │
│  │  - GameLiveTransformation                            │  │
│  │  - ScheduleTransformation                            │  │
│  │  - PersonTransformation                              │  │
│  │  - (Auto-generated from schema mappings)             │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                             │
│  - PostgreSQL (normalized relational tables)                │
│  - S3/MinIO (Parquet/Delta Lake for analytics)              │
│  - Defensive upsert logic (timestamp-based)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 📋 Schema Mapping Format

### File Structure

```
config/schemas/mappings/
├── game/
│   ├── live_game_v1.yaml          # Maps game.live_game_v1 → 17 normalized tables
│   ├── boxscore.yaml
│   └── linescore.yaml
├── schedule/
│   ├── schedule.yaml
│   └── postseason.yaml
├── person/
│   └── person.yaml
└── team/
    ├── team.yaml
    └── roster.yaml
```

### Example: `game/live_game_v1.yaml`

```yaml
# Mapping configuration for Game.liveGameV1() → normalized tables
version: "1.0"
source:
  endpoint: game
  method: live_game_v1
  raw_table: game.live_game_v1

# Target tables to create from this source
targets:
  - name: game.live_game_metadata
    description: "Top-level game metadata (teams, venue, status, timing)"
    partition_by: game_date
    primary_key: [game_pk]
    upsert_keys: [game_pk, captured_at]  # Defensive: don't overwrite if captured_at is older

    fields:
      - name: game_pk
        type: BIGINT
        json_path: $.gamePk
        nullable: false

      - name: game_type
        type: VARCHAR(5)
        json_path: $.gameData.game.type

      - name: season
        type: VARCHAR(4)
        json_path: $.gameData.game.season

      - name: game_date
        type: DATE
        json_path: $.gameData.datetime.officialDate

      - name: game_datetime
        type: TIMESTAMPTZ
        json_path: $.gameData.datetime.dateTime

      - name: venue_id
        type: INT
        json_path: $.gameData.venue.id

      - name: venue_name
        type: VARCHAR(100)
        json_path: $.gameData.venue.name

      - name: home_team_id
        type: INT
        json_path: $.gameData.teams.home.id

      - name: away_team_id
        type: INT
        json_path: $.gameData.teams.away.id

      - name: abstract_game_state
        type: VARCHAR(20)
        json_path: $.gameData.status.abstractGameState

      - name: detailed_state
        type: VARCHAR(50)
        json_path: $.gameData.status.detailedState

      # Metadata tracking
      - name: source_captured_at
        type: TIMESTAMPTZ
        source_field: captured_at  # From raw table, not JSON

      - name: transform_timestamp
        type: TIMESTAMPTZ
        expression: current_timestamp()

    relationships:
      - to_table: venue.venue
        from_field: venue_id
        to_field: venue_id
        type: many_to_one

      - to_table: team.team
        from_field: home_team_id
        to_field: team_id
        type: many_to_one

      - to_table: team.team
        from_field: away_team_id
        to_field: team_id
        type: many_to_one

    indexes:
      - columns: [game_date, game_pk]
        type: btree
      - columns: [venue_id]
        type: btree
      - columns: [home_team_id, away_team_id]
        type: btree

    data_quality:
      - rule: game_pk > 0
        severity: error
      - rule: game_date >= '2000-01-01'
        severity: error
      - rule: home_team_id != away_team_id
        severity: error

  - name: game.live_game_plays
    description: "Play-by-play events (at-bats, pitches, outcomes)"
    partition_by: game_date
    primary_key: [game_pk, play_id]
    upsert_keys: [game_pk, play_id, captured_at]

    # Array extraction: $.liveData.plays.allPlays[*]
    array_source: $.liveData.plays.allPlays

    fields:
      - name: game_pk
        type: BIGINT
        json_path: $.game_pk  # Inherited from parent context
        nullable: false

      - name: play_id
        type: VARCHAR(50)
        json_path: $.playId
        nullable: false

      - name: inning
        type: INT
        json_path: $.about.inning

      - name: half_inning
        type: VARCHAR(10)
        json_path: $.about.halfInning

      - name: at_bat_index
        type: INT
        json_path: $.atBatIndex

      - name: event_type
        type: VARCHAR(50)
        json_path: $.result.eventType

      - name: description
        type: TEXT
        json_path: $.result.description

      - name: rbi
        type: INT
        json_path: $.result.rbi
        default: 0

      - name: away_score
        type: INT
        json_path: $.result.awayScore

      - name: home_score
        type: INT
        json_path: $.result.homeScore

    relationships:
      - to_table: game.live_game_metadata
        from_field: game_pk
        to_field: game_pk
        type: many_to_one

  - name: game.live_game_pitch_events
    description: "Every pitch thrown in the game (location, velocity, type)"
    partition_by: game_date
    primary_key: [game_pk, play_id, pitch_number]

    # Nested array: $.liveData.plays.allPlays[*].playEvents[*]
    array_source: $.liveData.plays.allPlays[*].playEvents
    filter: $.isPitch == true

    fields:
      - name: game_pk
        type: BIGINT
        json_path: parent.game_pk

      - name: play_id
        type: VARCHAR(50)
        json_path: parent.playId

      - name: pitch_number
        type: INT
        json_path: $.pitchNumber

      - name: pitch_type
        type: VARCHAR(10)
        json_path: $.details.type.code

      - name: pitch_description
        type: VARCHAR(100)
        json_path: $.details.type.description

      - name: start_speed
        type: NUMERIC(5,2)
        json_path: $.pitchData.startSpeed

      - name: end_speed
        type: NUMERIC(5,2)
        json_path: $.pitchData.endSpeed

      - name: strike_zone_top
        type: NUMERIC(5,2)
        json_path: $.pitchData.strikeZoneTop

      - name: strike_zone_bottom
        type: NUMERIC(5,2)
        json_path: $.pitchData.strikeZoneBottom

      - name: coordinates_x
        type: NUMERIC(6,2)
        json_path: $.pitchData.coordinates.x

      - name: coordinates_y
        type: NUMERIC(6,2)
        json_path: $.pitchData.coordinates.y

      - name: pitch_call
        type: VARCHAR(50)
        json_path: $.details.call.description

    indexes:
      - columns: [game_pk, play_id]
        type: btree
      - columns: [pitch_type]
        type: btree

# Continue for all 17 target tables...
# (This is a representative sample)
```

---

## 🔄 Unified Batch + Streaming Logic

### Core Pattern

```python
class BaseTransformation(ABC):
    """Base class for all transformations.

    Handles both batch and streaming with same logic.
    """

    def __init__(
        self,
        spark: SparkSession,
        mapping_config: MappingConfig,
        mode: TransformMode = TransformMode.BATCH
    ):
        self.spark = spark
        self.config = mapping_config
        self.mode = mode

    def read_source(self) -> DataFrame:
        """Read source data (raw JSONB table).

        Returns same DataFrame structure for batch and streaming.
        Streaming uses readStream, batch uses read.
        """
        if self.mode == TransformMode.STREAMING:
            return (
                self.spark
                .readStream
                .format("postgresql")  # or use custom source
                .option("table", self.config.source.raw_table)
                .option("inferSchema", "false")
                .schema(self._get_source_schema())
                .load()
            )
        else:
            return (
                self.spark
                .read
                .format("postgresql")
                .option("table", self.config.source.raw_table)
                .load()
            )

    def transform(self, source_df: DataFrame) -> Dict[str, DataFrame]:
        """Transform source → targets.

        Same logic for batch and streaming.
        """
        target_dfs = {}

        for target_config in self.config.targets:
            if target_config.array_source:
                # Explode nested arrays
                df = self._explode_array(source_df, target_config)
            else:
                # Direct mapping
                df = self._map_fields(source_df, target_config)

            # Apply data quality rules
            df = self._apply_quality_rules(df, target_config)

            # Add metadata columns
            df = self._add_metadata(df, target_config)

            # Convert to Avro schema (validation)
            df = self._convert_to_avro(df, target_config)

            target_dfs[target_config.name] = df

        return target_dfs

    def write_targets(
        self,
        target_dfs: Dict[str, DataFrame],
        checkpoint_location: Optional[str] = None
    ) -> Dict[str, Any]:
        """Write target DataFrames to storage.

        Uses defensive upsert for both batch and streaming.
        """
        metrics = {}

        for table_name, df in target_dfs.items():
            target_config = self._get_target_config(table_name)

            if self.mode == TransformMode.STREAMING:
                query = self._write_streaming(
                    df,
                    target_config,
                    checkpoint_location
                )
                metrics[table_name] = {"stream_query": query}
            else:
                count = self._write_batch(df, target_config)
                metrics[table_name] = {"rows_written": count}

        return metrics

    def _write_batch(
        self,
        df: DataFrame,
        target_config: TargetConfig
    ) -> int:
        """Batch write with defensive upsert."""
        # Use foreachBatch to enable custom upsert logic
        return defensive_upsert(
            df=df,
            table=target_config.name,
            primary_keys=target_config.primary_key,
            upsert_keys=target_config.upsert_keys,
            timestamp_column="source_captured_at"
        )

    def _write_streaming(
        self,
        df: DataFrame,
        target_config: TargetConfig,
        checkpoint_location: str
    ):
        """Streaming write with defensive upsert."""
        return (
            df.writeStream
            .foreachBatch(
                lambda batch_df, batch_id: self._write_batch(
                    batch_df, target_config
                )
            )
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime="30 seconds")
            .start()
        )
```

---

## 🛡️ Defensive Upsert Logic

### Timestamp-Based Conflict Resolution

```python
def defensive_upsert(
    df: DataFrame,
    table: str,
    primary_keys: List[str],
    upsert_keys: List[str],
    timestamp_column: str = "source_captured_at"
) -> int:
    """Defensive UPSERT: never overwrite newer data with older data.

    Logic:
    1. For each row in df, check if matching row exists in target table
    2. If exists:
       - Compare timestamp_column values
       - Only update if incoming timestamp >= existing timestamp
    3. If not exists:
       - Insert new row

    Args:
        df: Source DataFrame to upsert
        table: Target table name
        primary_keys: Columns that uniquely identify a row
        upsert_keys: Columns to check for conflicts (includes timestamp)
        timestamp_column: Column to compare for recency

    Returns:
        Number of rows inserted/updated
    """

    # Create temp table from DataFrame
    temp_table = f"{table}_temp_{uuid.uuid4().hex[:8]}"
    df.createOrReplaceTempView(temp_table)

    # Build upsert SQL
    pk_condition = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])

    # Timestamp comparison: only update if source is newer or equal
    timestamp_check = f"source.{timestamp_column} >= target.{timestamp_column}"

    upsert_sql = f"""
    MERGE INTO {table} AS target
    USING {temp_table} AS source
    ON {pk_condition}
    WHEN MATCHED AND {timestamp_check} THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """

    # Execute merge
    result = spark.sql(upsert_sql)

    # Get metrics
    rows_affected = result.collect()[0][0] if result else 0

    return rows_affected
```

### Alternative: Delta Lake Merge

```python
from delta.tables import DeltaTable

def defensive_upsert_delta(
    df: DataFrame,
    table_path: str,
    primary_keys: List[str],
    timestamp_column: str = "source_captured_at"
) -> int:
    """Defensive upsert using Delta Lake MERGE.

    Benefits:
    - ACID transactions
    - Time travel (can query old versions)
    - Z-ordering for better query performance
    """
    delta_table = DeltaTable.forPath(spark, table_path)

    pk_condition = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])
    timestamp_check = f"source.{timestamp_column} >= target.{timestamp_column}"

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            pk_condition
        )
        .whenMatchedUpdate(
            condition=timestamp_check,
            set={"*": "source.*"}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return df.count()
```

---

## 🎯 Data Quality Gates

### Schema Validation

```python
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import fastavro

class DataQualityValidator:
    """Validates data at transformation boundaries."""

    def validate_avro_schema(
        self,
        df: DataFrame,
        avro_schema_path: str
    ) -> Tuple[bool, List[str]]:
        """Validate DataFrame against Avro schema.

        Returns:
            (is_valid, errors)
        """
        with open(avro_schema_path) as f:
            schema = fastavro.schema.load_schema(f)

        errors = []

        # Check field names match
        df_fields = set(df.columns)
        schema_fields = set([field['name'] for field in schema['fields']])

        missing = schema_fields - df_fields
        extra = df_fields - schema_fields

        if missing:
            errors.append(f"Missing required fields: {missing}")
        if extra:
            errors.append(f"Extra fields not in schema: {extra}")

        # Check data types
        for field in schema['fields']:
            if field['name'] not in df.columns:
                continue

            avro_type = field['type']
            spark_type = str(df.schema[field['name']].dataType)

            if not self._types_compatible(avro_type, spark_type):
                errors.append(
                    f"Type mismatch for {field['name']}: "
                    f"expected {avro_type}, got {spark_type}"
                )

        return len(errors) == 0, errors

    def validate_constraints(
        self,
        df: DataFrame,
        rules: List[QualityRule]
    ) -> Tuple[bool, List[str]]:
        """Validate data quality rules using Great Expectations."""
        ge_df = SparkDFDataset(df)

        errors = []

        for rule in rules:
            if rule.rule_type == "range":
                result = ge_df.expect_column_values_to_be_between(
                    column=rule.column,
                    min_value=rule.min_value,
                    max_value=rule.max_value
                )
            elif rule.rule_type == "not_null":
                result = ge_df.expect_column_values_to_not_be_null(
                    column=rule.column
                )
            elif rule.rule_type == "unique":
                result = ge_df.expect_column_values_to_be_unique(
                    column=rule.column
                )
            elif rule.rule_type == "sql":
                result = ge_df.expect_compound_columns_to_be_unique(
                    column_list=rule.columns
                )

            if not result.success:
                if rule.severity == "error":
                    errors.append(f"FAILED: {rule.description}")
                else:
                    print(f"WARNING: {rule.description}")

        return len(errors) == 0, errors
```

---

## 📦 S3/Delta Lake Export Strategy

### Layered Storage Pattern

```
s3://mlb-data-platform/
├── raw/                           # Layer 1: Raw JSONB (Avro format)
│   ├── game/live_game_v1/
│   │   └── year=2024/
│   │       └── month=11/
│   │           └── day=15/
│   │               └── part-00000.avro
│   └── schedule/schedule/
│       └── date=2024-11-15/
│           └── part-00000.avro
│
├── normalized/                    # Layer 2: Normalized tables (Parquet)
│   ├── game/
│   │   ├── live_game_metadata/
│   │   │   └── game_date=2024-11-15/
│   │   │       └── part-00000.parquet
│   │   ├── live_game_plays/
│   │   └── live_game_pitch_events/
│   └── schedule/
│       └── schedule/
│
└── analytics/                     # Layer 3: Aggregated (Delta Lake)
    ├── team_travel/
    │   └── season=2024/
    │       └── _delta_log/
    └── player_performance/
        └── season=2024/
```

### Export Configuration

```yaml
# config/exports/s3_export.yaml
version: "1.0"

storage:
  backend: s3
  bucket: mlb-data-platform
  region: us-east-1

layers:
  raw:
    path: raw/{endpoint}/{method}
    format: avro
    compression: snappy
    partition_by: [year, month, day]
    retention_days: 90

  normalized:
    path: normalized/{schema}/{table}
    format: parquet
    compression: snappy
    partition_by: [game_date]  # or custom per table
    retention_days: 365

  analytics:
    path: analytics/{table}
    format: delta
    partition_by: [season]
    z_order_by: [game_pk, team_id]  # Optimize queries
    retention_days: -1  # Keep forever
```

---

## 🔄 Workflow Generation

### Dynamic Workflow Generator

```python
class WorkflowGenerator:
    """Generates Argo Workflows from configuration."""

    def generate_season_workflow(
        self,
        season: str,
        start_date: date,
        end_date: date
    ) -> dict:
        """Generate workflow for entire season.

        Workflow structure:
        1. Fetch season metadata
        2. For each game day:
           a. Fetch schedule
           b. For each game:
              - Fetch live game data
              - Transform to normalized tables
              - Export to S3
        """
        workflow = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {
                "generateName": f"season-{season}-",
                "labels": {
                    "season": season,
                    "workflow-type": "season-batch"
                }
            },
            "spec": {
                "entrypoint": "season-pipeline",
                "arguments": {
                    "parameters": [
                        {"name": "season", "value": season},
                        {"name": "start_date", "value": start_date.isoformat()},
                        {"name": "end_date", "value": end_date.isoformat()}
                    ]
                },
                "templates": [
                    self._build_season_pipeline_template(),
                    self._build_daily_schedule_template(),
                    self._build_game_transform_template(),
                ]
            }
        }

        return workflow

    def generate_game_streaming_workflow(
        self,
        game_pk: int,
        start_time: datetime
    ) -> dict:
        """Generate streaming workflow for live game.

        Workflow:
        1. Wait until 30 minutes before game start
        2. Start streaming ingestion (poll every 30s)
        3. Transform and write to database continuously
        4. Stop when game state = "Final"
        5. Run final batch transform for completeness
        """
        # Implementation...
        pass
```

---

## 📊 Monitoring & Lineage

### Data Lineage Tracking

Every record tracks its lineage:

```sql
-- Lineage metadata added to all normalized tables
ALTER TABLE game.live_game_metadata
ADD COLUMN lineage_metadata JSONB DEFAULT '{
  "source_table": "game.live_game_v1",
  "source_row_id": null,
  "transform_job": null,
  "transform_timestamp": null,
  "data_quality_passed": true,
  "quality_checks_run": []
}'::jsonb;
```

## 🎯 Data Quality with PyDeequ

We use **PyDeequ** (AWS's data quality library) instead of Great Expectations because:
- **Native PySpark integration**: Better performance on large datasets
- **Designed for big data**: Scales to billions of rows
- **Apache 2.0 license**: Permissive open source
- **AWS-maintained**: Reliable and well-supported
- **Works with Kappa architecture**: Same checks for batch and streaming

### PyDeequ Integration

```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

from mlb_data_platform.quality import DataQualityValidator

# Validate DataFrame against schema rules
validator = DataQualityValidator(spark)
result = validator.validate(df, target_config)

if not result.is_valid():
    print(f"Validation failed: {result.errors}")
    # Fail job or log warning based on severity

# Profile data to understand characteristics
profiles = validator.profile_data(df)
print(f"Completeness: {profiles['game_pk']['completeness']}")

# Suggest new constraints based on data
suggestions = validator.suggest_constraints(df, target_config)
for rule in suggestions:
    print(f"Suggested: {rule.rule} ({rule.severity})")
```

### Integration with Apache Atlas / DataHub

```python
class LineageReporter:
    """Report lineage to Apache Atlas or DataHub."""

    def report_transform(
        self,
        source_table: str,
        target_tables: List[str],
        job_id: str,
        metrics: dict
    ):
        """Report transformation lineage."""
        # Send to Atlas/DataHub REST API
        pass
```

---

## 🚀 Next Steps

1. Implement `BaseTransformation` class
2. Create schema mapping for `game.live_game_v1`
3. Build defensive upsert logic
4. Add Avro schema validation
5. Create workflow generator
6. Setup Apache Atlas/DataHub for lineage

---

**Status**: Ready for implementation ✅
