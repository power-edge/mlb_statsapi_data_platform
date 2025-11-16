# Defensive Upsert Implementation

**Status**: ✅ Completed
**Date**: 2024-11-15

## Overview

Implemented timestamp-based defensive MERGE operations that ensure **older data never overwrites newer data**. This is critical for:
- **Idempotent pipelines** - Safe to re-run transformations without data corruption
- **Correct backfills** - Can replay historical data without overwriting live updates
- **Data integrity** - Prevents race conditions in distributed systems

## Core Pattern

```python
# Create transformation with defensive upserts enabled (default)
transform = GameLiveV1Transformation(
    spark,
    mode=TransformMode.BATCH,
    enable_defensive_upsert=True  # Default
)

# When calling write_targets(), defensive upserts are automatic
metrics = transform(game_pks=[747175])

# Metrics show what happened:
{
    "inserted_rows": 10,    # New rows inserted
    "updated_rows": 5,      # Existing rows updated (source was newer)
    "skipped_rows": 2,      # Rows skipped (target was newer)
    "error_rows": 0         # Errors during upsert
}
```

## How It Works

### PostgreSQL Implementation

Uses `INSERT ... ON CONFLICT UPDATE` with timestamp comparison:

```sql
INSERT INTO target_table (game_pk, game_date, home_team_id, captured_at)
SELECT game_pk, game_date, home_team_id, captured_at
FROM temp_table
ON CONFLICT (game_pk)  -- Match on primary key
DO UPDATE SET
    game_date = EXCLUDED.game_date,
    home_team_id = EXCLUDED.home_team_id,
    captured_at = EXCLUDED.captured_at
WHERE EXCLUDED.captured_at >= target_table.captured_at  -- DEFENSIVE!
```

**Key components**:
1. **Primary key match**: `ON CONFLICT (game_pk)` - identifies existing rows
2. **Timestamp comparison**: `WHERE EXCLUDED.captured_at >= target_table.captured_at`
3. **Defensive behavior**: Only updates if source timestamp ≥ target timestamp

### Delta Lake Implementation

Uses Delta Lake's `MERGE INTO` with timestamp condition:

```python
from delta.tables import DeltaTable

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.game_pk = source.game_pk"  # Match condition
).whenMatchedUpdate(
    condition="source.captured_at >= target.captured_at",  # DEFENSIVE!
    set={"*": "source.*"}
).whenNotMatchedInsertAll().execute()
```

## Example Scenarios

### Scenario 1: Live Game Updates (No Conflicts)

```python
# 10:00 AM - Initial load
transform(game_pks=[747175])
# Result: inserted_rows=1

# 10:30 AM - Game update (newer data)
transform(game_pks=[747175])
# Result: updated_rows=1 (source timestamp > target timestamp)
```

### Scenario 2: Backfill with Defensive Protection

```python
# Current state: Live game data captured at 2:00 PM
# Database has: captured_at = 2024-10-25 14:00:00

# Backfill historical data from 10:00 AM
transform(game_pks=[747175])
# Source has: captured_at = 2024-10-25 10:00:00

# Result: skipped_rows=1 (source timestamp < target timestamp)
# ✅ Live data preserved, old data rejected
```

### Scenario 3: Re-running Same Data (Idempotency)

```python
# First run
metrics1 = transform(game_pks=[747175])
# Result: inserted_rows=1

# Second run (exact same data)
metrics2 = transform(game_pks=[747175])
# Result: updated_rows=1 (timestamps equal, update allowed)

# Data in database is identical
# ✅ Idempotent - safe to re-run
```

### Scenario 4: Parallel Processing (Race Conditions)

```python
# Two workers processing same game concurrently
# Worker 1: captured_at = 2024-10-25 14:00:05
# Worker 2: captured_at = 2024-10-25 14:00:10

# Worker 1 writes first
transform(game_pks=[747175])  # inserted_rows=1

# Worker 2 writes second (newer data)
transform(game_pks=[747175])  # updated_rows=1
# ✅ Newest data wins

# Reverse scenario:
# Worker 2 writes first (newer)
transform(game_pks=[747175])  # inserted_rows=1

# Worker 1 writes second (older)
transform(game_pks=[747175])  # skipped_rows=1
# ✅ Older data rejected, newest data preserved
```

## Architecture Integration

### BaseTransformation

The `BaseTransformation` class automatically uses defensive upserts:

```python
# src/mlb_data_platform/transform/base.py

class BaseTransformation(ABC):
    def __init__(
        self,
        spark: SparkSession,
        endpoint: str,
        method: str,
        enable_defensive_upsert: bool = True,  # Default enabled
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
    ):
        self.enable_defensive_upsert = enable_defensive_upsert
        # ...

    def _write_batch(self, df: DataFrame, target: TargetTable) -> Dict[str, Any]:
        """Write with defensive upserts."""
        if self.enable_defensive_upsert and target.upsert_keys:
            # Use defensive_upsert_postgres()
            metrics = defensive_upsert_postgres(
                spark=self.spark,
                source_df=df,
                target_table=target.name,
                primary_keys=target.primary_key,
                timestamp_column="captured_at",
                jdbc_url=self.jdbc_url,
                jdbc_properties=self.jdbc_properties,
            )
            return metrics.to_dict()
        else:
            # Fallback: regular JDBC write (not defensive)
            df.write.jdbc(...)
```

### Schema Mapping YAML

Tables must define `upsert_keys` to enable defensive upserts:

```yaml
# config/schemas/mappings/game/live_game_v1.yaml

targets:
  - name: game.live_game_metadata
    primary_key: [game_pk]
    upsert_keys: [game_pk, captured_at]  # Enable defensive upsert

    fields:
      - name: game_pk
        type: BIGINT
        nullable: false

      - name: captured_at
        type: TIMESTAMPTZ
        nullable: false
        # This timestamp is used for defensive comparison
```

## Configuration Options

### Enable/Disable Defensive Upserts

```python
# Option 1: Enable globally (default)
transform = GameLiveV1Transformation(
    spark,
    enable_defensive_upsert=True  # Default
)

# Option 2: Disable globally (use regular writes)
transform = GameLiveV1Transformation(
    spark,
    enable_defensive_upsert=False
)

# Option 3: Disable per-table (remove upsert_keys from YAML)
# If target.upsert_keys is None, falls back to regular JDBC write
```

### Custom JDBC Configuration

```python
transform = GameLiveV1Transformation(
    spark,
    jdbc_url="jdbc:postgresql://prod-db:5432/mlb_games",
    jdbc_properties={
        "user": "etl_user",
        "password": "secure_password",
        "driver": "org.postgresql.Driver",
    }
)
```

## Testing

### Unit Tests

Tests validate defensive behavior without requiring Spark/PostgreSQL:

```bash
# Run upsert tests
uv run pytest tests/unit/test_upsert.py -v

# Tests include:
# - Metrics initialization and conversion
# - PostgreSQL MERGE SQL generation
# - Delta Lake upsert logic (requires Spark)
```

**Test results**:
```
test_upsert_metrics_initialization PASSED
test_upsert_metrics_to_dict PASSED
test_build_postgres_merge_sql PASSED
test_build_postgres_merge_sql_composite_key PASSED
```

### Integration Tests (Future)

Will test with actual PostgreSQL and Delta Lake:

```python
def test_defensive_upsert_rejects_old_data():
    """Verify old data doesn't overwrite new data."""
    # Write new data first
    new_df = create_df(captured_at="2024-10-25 14:00:00")
    transform(df=new_df)

    # Try to write old data
    old_df = create_df(captured_at="2024-10-25 10:00:00")
    metrics = transform(df=old_df)

    assert metrics.skipped_rows == 1

    # Verify database still has new data
    result = read_from_db()
    assert result.captured_at == datetime(2024, 10, 25, 14, 0, 0)
```

## Performance Considerations

### PostgreSQL

- **Temporary table overhead**: Source data written to temp table first
- **MERGE operation**: Single SQL statement, efficient for small-medium batches
- **Index usage**: Ensure primary keys are indexed for fast lookups
- **Partitioning**: Time-based partitioning on `captured_at` improves performance

### Delta Lake

- **Native MERGE**: Delta Lake optimized for MERGE operations
- **Z-ordering**: Use Z-order on primary keys for faster lookups
- **File compaction**: Run `OPTIMIZE` periodically
- **Statistics**: Delta Lake maintains statistics for efficient pruning

### Recommendations

```python
# For large batches (>10M rows), consider:
# 1. Partition source data before upsert
source_df.repartition(100, "game_pk")

# 2. Use broadcast joins for small dimension tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

# 3. Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

## Monitoring & Observability

### Metrics Returned

Every upsert returns comprehensive metrics:

```python
metrics = {
    "total_source_rows": 100,
    "inserted_rows": 40,      # New records
    "updated_rows": 50,       # Updated because source was newer
    "skipped_rows": 10,       # Skipped because target was newer
    "error_rows": 0,          # Errors during upsert
    "total_written": 90       # inserted + updated
}
```

### Logging

Upserts automatically log to console using Rich:

```
[cyan]Upserting 100 rows to game.live_game_metadata[/cyan]
[dim]Writing to temporary table game.live_game_metadata_temp_20241115_120000[/dim]
[dim]Executing defensive MERGE[/dim]
[dim]Cleaning up temporary table[/dim]
[green]✓[/green] Upsert complete: [yellow]40[/yellow] inserted, [yellow]50[/yellow] updated, [yellow]10[/yellow] skipped
```

### Alerts

Set up alerts for:
- High skip rates (may indicate time skew issues)
- High error rates (database connection issues)
- Zero updates for live data (data pipeline lag)

## Troubleshooting

### Issue: All rows skipped (skipped_rows = total_source_rows)

**Cause**: Source data has older timestamps than target

**Solution**: Check if source data is stale or if there's a clock skew issue

```python
# Debug: Check timestamps
source_df.select("game_pk", "captured_at").show()
target_df = spark.read.jdbc(url, "game.live_game_metadata")
target_df.select("game_pk", "captured_at").show()
```

### Issue: No updates when expected (updated_rows = 0)

**Cause**: Source timestamp equals target timestamp, but data is different

**Solution**: Ensure `captured_at` is updated when data changes

```python
# When reading from API, always use fresh timestamp
df = df.withColumn("captured_at", F.current_timestamp())
```

### Issue: Temporary table not cleaned up

**Cause**: Exception during MERGE operation

**Solution**: Cleanup happens in try/finally, but verify manually

```sql
-- Find orphaned temp tables
SELECT tablename
FROM pg_tables
WHERE tablename LIKE '%_temp_%';

-- Drop manually if needed
DROP TABLE IF EXISTS game.live_game_metadata_temp_20241115_120000;
```

## Future Enhancements

1. **Conflict logging**: Log skipped rows to audit table for analysis
2. **Configurable timestamp column**: Allow custom timestamp column per table
3. **Multi-column timestamps**: Support version vectors for distributed systems
4. **Delta Lake optimization**: Auto-run OPTIMIZE after large upserts
5. **Metrics export**: Push metrics to Prometheus/Grafana

## References

- **Implementation**: `src/mlb_data_platform/transform/upsert.py`
- **Tests**: `tests/unit/test_upsert.py`
- **Integration**: `src/mlb_data_platform/transform/base.py`
- **Pattern docs**: `docs/CALLABLE_PATTERN.md`
- **Architecture**: `docs/TRANSFORMATION_ARCHITECTURE.md`

---

**Made defensive by design** ✅
