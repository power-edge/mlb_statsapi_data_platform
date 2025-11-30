"""Unit tests for defensive upsert logic.

These tests validate the timestamp-based defensive MERGE operations using
in-memory DataFrames and stub data.

Delta Lake tests require:
- Java 17+ runtime
- Run via Docker for full environment:
    docker compose --profile spark run --rm spark pytest tests/unit/test_upsert.py
"""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mlb_data_platform.transform.upsert import (
    UpsertMetrics,
    _build_postgres_merge_sql,
)


def _check_java_version():
    """Check if Java 17+ is available."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        # Java version appears in stderr
        version_output = result.stderr
        # Look for version pattern like "17.0" or "21.0"
        import re

        match = re.search(r'"(\d+)\.', version_output)
        if match:
            major_version = int(match.group(1))
            return major_version >= 17
        return False
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


# Check Java availability once at module load
_JAVA_17_AVAILABLE = _check_java_version()


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing.

    Skips if Java 17+ is not available (required for PySpark 3.5+).
    """
    if not _JAVA_17_AVAILABLE:
        pytest.skip("Java 17+ required for Delta Lake tests (run via Docker Spark container)")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("test-upsert")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return StructType(
        [
            StructField("game_pk", LongType(), nullable=False),
            StructField("game_date", StringType(), nullable=True),
            StructField("home_team_id", LongType(), nullable=True),
            StructField("away_team_id", LongType(), nullable=True),
            StructField("captured_at", TimestampType(), nullable=False),
        ]
    )


def test_upsert_metrics_initialization():
    """Test UpsertMetrics initialization and properties."""
    metrics = UpsertMetrics(
        total_source_rows=100, inserted_rows=40, updated_rows=50, skipped_rows=10
    )

    assert metrics.total_source_rows == 100
    assert metrics.inserted_rows == 40
    assert metrics.updated_rows == 50
    assert metrics.skipped_rows == 10
    assert metrics.error_rows == 0
    assert metrics.total_written == 90  # inserted + updated


def test_upsert_metrics_to_dict():
    """Test UpsertMetrics to_dict conversion."""
    metrics = UpsertMetrics(
        total_source_rows=100, inserted_rows=40, updated_rows=50, skipped_rows=10
    )

    result = metrics.to_dict()

    assert result["total_source_rows"] == 100
    assert result["inserted_rows"] == 40
    assert result["updated_rows"] == 50
    assert result["skipped_rows"] == 10
    assert result["total_written"] == 90


def test_build_postgres_merge_sql():
    """Test PostgreSQL MERGE SQL generation."""
    target_table = "game.live_game_metadata"
    temp_table = "game.live_game_metadata_temp_20241115_120000"
    primary_keys = ["game_pk"]
    timestamp_column = "captured_at"
    columns = ["game_pk", "game_date", "home_team_id", "away_team_id", "captured_at"]

    sql = _build_postgres_merge_sql(
        target_table=target_table,
        temp_table=temp_table,
        primary_keys=primary_keys,
        timestamp_column=timestamp_column,
        columns=columns,
    )

    # Verify SQL contains key components
    assert "INSERT INTO game.live_game_metadata" in sql
    assert "FROM game.live_game_metadata_temp_20241115_120000" in sql
    assert "ON CONFLICT (game_pk)" in sql
    assert "DO UPDATE SET" in sql
    assert "EXCLUDED.captured_at >= game.live_game_metadata.captured_at" in sql

    # Verify primary key is not in UPDATE SET clause
    assert "game_pk = EXCLUDED.game_pk" not in sql

    # Verify other columns are in UPDATE SET clause
    assert "game_date = EXCLUDED.game_date" in sql
    assert "home_team_id = EXCLUDED.home_team_id" in sql


def test_build_postgres_merge_sql_composite_key():
    """Test PostgreSQL MERGE SQL with composite primary key."""
    target_table = "game.live_game_plays"
    temp_table = "game.live_game_plays_temp_20241115_120000"
    primary_keys = ["game_pk", "play_index"]
    timestamp_column = "captured_at"
    columns = ["game_pk", "play_index", "inning", "at_bat_index", "captured_at"]

    sql = _build_postgres_merge_sql(
        target_table=target_table,
        temp_table=temp_table,
        primary_keys=primary_keys,
        timestamp_column=timestamp_column,
        columns=columns,
    )

    # Verify composite key in ON CONFLICT clause
    assert "ON CONFLICT (game_pk, play_index)" in sql

    # Verify neither primary key is in UPDATE SET clause
    assert "game_pk = EXCLUDED.game_pk" not in sql
    assert "play_index = EXCLUDED.play_index" not in sql

    # Verify other columns are in UPDATE SET clause
    assert "inning = EXCLUDED.inning" in sql


def test_defensive_upsert_delta_new_table(spark, sample_schema, tmp_path):
    """Test defensive upsert to new Delta table (no existing data)."""
    from mlb_data_platform.transform.upsert import defensive_upsert_delta

    # Create source DataFrame
    now = datetime.now(timezone.utc)
    source_data = [
        (747175, "2024-10-25", 147, 119, now),
        (747176, "2024-10-26", 147, 119, now),
        (747177, "2024-10-27", 147, 119, now),
    ]
    source_df = spark.createDataFrame(source_data, schema=sample_schema)

    # Target path
    target_path = str(tmp_path / "delta_table")

    # Perform upsert
    metrics = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk"],
        timestamp_column="captured_at",
    )

    # Verify metrics
    assert metrics.total_source_rows == 3
    assert metrics.inserted_rows == 3  # All rows inserted (new table)
    assert metrics.updated_rows == 0
    assert metrics.skipped_rows == 0

    # Verify data written
    result_df = spark.read.format("delta").load(target_path)
    assert result_df.count() == 3


def test_defensive_upsert_delta_update_with_newer_data(spark, sample_schema, tmp_path):
    """Test defensive upsert updates existing rows when source is newer."""
    from mlb_data_platform.transform.upsert import defensive_upsert_delta

    # Create initial target data (older timestamp)
    old_time = datetime(2024, 10, 25, 10, 0, 0, tzinfo=timezone.utc)
    target_data = [
        (747175, "2024-10-25", 147, 119, old_time),
        (747176, "2024-10-26", 147, 119, old_time),
    ]
    target_df = spark.createDataFrame(target_data, schema=sample_schema)

    # Write initial target table
    target_path = str(tmp_path / "delta_table")
    target_df.write.format("delta").mode("overwrite").save(target_path)

    # Create source data with newer timestamp
    new_time = datetime(2024, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    source_data = [
        (747175, "2024-10-25", 147, 143, new_time),  # Updated away_team_id
        (747177, "2024-10-27", 147, 119, new_time),  # New game
    ]
    source_df = spark.createDataFrame(source_data, schema=sample_schema)

    # Perform upsert
    metrics = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk"],
        timestamp_column="captured_at",
    )

    # Verify metrics
    assert metrics.total_source_rows == 2
    assert metrics.inserted_rows == 1  # game_pk 747177
    assert metrics.updated_rows == 1  # game_pk 747175

    # Verify data
    result_df = spark.read.format("delta").load(target_path)
    assert result_df.count() == 3  # 2 original + 1 new

    # Verify updated row has new data
    updated_row = (
        result_df.filter("game_pk = 747175").select("away_team_id", "captured_at").first()
    )
    assert updated_row["away_team_id"] == 143  # Updated value
    assert updated_row["captured_at"] == new_time


def test_defensive_upsert_delta_skip_with_older_data(spark, sample_schema, tmp_path):
    """Test defensive upsert skips updates when source is older."""
    from mlb_data_platform.transform.upsert import defensive_upsert_delta

    # Create initial target data (newer timestamp)
    new_time = datetime(2024, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    target_data = [
        (747175, "2024-10-25", 147, 119, new_time),
    ]
    target_df = spark.createDataFrame(target_data, schema=sample_schema)

    # Write initial target table
    target_path = str(tmp_path / "delta_table")
    target_df.write.format("delta").mode("overwrite").save(target_path)

    # Create source data with older timestamp
    old_time = datetime(2024, 10, 25, 10, 0, 0, tzinfo=timezone.utc)
    source_data = [
        (747175, "2024-10-25", 147, 143, old_time),  # Older data
    ]
    source_df = spark.createDataFrame(source_data, schema=sample_schema)

    # Perform upsert
    metrics = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk"],
        timestamp_column="captured_at",
    )

    # Verify metrics
    assert metrics.total_source_rows == 1
    assert metrics.inserted_rows == 0
    assert metrics.updated_rows == 0  # Skipped because source was older
    assert metrics.skipped_rows == 1

    # Verify data not overwritten
    result_df = spark.read.format("delta").load(target_path)
    assert result_df.count() == 1

    # Verify row still has original data
    row = result_df.filter("game_pk = 747175").select("away_team_id", "captured_at").first()
    assert row["away_team_id"] == 119  # Original value preserved
    assert row["captured_at"] == new_time


def test_defensive_upsert_delta_idempotency(spark, sample_schema, tmp_path):
    """Test defensive upsert is idempotent (running twice produces same result)."""
    from mlb_data_platform.transform.upsert import defensive_upsert_delta

    # Create source data
    now = datetime.now(timezone.utc)
    source_data = [
        (747175, "2024-10-25", 147, 119, now),
        (747176, "2024-10-26", 147, 119, now),
    ]
    source_df = spark.createDataFrame(source_data, schema=sample_schema)

    target_path = str(tmp_path / "delta_table")

    # First upsert
    metrics1 = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk"],
        timestamp_column="captured_at",
    )

    # Second upsert (same data)
    metrics2 = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk"],
        timestamp_column="captured_at",
    )

    # First run: all inserts
    assert metrics1.inserted_rows == 2
    assert metrics1.updated_rows == 0

    # Second run: should update (same timestamp, so update condition passes)
    # or skip (depending on implementation)
    # Delta Lake MERGE will update when timestamps are equal
    assert metrics2.inserted_rows == 0
    assert metrics2.updated_rows == 2  # Updated with same data

    # Verify final state is correct
    result_df = spark.read.format("delta").load(target_path)
    assert result_df.count() == 2


def test_defensive_upsert_delta_composite_key(spark, tmp_path):
    """Test defensive upsert with composite primary key."""
    from mlb_data_platform.transform.upsert import defensive_upsert_delta

    # Create schema for plays table
    schema = StructType(
        [
            StructField("game_pk", LongType(), nullable=False),
            StructField("play_index", LongType(), nullable=False),
            StructField("inning", LongType(), nullable=True),
            StructField("at_bat_index", LongType(), nullable=True),
            StructField("captured_at", TimestampType(), nullable=False),
        ]
    )

    # Create initial target data
    old_time = datetime(2024, 10, 25, 10, 0, 0, tzinfo=timezone.utc)
    target_data = [
        (747175, 1, 1, 1, old_time),
        (747175, 2, 1, 2, old_time),
    ]
    target_df = spark.createDataFrame(target_data, schema=schema)

    target_path = str(tmp_path / "delta_table")
    target_df.write.format("delta").mode("overwrite").save(target_path)

    # Create source data with updates and new rows
    new_time = datetime(2024, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    source_data = [
        (747175, 1, 1, 1, new_time),  # Same play, newer timestamp
        (747175, 3, 2, 3, new_time),  # New play
    ]
    source_df = spark.createDataFrame(source_data, schema=schema)

    # Perform upsert
    metrics = defensive_upsert_delta(
        source_df=source_df,
        target_table=target_path,
        primary_keys=["game_pk", "play_index"],  # Composite key
        timestamp_column="captured_at",
    )

    # Verify metrics
    assert metrics.total_source_rows == 2
    assert metrics.inserted_rows == 1  # play_index 3
    assert metrics.updated_rows == 1  # play_index 1

    # Verify data
    result_df = spark.read.format("delta").load(target_path)
    assert result_df.count() == 3  # 2 original + 1 new
