"""Unit tests for DeequValidator.

These tests require:
- SPARK_VERSION environment variable (e.g., "3.5")
- Java 17+ runtime

Run via Docker for full environment:
    docker compose --profile spark run --rm spark pytest tests/unit/test_deequ_validator.py
"""

import os
from datetime import datetime

import pytest

# Skip entire module if SPARK_VERSION not set (PyDeequ requirement)
if os.environ.get("SPARK_VERSION") is None:
    pytest.skip(
        "SPARK_VERSION env required for PyDeequ (run tests via Docker Spark container)",
        allow_module_level=True,
    )

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mlb_data_platform.quality.deequ_validator import (
    DeequValidationResult,
    DeequValidator,
    ValidationStatus,
)


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for tests with PyDeequ JAR."""
    spark_session = (
        SparkSession.builder.appName("test_deequ_validator")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def validator(spark):
    """Create DeequValidator instance."""
    return DeequValidator(spark)


class TestDeequValidationResult:
    """Test DeequValidationResult dataclass."""

    def test_is_success_with_success_status(self):
        """Test is_success returns True for SUCCESS status."""
        result = DeequValidationResult(
            status=ValidationStatus.SUCCESS,
            passed_count=5,
            failed_count=0,
            warning_count=0,
        )
        assert result.is_success() is True

    def test_is_success_with_warning_status(self):
        """Test is_success returns True for WARNING status."""
        result = DeequValidationResult(
            status=ValidationStatus.WARNING,
            passed_count=4,
            failed_count=0,
            warning_count=1,
        )
        assert result.is_success() is True

    def test_is_success_with_error_status(self):
        """Test is_success returns False for ERROR status."""
        result = DeequValidationResult(
            status=ValidationStatus.ERROR,
            passed_count=3,
            failed_count=1,
            warning_count=0,
        )
        assert result.is_success() is False

    def test_has_warnings(self):
        """Test has_warnings returns correct value."""
        result_with_warnings = DeequValidationResult(
            status=ValidationStatus.WARNING,
            passed_count=4,
            failed_count=0,
            warning_count=1,
        )
        assert result_with_warnings.has_warnings() is True

        result_no_warnings = DeequValidationResult(
            status=ValidationStatus.SUCCESS,
            passed_count=5,
            failed_count=0,
            warning_count=0,
        )
        assert result_no_warnings.has_warnings() is False

    def test_to_dict(self):
        """Test to_dict returns correct dictionary representation."""
        result = DeequValidationResult(
            status=ValidationStatus.SUCCESS,
            passed_count=5,
            failed_count=0,
            warning_count=0,
            error_messages=["error1"],
            warning_messages=["warning1"],
            metrics={"completeness": 1.0},
        )

        result_dict = result.to_dict()
        assert result_dict["status"] == "success"
        assert result_dict["passed_count"] == 5
        assert result_dict["failed_count"] == 0
        assert result_dict["warning_count"] == 0
        assert result_dict["errors"] == ["error1"]
        assert result_dict["warnings"] == ["warning1"]
        assert result_dict["metrics"] == {"completeness": 1.0}


class TestDeequValidator:
    """Test DeequValidator class."""

    def test_validator_initialization(self, spark):
        """Test validator can be initialized with SparkSession."""
        validator = DeequValidator(spark)
        assert validator.spark == spark

    def test_validate_game_data_valid(self, spark, validator):
        """Test validate_game_data with valid game data."""
        # Create valid game data
        game_schema = StructType(
            [
                StructField("game_pk", IntegerType(), False),
                StructField("game_date", StringType(), False),
                StructField("home_team_id", IntegerType(), False),
                StructField("away_team_id", IntegerType(), False),
                StructField("game_state", StringType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        game_data = [
            (12345, "2024-07-15", 111, 112, "Final", datetime(2024, 7, 15, 22, 0, 0)),
            (12346, "2024-07-15", 113, 114, "Live", datetime(2024, 7, 15, 20, 30, 0)),
            (
                12347,
                "2024-07-16",
                115,
                116,
                "Scheduled",
                datetime(2024, 7, 16, 10, 0, 0),
            ),
        ]

        game_df = spark.createDataFrame(game_data, game_schema)

        # Validate
        result = validator.validate_game_data(game_df, log_results=False)

        # Should pass validation
        assert result.is_success()
        assert result.failed_count == 0
        assert result.status in [ValidationStatus.SUCCESS, ValidationStatus.WARNING]

    def test_validate_game_data_invalid_game_pk(self, spark, validator):
        """Test validate_game_data with invalid game_pk (negative value)."""
        game_schema = StructType(
            [
                StructField("game_pk", IntegerType(), False),
                StructField("game_date", StringType(), False),
                StructField("home_team_id", IntegerType(), False),
                StructField("away_team_id", IntegerType(), False),
                StructField("game_state", StringType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        # Invalid data: negative game_pk
        game_data = [
            (-1, "2024-07-15", 111, 112, "Final", datetime(2024, 7, 15, 22, 0, 0)),
        ]

        game_df = spark.createDataFrame(game_data, game_schema)

        # Validate
        result = validator.validate_game_data(game_df, log_results=False)

        # Should fail validation
        assert not result.is_success()
        assert result.failed_count > 0

    def test_validate_game_data_same_teams(self, spark, validator):
        """Test validate_game_data with same home and away team."""
        game_schema = StructType(
            [
                StructField("game_pk", IntegerType(), False),
                StructField("game_date", StringType(), False),
                StructField("home_team_id", IntegerType(), False),
                StructField("away_team_id", IntegerType(), False),
                StructField("game_state", StringType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        # Invalid data: same home and away team
        game_data = [
            (12345, "2024-07-15", 111, 111, "Final", datetime(2024, 7, 15, 22, 0, 0)),
        ]

        game_df = spark.createDataFrame(game_data, game_schema)

        # Validate
        result = validator.validate_game_data(game_df, log_results=False)

        # Should fail validation
        assert not result.is_success()
        assert result.failed_count > 0

    def test_validate_schedule_data_valid(self, spark, validator):
        """Test validate_schedule_data with valid schedule data."""
        schedule_schema = StructType(
            [
                StructField("schedule_date", StringType(), False),
                StructField("sport_id", IntegerType(), False),
                StructField("total_games", IntegerType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        schedule_data = [
            ("2024-07-15", 1, 15, datetime(2024, 7, 15, 10, 0, 0)),
            ("2024-07-16", 1, 14, datetime(2024, 7, 16, 10, 0, 0)),
        ]

        schedule_df = spark.createDataFrame(schedule_data, schedule_schema)

        # Validate
        result = validator.validate_schedule_data(schedule_df, log_results=False)

        # Should pass validation
        assert result.is_success()
        assert result.failed_count == 0

    def test_validate_schedule_data_invalid_sport_id(self, spark, validator):
        """Test validate_schedule_data with invalid sport_id (negative)."""
        schedule_schema = StructType(
            [
                StructField("schedule_date", StringType(), False),
                StructField("sport_id", IntegerType(), False),
                StructField("total_games", IntegerType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        # Invalid data: negative sport_id
        schedule_data = [
            ("2024-07-15", -1, 15, datetime(2024, 7, 15, 10, 0, 0)),
        ]

        schedule_df = spark.createDataFrame(schedule_data, schedule_schema)

        # Validate
        result = validator.validate_schedule_data(schedule_df, log_results=False)

        # Should fail validation
        assert not result.is_success()
        assert result.failed_count > 0

    def test_validate_season_data_valid(self, spark, validator):
        """Test validate_season_data with valid season data."""
        season_schema = StructType(
            [
                StructField("sport_id", IntegerType(), False),
                StructField("season_id", StringType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        season_data = [
            (1, "2024", datetime(2024, 7, 15, 10, 0, 0)),
            (1, "2023", datetime(2024, 7, 15, 10, 0, 0)),
        ]

        season_df = spark.createDataFrame(season_data, season_schema)

        # Validate
        result = validator.validate_season_data(season_df, log_results=False)

        # Should pass validation
        assert result.is_success()
        assert result.failed_count == 0

    def test_validate_player_data_valid(self, spark, validator):
        """Test validate_player_data with valid player data."""
        player_schema = StructType(
            [
                StructField("player_id", IntegerType(), False),
                StructField("full_name", StringType(), False),
                StructField("captured_at", TimestampType(), False),
            ]
        )

        player_data = [
            (123456, "John Doe", datetime(2024, 7, 15, 10, 0, 0)),
            (123457, "Jane Smith", datetime(2024, 7, 15, 10, 0, 0)),
        ]

        player_df = spark.createDataFrame(player_data, player_schema)

        # Validate
        result = validator.validate_player_data(player_df, log_results=False)

        # Should pass validation
        assert result.is_success()
        assert result.failed_count == 0

    def test_custom_validation(self, spark, validator):
        """Test custom validation using validate() method."""
        # Create simple DataFrame
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), False),
            ]
        )

        data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        df = spark.createDataFrame(data, schema)

        # Define custom validation
        result = validator.validate(
            df=df,
            check_builder=lambda check: check.hasSize(lambda sz: sz >= 3)
            .isComplete("id")
            .isComplete("name")
            .isUnique("id"),
            check_name="custom_validation",
            log_results=False,
        )

        # Should pass
        assert result.is_success()
        assert result.failed_count == 0

    def test_custom_validation_with_failure(self, spark, validator):
        """Test custom validation that fails."""
        # Create DataFrame with non-unique IDs
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), False),
            ]
        )

        # Duplicate ID
        data = [(1, "Alice"), (1, "Bob"), (2, "Charlie")]
        df = spark.createDataFrame(data, schema)

        # Define custom validation requiring unique ID
        result = validator.validate(
            df=df,
            check_builder=lambda check: check.isUnique("id"),
            check_name="uniqueness_check",
            log_results=False,
        )

        # Should fail
        assert not result.is_success()
        assert result.failed_count > 0
        assert len(result.get_errors()) > 0
