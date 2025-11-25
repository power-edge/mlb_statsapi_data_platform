#!/usr/bin/env python3
"""Example demonstrating PyDeequ data quality validation.

This script shows how to use the DeequValidator for validating MLB data
with predefined quality rules.

Requirements:
    - PySpark with PyDeequ package
    - Sample data (creates mock data if not available)

Usage:
    python examples/deequ_validation_example.py
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mlb_data_platform.quality.deequ_validator import DeequValidator


def create_sample_game_data(spark: SparkSession):
    """Create sample game data for validation.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with sample game data
    """
    schema = StructType(
        [
            StructField("game_pk", IntegerType(), False),
            StructField("game_date", StringType(), False),
            StructField("home_team_id", IntegerType(), False),
            StructField("away_team_id", IntegerType(), False),
            StructField("game_state", StringType(), False),
            StructField("captured_at", TimestampType(), False),
        ]
    )

    data = [
        (12345, "2024-07-15", 111, 112, "Final", datetime(2024, 7, 15, 22, 0, 0)),
        (12346, "2024-07-15", 113, 114, "Live", datetime(2024, 7, 15, 20, 30, 0)),
        (12347, "2024-07-16", 115, 116, "Scheduled", datetime(2024, 7, 16, 10, 0, 0)),
        (12348, "2024-07-16", 117, 118, "Preview", datetime(2024, 7, 16, 9, 0, 0)),
    ]

    return spark.createDataFrame(data, schema)


def create_sample_schedule_data(spark: SparkSession):
    """Create sample schedule data for validation.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with sample schedule data
    """
    schema = StructType(
        [
            StructField("schedule_date", StringType(), False),
            StructField("sport_id", IntegerType(), False),
            StructField("total_games", IntegerType(), False),
            StructField("captured_at", TimestampType(), False),
        ]
    )

    data = [
        ("2024-07-15", 1, 15, datetime(2024, 7, 15, 10, 0, 0)),
        ("2024-07-16", 1, 14, datetime(2024, 7, 16, 10, 0, 0)),
        ("2024-07-17", 1, 16, datetime(2024, 7, 17, 10, 0, 0)),
    ]

    return spark.createDataFrame(data, schema)


def create_invalid_game_data(spark: SparkSession):
    """Create invalid game data to demonstrate validation failures.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with invalid game data
    """
    schema = StructType(
        [
            StructField("game_pk", IntegerType(), False),
            StructField("game_date", StringType(), False),
            StructField("home_team_id", IntegerType(), False),
            StructField("away_team_id", IntegerType(), False),
            StructField("game_state", StringType(), False),
            StructField("captured_at", TimestampType(), False),
        ]
    )

    # Invalid data: negative game_pk, same home/away team, invalid game_state
    data = [
        (-1, "2024-07-15", 111, 112, "Final", datetime(2024, 7, 15, 22, 0, 0)),  # negative game_pk
        (12346, "2024-07-15", 113, 113, "Live", datetime(2024, 7, 15, 20, 30, 0)),  # same teams
        (
            12347,
            "2024-07-16",
            115,
            116,
            "InvalidState",
            datetime(2024, 7, 16, 10, 0, 0),
        ),  # invalid state
    ]

    return spark.createDataFrame(data, schema)


def print_validation_result(name: str, result):
    """Print validation result in a formatted way.

    Args:
        name: Name of the validation
        result: DeequValidationResult object
    """
    print("\n" + "=" * 80)
    print(f"Validation: {name}")
    print("=" * 80)
    print(f"Status: {result.status.value}")
    print(f"Passed: {result.passed_count}")
    print(f"Failed: {result.failed_count}")
    print(f"Warnings: {result.warning_count}")

    if not result.is_success():
        print("\nErrors:")
        for error in result.get_errors():
            print(f"  ❌ {error}")

    if result.has_warnings():
        print("\nWarnings:")
        for warning in result.get_warnings():
            print(f"  ⚠️  {warning}")

    if result.get_metrics():
        print("\nMetrics:")
        for metric_name, metric_value in result.get_metrics().items():
            print(f"  📊 {metric_name}: {metric_value}")


def main():
    """Run validation examples."""
    print("PyDeequ Data Quality Validation Example")
    print("=" * 80)

    # Create SparkSession
    print("\n1. Creating SparkSession...")
    spark = (
        SparkSession.builder.appName("deequ_validation_example")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )

    # Initialize validator
    print("2. Initializing DeequValidator...")
    validator = DeequValidator(spark)

    # Example 1: Validate valid game data
    print("\n3. Validating valid game data...")
    valid_game_df = create_sample_game_data(spark)
    print("\nSample data:")
    valid_game_df.show(truncate=False)

    game_result = validator.validate_game_data(valid_game_df, log_results=False)
    print_validation_result("Valid Game Data", game_result)

    # Example 2: Validate valid schedule data
    print("\n4. Validating valid schedule data...")
    schedule_df = create_sample_schedule_data(spark)
    print("\nSample data:")
    schedule_df.show(truncate=False)

    schedule_result = validator.validate_schedule_data(schedule_df, log_results=False)
    print_validation_result("Valid Schedule Data", schedule_result)

    # Example 3: Validate invalid game data
    print("\n5. Validating invalid game data (demonstrating failures)...")
    invalid_game_df = create_invalid_game_data(spark)
    print("\nSample data:")
    invalid_game_df.show(truncate=False)

    invalid_result = validator.validate_game_data(invalid_game_df, log_results=False)
    print_validation_result("Invalid Game Data", invalid_result)

    # Example 4: Custom validation
    print("\n6. Custom validation example...")
    custom_result = validator.validate(
        df=valid_game_df,
        check_builder=lambda check: (
            check.hasSize(lambda sz: sz >= 3, "Should have at least 3 rows")
            .isComplete("game_pk")
            .isUnique("game_pk")
        ),
        check_name="custom_game_validation",
        log_results=False,
    )
    print_validation_result("Custom Game Validation", custom_result)

    # Example 5: Show result as dictionary (for logging/storage)
    print("\n7. Result as dictionary (for logging/storage):")
    print(game_result.to_dict())

    # Cleanup
    print("\n8. Cleaning up...")
    spark.stop()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
