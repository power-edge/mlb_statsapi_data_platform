#!/usr/bin/env python3
"""Spark job entry point for schedule data transformation.

This script is executed by the SparkOperator in Kubernetes.
It reads environment variables for configuration and runs the ScheduleTransformation.

Environment variables:
    START_DATE: Start date for filtering (YYYY-MM-DD)
    END_DATE: End date for filtering (YYYY-MM-DD)
    SPORT_IDS: JSON array of sport IDs (e.g., "[1]")
    EXPORT_TO_DELTA: "true" or "false"
    DELTA_PATH: S3/MinIO path for Delta Lake export
    POSTGRES_HOST: PostgreSQL hostname
    POSTGRES_USER: PostgreSQL username
    POSTGRES_PASSWORD: PostgreSQL password

Usage (local testing):
    START_DATE=2024-07-01 END_DATE=2024-07-31 spark-submit transform_schedule.py

Usage (in Kubernetes):
    Executed by SparkOperator using workflow-schedule-transform.yaml
"""

import json
import os
import sys
from datetime import datetime
from typing import List, Optional

from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.mlb_data_platform.transform.schedule import ScheduleTransformation


def get_env_var(name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Required environment variable {name} not set")
    return value


def parse_sport_ids(sport_ids_str: str) -> List[int]:
    """Parse sport IDs from JSON string."""
    try:
        sport_ids = json.loads(sport_ids_str)
        if not isinstance(sport_ids, list):
            raise ValueError("SPORT_IDS must be a JSON array")
        return [int(sid) for sid in sport_ids]
    except (json.JSONDecodeError, ValueError) as e:
        raise ValueError(f"Invalid SPORT_IDS format: {e}")


def validate_date(date_str: str) -> str:
    """Validate date format (YYYY-MM-DD)."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str} (expected YYYY-MM-DD)")


def main():
    """Main entry point for schedule transformation Spark job."""
    print("=" * 80)
    print("MLB Data Platform - Schedule Transformation Spark Job")
    print("=" * 80)

    # Parse environment variables
    start_date = get_env_var("START_DATE", datetime.today().strftime("%Y-%m-%d"))
    end_date = get_env_var("END_DATE", datetime.today().strftime("%Y-%m-%d"))
    sport_ids_str = get_env_var("SPORT_IDS", "[1]")
    export_to_delta = get_env_var("EXPORT_TO_DELTA", "false").lower() == "true"
    delta_path = get_env_var("DELTA_PATH", "s3://mlb-data/schedule/games")

    postgres_host = get_env_var("POSTGRES_HOST", "localhost")
    postgres_user = get_env_var("POSTGRES_USER", "mlb_admin")
    postgres_password = get_env_var("POSTGRES_PASSWORD")
    postgres_db = get_env_var("POSTGRES_DB", "mlb_games")

    # Validate and parse inputs
    start_date = validate_date(start_date)
    end_date = validate_date(end_date)
    sport_ids = parse_sport_ids(sport_ids_str)

    print(f"Configuration:")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Sport IDs: {sport_ids}")
    print(f"  Export to Delta: {export_to_delta}")
    print(f"  Delta path: {delta_path}")
    print(f"  PostgreSQL: {postgres_host}:5432/{postgres_db}")
    print()

    # Initialize Spark session
    print("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("mlb-schedule-transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Increase shuffle partitions for schedule data (can have many games)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    # Configure JDBC properties
    jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"
    jdbc_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    print("Spark session initialized")
    print(f"Spark version: {spark.version}")
    print()

    # Initialize transformation
    print("Initializing ScheduleTransformation...")
    transform = ScheduleTransformation(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        enable_quality_checks=True,
    )

    # Run transformation
    print("Running transformation pipeline...")
    metrics = transform(
        start_date=start_date,
        end_date=end_date,
        sport_ids=sport_ids,
        export_to_delta=export_to_delta,
        delta_path=delta_path if export_to_delta else None,
    )

    # Print summary
    print()
    print("=" * 80)
    print("Transformation Complete!")
    print("=" * 80)
    print(f"Total schedules processed: {metrics['total_schedules']}")
    print(f"Total games extracted: {metrics['total_games']}")
    print(f"Games by sport: {metrics['games_by_sport']}")
    if metrics['top_dates']:
        print(f"Top dates by game count:")
        for date, count in metrics['top_dates']:
            print(f"  {date}: {count} games")
    if metrics['games_by_status']:
        print(f"Games by status: {metrics['games_by_status']}")
    print()

    # Stop Spark session
    spark.stop()

    print("✓ Job complete - Spark session stopped")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
