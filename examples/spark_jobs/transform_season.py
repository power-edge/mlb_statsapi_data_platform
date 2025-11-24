#!/usr/bin/env python3
"""Spark job entry point for season data transformation.

This script is executed by the SparkOperator in Kubernetes.
It reads environment variables for configuration and runs the SeasonTransformation.

Environment variables:
    SPORT_IDS: JSON array of sport IDs (e.g., "[1]")
    EXPORT_TO_DELTA: "true" or "false"
    DELTA_PATH: S3/MinIO path for Delta Lake export
    POSTGRES_HOST: PostgreSQL hostname
    POSTGRES_USER: PostgreSQL username
    POSTGRES_PASSWORD: PostgreSQL password

Usage (local testing):
    spark-submit transform_season.py

Usage (in Kubernetes):
    Executed by SparkOperator using workflow-season-transform.yaml
"""

import json
import os
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.mlb_data_platform.transform.season import SeasonTransformation


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


def main():
    """Main entry point for season transformation Spark job."""
    print("=" * 80)
    print("MLB Data Platform - Season Transformation Spark Job")
    print("=" * 80)

    # Parse environment variables
    sport_ids_str = get_env_var("SPORT_IDS", "[1]")
    export_to_delta = get_env_var("EXPORT_TO_DELTA", "false").lower() == "true"
    delta_path = get_env_var("DELTA_PATH", "s3://mlb-data/season/seasons")

    postgres_host = get_env_var("POSTGRES_HOST", "localhost")
    postgres_user = get_env_var("POSTGRES_USER", "mlb_admin")
    postgres_password = get_env_var("POSTGRES_PASSWORD")
    postgres_db = get_env_var("POSTGRES_DB", "mlb_games")

    sport_ids = parse_sport_ids(sport_ids_str)

    print(f"Configuration:")
    print(f"  Sport IDs: {sport_ids}")
    print(f"  Export to Delta: {export_to_delta}")
    print(f"  Delta path: {delta_path}")
    print(f"  PostgreSQL: {postgres_host}:5432/{postgres_db}")
    print()

    # Initialize Spark session
    print("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("mlb-season-transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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
    print("Initializing SeasonTransformation...")
    transform = SeasonTransformation(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        enable_quality_checks=True,
    )

    # Run transformation
    print("Running transformation pipeline...")
    metrics = transform(
        sport_ids=sport_ids,
        export_to_delta=export_to_delta,
        delta_path=delta_path if export_to_delta else None,
    )

    # Print summary
    print()
    print("=" * 80)
    print("Transformation Complete!")
    print("=" * 80)
    print(f"Total seasons processed: {metrics['total_seasons']}")
    print(f"Unique seasons: {metrics['unique_seasons']}")
    print(f"Seasons by sport: {metrics['seasons_by_sport']}")
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
