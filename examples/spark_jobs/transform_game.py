#!/usr/bin/env python3
"""Spark job entry point for game data transformation.

This script is executed by the SparkOperator in Kubernetes.
It reads environment variables for configuration and runs the LiveGameTransformation.

Environment variables:
    GAME_PKS: Comma-separated game PKs (e.g., "744834,745123")
    START_DATE: Start date for filtering (YYYY-MM-DD)
    END_DATE: End date for filtering (YYYY-MM-DD)
    EXPORT_TO_DELTA: "true" or "false"
    DELTA_PATH: S3/MinIO path for Delta Lake export
    POSTGRES_HOST: PostgreSQL hostname
    POSTGRES_USER: PostgreSQL username
    POSTGRES_PASSWORD: PostgreSQL password

Usage (local testing):
    GAME_PKS=744834 spark-submit transform_game.py

Usage (in Kubernetes):
    Executed by SparkOperator using workflow-game-transform.yaml
"""

import json
import os
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.mlb_data_platform.transform.game import LiveGameTransformation


def get_env_var(name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Required environment variable {name} not set")
    return value


def parse_game_pks(game_pks_str: str) -> Optional[List[int]]:
    """Parse game PKs from comma-separated string."""
    if not game_pks_str:
        return None
    try:
        return [int(pk.strip()) for pk in game_pks_str.split(",")]
    except ValueError as e:
        raise ValueError(f"Invalid GAME_PKS format: {e}")


def main():
    """Main entry point for game transformation Spark job."""
    print("=" * 80)
    print("MLB Data Platform - Game Transformation Spark Job")
    print("=" * 80)

    # Parse environment variables
    game_pks_str = get_env_var("GAME_PKS", "")
    start_date = get_env_var("START_DATE", "")
    end_date = get_env_var("END_DATE", "")
    export_to_delta = get_env_var("EXPORT_TO_DELTA", "false").lower() == "true"
    delta_path = get_env_var("DELTA_PATH", "s3://mlb-data/game/live")

    postgres_host = get_env_var("POSTGRES_HOST", "localhost")
    postgres_user = get_env_var("POSTGRES_USER", "mlb_admin")
    postgres_password = get_env_var("POSTGRES_PASSWORD")
    postgres_db = get_env_var("POSTGRES_DB", "mlb_games")

    # Parse game PKs
    game_pks = parse_game_pks(game_pks_str) if game_pks_str else None

    print(f"Configuration:")
    print(f"  Game PKs: {game_pks if game_pks else 'All'}")
    print(f"  Date range: {start_date or 'Any'} to {end_date or 'Any'}")
    print(f"  Export to Delta: {export_to_delta}")
    print(f"  Delta path: {delta_path}")
    print(f"  PostgreSQL: {postgres_host}:5432/{postgres_db}")
    print()

    # Initialize Spark session
    print("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("mlb-game-transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Game data can be large - increase shuffle partitions
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
    print("Initializing LiveGameTransformation...")
    transform = LiveGameTransformation(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        enable_quality_checks=True,
    )

    # Run transformation
    print("Running transformation pipeline...")
    metrics = transform(
        game_pks=game_pks,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
        export_to_delta=export_to_delta,
        delta_path=delta_path if export_to_delta else None,
    )

    # Print summary
    print()
    print("=" * 80)
    print("Transformation Complete!")
    print("=" * 80)
    print(f"Total games processed: {metrics['total_games']}")
    print(f"Team records created: {metrics['total_team_records']}")
    if metrics['games_by_state']:
        print(f"Games by state: {metrics['games_by_state']}")
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
