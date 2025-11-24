#!/usr/bin/env python3
"""Spark job entry point for complete game transformation.

This job transforms raw game.live_game_v1 JSONB data into 19 normalized
PostgreSQL tables covering metadata, players, lineups, scoring, and play-by-play.

Usage:
    # Transform specific game
    GAME_PKS='[744834]' python transform_game_complete.py

    # Transform date range
    START_DATE='2024-07-01' END_DATE='2024-07-31' python transform_game_complete.py

    # Transform and write to PostgreSQL
    WRITE_TO_POSTGRES=true GAME_PKS='[744834]' python transform_game_complete.py

Environment Variables:
    GAME_PKS: JSON array of game PKs to transform (e.g., '[744834, 745001]')
    START_DATE: Start date for game filter (YYYY-MM-DD)
    END_DATE: End date for game filter (YYYY-MM-DD)
    WRITE_TO_POSTGRES: Write results to PostgreSQL (true/false, default: false)
    EXPORT_TO_DELTA: Export to Delta Lake (true/false, default: false)
    DELTA_PATH: Delta Lake base path (e.g., s3://mlb-data/delta/game/)

    POSTGRES_HOST: PostgreSQL host (default: localhost)
    POSTGRES_PORT: PostgreSQL port (default: 5432)
    POSTGRES_DB: Database name (default: mlb_games)
    POSTGRES_USER: Database user (default: mlb_admin)
    POSTGRES_PASSWORD: Database password (required)

Examples:
    # Local development - transform one game
    export POSTGRES_PASSWORD=mlb_dev_password
    export GAME_PKS='[744834]'
    export WRITE_TO_POSTGRES=true
    spark-submit \
        --packages org.postgresql:postgresql:42.6.0 \
        transform_game_complete.py

    # Production - transform yesterday's games
    export POSTGRES_PASSWORD=$(aws secretsmanager get-secret-value ...)
    export START_DATE=$(date -d yesterday +%Y-%m-%d)
    export END_DATE=$(date -d yesterday +%Y-%m-%d)
    export WRITE_TO_POSTGRES=true
    spark-submit \
        --master k8s://https://kubernetes.default.svc:443 \
        --deploy-mode cluster \
        --packages org.postgresql:postgresql:42.6.0 \
        transform_game_complete.py
"""

import os
import sys
import json
from typing import List, Optional

from pyspark.sql import SparkSession

# Add src to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mlb_data_platform.transform.game.live_game_complete import LiveGameCompleteTransformation


def parse_env_list(env_var: str, default: Optional[List[int]] = None) -> Optional[List[int]]:
    """Parse JSON array from environment variable."""
    value = os.getenv(env_var)
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        print(f"Error: {env_var} must be a valid JSON array (e.g., '[744834, 745001]')")
        sys.exit(1)


def parse_env_bool(env_var: str, default: bool = False) -> bool:
    """Parse boolean from environment variable."""
    value = os.getenv(env_var, "").lower()
    if value in ("true", "1", "yes"):
        return True
    if value in ("false", "0", "no", ""):
        return False if not value else default
    return default


def main():
    """Main entry point for game transformation Spark job."""
    print("=" * 80)
    print("Complete Game Transformation Spark Job")
    print("=" * 80)

    # Parse environment variables
    game_pks = parse_env_list("GAME_PKS")
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")
    write_to_postgres = parse_env_bool("WRITE_TO_POSTGRES", default=False)
    export_to_delta = parse_env_bool("EXPORT_TO_DELTA", default=False)
    delta_path = os.getenv("DELTA_PATH")

    # Validate inputs
    if not game_pks and not start_date and not end_date:
        print("\nError: Must specify either GAME_PKS or START_DATE/END_DATE")
        print("\nExamples:")
        print("  GAME_PKS='[744834]' python transform_game_complete.py")
        print("  START_DATE='2024-07-01' END_DATE='2024-07-31' python transform_game_complete.py")
        sys.exit(1)

    if export_to_delta and not delta_path:
        print("\nError: DELTA_PATH must be set when EXPORT_TO_DELTA=true")
        sys.exit(1)

    # Validate PostgreSQL password
    pg_password = os.getenv("POSTGRES_PASSWORD")
    if not pg_password:
        print("\nError: POSTGRES_PASSWORD environment variable is required")
        sys.exit(1)

    # Display configuration
    print("\nConfiguration:")
    print(f"  Game PKs: {game_pks or 'All'}")
    print(f"  Date Range: {start_date or 'Any'} to {end_date or 'Any'}")
    print(f"  Write to PostgreSQL: {write_to_postgres}")
    print(f"  Export to Delta: {export_to_delta}")
    if delta_path:
        print(f"  Delta Path: {delta_path}")
    print()

    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("mlb-game-complete-transform") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")
    print()

    # Create transformation instance
    transform = LiveGameCompleteTransformation(
        spark=spark,
        enable_quality_checks=True,
    )

    # Execute transformation
    try:
        results = transform(
            game_pks=game_pks,
            start_date=start_date,
            end_date=end_date,
            write_to_postgres=write_to_postgres,
            export_to_delta=export_to_delta,
            delta_path=delta_path,
        )

        # Print summary
        print("\n" + "=" * 80)
        print("Transformation Results")
        print("=" * 80)
        summary = results.get("summary", {})
        print(f"  Games Processed: {summary.get('games_count', 0)}")
        print(f"  Tables Generated: {summary.get('tables_generated', 0)}")
        print(f"  Total Rows: {summary.get('total_rows', 0)}")
        print()

        if write_to_postgres:
            write_metrics = results.get("write_metrics", {})
            if write_metrics:
                print("Rows written to PostgreSQL:")
                for table, count in sorted(write_metrics.items()):
                    print(f"  {table}: {count} rows")
            else:
                print("  No data written (all tables empty)")
            print()

        print("✓ Job completed successfully")
        spark.stop()
        sys.exit(0)

    except Exception as e:
        print(f"\n✗ Job failed with error: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
