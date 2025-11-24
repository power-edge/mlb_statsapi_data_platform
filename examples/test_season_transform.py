#!/usr/bin/env python3
"""Test script for season transformation without spark-submit."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession

def main():
    """Test season transformation locally."""
    print("=" * 80)
    print("Testing Season Transformation")
    print("=" * 80)

    # Initialize Spark session
    print("\n1. Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("test-season-transform")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    print(f"   Spark version: {spark.version}")
    print(f"   Spark master: {spark.sparkContext.master}")

    # Configure JDBC
    jdbc_url = "jdbc:postgresql://localhost:5432/mlb_games"
    jdbc_properties = {
        "user": "mlb_admin",
        "password": "mlb_dev_password",
        "driver": "org.postgresql.Driver",
    }

    # Initialize transformation
    print("\n2. Initializing SeasonTransformation...")
    from mlb_data_platform.transform.season import SeasonTransformation

    transform = SeasonTransformation(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        enable_quality_checks=True,
    )

    # Run transformation
    print("\n3. Running transformation pipeline...")
    metrics = transform(
        sport_ids=[1],  # MLB only
        export_to_delta=False,
    )

    # Print results
    print("\n" + "=" * 80)
    print("Transformation Complete!")
    print("=" * 80)
    print(f"Total seasons processed: {metrics['total_seasons']}")
    print(f"Unique seasons: {metrics['unique_seasons']}")
    print(f"Seasons by sport: {metrics['seasons_by_sport']}")

    # Stop Spark
    spark.stop()
    print("\n✓ Test complete - Spark session stopped")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
