#!/usr/bin/env python3
"""Test script for refactored transformation architecture.

This script validates that all three transforms (game, season, schedule) work
correctly with the new base_v2.py extraction registry pattern.

Usage:
    python test_refactored_transforms.py

Environment Variables:
    POSTGRES_HOST: PostgreSQL host (default: localhost)
    POSTGRES_PORT: PostgreSQL port (default: 5432)
    POSTGRES_DB: Database name (default: mlb_games)
    POSTGRES_USER: Database user (default: mlb_admin)
    POSTGRES_PASSWORD: Database password (required)
"""

import os
import sys
from pyspark.sql import SparkSession

# Add src to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mlb_data_platform.transform.game.live_game_v2 import LiveGameTransformV2
from mlb_data_platform.transform.season.seasons_v2 import SeasonTransformV2
from mlb_data_platform.transform.schedule.schedule_v2 import ScheduleTransformV2


def test_game_transform(spark: SparkSession) -> bool:
    """Test game transformation with extraction registry pattern."""
    print("\n" + "=" * 80)
    print("TEST 1: Game Transform (19 table decomposition)")
    print("=" * 80)

    try:
        transform = LiveGameTransformV2(spark, enable_quality_checks=True)

        # Test with game 745123 (Cardinals vs Guardians with complete Statcast data)
        results = transform(
            game_pks=[745123],
            write_to_postgres=False,  # Dry-run mode
        )

        # Validate results
        assert results["status"] == "success", "Transform should succeed"
        assert results["filtered_count"] > 0, "Should find game records"

        extractions = results.get("extractions", {})
        print(f"\n✓ Game transform successful!")
        print(f"  Extractions registered: {len(transform._extractions)}")
        print(f"  Extractions executed: {len(extractions)}")
        print(f"  Expected: 19 tables")

        # Check that metadata extraction worked
        if "metadata" in extractions:
            print(f"  ✓ Metadata: {extractions['metadata']['row_count']} rows")

        return True

    except Exception as e:
        print(f"\n✗ Game transform failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_season_transform(spark: SparkSession) -> bool:
    """Test season transformation with extraction registry pattern."""
    print("\n" + "=" * 80)
    print("TEST 2: Season Transform (1 table)")
    print("=" * 80)

    try:
        transform = SeasonTransformV2(spark, enable_quality_checks=True)

        # Test with MLB sport_id=1
        results = transform(
            sport_ids=[1],
            write_to_postgres=False,  # Dry-run mode
        )

        # Validate results
        assert results["status"] == "success", "Transform should succeed"

        extractions = results.get("extractions", {})
        print(f"\n✓ Season transform successful!")
        print(f"  Extractions registered: {len(transform._extractions)}")
        print(f"  Extractions executed: {len(extractions)}")
        print(f"  Expected: 1 table")

        # Check that seasons extraction worked
        if "seasons" in extractions:
            print(f"  ✓ Seasons: {extractions['seasons']['row_count']} rows")

        return True

    except Exception as e:
        print(f"\n✗ Season transform failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_schedule_transform(spark: SparkSession) -> bool:
    """Test schedule transformation with extraction registry pattern."""
    print("\n" + "=" * 80)
    print("TEST 3: Schedule Transform (2 tables)")
    print("=" * 80)

    try:
        transform = ScheduleTransformV2(spark, enable_quality_checks=True)

        # Test with July 2024 date (we have data for 2024-07-04)
        results = transform(
            start_date="2024-07-01",
            end_date="2024-07-31",
            sport_ids=[1],
            write_to_postgres=False,  # Dry-run mode
        )

        # Validate results
        assert results["status"] == "success", "Transform should succeed"

        extractions = results.get("extractions", {})
        print(f"\n✓ Schedule transform successful!")
        print(f"  Extractions registered: {len(transform._extractions)}")
        print(f"  Extractions executed: {len(extractions)}")
        print(f"  Expected: 2 tables")

        # Check that both extractions worked
        if "metadata" in extractions:
            print(f"  ✓ Metadata: {extractions['metadata']['row_count']} rows")
        if "games" in extractions:
            print(f"  ✓ Games: {extractions['games']['row_count']} rows")

        return True

    except Exception as e:
        print(f"\n✗ Schedule transform failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all transformation tests."""
    print("=" * 80)
    print("Refactored Transformation Architecture Test Suite")
    print("=" * 80)
    print("\nTesting base_v2.py extraction registry pattern")
    print("All transforms use generic __call__() from BaseTransformation")
    print()

    # Validate PostgreSQL password
    pg_password = os.getenv("POSTGRES_PASSWORD")
    if not pg_password:
        print("\n✗ Error: POSTGRES_PASSWORD environment variable is required")
        print("Example: export POSTGRES_PASSWORD=mlb_dev_password")
        sys.exit(1)

    # Create Spark session
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("test-refactored-transforms") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")

    # Run tests
    results = []
    results.append(("Game Transform", test_game_transform(spark)))
    results.append(("Season Transform", test_season_transform(spark)))
    results.append(("Schedule Transform", test_schedule_transform(spark)))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    # Cleanup
    spark.stop()

    # Exit with appropriate code
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
