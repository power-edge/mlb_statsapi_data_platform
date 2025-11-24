#!/usr/bin/env python3
"""Quick test for Statcast data extraction (plays + pitch_events).

Tests that we can successfully extract:
- Plays (at-bats) with matchup info
- Pitch events with full Statcast data (pitchData + hitData)

Usage:
    python test_statcast_extraction.py
"""

import os
import sys
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from mlb_data_platform.transform.game.live_game_v2 import LiveGameTransformV2


def main():
    print("Testing Statcast Data Extraction")
    print("=" * 80)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("test-statcast") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    # Create transform
    transform = LiveGameTransformV2(spark)

    # Test with game 745123 (Cardinals vs Guardians - has complete Statcast data)
    print("\nExtracting game 745123...")
    results = transform(
        game_pks=[745123],
        extractions=["plays", "pitch_events"],  # Test only these two
        write_to_postgres=False,
    )

    # Check results
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)

    extractions = results.get("extractions", {})

    if "plays" in extractions:
        play_count = extractions["plays"]["row_count"]
        print(f"✅ Plays: {play_count} at-bats extracted")

        # Show sample
        if play_count > 0:
            print("\nSample play:")
            plays_df = transform._extract_plays(
                spark.read.jdbc(
                    transform.jdbc_url,
                    "(SELECT * FROM game.live_game_v1 WHERE game_pk = 745123) as t",
                    properties=transform.jdbc_properties
                )
            )
            plays_df.select("game_pk", "at_bat_index", "inning", "half_inning",
                           "batter_name", "pitcher_name", "event", "description") \
                    .show(3, truncate=False)

    if "pitch_events" in extractions:
        pitch_count = extractions["pitch_events"]["row_count"]
        print(f"\n✅ Pitch Events: {pitch_count} pitches extracted")

        # Show sample with hitData
        if pitch_count > 0:
            print("\nSample pitches (with hitData when in play):")
            pitches_df = transform._extract_pitch_events(
                spark.read.jdbc(
                    transform.jdbc_url,
                    "(SELECT * FROM game.live_game_v1 WHERE game_pk = 745123) as t",
                    properties=transform.jdbc_properties
                )
            )

            # Show pitches with hit data
            print("\nPitches with hit data (balls in play):")
            pitches_df.filter("launch_speed IS NOT NULL") \
                      .select("game_pk", "pitch_number", "pitch_type", "start_speed",
                             "launch_speed", "launch_angle", "total_distance",
                             "trajectory", "hardness") \
                      .show(5, truncate=False)

            # Stats
            hit_count = pitches_df.filter("launch_speed IS NOT NULL").count()
            print(f"\n📊 Statcast Stats:")
            print(f"   Total pitches: {pitch_count}")
            print(f"   Balls in play (with hitData): {hit_count}")
            print(f"   Percentage with hitData: {100*hit_count/pitch_count:.1f}%")

    spark.stop()
    print("\n✅ Test complete!")


if __name__ == "__main__":
    main()
