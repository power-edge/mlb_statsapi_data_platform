#!/usr/bin/env python3
"""Example: Transform raw JSONB to normalized metadata table.

This example demonstrates a simple transformation from:
- Source: game.live_game_v1_raw (JSONB with full API response)
- Target: game.live_game_metadata (normalized relational table)

This is a minimal proof-of-concept showing the transformation pattern.
For production, use the full PySpark transformation framework in src/mlb_data_platform/transform/
"""

from datetime import datetime, timezone
from typing import Dict, Any

from sqlmodel import Session, select
from mlb_data_platform.database import get_session
from mlb_data_platform.models import RawLiveGameV1
from mlb_data_platform.models.game_live import LiveGameMetadata


def extract_metadata_from_jsonb(raw_game: RawLiveGameV1) -> Dict[str, Any]:
    """Extract metadata fields from raw JSONB data.

    Args:
        raw_game: Raw game record with JSONB data

    Returns:
        Dict of metadata fields ready for LiveGameMetadata model
    """
    data = raw_game.data

    # Extract nested fields from JSONB
    game_data = data.get("gameData", {})
    live_data = data.get("liveData", {})

    game_info = game_data.get("game", {})
    datetime_info = game_data.get("datetime", {})
    venue_info = game_data.get("venue", {})
    teams_info = game_data.get("teams", {})
    status_info = game_data.get("status", {})
    weather_info = game_data.get("weather", {})
    linescore = live_data.get("linescore", {})
    linescore_teams = linescore.get("teams", {})

    # Build metadata dict
    metadata = {
        # Primary key
        "game_pk": data.get("gamePk"),

        # Game classification
        "game_type": game_info.get("type"),
        "season": game_info.get("season"),
        "season_type": game_info.get("seasonDisplay"),

        # Timing
        "game_date": datetime_info.get("officialDate"),
        "game_datetime": datetime_info.get("dateTime"),
        "day_night": datetime_info.get("dayNight"),
        "time_zone": datetime_info.get("time"),

        # Venue
        "venue_id": venue_info.get("id"),
        "venue_name": venue_info.get("name"),

        # Teams
        "home_team_id": teams_info.get("home", {}).get("id"),
        "home_team_name": teams_info.get("home", {}).get("name"),
        "away_team_id": teams_info.get("away", {}).get("id"),
        "away_team_name": teams_info.get("away", {}).get("name"),

        # Game status
        "abstract_game_state": status_info.get("abstractGameState"),
        "coded_game_state": status_info.get("codedGameState"),
        "detailed_state": status_info.get("detailedState"),
        "status_code": status_info.get("statusCode"),
        "reason": status_info.get("reason"),

        # Scores
        "home_score": linescore_teams.get("home", {}).get("runs"),
        "away_score": linescore_teams.get("away", {}).get("runs"),

        # Weather
        "weather_condition": weather_info.get("condition"),
        "weather_temp": weather_info.get("temp"),
        "weather_wind": weather_info.get("wind"),

        # Source metadata
        "source_raw_id": raw_game.game_pk,  # We don't have a separate ID column yet
        "source_captured_at": raw_game.captured_at,
        "transform_timestamp": datetime.now(timezone.utc),
    }

    return metadata


def main():
    """Transform raw data to normalized metadata."""
    print("=" * 80)
    print("Transform Example: Raw JSONB → Normalized Metadata")
    print("=" * 80)
    print()

    with get_session() as session:
        # ========================================================================
        # 1. Load raw data
        # ========================================================================
        print("1. Loading raw data from game.live_game_v1_raw...")
        print()

        stmt = select(RawLiveGameV1).order_by(RawLiveGameV1.captured_at.desc())
        raw_games = session.exec(stmt).all()

        print(f"   ✓ Found {len(raw_games)} raw game records")
        print()

        if not raw_games:
            print("   No raw data found. Run examples/raw_ingestion_example.py first!")
            return

        # ========================================================================
        # 2. Transform each raw record
        # ========================================================================
        print("2. Transforming raw JSONB to normalized metadata...")
        print()

        transformed_count = 0
        for raw_game in raw_games:
            # Extract metadata
            metadata_dict = extract_metadata_from_jsonb(raw_game)

            # Check if already exists (defensive upsert pattern)
            existing = session.exec(
                select(LiveGameMetadata).where(
                    LiveGameMetadata.game_pk == metadata_dict["game_pk"]
                )
            ).first()

            if existing:
                # Update existing (simple overwrite for now)
                for key, value in metadata_dict.items():
                    setattr(existing, key, value)
                print(f"   ✓ Updated metadata for game_pk={metadata_dict['game_pk']}")
            else:
                # Insert new
                metadata = LiveGameMetadata(**metadata_dict)
                session.add(metadata)
                print(f"   ✓ Inserted metadata for game_pk={metadata_dict['game_pk']}")

            transformed_count += 1

        # Commit all changes
        session.commit()

        print()
        print(f"   ✓ Transformed {transformed_count} games")
        print()

        # ========================================================================
        # 3. Query normalized data
        # ========================================================================
        print("3. Querying normalized metadata table...")
        print()

        stmt = select(LiveGameMetadata).order_by(LiveGameMetadata.game_date.desc())
        metadata_records = session.exec(stmt).all()

        print(f"   ✓ Found {len(metadata_records)} metadata records")
        print()

        # Display sample
        if metadata_records:
            sample = metadata_records[0]
            print("   Sample record:")
            print(f"      game_pk: {sample.game_pk}")
            print(f"      game_date: {sample.game_date}")
            print(f"      home_team: {sample.home_team_name}")
            print(f"      away_team: {sample.away_team_name}")
            print(f"      home_score: {sample.home_score}")
            print(f"      away_score: {sample.away_score}")
            print(f"      game_state: {sample.abstract_game_state}")
            print(f"      venue: {sample.venue_name}")
            print(f"      weather: {sample.weather_condition}, {sample.weather_temp}°F")
            print(f"      source_captured_at: {sample.source_captured_at}")
            print(f"      transform_timestamp: {sample.transform_timestamp}")

        print()

    print("=" * 80)
    print("✓ Transformation Complete!")
    print("=" * 80)
    print()
    print("Data Flow:")
    print("  1. Raw JSONB (game.live_game_v1_raw)")
    print("  2. Extract fields using JSON path navigation")
    print("  3. Defensive upsert to normalized table (game.live_game_metadata)")
    print("  4. Query normalized table for analytics")
    print()
    print("Next Steps:")
    print("  • Add transformations for other tables (plays, pitches, players, etc.)")
    print("  • Use PySpark for scalable batch transformation")
    print("  • Add checkpoint tracking for incremental processing")
    print("  • Implement data quality validation")
    print()


if __name__ == "__main__":
    main()
