#!/usr/bin/env python3
"""End-to-End Pipeline Example: Ingest → Transform → Query

This demonstrates the complete data flow:
1. Ingest raw API response → PostgreSQL raw table (JSONB)
2. Transform raw JSONB → Normalized relational tables
3. Query normalized data for analytics

This is the core pattern for the MLB Data Platform.
"""

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from sqlmodel import select
from mlb_data_platform.database import get_session
from mlb_data_platform.ingestion import RawStorageClient
from mlb_data_platform.models import RawLiveGameV1
from mlb_data_platform.models.game_live import LiveGameMetadata


def load_stub_data() -> dict:
    """Load stub data from pymlb_statsapi."""
    stub_file = Path.home() / (
        "github.com/power-edge/pymlb_statsapi/tests/bdd/stubs/game/"
        "liveGameV1/liveGameV1_game_pk=747175_4240fc08a038.json.gz"
    )

    if not stub_file.exists():
        raise FileNotFoundError(f"Stub file not found: {stub_file}")

    with gzip.open(stub_file, "rt") as f:
        stub = json.load(f)

    return {
        "data": stub.get("response"),
        "metadata": {
            "endpoint": "game",
            "method": "liveGameV1",
            "params": {"game_pk": 747175},
            "url": "https://statsapi.mlb.com/api/v1.1/game/747175/feed/live",
            "status_code": 200,
            "captured_at": "2024-11-15T20:30:00Z",
        },
    }


def extract_metadata_from_jsonb(raw_game: RawLiveGameV1) -> Dict[str, Any]:
    """Extract metadata fields from raw JSONB."""
    data = raw_game.data
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

    return {
        "game_pk": data.get("gamePk"),
        "game_type": game_info.get("type"),
        "season": game_info.get("season"),
        "season_type": game_info.get("seasonDisplay"),
        "game_date": datetime_info.get("officialDate"),
        "game_datetime": datetime_info.get("dateTime"),
        "day_night": datetime_info.get("dayNight"),
        "time_zone": datetime_info.get("time"),
        "venue_id": venue_info.get("id"),
        "venue_name": venue_info.get("name"),
        "home_team_id": teams_info.get("home", {}).get("id"),
        "home_team_name": teams_info.get("home", {}).get("name"),
        "away_team_id": teams_info.get("away", {}).get("id"),
        "away_team_name": teams_info.get("away", {}).get("name"),
        "abstract_game_state": status_info.get("abstractGameState"),
        "coded_game_state": status_info.get("codedGameState"),
        "detailed_state": status_info.get("detailedState"),
        "status_code": status_info.get("statusCode"),
        "reason": status_info.get("reason"),
        "home_score": linescore_teams.get("home", {}).get("runs"),
        "away_score": linescore_teams.get("away", {}).get("runs"),
        "weather_condition": weather_info.get("condition"),
        "weather_temp": weather_info.get("temp"),
        "weather_wind": weather_info.get("wind"),
        "source_raw_id": raw_game.game_pk,
        "source_captured_at": raw_game.captured_at,
        "transform_timestamp": datetime.now(timezone.utc),
    }


def main():
    """Run complete end-to-end pipeline."""
    print("=" * 80)
    print("End-to-End Pipeline: Ingest → Transform → Query")
    print("=" * 80)
    print()

    storage = RawStorageClient()

    # ========================================================================
    # STEP 1: INGEST - Raw API Response → PostgreSQL JSONB
    # ========================================================================
    print("STEP 1: INGEST (Raw API Response → PostgreSQL JSONB)")
    print("-" * 80)
    print()

    # Load stub data
    response = load_stub_data()
    game_pk = response["data"].get("gamePk")

    print(f"   📦 Loaded stub data: game_pk={game_pk}")
    print(f"   📡 Endpoint: {response['metadata']['endpoint']}.{response['metadata']['method']}")
    print()

    # Store in raw table
    with get_session() as session:
        # Check if already exists
        existing = session.exec(
            select(RawLiveGameV1).where(RawLiveGameV1.game_pk == game_pk)
        ).first()

        if existing:
            print(f"   ⏭  Raw data already exists for game_pk={game_pk}, skipping ingestion")
        else:
            raw_game = storage.save_live_game(session, response)
            print(f"   ✓ Saved to game.live_game_v1_raw")
            print(f"   ✓ Primary key: (game_pk={raw_game.game_pk}, captured_at={raw_game.captured_at})")

    print()

    # ========================================================================
    # STEP 2: TRANSFORM - JSONB → Normalized Relational Tables
    # ========================================================================
    print("STEP 2: TRANSFORM (JSONB → Normalized Relational)")
    print("-" * 80)
    print()

    with get_session() as session:
        # Load raw data
        raw_game = session.exec(
            select(RawLiveGameV1)
            .where(RawLiveGameV1.game_pk == game_pk)
            .order_by(RawLiveGameV1.captured_at.desc())
        ).first()

        if not raw_game:
            print("   ✗ No raw data found!")
            return

        print(f"   🔄 Transforming game_pk={raw_game.game_pk}")
        print(f"   🔄 Source: game.live_game_v1_raw (captured_at={raw_game.captured_at})")
        print()

        # Extract metadata
        metadata_dict = extract_metadata_from_jsonb(raw_game)

        # Defensive upsert
        existing_metadata = session.exec(
            select(LiveGameMetadata).where(LiveGameMetadata.game_pk == game_pk)
        ).first()

        if existing_metadata:
            for key, value in metadata_dict.items():
                setattr(existing_metadata, key, value)
            action = "Updated"
        else:
            metadata = LiveGameMetadata(**metadata_dict)
            session.add(metadata)
            action = "Inserted"

        session.commit()

        print(f"   ✓ {action} game.live_game_metadata")
        print(f"   ✓ Extracted {len(metadata_dict)} fields from JSONB")
        print()

    # ========================================================================
    # STEP 3: QUERY - Analytics on Normalized Data
    # ========================================================================
    print("STEP 3: QUERY (Analytics on Normalized Data)")
    print("-" * 80)
    print()

    with get_session() as session:
        # Query normalized metadata
        metadata = session.exec(
            select(LiveGameMetadata).where(LiveGameMetadata.game_pk == game_pk)
        ).first()

        if not metadata:
            print("   ✗ No metadata found!")
            return

        print("   📊 Game Summary:")
        print(f"      Game: {metadata.away_team_name} @ {metadata.home_team_name}")
        print(f"      Date: {metadata.game_date} ({metadata.day_night})")
        print(f"      Score: {metadata.away_score}-{metadata.home_score}")
        print(f"      Status: {metadata.abstract_game_state}")
        print(f"      Venue: {metadata.venue_name}")
        print(f"      Weather: {metadata.weather_condition}, {metadata.weather_temp}°F, {metadata.weather_wind}")
        print()

        print("   📈 Data Lineage:")
        print(f"      Raw captured: {metadata.source_captured_at}")
        print(f"      Transformed: {metadata.transform_timestamp}")
        print(f"      Latency: {(metadata.transform_timestamp - metadata.source_captured_at).total_seconds():.2f} seconds")
        print()

    # ========================================================================
    # COMPARISON: Raw vs Normalized
    # ========================================================================
    print("COMPARISON: Raw JSONB vs Normalized Tables")
    print("-" * 80)
    print()

    with get_session() as session:
        # Raw query
        raw_game = session.exec(
            select(RawLiveGameV1).where(RawLiveGameV1.game_pk == game_pk)
        ).first()

        # Normalized query
        metadata = session.exec(
            select(LiveGameMetadata).where(LiveGameMetadata.game_pk == game_pk)
        ).first()

        print("   Raw Table (game.live_game_v1_raw):")
        print(f"      Storage: JSONB column (~850 KB for full response)")
        print(f"      Query: data->>'gameData'->>'teams'->>'home'->>'name'")
        print(f"      Indexing: Limited (can index top-level fields only)")
        print(f"      Use case: Full replay, historical versioning")
        print()

        print("   Normalized Table (game.live_game_metadata):")
        print(f"      Storage: Individual columns (~1 KB per row)")
        print(f"      Query: SELECT home_team_name FROM game.live_game_metadata")
        print(f"      Indexing: Full B-tree indexes on all columns")
        print(f"      Use case: Fast analytics, joins, aggregations")
        print()

    print("=" * 80)
    print("✓ End-to-End Pipeline Complete!")
    print("=" * 80)
    print()
    print("Summary:")
    print("  1. ✓ Ingested raw API response to PostgreSQL (JSONB)")
    print("  2. ✓ Transformed JSONB to normalized relational tables")
    print("  3. ✓ Queried normalized data for analytics")
    print()
    print("Architecture Benefits:")
    print("  • Two-tier design: Raw (versioned, complete) + Normalized (fast, queryable)")
    print("  • Defensive upserts: Idempotent, handles late-arriving/duplicate data")
    print("  • Full replay capability: Re-transform raw data at any time")
    print("  • Incremental processing: Checkpoints track what's been transformed")
    print()
    print("Next Steps:")
    print("  • Add more normalized tables (plays, pitches, players)")
    print("  • Implement PySpark for scalable transformation")
    print("  • Add data quality validation (PyDeequ)")
    print("  • Setup incremental processing with checkpoints")
    print()


if __name__ == "__main__":
    main()
