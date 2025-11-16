#!/usr/bin/env python3
"""Example: Raw data ingestion with full replay capability.

This example demonstrates:
1. Loading stub data from pymlb_statsapi (compressed JSON)
2. Storing raw responses in PostgreSQL with metadata
3. Querying raw data for replay/reprocessing
4. Using the RawStorageClient for ingestion

Full replay architecture:
- PostgreSQL as single source of truth
- Append-only raw tables (versioned by captured_at)
- No MinIO/S3 dependency
- Complete metadata from pymlb_statsapi
"""

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlmodel import select

from mlb_data_platform.database import get_session
from mlb_data_platform.ingestion import RawStorageClient
from mlb_data_platform.models import RawLiveGameV1


def load_stub_data() -> dict:
    """Load stub data from pymlb_statsapi tests directory.

    Returns:
        Dict with 'data' and 'metadata' keys (pymlb_statsapi format)
    """
    stub_file = Path.home() / (
        "github.com/power-edge/pymlb_statsapi/tests/bdd/stubs/game/"
        "liveGameV1/liveGameV1_game_pk=747175_4240fc08a038.json.gz"
    )

    if not stub_file.exists():
        raise FileNotFoundError(
            f"Stub file not found: {stub_file}\n"
            "Make sure pymlb_statsapi is cloned at ~/github.com/power-edge/pymlb_statsapi"
        )

    print(f"📦 Loading stub data from: {stub_file}")

    with gzip.open(stub_file, "rt") as f:
        stub = json.load(f)

    # pymlb_statsapi stub format: {"response": {...}, "metadata": {...}}
    return {
        "data": stub.get("response"),
        "metadata": {
            "endpoint": "game",
            "method": "liveGameV1",
            "params": {"game_pk": 747175},
            "url": "https://statsapi.mlb.com/api/v1.1/game/747175/feed/live",
            "status_code": 200,
            "captured_at": "2024-11-15T20:30:00Z",  # Simulate capture time
        },
    }


def main():
    """Run raw ingestion example."""
    print("=" * 80)
    print("Raw Ingestion Example: PostgreSQL as Single Source of Truth")
    print("=" * 80)
    print()

    storage = RawStorageClient()

    # ========================================================================
    # 1. Load stub data (simulates pymlb_statsapi response)
    # ========================================================================
    print("1. Loading stub data from pymlb_statsapi...")
    print()

    try:
        response = load_stub_data()
        data = response["data"]
        metadata = response["metadata"]

        game_pk = data.get("gamePk")
        print(f"   ✓ Loaded game_pk={game_pk}")
        print(f"   ✓ Endpoint: {metadata['endpoint']}.{metadata['method']}")
        print(f"   ✓ Captured at: {metadata['captured_at']}")
        print(f"   ✓ Status: {metadata['status_code']}")
        print(f"   ✓ Data size: {len(json.dumps(data))} bytes")
    except FileNotFoundError as e:
        print(f"   ✗ {e}")
        print("\n   Falling back to mock data...")

        # Fallback: Create mock response
        response = {
            "data": {
                "gamePk": 999999,
                "gameData": {
                    "game": {"pk": 999999},
                    "datetime": {"dateTime": "2024-11-15T20:00:00Z"},
                    "teams": {
                        "home": {"name": "Test Home Team"},
                        "away": {"name": "Test Away Team"},
                    },
                    "status": {"abstractGameState": "Final"},
                },
                "liveData": {"plays": {"allPlays": []}},
            },
            "metadata": {
                "endpoint": "game",
                "method": "liveGameV1",
                "params": {"game_pk": 999999},
                "url": "https://statsapi.mlb.com/api/v1.1/game/999999/feed/live",
                "status_code": 200,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            },
        }
        game_pk = 999999
        print(f"   ✓ Using mock game_pk={game_pk}")

    print()

    # ========================================================================
    # 2. Store raw response in PostgreSQL
    # ========================================================================
    print("2. Storing raw response in PostgreSQL...")
    print()

    with get_session() as session:
        raw_game = storage.save_live_game(
            session=session,
            response=response,
        )

        print(f"   ✓ Saved to game.live_game_v1_raw")
        print(f"   ✓ Primary key: (game_pk={raw_game.game_pk}, captured_at={raw_game.captured_at})")
        print(f"   ✓ Endpoint: {raw_game.endpoint}.{raw_game.method}")
        print(f"   ✓ Status: {raw_game.status_code}")
        print(f"   ✓ Data keys: {list(raw_game.data.keys())}")

    print()

    # ========================================================================
    # 3. Store ANOTHER version (simulate re-capturing same game)
    # ========================================================================
    print("3. Storing second version (simulate recapture)...")
    print()

    # Create modified response (simulate updated game state)
    response_v2 = response.copy()
    response_v2["metadata"] = response["metadata"].copy()
    response_v2["metadata"]["captured_at"] = datetime.now(timezone.utc).isoformat()

    with get_session() as session:
        raw_game_v2 = storage.save_live_game(
            session=session,
            response=response_v2,
        )

        print(f"   ✓ Saved version 2 to game.live_game_v1_raw")
        print(f"   ✓ Primary key: (game_pk={raw_game_v2.game_pk}, captured_at={raw_game_v2.captured_at})")
        print(f"   ✓ Both versions coexist (append-only)")

    print()

    # ========================================================================
    # 4. Query raw data for replay
    # ========================================================================
    print("4. Querying raw data for replay...")
    print()

    with get_session() as session:
        # Get all versions of this game
        statement = (
            select(RawLiveGameV1)
            .where(RawLiveGameV1.game_pk == game_pk)
            .order_by(RawLiveGameV1.captured_at.asc())
        )
        all_versions = session.exec(statement).all()

        print(f"   ✓ Found {len(all_versions)} version(s) of game {game_pk}:")
        for i, version in enumerate(all_versions, 1):
            print(f"      Version {i}: captured_at={version.captured_at}")

        # Get latest version using helper method
        latest = storage.get_latest_live_game(session, game_pk)
        if latest:
            print(f"\n   ✓ Latest version: {latest.captured_at}")
            print(f"   ✓ Status code: {latest.status_code}")

    print()

    # ========================================================================
    # 5. Query unprocessed data (incremental processing)
    # ========================================================================
    print("5. Simulating incremental processing...")
    print()

    with get_session() as session:
        # Get games captured in last hour
        from datetime import timedelta

        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        unprocessed = storage.get_unprocessed_games(session, since=one_hour_ago)

        print(f"   ✓ Found {len(unprocessed)} game(s) captured since {one_hour_ago}")
        for game in unprocessed:
            print(f"      game_pk={game.game_pk}, captured_at={game.captured_at}")

    print()

    # ========================================================================
    # 6. Demonstrate full replay capability
    # ========================================================================
    print("6. Full replay capability demonstration...")
    print()

    with get_session() as session:
        # Count total raw records
        total = storage.count_raw_games(session)
        print(f"   ✓ Total raw game records in database: {total}")

        # Query by date range
        cutoff_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        statement = (
            select(RawLiveGameV1)
            .where(RawLiveGameV1.captured_at >= cutoff_date)
            .order_by(RawLiveGameV1.captured_at.desc())
        )
        recent = session.exec(statement).all()
        print(f"   ✓ Games captured since 2024-01-01: {len(recent)}")

        # Query by status code (only successful responses)
        statement = select(RawLiveGameV1).where(RawLiveGameV1.status_code == 200)
        successful = session.exec(statement).all()
        print(f"   ✓ Successful responses (status=200): {len(successful)}")

    print()

    # ========================================================================
    # 7. Demonstrate JSON querying (PostgreSQL JSONB)
    # ========================================================================
    print("7. Querying JSONB data (PostgreSQL JSON operators)...")
    print()

    with get_session() as session:
        # Raw SQL query using JSONB operators
        from sqlalchemy import text

        stmt = text("""
            SELECT
                game_pk,
                captured_at,
                data->'gameData'->'status'->>'abstractGameState' as game_state,
                data->'gameData'->'teams'->'home'->>'name' as home_team,
                data->'gameData'->'teams'->'away'->>'name' as away_team
            FROM game.live_game_v1_raw
            WHERE game_pk = :game_pk
            ORDER BY captured_at DESC
            LIMIT 1
        """)
        result = session.exec(stmt.bindparams(game_pk=game_pk))

        row = result.first()
        if row:
            print(f"   ✓ Extracted from JSONB:")
            print(f"      Game State: {row[2]}")
            print(f"      Home Team: {row[3]}")
            print(f"      Away Team: {row[4]}")

    print()

    print("=" * 80)
    print("✓ Raw Ingestion Example Complete!")
    print("=" * 80)
    print()
    print("Key Benefits:")
    print("  • PostgreSQL as single source of truth (no MinIO/S3)")
    print("  • Append-only versioning (full historical data)")
    print("  • Complete metadata from pymlb_statsapi")
    print("  • JSONB querying with PostgreSQL operators")
    print("  • Full replay capability (reprocess any time period)")
    print("  • Incremental processing support")
    print()
    print("Next Steps:")
    print("  • Build transformation layer to flatten JSONB → normalized tables")
    print("  • Implement defensive upserts based on captured_at")
    print("  • Setup checkpoint tracking for incremental processing")
    print()


if __name__ == "__main__":
    main()
