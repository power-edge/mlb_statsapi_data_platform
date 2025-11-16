#!/usr/bin/env python3
"""Example: Using context managers for database operations.

This example demonstrates the cleaner API with context managers:
1. Automatic session management
2. Automatic commits/rollbacks
3. Connection pooling
4. Configuration from environment
"""

from datetime import date, datetime

from sqlmodel import select

from mlb_data_platform.database import DatabaseConfig, get_session
from mlb_data_platform.models import (
    LiveGameMetadata,
    LiveGamePitchEvents,
    LiveGamePlayActions,
    LiveGamePlayers,
)


def main():
    """Run context manager example."""
    print("=" * 80)
    print("Context Manager Example: Clean Database Operations")
    print("=" * 80)
    print()

    # ===================================================================
    # 1. Using Default Configuration (local development)
    # ===================================================================
    print("1. Insert data using context manager...")
    print("   (Automatically commits on exit)")
    print()

    with get_session() as session:
        # Create game
        game = LiveGameMetadata(
            game_pk=747176,
            game_type="W",
            season="2024",
            game_date=date(2024, 10, 26),
            home_team_name="Los Angeles Dodgers",
            away_team_name="New York Yankees",
            home_score=4,
            away_score=2,
            abstract_game_state="Final",
            source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
        )

        # Create players
        players = [
            LiveGamePlayers(
                game_pk=747176,
                player_id=660271,
                full_name="Shohei Ohtani",
                primary_position_code="DH",
                source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
            ),
            LiveGamePlayers(
                game_pk=747176,
                player_id=665742,
                full_name="Mookie Betts",
                primary_position_code="RF",
                source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
            ),
        ]

        session.add(game)
        session.add_all(players)

        # Automatically commits on successful exit
        print(f"   ✓ Inserted game {game.game_pk}")
        print(f"   ✓ Inserted {len(players)} players")

    print("   ✓ Transaction committed automatically")
    print()

    # ===================================================================
    # 2. Query Data (Read-Only Pattern)
    # ===================================================================
    print("2. Query data using context manager...")
    print()

    with get_session() as session:
        # Type-safe query
        statement = select(LiveGameMetadata).where(LiveGameMetadata.game_pk == 747176)
        game = session.exec(statement).first()

        if game:
            print(f"   Game: {game.away_team_name} @ {game.home_team_name}")
            print(f"   Score: {game.away_score} - {game.home_score}")
            print(f"   Status: {game.abstract_game_state}")
        print()

        # Query players
        statement = select(LiveGamePlayers).where(LiveGamePlayers.game_pk == 747176)
        players = session.exec(statement).all()

        print(f"   Players ({len(players)}):")
        for player in players:
            print(f"   - {player.full_name} ({player.primary_position_code})")

    print()

    # ===================================================================
    # 3. Demonstrate Event Type Splitting
    # ===================================================================
    print("3. Insert events with type splitting (pitch vs play actions)...")
    print()

    with get_session() as session:
        # Pitch events (isPitch=true)
        pitches = [
            LiveGamePitchEvents(
                game_pk=747176,
                play_id="747176-play-1",
                pitch_number=1,
                pitch_type_code="FF",
                pitch_type_description="Four-Seam Fastball",
                start_speed=95.2,
                pitch_call_description="Strike",
                source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
            ),
            LiveGamePitchEvents(
                game_pk=747176,
                play_id="747176-play-1",
                pitch_number=2,
                pitch_type_code="SL",
                pitch_type_description="Slider",
                start_speed=87.5,
                pitch_call_description="Ball",
                source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
            ),
        ]

        # Play action (isPitch=false)
        action = LiveGamePlayActions(
            game_pk=747176,
            play_id="747176-play-1",
            action_index=1,
            action_type="mound_visit",
            description="Mound visit by pitching coach",
            is_substitution=False,
            source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
        )

        session.add_all(pitches)
        session.add(action)

        print(f"   ✓ Inserted {len(pitches)} pitch events → live_game_pitch_events table")
        print(f"   ✓ Inserted 1 play action → live_game_play_actions table")

    print("   ✓ Events automatically routed to correct tables")
    print()

    # ===================================================================
    # 4. Custom Configuration Example
    # ===================================================================
    print("4. Using custom configuration (same local DB for demo)...")
    print()

    custom_config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="mlb_games",
        user="mlb_admin",
        password="mlb_dev_password",
        pool_size=10,  # Larger pool
        echo=False,  # Disable SQL logging
    )

    with get_session(custom_config) as session:
        count = session.exec(select(LiveGameMetadata)).all()
        print(f"   ✓ Connected with custom config")
        print(f"   ✓ Found {len(count)} games in database")

    print()

    # ===================================================================
    # 5. Error Handling (Automatic Rollback)
    # ===================================================================
    print("5. Error handling (automatic rollback)...")
    print()

    try:
        with get_session() as session:
            # This will fail - duplicate primary key
            duplicate_game = LiveGameMetadata(
                game_pk=747176,  # Already exists!
                game_date=date(2024, 10, 26),
                source_captured_at=datetime(2024, 10, 26, 23, 30, 0),
            )
            session.add(duplicate_game)

            # Exception raised before commit
            # Context manager automatically rolls back

    except Exception as e:
        print(f"   ✗ Error caught: {type(e).__name__}")
        print(f"   ✓ Transaction automatically rolled back")
        print(f"   ✓ Database remains consistent")

    print()

    # ===================================================================
    # 6. Configuration from Environment Variables
    # ===================================================================
    print("6. Configuration from environment variables...")
    print()

    # Set environment variables (in production, these come from .env or k8s secrets)
    import os

    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    os.environ["POSTGRES_DB"] = "mlb_games"
    os.environ["POSTGRES_USER"] = "mlb_admin"
    os.environ["POSTGRES_PASSWORD"] = "mlb_dev_password"

    # Load from environment
    env_config = DatabaseConfig.from_env()

    with get_session(env_config) as session:
        count = session.exec(select(LiveGameMetadata)).all()
        print(f"   ✓ Loaded config from environment variables")
        print(f"   ✓ Connected to {env_config.database}")
        print(f"   ✓ Found {len(count)} games")

    print()

    print("=" * 80)
    print("✓ Context Manager Example Complete!")
    print("=" * 80)
    print()
    print("Key Benefits:")
    print("  • Automatic session cleanup")
    print("  • Automatic commits on success")
    print("  • Automatic rollback on errors")
    print("  • Connection pooling (reuses connections)")
    print("  • Clean, Pythonic API")
    print("  • Type-safe queries with SQLModel")
    print()


if __name__ == "__main__":
    main()
