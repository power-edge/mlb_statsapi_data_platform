#!/usr/bin/env python3
"""Example: Using ORM models to insert and query game data.

This example demonstrates:
1. Creating SQLModel engine and session
2. Inserting data using ORM models
3. Querying data with type-safe queries
4. Working with relationships across tables
"""

from datetime import date, datetime
from typing import List

from sqlmodel import Session, create_engine, select

from mlb_data_platform.models import (
    LiveGameFieldingCredits,
    LiveGameMetadata,
    LiveGamePitchEvents,
    LiveGamePlayActions,
    LiveGamePlayers,
    LiveGamePlays,
)


def main():
    """Run ORM example."""
    # Create database connection (using psycopg driver)
    DATABASE_URL = "postgresql+psycopg://mlb_admin:mlb_dev_password@localhost:5432/mlb_games"
    engine = create_engine(DATABASE_URL, echo=False)  # echo=True shows SQL queries

    print("=" * 80)
    print("ORM Example: Inserting and Querying Game Data")
    print("=" * 80)
    print()

    with Session(engine) as session:
        # ===================================================================
        # 1. Insert Game Metadata
        # ===================================================================
        print("1. Creating game metadata...")
        game = LiveGameMetadata(
            game_pk=747175,
            game_type="W",  # World Series
            season="2024",
            season_type="Postseason",
            game_date=date(2024, 10, 25),
            game_datetime=datetime(2024, 10, 25, 20, 8, 0),
            day_night="night",
            venue_id=22,
            venue_name="Dodger Stadium",
            home_team_id=119,
            home_team_name="Los Angeles Dodgers",
            away_team_id=147,
            away_team_name="New York Yankees",
            abstract_game_state="Final",
            status_code="F",
            home_score=7,
            away_score=6,
            weather_condition="Clear",
            weather_temp=72,
            source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
        )

        session.add(game)
        session.commit()
        session.refresh(game)
        print(f"✓ Created game: {game.away_team_name} @ {game.home_team_name}")
        print(f"  Score: {game.away_score} - {game.home_score}")
        print()

        # ===================================================================
        # 2. Insert Players
        # ===================================================================
        print("2. Creating players...")
        players = [
            LiveGamePlayers(
                game_pk=747175,
                player_id=660271,
                full_name="Shohei Ohtani",
                first_name="Shohei",
                last_name="Ohtani",
                primary_number="17",
                bats="L",
                throws="R",
                primary_position_code="DH",
                primary_position_name="Designated Hitter",
                source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
            ),
            LiveGamePlayers(
                game_pk=747175,
                player_id=518692,
                full_name="Freddie Freeman",
                first_name="Freddie",
                last_name="Freeman",
                primary_number="5",
                bats="L",
                throws="R",
                primary_position_code="1B",
                primary_position_name="First Base",
                source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
            ),
        ]

        session.add_all(players)
        session.commit()
        print(f"✓ Created {len(players)} players")
        for player in players:
            print(f"  - {player.full_name} (#{player.primary_number}) - {player.primary_position_name}")
        print()

        # ===================================================================
        # 3. Insert Play
        # ===================================================================
        print("3. Creating a play...")
        play = LiveGamePlays(
            game_pk=747175,
            play_id="747175-2024-10-25-110",
            at_bat_index=1,
            inning=1,
            half_inning="bottom",
            is_top_inning=False,
            outs_before=0,
            outs_after=0,
            event_type="walk_off_home_run",
            event="Home Run",
            description="Freddie Freeman homers",
            rbi=1,
            away_score=6,
            home_score=7,
            batter_id=518692,
            pitcher_id=641154,
            source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
        )

        session.add(play)
        session.commit()
        print(f"✓ Created play: {play.event} - {play.description}")
        print()

        # ===================================================================
        # 4. Insert Pitch Events (isPitch=true)
        # ===================================================================
        print("4. Creating pitch events...")
        pitches = [
            LiveGamePitchEvents(
                game_pk=747175,
                play_id="747175-2024-10-25-110",
                pitch_number=1,
                pitch_type_code="FF",
                pitch_type_description="Four-Seam Fastball",
                pitch_call_code="B",
                pitch_call_description="Ball",
                is_strike=False,
                is_ball=True,
                is_in_play=False,
                start_speed=94.5,
                balls_before=0,
                strikes_before=0,
                outs=0,
                source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
            ),
            LiveGamePitchEvents(
                game_pk=747175,
                play_id="747175-2024-10-25-110",
                pitch_number=2,
                pitch_type_code="FF",
                pitch_type_description="Four-Seam Fastball",
                pitch_call_code="X",
                pitch_call_description="In play, run(s)",
                is_strike=False,
                is_ball=False,
                is_in_play=True,
                start_speed=93.8,
                balls_before=1,
                strikes_before=0,
                outs=0,
                source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
            ),
        ]

        session.add_all(pitches)
        session.commit()
        print(f"✓ Created {len(pitches)} pitch events")
        for pitch in pitches:
            print(f"  - Pitch #{pitch.pitch_number}: {pitch.pitch_type_description} - {pitch.start_speed} MPH - {pitch.pitch_call_description}")
        print()

        # ===================================================================
        # 5. Insert Play Action (isPitch=false)
        # ===================================================================
        print("5. Creating a play action (non-pitch event)...")
        action = LiveGamePlayActions(
            game_pk=747175,
            play_id="747175-2024-10-25-110",
            action_index=1,
            action_type="substitution",
            event_type="Defensive Sub",
            description="Defensive Substitution: Pitcher change",
            is_substitution=True,
            player_in_id=641154,
            position="P",
            source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
        )

        session.add(action)
        session.commit()
        print(f"✓ Created play action: {action.description}")
        print()

        # ===================================================================
        # 6. Insert Fielding Credit
        # ===================================================================
        print("6. Creating fielding credit...")
        credit = LiveGameFieldingCredits(
            game_pk=747175,
            play_id="747175-2024-10-25-100",
            event_index=1,
            credit_index=1,
            player_id=518692,
            position_code="1B",
            credit_type="f_putout",
            source_captured_at=datetime(2024, 10, 25, 23, 30, 0),
        )

        session.add(credit)
        session.commit()
        print(f"✓ Created fielding credit: {credit.credit_type} at {credit.position_code}")
        print()

    # ===================================================================
    # 7. Query Data with Type-Safe Queries
    # ===================================================================
    print("=" * 80)
    print("Querying Data")
    print("=" * 80)
    print()

    with Session(engine) as session:
        # Query game
        print("1. Query game metadata...")
        statement = select(LiveGameMetadata).where(LiveGameMetadata.game_pk == 747175)
        game = session.exec(statement).first()
        if game:
            print(f"   Game {game.game_pk}: {game.away_team_name} @ {game.home_team_name}")
            print(f"   Final Score: {game.away_score} - {game.home_score}")
            print(f"   Date: {game.game_date}")
        print()

        # Query all players
        print("2. Query all players...")
        statement = select(LiveGamePlayers).where(LiveGamePlayers.game_pk == 747175)
        players = session.exec(statement).all()
        print(f"   Found {len(players)} players:")
        for player in players:
            print(f"   - {player.full_name} (#{player.primary_number})")
        print()

        # Query plays with specific event type
        print("3. Query home runs...")
        statement = select(LiveGamePlays).where(
            LiveGamePlays.game_pk == 747175,
            LiveGamePlays.event == "Home Run"
        )
        home_runs = session.exec(statement).all()
        print(f"   Found {len(home_runs)} home runs:")
        for hr in home_runs:
            print(f"   - {hr.description}")
        print()

        # Query pitch events only (isPitch=true events)
        print("4. Query pitch events (isPitch=true)...")
        statement = select(LiveGamePitchEvents).where(
            LiveGamePitchEvents.game_pk == 747175,
            LiveGamePitchEvents.play_id == "747175-2024-10-25-110"
        )
        pitches = session.exec(statement).all()
        print(f"   Found {len(pitches)} pitches:")
        for pitch in pitches:
            print(f"   - Pitch #{pitch.pitch_number}: {pitch.pitch_type_description} ({pitch.start_speed} MPH)")
        print()

        # Query play actions only (isPitch=false events)
        print("5. Query play actions (isPitch=false)...")
        statement = select(LiveGamePlayActions).where(
            LiveGamePlayActions.game_pk == 747175,
            LiveGamePlayActions.is_substitution == True
        )
        actions = session.exec(statement).all()
        print(f"   Found {len(actions)} substitutions:")
        for action in actions:
            print(f"   - {action.description}")
        print()

        # Query fielding credits
        print("6. Query fielding credits...")
        statement = select(LiveGameFieldingCredits).where(
            LiveGameFieldingCredits.game_pk == 747175
        )
        credits = session.exec(statement).all()
        print(f"   Found {len(credits)} fielding credits:")
        for credit in credits:
            print(f"   - Player {credit.player_id}: {credit.credit_type} at {credit.position_code}")
        print()

    print("=" * 80)
    print("✓ ORM Example Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
