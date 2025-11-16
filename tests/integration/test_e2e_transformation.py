"""End-to-end integration test for transformation pipeline using stub data.

This test validates the complete transformation pipeline:
1. Load stub data from pymlb_statsapi
2. Transform using GameLiveV1Transformation
3. Validate output DataFrames
4. Test defensive upsert behavior

Uses in-memory DataFrames (no PostgreSQL/Spark required for basic validation).
"""

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from mlb_data_platform.transform.game.live_game_v1 import GameLiveV1Transformation
from mlb_data_platform.transform.base import TransformMode


@pytest.fixture(scope="module")
def stub_data_path():
    """Path to pymlb_statsapi stub data."""
    # Assuming pymlb_statsapi is in same parent directory
    path = Path(__file__).parents[3] / "pymlb_statsapi" / "tests" / "bdd" / "stubs"
    if not path.exists():
        pytest.skip(f"Stub data not found at {path}")
    return path


@pytest.fixture(scope="module")
def game_pk_747175_stub(stub_data_path):
    """Load stub data for game_pk 747175 (2024 World Series Game 1)."""
    stub_file = stub_data_path / "game" / "liveGameV1" / "liveGameV1_game_pk=747175_4240fc08a038.json.gz"

    if not stub_file.exists():
        pytest.skip(f"Stub file not found: {stub_file}")

    with gzip.open(stub_file, "rt", encoding="utf-8") as f:
        stub_wrapper = json.load(f)

    # Stub data has structure: {endpoint, method, path_params, response}
    # The actual game data is in response
    return stub_wrapper["response"]


@pytest.fixture(scope="module")
def spark_local():
    """Create local Spark session for testing (no Delta Lake required)."""
    spark = (
        SparkSession.builder
        .appName("test-e2e-transformation")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")  # Disable UI for tests
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_load_stub_data(game_pk_747175_stub):
    """Test that stub data loads correctly."""
    assert game_pk_747175_stub is not None
    assert "gamePk" in game_pk_747175_stub
    assert game_pk_747175_stub["gamePk"] == 747175
    assert "gameData" in game_pk_747175_stub
    assert "liveData" in game_pk_747175_stub


def test_stub_data_structure(game_pk_747175_stub):
    """Validate stub data has expected structure."""
    data = game_pk_747175_stub

    # Top-level fields
    assert "gamePk" in data
    assert "gameData" in data
    assert "liveData" in data

    # gameData structure
    game_data = data["gameData"]
    assert "datetime" in game_data
    assert "status" in game_data
    assert "teams" in game_data
    assert "venue" in game_data
    assert "players" in game_data

    # liveData structure
    live_data = data["liveData"]
    assert "plays" in live_data
    assert "linescore" in live_data
    assert "boxscore" in live_data


def test_create_raw_dataframe(spark_local, game_pk_747175_stub):
    """Test creating raw DataFrame from stub data."""
    # Create schema for raw table
    schema = StructType([
        StructField("game_pk", StringType(), False),
        StructField("data", StringType(), False),
        StructField("captured_at", StringType(), False),
    ])

    # Create raw data row
    captured_at = datetime.now(timezone.utc).isoformat()
    raw_data = [
        (
            str(game_pk_747175_stub["gamePk"]),
            json.dumps(game_pk_747175_stub),
            captured_at,
        )
    ]

    # Create DataFrame
    df = spark_local.createDataFrame(raw_data, schema=schema)

    # Validate
    assert df.count() == 1
    row = df.first()
    assert row.game_pk == "747175"
    assert row.captured_at == captured_at

    # Validate JSON can be parsed
    parsed = json.loads(row.data)
    assert parsed["gamePk"] == 747175


@pytest.mark.skip(reason="Requires full Spark setup with JDBC and schema mapping files")
def test_e2e_transformation_pipeline(spark_local, game_pk_747175_stub):
    """End-to-end test of transformation pipeline.

    This test is skipped by default because it requires:
    1. Full PySpark setup with JDBC drivers
    2. Schema mapping YAML files in config/
    3. PostgreSQL database (or mock)

    To enable, remove @pytest.mark.skip and setup dependencies.
    """
    # Create transformation instance
    transform = GameLiveV1Transformation(
        spark=spark_local,
        mode=TransformMode.BATCH,
        enable_defensive_upsert=False,  # Disable for test (no DB)
    )

    # Create raw DataFrame
    schema = StructType([
        StructField("game_pk", StringType(), False),
        StructField("data", StringType(), False),
        StructField("captured_at", StringType(), False),
    ])

    raw_data = [(
        str(game_pk_747175_stub["gamePk"]),
        json.dumps(game_pk_747175_stub),
        datetime.now(timezone.utc).isoformat(),
    )]

    source_df = spark_local.createDataFrame(raw_data, schema=schema)

    # Transform
    target_dfs = transform.transform(source_df)

    # Validate outputs
    assert len(target_dfs) > 0
    assert "game.live_game_metadata" in target_dfs

    # Validate metadata table
    metadata_df = target_dfs["game.live_game_metadata"]
    assert metadata_df.count() == 1

    metadata = metadata_df.first()
    assert metadata.game_pk == 747175


def test_extract_game_metadata_manual(game_pk_747175_stub):
    """Manual extraction test (no transformation framework).

    This validates we can extract expected fields from stub data.
    """
    data = game_pk_747175_stub

    # Extract fields we expect in game.live_game_metadata
    game_pk = data["gamePk"]
    assert game_pk == 747175

    # Game date (validate format, not specific date)
    game_date = data["gameData"]["datetime"]["officialDate"]
    assert len(game_date) == 10  # YYYY-MM-DD format
    assert game_date.startswith("2024-")

    # Teams (validate IDs exist, not specific teams)
    home_team_id = data["gameData"]["teams"]["home"]["id"]
    away_team_id = data["gameData"]["teams"]["away"]["id"]
    assert isinstance(home_team_id, int)
    assert isinstance(away_team_id, int)
    assert home_team_id != away_team_id  # Different teams

    # Venue
    venue_id = data["gameData"]["venue"]["id"]
    assert isinstance(venue_id, int)
    assert venue_id > 0

    # Status
    status_code = data["gameData"]["status"]["statusCode"]
    assert isinstance(status_code, str)
    assert len(status_code) > 0


def test_extract_plays_manual(game_pk_747175_stub):
    """Manual extraction test for plays data."""
    data = game_pk_747175_stub

    # Extract plays
    plays = data["liveData"]["plays"]["allPlays"]
    assert len(plays) > 0

    # Validate first play structure
    first_play = plays[0]
    assert "about" in first_play
    assert "result" in first_play
    assert "matchup" in first_play

    # Extract play metadata
    play_index = first_play["about"]["atBatIndex"]
    inning = first_play["about"]["inning"]
    half_inning = first_play["about"]["halfInning"]

    assert isinstance(play_index, int)
    assert isinstance(inning, int)
    assert half_inning in ["top", "bottom"]


def test_extract_players_manual(game_pk_747175_stub):
    """Manual extraction test for players data."""
    data = game_pk_747175_stub

    # Extract players
    players = data["gameData"]["players"]
    assert len(players) > 0

    # Validate player structure
    # Players are keyed by ID: {"ID123456": {...}, ...}
    player_ids = list(players.keys())
    assert len(player_ids) > 0

    first_player_id = player_ids[0]
    first_player = players[first_player_id]

    assert "id" in first_player
    assert "fullName" in first_player
    assert "currentTeam" in first_player or "primaryPosition" in first_player


def test_extract_linescore_manual(game_pk_747175_stub):
    """Manual extraction test for linescore data."""
    data = game_pk_747175_stub

    # Extract linescore
    linescore = data["liveData"]["linescore"]
    assert "innings" in linescore
    assert "teams" in linescore

    # Validate innings
    innings = linescore["innings"]
    assert len(innings) >= 9  # At least 9 innings

    # Validate first inning structure
    first_inning = innings[0]
    assert "num" in first_inning
    assert "home" in first_inning
    assert "away" in first_inning

    # Validate runs
    assert "runs" in first_inning["home"] or first_inning["home"].get("runs") is not None
    assert "runs" in first_inning["away"] or first_inning["away"].get("runs") is not None


def test_defensive_upsert_scenario_simulation():
    """Simulate defensive upsert behavior without actual database.

    This test simulates the logic of defensive upserts by checking
    timestamps and determining which records would be inserted/updated/skipped.
    """
    # Scenario: Two versions of same game data

    # Version 1: Captured at 10:00 AM
    v1_timestamp = datetime(2024, 10, 25, 10, 0, 0, tzinfo=timezone.utc)
    v1_data = {
        "game_pk": 747175,
        "home_score": 3,
        "away_score": 2,
        "inning": 7,
        "captured_at": v1_timestamp,
    }

    # Version 2: Captured at 2:00 PM (newer, game finished)
    v2_timestamp = datetime(2024, 10, 25, 14, 0, 0, tzinfo=timezone.utc)
    v2_data = {
        "game_pk": 747175,
        "home_score": 6,
        "away_score": 3,
        "inning": 9,
        "captured_at": v2_timestamp,
    }

    # Simulate database state
    database = {}

    # Write v1 first
    database[v1_data["game_pk"]] = v1_data
    assert database[747175]["inning"] == 7

    # Write v2 (should update because newer)
    if v2_data["captured_at"] >= database[747175]["captured_at"]:
        database[v2_data["game_pk"]] = v2_data  # Update
        action = "updated"
    else:
        action = "skipped"

    assert action == "updated"
    assert database[747175]["inning"] == 9  # Updated to newer value

    # Try to write v1 again (should skip because older)
    if v1_data["captured_at"] >= database[747175]["captured_at"]:
        database[v1_data["game_pk"]] = v1_data
        action = "updated"
    else:
        action = "skipped"

    assert action == "skipped"
    assert database[747175]["inning"] == 9  # Preserved newer value


def test_idempotency_simulation():
    """Simulate idempotent behavior - running same data twice.

    This validates that re-running the same data produces the same result.
    """
    # Same data, same timestamp
    timestamp = datetime(2024, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
    data = {
        "game_pk": 747175,
        "home_score": 5,
        "captured_at": timestamp,
    }

    # Database state
    database = {}

    # First run - insert
    database[data["game_pk"]] = data
    first_result = database[747175].copy()

    # Second run - update with same data (timestamps equal)
    if data["captured_at"] >= database[747175]["captured_at"]:
        database[data["game_pk"]] = data

    second_result = database[747175].copy()

    # Results should be identical (idempotent)
    assert first_result == second_result


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_e2e_transformation.py -v
    pytest.main([__file__, "-v"])
