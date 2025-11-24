"""Pytest configuration and fixtures for all tests."""

import os
from datetime import date, datetime
from pathlib import Path
from typing import Generator

import pytest
from psycopg.rows import dict_row

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.ingestion.config import JobConfig, StubMode
from mlb_data_platform.schema.models import SCHEMA_METADATA_REGISTRY
from mlb_data_platform.storage.postgres import PostgresConfig, PostgresStorageBackend


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def db_config() -> PostgresConfig:
    """Database configuration for tests.

    Uses environment variables or defaults to local test database.
    """
    return PostgresConfig(
        host=os.getenv("TEST_DB_HOST", "localhost"),
        port=int(os.getenv("TEST_DB_PORT", "5432")),
        database=os.getenv("TEST_DB_NAME", "mlb_games"),
        user=os.getenv("TEST_DB_USER", "mlb_admin"),
        password=os.getenv("TEST_DB_PASSWORD", "mlb_dev_password"),
    )


@pytest.fixture
def storage_backend(db_config: PostgresConfig) -> Generator[PostgresStorageBackend, None, None]:
    """PostgreSQL storage backend for tests.

    Yields a connected storage backend and handles cleanup.
    """
    backend = PostgresStorageBackend(db_config)
    yield backend
    # Cleanup happens automatically when connection pool closes


@pytest.fixture
def clean_tables(storage_backend: PostgresStorageBackend) -> Generator[None, None, None]:
    """Clean test tables before and after test.

    Truncates tables to ensure test isolation.
    """
    # Clean before test
    with storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            # Truncate tables (CASCADE to handle foreign keys)
            cur.execute("TRUNCATE TABLE season.seasons CASCADE")
            cur.execute("TRUNCATE TABLE schedule.schedule CASCADE")
            cur.execute("TRUNCATE TABLE game.live_game_v1 CASCADE")
            conn.commit()

    yield

    # Clean after test
    with storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE season.seasons CASCADE")
            cur.execute("TRUNCATE TABLE schedule.schedule CASCADE")
            cur.execute("TRUNCATE TABLE game.live_game_v1 CASCADE")
            conn.commit()


# ============================================================================
# Job Config Fixtures
# ============================================================================

@pytest.fixture
def season_job_config() -> JobConfig:
    """Sample season job configuration."""
    return JobConfig(
        name="test_season_job",
        description="Test season ingestion",
        type="batch",
        source={
            "endpoint": "season",
            "method": "seasons",
            "parameters": {
                "sportId": 1
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 30
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "season.seasons"
            }
        }
    )


@pytest.fixture
def schedule_job_config() -> JobConfig:
    """Sample schedule job configuration."""
    return JobConfig(
        name="test_schedule_job",
        description="Test schedule ingestion",
        type="batch",
        source={
            "endpoint": "schedule",
            "method": "schedule",
            "parameters": {
                "sportId": 1,
                "date": "2024-07-04"  # Known date with games
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 30
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "schedule.schedule"
            }
        }
    )


@pytest.fixture
def game_job_config() -> JobConfig:
    """Sample game job configuration."""
    return JobConfig(
        name="test_game_job",
        description="Test game ingestion",
        type="batch",
        source={
            "endpoint": "game",
            "method": "liveGameV1",
            "parameters": {
                "game_pk": 744834  # Known game from July 4, 2024
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 45
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "game.live_game_v1"
            }
        }
    )


# ============================================================================
# Client Fixtures
# ============================================================================

@pytest.fixture
def season_client(season_job_config: JobConfig) -> MLBStatsAPIClient:
    """MLB Stats API client for season data (stub mode)."""
    return MLBStatsAPIClient(season_job_config, stub_mode=StubMode.REPLAY)


@pytest.fixture
def schedule_client(schedule_job_config: JobConfig) -> MLBStatsAPIClient:
    """MLB Stats API client for schedule data (stub mode)."""
    return MLBStatsAPIClient(schedule_job_config, stub_mode=StubMode.REPLAY)


@pytest.fixture
def game_client(game_job_config: JobConfig) -> MLBStatsAPIClient:
    """MLB Stats API client for game data (stub mode)."""
    return MLBStatsAPIClient(game_job_config, stub_mode=StubMode.REPLAY)


# ============================================================================
# Schema Fixtures
# ============================================================================

@pytest.fixture
def season_schema():
    """Season schema metadata."""
    return SCHEMA_METADATA_REGISTRY.get("season.seasons")


@pytest.fixture
def schedule_schema():
    """Schedule schema metadata."""
    return SCHEMA_METADATA_REGISTRY.get("schedule.schedule")


@pytest.fixture
def game_schema():
    """Game schema metadata."""
    return SCHEMA_METADATA_REGISTRY.get("game.live_game_v1")


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_season_response() -> dict:
    """Sample season API response data."""
    return {
        "seasons": [
            {
                "seasonId": "2025",
                "allStarDate": "2025-07-15",
                "hasWildcard": True,
                "seasonEndDate": "2025-11-01",
                "springEndDate": "2025-03-25",
                "lastDate1stHalf": "2025-07-14",
                "seasonStartDate": "2025-02-20",
                "springStartDate": "2025-02-20",
                "firstDate2ndHalf": "2025-07-18",
                "offSeasonEndDate": "2025-12-31",
                "preSeasonEndDate": "2025-02-19",
                "postSeasonEndDate": "2025-11-01",
                "offseasonStartDate": "2025-11-02",
                "preSeasonStartDate": "2025-01-01",
                "postSeasonStartDate": "2025-09-30",
                "gameLevelGamedayType": "P",
                "qualifierOutsPitched": 3.0,
                "regularSeasonEndDate": "2025-09-28",
                "regularSeasonStartDate": "2025-03-18",
                "seasonLevelGamedayType": "P",
                "qualifierPlateAppearances": 3.1
            }
        ]
    }


@pytest.fixture
def sample_schedule_response() -> dict:
    """Sample schedule API response data (July 4, 2024)."""
    return {
        "totalItems": 15,
        "totalEvents": 0,
        "totalGames": 15,
        "totalGamesInProgress": 0,
        "dates": [
            {
                "date": "2024-07-04",
                "totalItems": 15,
                "totalEvents": 0,
                "totalGames": 15,
                "totalGamesInProgress": 0,
                "games": [
                    {
                        "gamePk": 744834,
                        "gameType": "R",
                        "season": "2024",
                        "gameDate": "2024-07-04T17:05:00Z",
                        "status": {
                            "abstractGameState": "Final",
                            "codedGameState": "F",
                            "detailedState": "Final",
                            "statusCode": "F"
                        },
                        "teams": {
                            "away": {
                                "team": {"id": 121, "name": "New York Mets"}
                            },
                            "home": {
                                "team": {"id": 120, "name": "Washington Nationals"}
                            }
                        },
                        "venue": {
                            "id": 3309,
                            "name": "Nationals Park"
                        }
                    }
                    # ... more games would be here
                ]
            }
        ]
    }


@pytest.fixture
def sample_game_metadata() -> dict:
    """Sample game request/response metadata."""
    return {
        "request": {
            "url": "https://statsapi.mlb.com/api/v1.1/game/744834/feed/live",
            "query_params": {
                "game_pk": "744834"
            }
        },
        "response": {
            "status_code": 200,
            "elapsed_ms": 250.5
        },
        "schema_version": "v1"
    }


# ============================================================================
# Utility Fixtures
# ============================================================================

@pytest.fixture
def test_date() -> date:
    """Fixed test date for deterministic tests."""
    return date(2024, 7, 4)


@pytest.fixture
def test_timestamp() -> datetime:
    """Fixed test timestamp for deterministic tests."""
    return datetime(2024, 7, 4, 12, 30, 45)


# ============================================================================
# Helper Functions
# ============================================================================

def query_table(storage_backend: PostgresStorageBackend, sql: str) -> list[dict]:
    """Execute query and return results as list of dicts.

    Args:
        storage_backend: Storage backend
        sql: SQL query to execute

    Returns:
        List of row dicts
    """
    with storage_backend.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            return cur.fetchall()


def count_rows(storage_backend: PostgresStorageBackend, table: str) -> int:
    """Count rows in a table.

    Args:
        storage_backend: Storage backend
        table: Table name (schema.table)

    Returns:
        Row count
    """
    with storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*)::int as count FROM {table}")
            result = cur.fetchone()
            return result[0] if isinstance(result, tuple) else result["count"]


# Make helper functions available to tests
pytest.query_table = query_table
pytest.count_rows = count_rows
