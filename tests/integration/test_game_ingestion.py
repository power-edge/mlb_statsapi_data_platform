"""Integration tests for game data ingestion."""

from datetime import date

import pytest

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.storage.postgres import PostgresStorageBackend


class TestGameIngestionPipeline:
    """Test complete game ingestion pipeline end-to-end."""

    def test_game_ingestion_complete_flow(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test complete game ingestion from API to database.

        This tests the full pipeline:
        1. Fetch data from API (stub mode)
        2. Extract fields (including game_date, game_pk, etc.)
        3. Save to PostgreSQL
        4. Verify data persistence
        """
        # Verify table is empty before test
        count_before = pytest.count_rows(storage_backend, "game.live_game_v1")
        assert count_before == 0

        # Execute ingestion
        result = game_client.fetch_and_save(storage_backend)

        # Verify result
        assert "row_id" in result
        assert result["row_id"] > 0

        # Verify data was saved
        count_after = pytest.count_rows(storage_backend, "game.live_game_v1")
        assert count_after == 1

        # Query and verify the saved data
        rows = pytest.query_table(
            storage_backend,
            "SELECT * FROM game.live_game_v1 ORDER BY id DESC LIMIT 1"
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify metadata fields
        assert row["id"] == result["row_id"]
        assert row["schema_version"] == "v1"
        assert row["response_status"] == 200

        # Verify JSONB data exists
        assert row["data"] is not None
        assert "gamePk" in row["data"]
        assert "gameData" in row["data"]

        # Verify extracted fields
        assert row["game_pk"] is not None
        assert row["game_pk"] == 744834  # From fixture config
        assert row["game_date"] is not None
        assert row["game_date"] == date(2024, 7, 4)

    def test_game_field_extraction(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test extraction of all game fields from nested response data."""
        # Execute ingestion
        result = game_client.fetch_and_save(storage_backend)

        # Query all extracted fields
        rows = pytest.query_table(
            storage_backend,
            """
            SELECT game_pk, game_date, season, game_type, game_state,
                   home_team_id, away_team_id, venue_id
            FROM game.live_game_v1
            WHERE id = %s
            """ % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify all fields extracted correctly from nested JSON
        assert row["game_pk"] == 744834
        assert row["game_date"] == date(2024, 7, 4)
        assert row["season"] == "2024"
        assert row["game_type"] == "R"  # Regular season
        assert row["game_state"] in ["Final", "Live", "Preview"]  # Valid game states
        assert row["home_team_id"] == 120  # Washington Nationals
        assert row["away_team_id"] == 121  # New York Mets
        assert row["venue_id"] == 3309  # Nationals Park

    def test_game_complex_data_structure(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that game data has complete complex nested structure."""
        result = game_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM game.live_game_v1 WHERE id = %s" % result["row_id"]
        )

        game_data = rows[0]["data"]

        # Verify top-level structure
        assert "gamePk" in game_data
        assert "gameData" in game_data
        assert "liveData" in game_data

        # Verify gameData structure
        game_info = game_data["gameData"]
        assert "datetime" in game_info
        assert "teams" in game_info
        assert "players" in game_info
        assert "venue" in game_info

        # Verify liveData structure
        live_data = game_data["liveData"]
        assert "plays" in live_data or "linescore" in live_data

    def test_game_ingestion_idempotency(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that ingesting same game data twice creates two rows (INSERT mode)."""
        # First ingestion
        result1 = game_client.fetch_and_save(storage_backend)

        # Second ingestion
        result2 = game_client.fetch_and_save(storage_backend)

        # Should have two rows (INSERT mode, not UPSERT)
        count = pytest.count_rows(storage_backend, "game.live_game_v1")
        assert count == 2

        # Should have different IDs
        assert result1["row_id"] != result2["row_id"]

    def test_game_fetch_only(self, game_client: MLBStatsAPIClient):
        """Test fetching game data without saving (dry run)."""
        # Fetch without saving
        result = game_client.fetch()

        # Verify result structure
        assert "data" in result
        assert "metadata" in result
        assert "request" in result["metadata"]
        assert "response" in result["metadata"]

        # Verify data content
        assert "gamePk" in result["data"]
        assert "gameData" in result["data"]

        # Verify no row_id (not saved)
        assert "row_id" not in result

    def test_game_metadata_capture(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that request/response metadata is captured correctly."""
        result = game_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            """
            SELECT source_url, request_params, captured_at, ingestion_timestamp
            FROM game.live_game_v1
            WHERE id = %s
            """ % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify source URL exists
        assert row["source_url"] is not None
        assert len(row["source_url"]) > 0
        assert "game" in row["source_url"].lower()

        # Verify request params captured
        assert row["request_params"] is not None
        assert "game_pk" in row["request_params"]

        # Verify timestamps
        assert row["captured_at"] is not None
        assert row["ingestion_timestamp"] is not None


class TestGameDataValidation:
    """Test game data validation and structure."""

    def test_game_data_structure(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that game data has expected structure."""
        result = game_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM game.live_game_v1 WHERE id = %s" % result["row_id"]
        )

        game_data = rows[0]["data"]

        # Verify required top-level fields
        required_fields = ["gamePk", "gameData", "liveData"]
        for field in required_fields:
            assert field in game_data, f"Missing required field: {field}"

        # Verify gameData required fields
        game_info_fields = ["datetime", "teams", "venue"]
        for field in game_info_fields:
            assert field in game_data["gameData"], f"Missing gameData field: {field}"

    def test_game_team_data(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that team data is present and valid."""
        result = game_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM game.live_game_v1 WHERE id = %s" % result["row_id"]
        )

        teams = rows[0]["data"]["gameData"]["teams"]

        # Verify both teams present
        assert "home" in teams
        assert "away" in teams

        # Verify team structure
        assert "id" in teams["home"]
        assert "id" in teams["away"]

        # Verify team IDs are different
        assert teams["home"]["id"] != teams["away"]["id"]

    def test_game_datetime_format(
        self,
        game_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that game datetime fields are valid."""
        result = game_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM game.live_game_v1 WHERE id = %s" % result["row_id"]
        )

        datetime_info = rows[0]["data"]["gameData"]["datetime"]

        # Verify officialDate format (YYYY-MM-DD)
        import re
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        assert re.match(date_pattern, datetime_info["officialDate"]), \
            f"Invalid date format: {datetime_info['officialDate']}"


class TestGameErrorHandling:
    """Test error handling in game ingestion."""

    def test_database_connection_failure(self, game_client: MLBStatsAPIClient):
        """Test handling of database connection failure."""
        from mlb_data_platform.storage.postgres import PostgresConfig

        # Create storage with invalid config
        bad_config = PostgresConfig(
            host="invalid_host",
            port=9999,
            database="nonexistent",
            user="baduser",
            password="badpass"
        )

        # Should raise connection error
        with pytest.raises(Exception):
            bad_storage = PostgresStorageBackend(bad_config)
            game_client.fetch_and_save(bad_storage)

    def test_missing_schema_metadata(self):
        """Test behavior when schema metadata is missing."""
        from mlb_data_platform.schema.models import SCHEMA_METADATA_REGISTRY

        # Verify game schema exists
        assert "game.live_game_v1" in SCHEMA_METADATA_REGISTRY
        schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]

        # Verify schema has required metadata
        assert schema.endpoint == "game"
        assert schema.method == "liveGameV1"
        assert len(schema.fields) > 0

        # Verify critical fields exist
        field_names = [f.name for f in schema.fields]
        critical_fields = ["game_pk", "game_date", "home_team_id", "away_team_id"]
        for field in critical_fields:
            assert field in field_names, f"Missing critical field: {field}"
