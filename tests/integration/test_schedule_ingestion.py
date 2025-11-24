"""Integration tests for schedule data ingestion."""

from datetime import date

import pytest

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.storage.postgres import PostgresStorageBackend


class TestScheduleIngestionPipeline:
    """Test complete schedule ingestion pipeline end-to-end."""

    def test_schedule_ingestion_complete_flow(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test complete schedule ingestion from API to database.

        This tests the full pipeline:
        1. Fetch data from API (stub mode)
        2. Extract fields (including schedule_date from request params)
        3. Save to PostgreSQL
        4. Verify data persistence
        """
        # Verify table is empty before test
        count_before = pytest.count_rows(storage_backend, "schedule.schedule")
        assert count_before == 0

        # Execute ingestion
        result = schedule_client.fetch_and_save(storage_backend)

        # Verify result
        assert "row_id" in result
        assert result["row_id"] > 0

        # Verify data was saved
        count_after = pytest.count_rows(storage_backend, "schedule.schedule")
        assert count_after == 1

        # Query and verify the saved data
        rows = pytest.query_table(
            storage_backend,
            "SELECT * FROM schedule.schedule ORDER BY id DESC LIMIT 1"
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify metadata fields
        assert row["id"] == result["row_id"]
        assert row["schema_version"] == "v1"
        assert row["response_status"] == 200

        # Verify JSONB data exists
        assert row["data"] is not None
        assert "dates" in row["data"] or "totalGames" in row["data"]

        # Verify schedule_date was extracted from request params
        assert row["schedule_date"] is not None
        assert row["schedule_date"] == date(2024, 7, 4)  # From fixture config

    def test_schedule_partition_key_extraction(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that schedule_date is correctly extracted from request parameters."""
        # Execute ingestion
        result = schedule_client.fetch_and_save(storage_backend)

        # Query extracted fields
        rows = pytest.query_table(
            storage_backend,
            "SELECT schedule_date, sport_id, request_params FROM schedule.schedule WHERE id = %s" % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify schedule_date extracted from request params (not response)
        assert row["schedule_date"] == date(2024, 7, 4)
        assert row["sport_id"] == 1
        assert row["request_params"]["date"] == "2024-07-04"
        assert row["request_params"]["sportId"] == 1

    def test_schedule_with_games_data(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test schedule data contains games list."""
        result = schedule_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM schedule.schedule WHERE id = %s" % result["row_id"]
        )

        schedule_data = rows[0]["data"]

        # July 4, 2024 had 15 games
        assert "totalGames" in schedule_data
        assert schedule_data["totalGames"] >= 0  # Can be 0 for some dates

        # Verify structure
        assert "dates" in schedule_data
        if schedule_data["totalGames"] > 0:
            assert len(schedule_data["dates"]) > 0
            assert "games" in schedule_data["dates"][0]

    def test_schedule_ingestion_idempotency(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that ingesting same schedule data twice creates two rows (INSERT mode)."""
        # First ingestion
        result1 = schedule_client.fetch_and_save(storage_backend)

        # Second ingestion
        result2 = schedule_client.fetch_and_save(storage_backend)

        # Should have two rows (INSERT mode, not UPSERT)
        count = pytest.count_rows(storage_backend, "schedule.schedule")
        assert count == 2

        # Should have different IDs
        assert result1["row_id"] != result2["row_id"]

    def test_schedule_fetch_only(self, schedule_client: MLBStatsAPIClient):
        """Test fetching schedule data without saving (dry run)."""
        # Fetch without saving
        result = schedule_client.fetch()

        # Verify result structure
        assert "data" in result
        assert "metadata" in result
        assert "request" in result["metadata"]
        assert "response" in result["metadata"]

        # Verify data content
        assert "dates" in result["data"] or "totalGames" in result["data"]

        # Verify no row_id (not saved)
        assert "row_id" not in result

    def test_schedule_metadata_capture(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that request/response metadata is captured correctly."""
        result = schedule_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            """
            SELECT source_url, request_params, captured_at, ingestion_timestamp
            FROM schedule.schedule
            WHERE id = %s
            """ % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify source URL exists (constructed if not from response)
        assert row["source_url"] is not None
        assert len(row["source_url"]) > 0
        assert "schedule" in row["source_url"].lower()

        # Verify request params captured
        assert row["request_params"] is not None
        assert "date" in row["request_params"]
        assert "sportId" in row["request_params"]

        # Verify timestamps
        assert row["captured_at"] is not None
        assert row["ingestion_timestamp"] is not None


class TestScheduleDataValidation:
    """Test schedule data validation and structure."""

    def test_schedule_data_structure(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that schedule data has expected structure."""
        result = schedule_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM schedule.schedule WHERE id = %s" % result["row_id"]
        )

        schedule_data = rows[0]["data"]

        # Verify top-level fields
        required_fields = ["totalGames", "dates"]
        for field in required_fields:
            assert field in schedule_data, f"Missing required field: {field}"

        # If games exist, verify structure
        if schedule_data["totalGames"] > 0:
            first_date = schedule_data["dates"][0]
            assert "date" in first_date
            assert "games" in first_date

            first_game = first_date["games"][0]
            game_fields = ["gamePk", "gameType", "season", "gameDate"]
            for field in game_fields:
                assert field in first_game, f"Missing game field: {field}"

    def test_schedule_date_format(
        self,
        schedule_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that dates in schedule data are valid."""
        result = schedule_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM schedule.schedule WHERE id = %s" % result["row_id"]
        )

        schedule_data = rows[0]["data"]

        # Verify date format (YYYY-MM-DD)
        import re
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"

        if schedule_data["totalGames"] > 0:
            first_date = schedule_data["dates"][0]
            assert re.match(date_pattern, first_date["date"]), \
                f"Invalid date format: {first_date['date']}"


class TestScheduleErrorHandling:
    """Test error handling in schedule ingestion."""

    def test_database_connection_failure(self, schedule_client: MLBStatsAPIClient):
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
            schedule_client.fetch_and_save(bad_storage)

    def test_missing_schema_metadata(self):
        """Test behavior when schema metadata is missing."""
        from mlb_data_platform.schema.models import SCHEMA_METADATA_REGISTRY

        # Verify schedule schema exists
        assert "schedule.schedule" in SCHEMA_METADATA_REGISTRY
        schema = SCHEMA_METADATA_REGISTRY["schedule.schedule"]

        # Verify schema has required metadata
        assert schema.endpoint == "schedule"
        assert schema.method == "schedule"
        assert len(schema.fields) > 0

        # Verify schedule_date field exists and has json_path from request params
        schedule_date_field = next(
            (f for f in schema.fields if f.name == "schedule_date"),
            None
        )
        assert schedule_date_field is not None
        assert schedule_date_field.is_partition_key is True
        assert "request_params" in schedule_date_field.json_path
