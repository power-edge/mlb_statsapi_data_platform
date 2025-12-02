"""Integration tests for season data ingestion."""

import pytest

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.storage.postgres import PostgresStorageBackend


class TestSeasonIngestionPipeline:
    """Test complete season ingestion pipeline end-to-end."""

    def test_season_ingestion_complete_flow(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test complete season ingestion from API to database.

        This tests the full pipeline:
        1. Fetch data from API (stub mode)
        2. Extract fields
        3. Save to PostgreSQL
        4. Verify data persistence
        """
        # Verify table is empty before test
        count_before = pytest.count_rows(storage_backend, "season.seasons")
        assert count_before == 0

        # Execute ingestion
        result = season_client.fetch_and_save(storage_backend)

        # Verify result
        assert "row_id" in result
        assert result["row_id"] > 0

        # Verify data was saved
        count_after = pytest.count_rows(storage_backend, "season.seasons")
        assert count_after == 1

        # Query and verify the saved data
        rows = pytest.query_table(
            storage_backend,
            "SELECT * FROM season.seasons ORDER BY id DESC LIMIT 1"
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify metadata fields
        assert row["id"] == result["row_id"]
        assert row["schema_version"] == "v1"
        assert row["response_status"] == 200

        # Verify JSONB data exists
        assert row["data"] is not None
        assert "seasons" in row["data"]
        assert len(row["data"]["seasons"]) > 0

        # Verify first season data
        season = row["data"]["seasons"][0]
        assert "seasonId" in season
        assert "regularSeasonStartDate" in season
        assert "regularSeasonEndDate" in season

    def test_season_field_extraction(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test field extraction from request parameters."""
        # Execute ingestion
        result = season_client.fetch_and_save(storage_backend)

        # Query extracted fields
        rows = pytest.query_table(
            storage_backend,
            "SELECT sport_id, request_params FROM season.seasons WHERE id = %s" % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify sport_id extracted from request params
        assert row["sport_id"] == 1
        assert row["request_params"]["sportId"] == 1

    def test_season_ingestion_idempotency(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that ingesting same data twice creates two rows (not upsert by default)."""
        # First ingestion
        result1 = season_client.fetch_and_save(storage_backend)

        # Second ingestion
        result2 = season_client.fetch_and_save(storage_backend)

        # Should have two rows (INSERT mode, not UPSERT)
        count = pytest.count_rows(storage_backend, "season.seasons")
        assert count == 2

        # Should have different IDs
        assert result1["row_id"] != result2["row_id"]

    def test_season_fetch_only(self, season_client: MLBStatsAPIClient):
        """Test fetching data without saving (dry run)."""
        # Fetch without saving
        result = season_client.fetch()

        # Verify result structure
        assert "data" in result
        assert "metadata" in result
        assert "request" in result["metadata"]
        assert "response" in result["metadata"]

        # Verify data content
        assert "seasons" in result["data"]
        assert len(result["data"]["seasons"]) > 0

        # Verify no row_id (not saved)
        assert "row_id" not in result

    def test_season_metadata_capture(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that request/response metadata is captured correctly."""
        result = season_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            """
            SELECT source_url, request_params, captured_at, ingestion_timestamp
            FROM season.seasons
            WHERE id = %s
            """ % result["row_id"]
        )

        assert len(rows) == 1
        row = rows[0]

        # Verify source URL exists
        assert row["source_url"] is not None
        assert len(row["source_url"]) > 0

        # Verify request params captured
        assert row["request_params"] is not None
        assert "sportId" in row["request_params"]

        # Verify timestamps
        assert row["captured_at"] is not None
        assert row["ingestion_timestamp"] is not None


class TestSeasonDataValidation:
    """Test season data validation and structure."""

    def test_season_data_structure(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that season data has expected structure."""
        result = season_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM season.seasons WHERE id = %s" % result["row_id"]
        )

        season_data = rows[0]["data"]
        assert "seasons" in season_data

        # Verify season fields
        season = season_data["seasons"][0]
        required_fields = [
            "seasonId",
            "regularSeasonStartDate",
            "regularSeasonEndDate",
            "seasonStartDate",
            "seasonEndDate"
        ]

        for field in required_fields:
            assert field in season, f"Missing required field: {field}"

    def test_season_date_formats(
        self,
        season_client: MLBStatsAPIClient,
        storage_backend: PostgresStorageBackend,
        clean_tables
    ):
        """Test that dates in season data are valid."""
        result = season_client.fetch_and_save(storage_backend)

        rows = pytest.query_table(
            storage_backend,
            "SELECT data FROM season.seasons WHERE id = %s" % result["row_id"]
        )

        season = rows[0]["data"]["seasons"][0]

        # Verify date formats (YYYY-MM-DD)
        import re
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"

        date_fields = [
            "regularSeasonStartDate",
            "regularSeasonEndDate",
            "seasonStartDate",
            "seasonEndDate"
        ]

        for field in date_fields:
            if field in season:
                assert re.match(date_pattern, season[field]), \
                    f"{field} has invalid date format: {season[field]}"


class TestSeasonErrorHandling:
    """Test error handling in season ingestion."""

    def test_database_connection_failure(self, season_client: MLBStatsAPIClient):
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
            season_client.fetch_and_save(bad_storage)

    def test_missing_schema_metadata(self):
        """Test behavior when schema metadata is missing."""
        from mlb_data_platform.schema.models import SCHEMA_METADATA_REGISTRY

        # Verify season schema exists
        assert "season.seasons" in SCHEMA_METADATA_REGISTRY
        schema = SCHEMA_METADATA_REGISTRY["season.seasons"]

        # Verify schema has required metadata
        assert schema.endpoint == "season"
        assert schema.method == "seasons"
        assert len(schema.fields) > 0
