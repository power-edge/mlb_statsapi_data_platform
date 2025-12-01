"""Tests for the pipeline storage adapter.

Tests the StorageAdapter which bridges PipelineOrchestrator with PostgresStorageBackend.
"""

import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from mlb_data_platform.pipeline.storage_adapter import (
    StorageAdapter,
    create_storage_callback,
)


class TestStorageAdapter:
    """Tests for StorageAdapter class."""

    @pytest.fixture
    def mock_backend(self):
        """Create a mock PostgresStorageBackend."""
        backend = MagicMock()
        backend.insert_raw_data.return_value = 1
        backend.upsert_raw_data.return_value = 2
        return backend

    @pytest.fixture
    def adapter(self, mock_backend):
        """Create a StorageAdapter with mock backend."""
        return StorageAdapter(mock_backend, upsert=True)

    @pytest.fixture
    def adapter_insert(self, mock_backend):
        """Create a StorageAdapter configured for insert (not upsert)."""
        return StorageAdapter(mock_backend, upsert=False)

    # =========================================================================
    # TABLE NAME MAPPING TESTS
    # =========================================================================

    def test_get_table_name_simple(self, adapter):
        """Test simple endpoint/method mapping."""
        assert adapter._get_table_name("season", "seasons") == "season.seasons"

    def test_get_table_name_camel_case(self, adapter):
        """Test camelCase method to snake_case conversion."""
        assert adapter._get_table_name("game", "liveGameV1") == "game.live_game_v1"

    def test_get_table_name_mixed_case_endpoint(self, adapter):
        """Test endpoint case normalization."""
        assert adapter._get_table_name("Game", "liveGameV1") == "game.live_game_v1"
        assert adapter._get_table_name("SCHEDULE", "schedule") == "schedule.schedule"

    def test_get_table_name_complex(self, adapter):
        """Test complex method name conversion."""
        assert adapter._get_table_name("game", "liveTimestampv11") == "game.live_timestampv11"
        assert adapter._get_table_name("game", "liveGameDiffPatchV1") == "game.live_game_diff_patch_v1"

    def test_method_to_snake_case(self, adapter):
        """Test _method_to_snake_case helper."""
        assert adapter._method_to_snake_case("liveGameV1") == "live_game_v1"
        assert adapter._method_to_snake_case("seasons") == "seasons"
        assert adapter._method_to_snake_case("ABC") == "a_b_c"

    # =========================================================================
    # METADATA FORMATTING TESTS
    # =========================================================================

    def test_format_metadata_basic(self, adapter):
        """Test basic metadata formatting."""
        metadata = {"sport_id": 1}
        result = adapter._format_metadata("season", "seasons", metadata)

        assert "schema_version" in result
        assert result["schema_version"] == "v1"
        assert "request" in result
        assert "response" in result
        assert "pipeline" in result

    def test_format_metadata_extracts_params(self, adapter):
        """Test that metadata params are extracted to query_params."""
        metadata = {
            "sport_id": 1,
            "game_pk": 745123,
            "season": "2024",
        }
        result = adapter._format_metadata("game", "liveGameV1", metadata)

        query_params = result["request"]["query_params"]
        assert query_params["sportId"] == 1
        assert query_params["gamePk"] == 745123
        assert query_params["season"] == "2024"

    def test_format_metadata_preserves_original(self, adapter):
        """Test that original metadata is preserved in pipeline section."""
        metadata = {"custom_key": "custom_value", "sport_id": 1}
        result = adapter._format_metadata("season", "seasons", metadata)

        assert result["pipeline"]["original_metadata"] == metadata
        assert result["pipeline"]["endpoint"] == "season"
        assert result["pipeline"]["method"] == "seasons"

    # =========================================================================
    # PARTITION DATE EXTRACTION TESTS
    # =========================================================================

    def test_extract_partition_date_from_string(self, adapter):
        """Test extracting partition date from ISO string."""
        metadata = {"schedule_date": "2024-06-15"}
        result = adapter._extract_partition_date(metadata)
        assert result == date(2024, 6, 15)

    def test_extract_partition_date_from_date_object(self, adapter):
        """Test extracting partition date from date object."""
        target = date(2024, 7, 4)
        metadata = {"schedule_date": target}
        result = adapter._extract_partition_date(metadata)
        assert result == target

    def test_extract_partition_date_default_today(self, adapter):
        """Test default partition date is today when not specified."""
        metadata = {}
        result = adapter._extract_partition_date(metadata)
        assert result == date.today()

    # =========================================================================
    # STORE METHOD TESTS
    # =========================================================================

    def test_store_calls_upsert_when_configured(self, adapter, mock_backend):
        """Test that store() calls upsert_raw_data when upsert=True."""
        with patch.object(adapter, "registry") as mock_registry:
            mock_schema = MagicMock()
            mock_registry.get_schema_by_endpoint.return_value = mock_schema

            data = {"key": "value"}
            metadata = {"sport_id": 1}

            row_id = adapter.store("season", "seasons", data, metadata)

            assert row_id == 2  # upsert returns 2 in mock
            mock_backend.upsert_raw_data.assert_called_once()
            mock_backend.insert_raw_data.assert_not_called()

    def test_store_calls_insert_when_configured(self, adapter_insert, mock_backend):
        """Test that store() calls insert_raw_data when upsert=False."""
        with patch.object(adapter_insert, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            data = {"key": "value"}
            metadata = {}

            row_id = adapter_insert.store("season", "seasons", data, metadata)

            assert row_id == 1  # insert returns 1 in mock
            mock_backend.insert_raw_data.assert_called_once()
            mock_backend.upsert_raw_data.assert_not_called()

    def test_store_uses_insert_when_no_schema(self, adapter, mock_backend):
        """Test that store() falls back to insert when no schema found."""
        with patch.object(adapter, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            data = {"key": "value"}
            metadata = {}

            row_id = adapter.store("unknown", "endpoint", data, metadata)

            # Should fall back to insert when no schema
            mock_backend.insert_raw_data.assert_called_once()

    def test_store_handles_exception(self, adapter, mock_backend):
        """Test that store() handles exceptions gracefully."""
        mock_backend.insert_raw_data.side_effect = Exception("DB error")

        with patch.object(adapter, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            result = adapter.store("season", "seasons", {}, {})

            assert result is None
            assert adapter.stats["errors"] == 1

    def test_store_with_none_metadata(self, adapter, mock_backend):
        """Test that store() handles None metadata."""
        with patch.object(adapter, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            # Should not raise
            adapter.store("season", "seasons", {"data": "test"}, None)

            mock_backend.insert_raw_data.assert_called_once()

    # =========================================================================
    # STATISTICS TRACKING TESTS
    # =========================================================================

    def test_stats_initial_state(self, adapter):
        """Test initial stats are zero."""
        stats = adapter.get_stats()
        assert stats["inserts"] == 0
        assert stats["upserts"] == 0
        assert stats["errors"] == 0
        assert stats["by_table"] == {}

    def test_stats_track_upserts(self, adapter, mock_backend):
        """Test that upserts are tracked."""
        with patch.object(adapter, "registry") as mock_registry:
            mock_schema = MagicMock()
            mock_registry.get_schema_by_endpoint.return_value = mock_schema

            adapter.store("season", "seasons", {}, {})
            adapter.store("game", "liveGameV1", {}, {})

            stats = adapter.get_stats()
            assert stats["upserts"] == 2
            assert stats["inserts"] == 0
            assert "season.seasons" in stats["by_table"]
            assert "game.live_game_v1" in stats["by_table"]

    def test_stats_track_inserts(self, adapter_insert, mock_backend):
        """Test that inserts are tracked."""
        with patch.object(adapter_insert, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            adapter_insert.store("season", "seasons", {}, {})

            stats = adapter_insert.get_stats()
            assert stats["inserts"] == 1
            assert stats["upserts"] == 0

    def test_stats_track_errors(self, adapter, mock_backend):
        """Test that errors are tracked."""
        mock_backend.insert_raw_data.side_effect = Exception("fail")

        with patch.object(adapter, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            adapter.store("season", "seasons", {}, {})

            stats = adapter.get_stats()
            assert stats["errors"] == 1

    def test_reset_stats(self, adapter, mock_backend):
        """Test stats can be reset."""
        with patch.object(adapter, "registry") as mock_registry:
            mock_schema = MagicMock()
            mock_registry.get_schema_by_endpoint.return_value = mock_schema

            adapter.store("season", "seasons", {}, {})
            assert adapter.get_stats()["upserts"] == 1

            adapter.reset_stats()
            stats = adapter.get_stats()
            assert stats["upserts"] == 0
            assert stats["inserts"] == 0
            assert stats["errors"] == 0
            assert stats["by_table"] == {}

    def test_stats_are_copy(self, adapter):
        """Test that get_stats() returns a copy."""
        stats1 = adapter.get_stats()
        stats1["upserts"] = 999

        stats2 = adapter.get_stats()
        assert stats2["upserts"] == 0  # Not modified

    # =========================================================================
    # CUSTOM PARTITION DATE TESTS
    # =========================================================================

    def test_custom_partition_date(self, mock_backend):
        """Test that custom partition_date is used."""
        custom_date = date(2023, 1, 15)
        adapter = StorageAdapter(mock_backend, upsert=False, partition_date=custom_date)

        with patch.object(adapter, "registry") as mock_registry:
            mock_registry.get_schema_by_endpoint.return_value = None

            adapter.store("season", "seasons", {}, {})

            # Check partition_date was passed
            call_kwargs = mock_backend.insert_raw_data.call_args.kwargs
            assert call_kwargs["partition_date"] == custom_date


class TestCreateStorageCallback:
    """Tests for create_storage_callback factory function."""

    def test_create_storage_callback_returns_tuple(self):
        """Test that factory returns (adapter, backend) tuple."""
        with patch("mlb_data_platform.storage.create_postgres_backend") as mock_create:
            mock_backend = MagicMock()
            mock_create.return_value = mock_backend

            adapter, backend = create_storage_callback(
                host="testhost",
                port=5433,
                database="testdb",
                user="testuser",
                password="testpass",
                upsert=True,
            )

            assert isinstance(adapter, StorageAdapter)
            assert backend is mock_backend
            mock_create.assert_called_once_with(
                host="testhost",
                port=5433,
                database="testdb",
                user="testuser",
                password="testpass",
            )

    def test_create_storage_callback_default_upsert(self):
        """Test that factory creates adapter with upsert=True by default."""
        with patch("mlb_data_platform.storage.create_postgres_backend") as mock_create:
            mock_create.return_value = MagicMock()

            adapter, _ = create_storage_callback()

            assert adapter.upsert is True

    def test_create_storage_callback_insert_mode(self):
        """Test factory can create adapter with upsert=False."""
        with patch("mlb_data_platform.storage.create_postgres_backend") as mock_create:
            mock_create.return_value = MagicMock()

            adapter, _ = create_storage_callback(upsert=False)

            assert adapter.upsert is False


class TestStorageAdapterIntegration:
    """Integration-style tests for storage adapter with real registry."""

    def test_endpoint_method_mapping_matches_registry(self):
        """Test that adapter mapping matches schema registry conventions."""
        # These should match the patterns in schema/registry.py
        adapter = StorageAdapter.__new__(StorageAdapter)

        # Known endpoint/method pairs from the codebase
        test_cases = [
            ("season", "seasons", "season.seasons"),
            ("schedule", "schedule", "schedule.schedule"),
            ("game", "liveGameV1", "game.live_game_v1"),
            ("person", "person", "person.person"),
            ("team", "teams", "team.teams"),
        ]

        for endpoint, method, expected_table in test_cases:
            result = adapter._get_table_name(endpoint, method)
            assert result == expected_table, f"Mismatch for {endpoint}.{method}"
