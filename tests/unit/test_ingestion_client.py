"""Unit tests for MLBStatsAPIClient."""

import time
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from pymlb_statsapi.model.factory import APIResponse

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.ingestion.config import (
    BackoffStrategy,
    IngestionConfig,
    JobConfig,
    JobType,
    RawStorageConfig,
    RetryConfig,
    SourceConfig,
    StorageConfig,
    StubMode,
)
from mlb_data_platform.schema.models import FieldMetadata, SchemaMetadata
from mlb_data_platform.storage.postgres import PostgresStorageBackend


@pytest.fixture
def mock_schema_metadata():
    """Create mock schema metadata."""
    return SchemaMetadata(
        schema_name="game.live_game_v1",
        endpoint="game",
        method="liveGameV1",
        version="v1",
        fields=[
            FieldMetadata(
                name="game_pk",
                type="long",
                description="Game primary key",
                json_path="$.gamePk",
            ),
            FieldMetadata(
                name="season",
                type="string",
                description="Season",
                json_path="$.gameData.game.season",
            ),
        ],
        primary_keys=["game_pk"],
        partition_keys=["game_date"],
        relationships=[],
    )


@pytest.fixture
def minimal_job_config():
    """Create minimal job configuration."""
    return JobConfig(
        name="test_job",
        type=JobType.BATCH,
        source=SourceConfig(
            endpoint="game",
            method="liveGameV1",
            parameters={"game_pk": 744834},
        ),
        storage=StorageConfig(raw=RawStorageConfig()),
    )


@pytest.fixture
def job_config_with_rate_limit():
    """Create job configuration with rate limiting."""
    return JobConfig(
        name="test_job",
        type=JobType.BATCH,
        source=SourceConfig(
            endpoint="game",
            method="liveGameV1",
            parameters={"game_pk": 744834},
        ),
        ingestion=IngestionConfig(rate_limit=10),  # 10 requests per minute
        storage=StorageConfig(raw=RawStorageConfig()),
    )


@pytest.fixture
def job_config_with_retry():
    """Create job configuration with custom retry settings."""
    return JobConfig(
        name="test_job",
        type=JobType.BATCH,
        source=SourceConfig(
            endpoint="game",
            method="liveGameV1",
            parameters={"game_pk": 744834},
        ),
        ingestion=IngestionConfig(
            retry=RetryConfig(
                max_attempts=5,
                backoff=BackoffStrategy.LINEAR,
                initial_delay=2,
                max_delay=120,
            )
        ),
        storage=StorageConfig(raw=RawStorageConfig()),
    )


@pytest.fixture
def mock_api_response():
    """Create mock API response."""
    response = Mock(spec=APIResponse)
    response.json.return_value = {
        "gamePk": 744834,
        "gameData": {
            "game": {
                "season": "2024",
                "type": "R",
            }
        },
        "liveData": {
            "plays": {
                "allPlays": []
            }
        },
    }
    response.get_metadata.return_value = {
        "url": "https://statsapi.mlb.com/api/v1/game/744834/feed/live",
        "status_code": 200,
    }
    return response


class TestMLBStatsAPIClientInitialization:
    """Test MLBStatsAPIClient initialization."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_init_with_minimal_config(self, mock_get_registry, minimal_job_config):
        """Test client initialization with minimal config."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        assert client.job_config == minimal_job_config
        assert client.stub_mode == StubMode.PASSTHROUGH
        assert client.schema_metadata is None
        assert client._last_request_time is None
        assert client._request_count == 0
        mock_registry.get_schema_by_endpoint.assert_called_once_with("game", "liveGameV1")

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_init_with_stub_mode(self, mock_get_registry, minimal_job_config):
        """Test client initialization with stub mode."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config, stub_mode=StubMode.REPLAY)

        assert client.stub_mode == StubMode.REPLAY

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_init_with_schema_metadata(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test client initialization with schema metadata."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        assert client.schema_metadata == mock_schema_metadata

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_init_with_custom_rate_limit(
        self, mock_get_registry, job_config_with_rate_limit
    ):
        """Test client initialization with custom rate limit."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(job_config_with_rate_limit)

        assert client.job_config.ingestion.rate_limit == 10


class TestMLBStatsAPIClientFetch:
    """Test MLBStatsAPIClient.fetch() method."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_success(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_api_response,
    ):
        """Test successful fetch from API."""
        # Setup mocks
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Create client and fetch
        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch()

        # Verify API was called correctly
        mock_api.get_endpoint.assert_called_once_with("game")
        mock_method.assert_called_once_with(game_pk=744834)

        # Verify result structure
        assert "metadata" in result
        assert "data" in result
        assert "extracted_fields" in result
        assert "schema_metadata" in result

        # Verify metadata
        assert result["metadata"]["request"]["endpoint_name"] == "game"
        assert result["metadata"]["request"]["method_name"] == "liveGameV1"
        assert result["metadata"]["request"]["query_params"] == {"game_pk": 744834}
        assert result["metadata"]["ingestion"]["job_name"] == "test_job"
        assert result["metadata"]["ingestion"]["stub_mode"] == "passthrough"

        # Verify data
        assert result["data"]["gamePk"] == 744834
        assert result["data"]["gameData"]["game"]["season"] == "2024"

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_with_override_params(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_api_response,
    ):
        """Test fetch with override parameters."""
        # Setup mocks
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Create client and fetch with override
        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch(game_pk=999999)

        # Verify override parameter was used
        mock_method.assert_called_once_with(game_pk=999999)
        assert result["metadata"]["request"]["query_params"]["game_pk"] == 999999

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_endpoint_not_found(self, mock_api, mock_get_registry, minimal_job_config):
        """Test fetch with invalid endpoint."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_api.get_endpoint.side_effect = AttributeError("Endpoint not found")

        client = MLBStatsAPIClient(minimal_job_config)

        with pytest.raises(ValueError) as exc_info:
            client.fetch()

        assert "Endpoint not found: game" in str(exc_info.value)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_method_not_found(self, mock_api, mock_get_registry, minimal_job_config):
        """Test fetch with invalid method."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock(spec=[])  # No methods
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config)

        with pytest.raises(ValueError) as exc_info:
            client.fetch()

        assert "Method 'liveGameV1' not found on endpoint 'game'" in str(exc_info.value)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    @patch.dict("os.environ", {}, clear=True)
    def test_fetch_with_stub_mode_capture(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_api_response,
    ):
        """Test fetch with stub mode set to capture."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config, stub_mode=StubMode.CAPTURE)
        result = client.fetch()

        # Verify STUB_MODE environment variable was set
        import os
        assert os.environ.get("STUB_MODE") == "capture"
        assert result["metadata"]["ingestion"]["stub_mode"] == "capture"

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_constructs_url_when_missing(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
    ):
        """Test fetch constructs URL when not in response metadata."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        # Mock response with no URL in metadata
        mock_response = Mock(spec=APIResponse)
        mock_response.json.return_value = {"gamePk": 744834}
        mock_response.get_metadata.return_value = {}

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch()

        # Verify URL was constructed
        url = result["metadata"]["request"]["url"]
        assert url.startswith("https://statsapi.mlb.com/api/v1/game/liveGameV1")
        assert "game_pk=744834" in url

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_with_schema_metadata_extracts_fields(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_schema_metadata,
        mock_api_response,
    ):
        """Test fetch extracts fields when schema metadata is available."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch()

        # Verify fields were extracted
        assert "extracted_fields" in result
        assert result["extracted_fields"]["game_pk"] == 744834
        assert result["extracted_fields"]["season"] == "2024"

        # Verify schema metadata in result
        assert result["schema_metadata"] is not None
        assert result["metadata"]["schema"]["version"] == "v1"
        assert result["metadata"]["schema"]["table_name"] == "game.live_game_v1"


class TestMLBStatsAPIClientRateLimiting:
    """Test MLBStatsAPIClient rate limiting."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_rate_limit_initialization(
        self,
        mock_get_registry,
        job_config_with_rate_limit,
    ):
        """Test rate limiting state is initialized correctly."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(job_config_with_rate_limit)

        # Verify rate limiting state is initialized
        assert client._last_request_time is None
        assert client._request_count == 0
        assert client._minute_start_time is not None
        assert client.job_config.ingestion.rate_limit == 10

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_rate_limit_tracking(
        self,
        mock_api,
        mock_get_registry,
        job_config_with_rate_limit,
        mock_api_response,
    ):
        """Test rate limit counter increments with each request."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(job_config_with_rate_limit)

        # Verify initial state
        assert client._request_count == 0

        # Make first request
        client.fetch()
        assert client._request_count == 1
        assert client._last_request_time is not None

        # Make second request
        client.fetch()
        assert client._request_count == 2


class TestMLBStatsAPIClientRetry:
    """Test MLBStatsAPIClient retry logic."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_retry_on_connection_error(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_api_response,
    ):
        """Test retry on ConnectionError."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(side_effect=[
            ConnectionError("Connection failed"),
            ConnectionError("Connection failed"),
            mock_api_response,  # Third attempt succeeds
        ])
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch()

        # Verify it retried and eventually succeeded
        assert mock_method.call_count == 3
        assert result["data"]["gamePk"] == 744834

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_retry_exhausted_raises_error(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
    ):
        """Test retry exhaustion raises original error."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(side_effect=ConnectionError("Connection failed"))
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(minimal_job_config)

        with pytest.raises(ConnectionError):
            client.fetch()

        # Should have tried max_attempts times (default is 3)
        assert mock_method.call_count == 3

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_custom_retry_config(
        self,
        mock_api,
        mock_get_registry,
        job_config_with_retry,
        mock_api_response,
    ):
        """Test custom retry configuration."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        # Fail 4 times, succeed on 5th
        mock_method = Mock(side_effect=[
            ConnectionError("Connection failed"),
            ConnectionError("Connection failed"),
            ConnectionError("Connection failed"),
            ConnectionError("Connection failed"),
            mock_api_response,
        ])
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        client = MLBStatsAPIClient(job_config_with_retry)
        result = client.fetch()

        # Should have tried 5 times (custom max_attempts)
        assert mock_method.call_count == 5
        assert result["data"]["gamePk"] == 744834


class TestMLBStatsAPIClientExtractFields:
    """Test MLBStatsAPIClient field extraction."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_extract_fields_with_valid_paths(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test field extraction with valid JSON paths."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        data = {
            "gamePk": 744834,
            "gameData": {
                "game": {
                    "season": "2024"
                }
            }
        }

        extracted = client._extract_fields(data, mock_schema_metadata)

        assert extracted["game_pk"] == 744834
        assert extracted["season"] == "2024"

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_extract_fields_with_missing_paths(
        self, mock_get_registry, minimal_job_config
    ):
        """Test field extraction with missing JSON paths."""
        schema_metadata = SchemaMetadata(
            schema_name="test.schema",
            endpoint="test",
            method="test",
            version="v1",
            fields=[
                FieldMetadata(
                    name="missing_field",
                    type="string",
                    description="Missing field",
                    json_path="$.nonexistent.field",
                ),
            ],
            primary_keys=[],
            partition_keys=[],
            relationships=[],
        )

        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        data = {"gamePk": 744834}

        extracted = client._extract_fields(data, schema_metadata)

        # Missing field should not be in extracted dict
        assert "missing_field" not in extracted

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_extract_fields_without_json_path(
        self, mock_get_registry, minimal_job_config
    ):
        """Test field extraction when json_path is None."""
        schema_metadata = SchemaMetadata(
            schema_name="test.schema",
            endpoint="test",
            method="test",
            version="v1",
            fields=[
                FieldMetadata(
                    name="no_path_field",
                    type="string",
                    description="Field without JSON path",
                    json_path=None,
                ),
            ],
            primary_keys=[],
            partition_keys=[],
            relationships=[],
        )

        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        data = {"gamePk": 744834}

        extracted = client._extract_fields(data, schema_metadata)

        # Field without json_path should be skipped
        assert len(extracted) == 0


class TestMLBStatsAPIClientExtractJsonPath:
    """Test MLBStatsAPIClient._extract_json_path() static method."""

    def test_extract_simple_path(self):
        """Test extraction of simple JSON path."""
        data = {"gamePk": 744834}
        result = MLBStatsAPIClient._extract_json_path(data, "$.gamePk")
        assert result == 744834

    def test_extract_nested_path(self):
        """Test extraction of nested JSON path."""
        data = {
            "gameData": {
                "game": {
                    "season": "2024"
                }
            }
        }
        result = MLBStatsAPIClient._extract_json_path(data, "$.gameData.game.season")
        assert result == "2024"

    def test_extract_path_without_dollar(self):
        """Test extraction works without leading $."""
        data = {"gamePk": 744834}
        result = MLBStatsAPIClient._extract_json_path(data, "gamePk")
        assert result == 744834

    def test_extract_nonexistent_path(self):
        """Test extraction of nonexistent path returns None."""
        data = {"gamePk": 744834}
        result = MLBStatsAPIClient._extract_json_path(data, "$.nonexistent")
        assert result is None

    def test_extract_path_through_non_dict(self):
        """Test extraction through non-dict value returns None."""
        data = {
            "gameData": "not_a_dict"
        }
        result = MLBStatsAPIClient._extract_json_path(data, "$.gameData.game.season")
        assert result is None

    def test_extract_deeply_nested_path(self):
        """Test extraction of deeply nested path."""
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep_value"
                        }
                    }
                }
            }
        }
        result = MLBStatsAPIClient._extract_json_path(
            data, "$.level1.level2.level3.level4.value"
        )
        assert result == "deep_value"


class TestMLBStatsAPIClientGetSchemaInfo:
    """Test MLBStatsAPIClient.get_schema_info() method."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_get_schema_info_without_schema(
        self, mock_get_registry, minimal_job_config
    ):
        """Test get_schema_info when no schema metadata exists."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)
        info = client.get_schema_info()

        assert info["endpoint"] == "game"
        assert info["method"] == "liveGameV1"
        assert info["schema_found"] is False

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_get_schema_info_with_schema(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test get_schema_info when schema metadata exists."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)
        info = client.get_schema_info()

        assert info["endpoint"] == "game"
        assert info["method"] == "liveGameV1"
        assert info["schema_name"] == "game.live_game_v1"
        assert info["version"] == "v1"
        assert info["schema_found"] is True
        assert info["primary_keys"] == ["game_pk"]
        assert info["partition_keys"] == ["game_date"]
        assert info["num_fields"] == 2
        assert info["num_relationships"] == 0


class TestMLBStatsAPIClientSave:
    """Test MLBStatsAPIClient.save() method."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_save_with_insert(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test save with insert operation."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.insert_raw_data.return_value = 123

        # Create mock result
        result = {
            "data": {"gamePk": 744834},
            "metadata": {
                "request": {"endpoint_name": "game", "method_name": "liveGameV1"},
                "response": {"status_code": 200},
            },
            "extracted_fields": {"game_pk": 744834},
            "schema_metadata": mock_schema_metadata.model_dump(),
        }

        row_id = client.save(result, mock_storage, upsert=False)

        # Verify insert was called
        assert row_id == 123
        mock_storage.insert_raw_data.assert_called_once()
        call_kwargs = mock_storage.insert_raw_data.call_args[1]
        assert call_kwargs["table_name"] == "game.live_game_v1"
        assert call_kwargs["data"] == {"gamePk": 744834}
        assert "schema_version" in call_kwargs["metadata"]

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_save_with_upsert(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test save with upsert operation."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.upsert_raw_data.return_value = 123

        # Create mock result
        result = {
            "data": {"gamePk": 744834},
            "metadata": {
                "request": {"endpoint_name": "game", "method_name": "liveGameV1"},
                "response": {"status_code": 200},
            },
            "extracted_fields": {"game_pk": 744834},
            "schema_metadata": mock_schema_metadata.model_dump(),
        }

        row_id = client.save(result, mock_storage, upsert=True)

        # Verify upsert was called
        assert row_id == 123
        mock_storage.upsert_raw_data.assert_called_once()
        call_kwargs = mock_storage.upsert_raw_data.call_args[1]
        assert call_kwargs["table_name"] == "game.live_game_v1"
        assert call_kwargs["data"] == {"gamePk": 744834}

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_save_with_partition_date(
        self, mock_get_registry, minimal_job_config, mock_schema_metadata
    ):
        """Test save with custom partition date."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.insert_raw_data.return_value = 123

        # Create mock result
        result = {
            "data": {"gamePk": 744834},
            "metadata": {"request": {}, "response": {}},
            "extracted_fields": {},
            "schema_metadata": mock_schema_metadata.model_dump(),
        }

        partition_date = date(2024, 7, 4)
        row_id = client.save(result, mock_storage, partition_date=partition_date)

        # Verify partition_date was passed
        call_kwargs = mock_storage.insert_raw_data.call_args[1]
        assert call_kwargs["partition_date"] == partition_date

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_save_without_schema_metadata_raises_error(
        self, mock_get_registry, minimal_job_config
    ):
        """Test save raises error when schema metadata is missing."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)

        # Create mock result
        result = {
            "data": {"gamePk": 744834},
            "metadata": {"request": {}, "response": {}},
            "extracted_fields": {},
            "schema_metadata": None,
        }

        with pytest.raises(ValueError) as exc_info:
            client.save(result, mock_storage)

        assert "No schema metadata found" in str(exc_info.value)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_save_without_table_name_raises_error(
        self, mock_get_registry, minimal_job_config
    ):
        """Test save raises error when table name is missing."""
        # Create schema metadata without table name
        schema_metadata = SchemaMetadata(
            schema_name="",  # Empty table name
            endpoint="game",
            method="liveGameV1",
            version="v1",
            fields=[],
            primary_keys=[],
            partition_keys=[],
            relationships=[],
        )

        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = schema_metadata
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)

        # Create mock result
        result = {
            "data": {"gamePk": 744834},
            "metadata": {"request": {}, "response": {}},
            "extracted_fields": {},
            "schema_metadata": schema_metadata.model_dump(),
        }

        with pytest.raises(ValueError) as exc_info:
            client.save(result, mock_storage)

        assert "Schema metadata missing table name" in str(exc_info.value)


class TestMLBStatsAPIClientFetchAndSave:
    """Test MLBStatsAPIClient.fetch_and_save() method."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_and_save_success(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_schema_metadata,
        mock_api_response,
    ):
        """Test successful fetch and save in one operation."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.insert_raw_data.return_value = 456

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch_and_save(mock_storage)

        # Verify fetch happened
        mock_method.assert_called_once()

        # Verify save happened
        mock_storage.insert_raw_data.assert_called_once()

        # Verify row_id was added to result
        assert "row_id" in result
        assert result["row_id"] == 456

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_and_save_with_upsert(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_schema_metadata,
        mock_api_response,
    ):
        """Test fetch and save with upsert."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.upsert_raw_data.return_value = 789

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch_and_save(mock_storage, upsert=True)

        # Verify upsert was used
        mock_storage.upsert_raw_data.assert_called_once()
        assert result["row_id"] == 789

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_and_save_with_override_params(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_schema_metadata,
        mock_api_response,
    ):
        """Test fetch and save with override parameters."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.insert_raw_data.return_value = 999

        client = MLBStatsAPIClient(minimal_job_config)
        result = client.fetch_and_save(mock_storage, game_pk=888888)

        # Verify override parameter was used
        mock_method.assert_called_once_with(game_pk=888888)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    @patch("mlb_data_platform.ingestion.client.api")
    def test_fetch_and_save_with_partition_date(
        self,
        mock_api,
        mock_get_registry,
        minimal_job_config,
        mock_schema_metadata,
        mock_api_response,
    ):
        """Test fetch and save with custom partition date."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = mock_schema_metadata
        mock_get_registry.return_value = mock_registry

        mock_endpoint = Mock()
        mock_method = Mock(return_value=mock_api_response)
        mock_endpoint.liveGameV1 = mock_method
        mock_api.get_endpoint.return_value = mock_endpoint

        # Mock storage backend
        mock_storage = Mock(spec=PostgresStorageBackend)
        mock_storage.insert_raw_data.return_value = 111

        client = MLBStatsAPIClient(minimal_job_config)
        partition_date = date(2024, 7, 4)
        result = client.fetch_and_save(mock_storage, partition_date=partition_date)

        # Verify partition date was passed to save
        call_kwargs = mock_storage.insert_raw_data.call_args[1]
        assert call_kwargs["partition_date"] == partition_date


class TestMLBStatsAPIClientBuildRetryDecorator:
    """Test MLBStatsAPIClient._build_retry_decorator() method."""

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_build_retry_decorator_exponential(
        self, mock_get_registry, minimal_job_config
    ):
        """Test building retry decorator with exponential backoff."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(minimal_job_config)
        decorator = client._build_retry_decorator()

        # Verify decorator is callable
        assert callable(decorator)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_build_retry_decorator_linear(
        self, mock_get_registry, job_config_with_retry
    ):
        """Test building retry decorator with linear backoff."""
        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(job_config_with_retry)
        decorator = client._build_retry_decorator()

        # Verify decorator is callable
        assert callable(decorator)

    @patch("mlb_data_platform.ingestion.client.get_registry")
    def test_build_retry_decorator_constant(self, mock_get_registry, minimal_job_config):
        """Test building retry decorator with constant backoff."""
        # Modify config for constant backoff
        config = minimal_job_config.model_copy(deep=True)
        config.ingestion.retry.backoff = BackoffStrategy.CONSTANT

        mock_registry = Mock()
        mock_registry.get_schema_by_endpoint.return_value = None
        mock_get_registry.return_value = mock_registry

        client = MLBStatsAPIClient(config)
        decorator = client._build_retry_decorator()

        # Verify decorator is callable
        assert callable(decorator)
