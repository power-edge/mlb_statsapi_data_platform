"""Unit tests for PostgreSQL storage backend."""

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from mlb_data_platform.schema.models import FieldMetadata, SchemaMetadata
from mlb_data_platform.storage.postgres import (
    PostgresConfig,
    PostgresStorageBackend,
    create_postgres_backend,
)


class TestPostgresConfig:
    """Test PostgresConfig model."""

    def test_default_values(self):
        """Test default configuration values."""
        config = PostgresConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "mlb_games"
        assert config.user == "mlb_admin"
        assert config.password == "mlb_admin_password"
        assert config.min_connections == 2
        assert config.max_connections == 10
        assert config.connection_timeout == 30

    def test_custom_values(self):
        """Test custom configuration values."""
        config = PostgresConfig(
            host="db.example.com",
            port=5433,
            database="custom_db",
            user="custom_user",
            password="custom_pass",
            min_connections=5,
            max_connections=20,
            connection_timeout=60,
        )

        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.database == "custom_db"
        assert config.user == "custom_user"
        assert config.password == "custom_pass"
        assert config.min_connections == 5
        assert config.max_connections == 20
        assert config.connection_timeout == 60

    def test_config_validation(self):
        """Test configuration validation via Pydantic."""
        # Valid config
        config = PostgresConfig(port=5432)
        assert config.port == 5432

        # Invalid port type should raise validation error
        with pytest.raises(Exception):  # Pydantic validation error
            PostgresConfig(port="invalid")


class TestPostgresStorageBackendInit:
    """Test PostgresStorageBackend initialization."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_initialization_success(self, mock_pool_class):
        """Test successful initialization with connection pool."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Verify pool was created with correct parameters
        mock_pool_class.assert_called_once()
        call_kwargs = mock_pool_class.call_args

        assert "conninfo" in call_kwargs.kwargs
        assert "host=localhost" in call_kwargs.kwargs["conninfo"]
        assert "port=5432" in call_kwargs.kwargs["conninfo"]
        assert "dbname=mlb_games" in call_kwargs.kwargs["conninfo"]
        assert "user=mlb_admin" in call_kwargs.kwargs["conninfo"]
        assert "password=mlb_admin_password" in call_kwargs.kwargs["conninfo"]
        assert call_kwargs.kwargs["min_size"] == 2
        assert call_kwargs.kwargs["max_size"] == 10
        assert call_kwargs.kwargs["timeout"] == 30

        assert backend.pool == mock_pool

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_initialization_failure(self, mock_pool_class):
        """Test initialization failure when connection pool creation fails."""
        mock_pool_class.side_effect = Exception("Connection failed")

        config = PostgresConfig()

        with pytest.raises(Exception, match="Connection failed"):
            PostgresStorageBackend(config)

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_context_manager(self, mock_pool_class):
        """Test context manager support."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        config = PostgresConfig()

        with PostgresStorageBackend(config) as backend:
            assert backend.pool == mock_pool

        # Verify close was called
        mock_pool.close.assert_called_once()

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_close(self, mock_pool_class):
        """Test closing connection pool."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        backend.close()

        mock_pool.close.assert_called_once()


class TestInsertRawData:
    """Test insert_raw_data method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_insert_raw_data_without_schema_metadata(self, mock_pool_class):
        """Test inserting raw data without schema metadata (no field extraction)."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": 123}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Mock _ensure_partition_exists
        backend._ensure_partition_exists = MagicMock()

        # Test data
        table_name = "schedule.schedule"
        data = {"totalGames": 15, "dates": []}
        metadata = {
            "schema_version": "v1",
            "request": {
                "url": "https://api.example.com/schedule",
                "query_params": {"date": "2024-07-04", "sportId": 1},
            },
        }

        # Insert data
        row_id = backend.insert_raw_data(
            table_name=table_name,
            data=data,
            metadata=metadata,
            partition_date=date(2024, 7, 4),
        )

        # Verify partition was ensured
        backend._ensure_partition_exists.assert_called_once_with(
            table_name, date(2024, 7, 4)
        )

        # Verify SQL was executed
        assert mock_cursor.execute.called
        sql_call = mock_cursor.execute.call_args
        sql = sql_call[0][0]
        params = sql_call[0][1]

        # Verify SQL structure
        assert "INSERT INTO schedule.schedule" in sql
        assert "data, captured_at, schema_version, source_url, request_params" in sql
        assert "RETURNING id" in sql

        # Verify parameters
        assert params["data"] == json.dumps(data)
        assert params["schema_version"] == "v1"
        assert params["source_url"] == "https://api.example.com/schedule"
        assert params["request_params"] == json.dumps(
            {"date": "2024-07-04", "sportId": 1}
        )

        # Verify commit was called
        mock_conn.commit.assert_called_once()

        # Verify return value
        assert row_id == 123

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_insert_raw_data_with_schema_metadata(self, mock_pool_class):
        """Test inserting raw data with schema metadata (field extraction)."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": 456}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)
        backend._ensure_partition_exists = MagicMock()

        # Schema metadata with field extraction
        schema_metadata = SchemaMetadata(
            endpoint="schedule",
            method="schedule",
            schema_name="schedule.schedule",
            fields=[
                FieldMetadata(
                    name="schedule_date",
                    type="date",
                    json_path="$.request_params.date",
                    nullable=False,
                ),
                FieldMetadata(
                    name="sport_id", type="int", json_path="$.request_params.sportId"
                ),
            ],
        )

        # Test data
        data = {"totalGames": 15, "dates": []}
        metadata = {
            "schema_version": "v1",
            "request": {
                "url": "https://api.example.com/schedule",
                "query_params": {"date": "2024-07-04", "sportId": 1},
            },
        }

        # Insert data
        row_id = backend.insert_raw_data(
            table_name="schedule.schedule",
            data=data,
            metadata=metadata,
            schema_metadata=schema_metadata,
            partition_date=date(2024, 7, 4),
        )

        # Verify SQL was executed with extracted fields
        sql_call = mock_cursor.execute.call_args
        sql = sql_call[0][0]
        params = sql_call[0][1]

        # Verify extracted fields are in SQL
        assert "schedule_date" in sql
        assert "sport_id" in sql

        # Verify extracted values in parameters
        assert params["schedule_date"] == "2024-07-04"
        assert params["sport_id"] == 1

        assert row_id == 456

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_insert_raw_data_rollback_on_error(self, mock_pool_class):
        """Test rollback on insert failure."""
        # Setup mocks to raise error
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Insert failed")

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)
        backend._ensure_partition_exists = MagicMock()

        # Test data
        data = {"totalGames": 15}
        metadata = {"schema_version": "v1", "request": {"url": "", "query_params": {}}}

        # Insert should raise exception
        with pytest.raises(Exception, match="Insert failed"):
            backend.insert_raw_data(
                table_name="schedule.schedule",
                data=data,
                metadata=metadata,
            )

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()


class TestUpsertRawData:
    """Test upsert_raw_data method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_upsert_raw_data_success(self, mock_pool_class):
        """Test upserting raw data with conflict resolution."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": 789}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)
        backend._ensure_partition_exists = MagicMock()

        # Schema metadata with primary keys
        schema_metadata = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            primary_keys=["id", "game_pk", "game_date"],
            fields=[
                FieldMetadata(
                    name="game_pk", type="bigint", json_path="$.gamePk", nullable=False
                ),
                FieldMetadata(
                    name="game_date",
                    type="date",
                    json_path="$.gameData.datetime.officialDate",
                    nullable=False,
                ),
            ],
        )

        # Test data
        data = {"gamePk": 744834, "gameData": {"datetime": {"officialDate": "2024-07-04"}}}
        metadata = {
            "schema_version": "v1",
            "request": {"url": "https://api.example.com/game", "query_params": {}},
        }

        # Upsert data
        row_id = backend.upsert_raw_data(
            table_name="game.live_game_v1",
            data=data,
            metadata=metadata,
            schema_metadata=schema_metadata,
            partition_date=date(2024, 7, 4),
        )

        # Verify SQL was executed
        sql_call = mock_cursor.execute.call_args
        sql = sql_call[0][0]
        params = sql_call[0][1]

        # Verify UPSERT structure
        assert "INSERT INTO game.live_game_v1" in sql
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        assert "RETURNING id" in sql

        # Verify conflict columns (excluding auto-increment id)
        assert "game_pk, game_date" in sql

        # Verify extracted values
        assert params["game_pk"] == 744834
        assert params["game_date"] == "2024-07-04"

        # Verify commit
        mock_conn.commit.assert_called_once()

        assert row_id == 789


class TestExtractFieldsFromData:
    """Test _extract_fields_from_data method."""

    def test_extract_simple_fields(self):
        """Test extracting simple fields from flat structure."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        data = {"name": "John", "age": 30, "city": "NYC"}

        schema = SchemaMetadata(
            endpoint="test",
            method="test",
            schema_name="test.test",
            fields=[
                FieldMetadata(name="name", type="text", json_path="$.name"),
                FieldMetadata(name="age", type="int", json_path="$.age"),
            ],
        )

        extracted = backend._extract_fields_from_data(data, schema)

        assert extracted["name"] == "John"
        assert extracted["age"] == 30
        assert "city" not in extracted  # Not in schema

    def test_extract_nested_fields(self):
        """Test extracting nested fields."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        data = {
            "gameData": {
                "datetime": {"officialDate": "2024-07-04"},
                "game": {"season": "2024"},
            }
        }

        schema = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            fields=[
                FieldMetadata(
                    name="game_date",
                    type="date",
                    json_path="$.gameData.datetime.officialDate",
                ),
                FieldMetadata(
                    name="season", type="varchar", json_path="$.gameData.game.season"
                ),
            ],
        )

        extracted = backend._extract_fields_from_data(data, schema)

        assert extracted["game_date"] == "2024-07-04"
        assert extracted["season"] == "2024"

    def test_extract_nullable_fields(self):
        """Test extracting nullable fields that are missing."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        data = {"name": "John"}

        schema = SchemaMetadata(
            endpoint="test",
            method="test",
            schema_name="test.test",
            fields=[
                FieldMetadata(name="name", type="text", json_path="$.name"),
                FieldMetadata(
                    name="age", type="int", json_path="$.age", nullable=True
                ),
            ],
        )

        extracted = backend._extract_fields_from_data(data, schema)

        assert extracted["name"] == "John"
        assert "age" not in extracted  # Null and nullable, so skipped

    def test_extract_from_request_params(self):
        """Test extracting from request_params in combined data."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        combined_data = {
            "totalGames": 15,
            "request_params": {"date": "2024-07-04", "sportId": 1},
        }

        schema = SchemaMetadata(
            endpoint="schedule",
            method="schedule",
            schema_name="schedule.schedule",
            fields=[
                FieldMetadata(
                    name="schedule_date",
                    type="date",
                    json_path="$.request_params.date",
                ),
                FieldMetadata(
                    name="sport_id", type="int", json_path="$.request_params.sportId"
                ),
            ],
        )

        extracted = backend._extract_fields_from_data(combined_data, schema)

        assert extracted["schedule_date"] == "2024-07-04"
        assert extracted["sport_id"] == 1


class TestExtractJsonPath:
    """Test _extract_json_path static method."""

    def test_extract_root(self):
        """Test extracting root with $ path."""
        data = {"key": "value"}
        result = PostgresStorageBackend._extract_json_path(data, "$")
        assert result == data

    def test_extract_root_with_dot(self):
        """Test extracting root with $. path."""
        data = {"key": "value"}
        result = PostgresStorageBackend._extract_json_path(data, "$.")
        assert result == data

    def test_extract_simple_key(self):
        """Test extracting simple key."""
        data = {"name": "John", "age": 30}
        result = PostgresStorageBackend._extract_json_path(data, "$.name")
        assert result == "John"

    def test_extract_nested_key(self):
        """Test extracting nested key."""
        data = {"user": {"profile": {"name": "John"}}}
        result = PostgresStorageBackend._extract_json_path(
            data, "$.user.profile.name"
        )
        assert result == "John"

    def test_extract_missing_key(self):
        """Test extracting missing key returns None."""
        data = {"name": "John"}
        result = PostgresStorageBackend._extract_json_path(data, "$.age")
        assert result is None

    def test_extract_from_non_dict(self):
        """Test extracting from non-dict returns None."""
        data = {"value": 42}
        result = PostgresStorageBackend._extract_json_path(data, "$.value.nested")
        assert result is None


class TestBuildInsertSql:
    """Test _build_insert_sql method."""

    def test_build_insert_sql_base_columns_only(self):
        """Test building INSERT SQL with base columns only."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        sql = backend._build_insert_sql("schedule.schedule", {})

        assert "INSERT INTO schedule.schedule" in sql
        assert "data, captured_at, schema_version, source_url, request_params" in sql
        assert "%(data)s, %(captured_at)s, %(schema_version)s, %(source_url)s, %(request_params)s" in sql
        assert "RETURNING id" in sql

    def test_build_insert_sql_with_extracted_fields(self):
        """Test building INSERT SQL with extracted fields."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        extracted_fields = {"schedule_date": "2024-07-04", "sport_id": 1}
        sql = backend._build_insert_sql("schedule.schedule", extracted_fields)

        assert "INSERT INTO schedule.schedule" in sql
        assert "schedule_date" in sql
        assert "sport_id" in sql
        assert "%(schedule_date)s" in sql
        assert "%(sport_id)s" in sql
        assert "RETURNING id" in sql


class TestBuildUpsertSql:
    """Test _build_upsert_sql method."""

    def test_build_upsert_sql_with_conflict(self):
        """Test building UPSERT SQL with conflict resolution."""
        backend = PostgresStorageBackend.__new__(PostgresStorageBackend)

        schema_metadata = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            primary_keys=["id", "game_pk", "game_date"],
        )

        extracted_fields = {"game_pk": 744834, "game_date": "2024-07-04"}
        sql = backend._build_upsert_sql(
            "game.live_game_v1", schema_metadata, extracted_fields
        )

        assert "INSERT INTO game.live_game_v1" in sql
        assert "ON CONFLICT (game_pk, game_date)" in sql
        assert "DO UPDATE SET" in sql
        assert "RETURNING id" in sql

        # Verify update columns exclude primary keys
        assert "data = EXCLUDED.data" in sql
        assert "captured_at = EXCLUDED.captured_at" in sql


class TestEnsurePartitionExists:
    """Test _ensure_partition_exists method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_partition_exists(self, mock_pool_class):
        """Test when partition already exists."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": True}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)
        backend._create_partition = MagicMock()

        # Ensure partition
        backend._ensure_partition_exists("schedule.schedule", date(2024, 7, 4))

        # Verify check query was executed
        assert mock_cursor.execute.called
        check_call = mock_cursor.execute.call_args_list[0]
        assert "pg_class" in check_call[0][0]
        assert check_call[0][1] == ("schedule", "schedule_2024_07")

        # Verify partition creation was NOT called
        backend._create_partition.assert_not_called()

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_partition_does_not_exist(self, mock_pool_class):
        """Test when partition does not exist."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": False}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)
        backend._create_partition = MagicMock()

        # Ensure partition
        backend._ensure_partition_exists("schedule.schedule", date(2024, 7, 4))

        # Verify partition creation WAS called
        backend._create_partition.assert_called_once_with(
            "schedule.schedule", date(2024, 7, 4)
        )


class TestCreatePartition:
    """Test _create_partition method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_create_partition_success(self, mock_pool_class):
        """Test successful partition creation."""
        # Setup mocks
        mock_cursor = MagicMock()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Create partition
        backend._create_partition("schedule.schedule", date(2024, 7, 15))

        # Verify SQL was executed
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]

        assert "CREATE TABLE IF NOT EXISTS schedule.schedule_2024_07" in sql
        assert "PARTITION OF schedule.schedule" in sql
        assert "FOR VALUES FROM ('2024-07-01') TO ('2024-07-31')" in sql

        # Verify commit
        mock_conn.commit.assert_called_once()

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_create_partition_handles_error(self, mock_pool_class):
        """Test partition creation handles errors gracefully."""
        # Setup mocks to raise error
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Partition already exists")

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Create partition should not raise (handles concurrent creation)
        backend._create_partition("schedule.schedule", date(2024, 7, 15))

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()


class TestQuery:
    """Test query method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_query_fetch_all(self, mock_pool_class):
        """Test query fetching all rows."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Game 1"},
            {"id": 2, "name": "Game 2"},
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Execute query
        result = backend.query("SELECT * FROM games WHERE season = %(season)s", {"season": "2024"})

        # Verify query execution
        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM games WHERE season = %(season)s", {"season": "2024"}
        )
        mock_cursor.fetchall.assert_called_once()

        assert result == [{"id": 1, "name": "Game 1"}, {"id": 2, "name": "Game 2"}]

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_query_fetch_one(self, mock_pool_class):
        """Test query fetching single row."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": 1, "name": "Game 1"}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Execute query
        result = backend.query("SELECT * FROM games WHERE id = 1", fetch_one=True)

        # Verify query execution
        mock_cursor.fetchone.assert_called_once()
        assert result == {"id": 1, "name": "Game 1"}

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_query_no_params(self, mock_pool_class):
        """Test query without parameters."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Execute query
        result = backend.query("SELECT COUNT(*) FROM games")

        # Verify query execution with empty params dict
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM games", {})


class TestExecute:
    """Test execute method."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_execute_success(self, mock_pool_class):
        """Test successful execute."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Execute statement
        affected = backend.execute(
            "DELETE FROM games WHERE season = %(season)s", {"season": "2023"}
        )

        # Verify execution
        mock_cursor.execute.assert_called_once_with(
            "DELETE FROM games WHERE season = %(season)s", {"season": "2023"}
        )
        mock_conn.commit.assert_called_once()

        assert affected == 5

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_execute_rollback_on_error(self, mock_pool_class):
        """Test rollback on execute failure."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Execute failed")

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Execute should raise exception
        with pytest.raises(Exception, match="Execute failed"):
            backend.execute("DELETE FROM games")

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()


class TestUtilityMethods:
    """Test utility methods."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_get_latest_captured_at(self, mock_pool_class):
        """Test getting latest captured_at timestamp."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"max_ts": datetime(2024, 7, 4, 12, 0, 0)}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Get latest captured_at
        result = backend.get_latest_captured_at("schedule.schedule")

        # Verify query
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert "SELECT MAX(captured_at)" in sql
        assert "FROM schedule.schedule" in sql

        assert result == datetime(2024, 7, 4, 12, 0, 0)

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_get_latest_captured_at_empty_table(self, mock_pool_class):
        """Test getting latest captured_at from empty table."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Get latest captured_at
        result = backend.get_latest_captured_at("schedule.schedule")

        assert result is None

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_table_exists_true(self, mock_pool_class):
        """Test table_exists returns True."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": True}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Check if table exists
        result = backend.table_exists("schedule.schedule")

        assert result is True

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_table_exists_false(self, mock_pool_class):
        """Test table_exists returns False."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": False}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Check if table exists
        result = backend.table_exists("nonexistent.table")

        assert result is False

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_refresh_materialized_view(self, mock_pool_class):
        """Test refreshing materialized view."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Refresh view
        backend.refresh_materialized_view("game.latest_live", concurrently=True)

        # Verify SQL
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert "REFRESH MATERIALIZED VIEW CONCURRENTLY game.latest_live" in sql

        mock_conn.commit.assert_called_once()

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_refresh_materialized_view_not_concurrently(self, mock_pool_class):
        """Test refreshing materialized view without CONCURRENTLY."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Refresh view
        backend.refresh_materialized_view("game.latest_live", concurrently=False)

        # Verify SQL (no CONCURRENTLY)
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert "REFRESH MATERIALIZED VIEW  game.latest_live" in sql

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_get_table_row_count(self, mock_pool_class):
        """Test getting table row count."""
        # Setup mocks
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"count": 12345}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = False

        mock_pool = MagicMock()
        mock_pool.connection.return_value.__enter__.return_value = mock_conn
        mock_pool.connection.return_value.__exit__.return_value = False

        mock_pool_class.return_value = mock_pool

        # Create backend
        config = PostgresConfig()
        backend = PostgresStorageBackend(config)

        # Get row count
        count = backend.get_table_row_count("schedule.schedule")

        # Verify query
        assert mock_cursor.execute.called
        sql = mock_cursor.execute.call_args[0][0]
        assert "SELECT COUNT(*)" in sql
        assert "FROM schedule.schedule" in sql

        assert count == 12345


class TestCreatePostgresBackend:
    """Test create_postgres_backend factory function."""

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_create_with_defaults(self, mock_pool_class):
        """Test creating backend with default values."""
        mock_pool_class.return_value = MagicMock()

        backend = create_postgres_backend()

        assert backend.config.host == "localhost"
        assert backend.config.port == 5432
        assert backend.config.database == "mlb_games"
        assert backend.config.user == "mlb_admin"
        assert backend.config.password == "mlb_admin_password"

    @patch("mlb_data_platform.storage.postgres.ConnectionPool")
    def test_create_with_custom_values(self, mock_pool_class):
        """Test creating backend with custom values."""
        mock_pool_class.return_value = MagicMock()

        backend = create_postgres_backend(
            host="custom.db.com",
            port=5433,
            database="custom_db",
            user="custom_user",
            password="custom_pass",
        )

        assert backend.config.host == "custom.db.com"
        assert backend.config.port == 5433
        assert backend.config.database == "custom_db"
        assert backend.config.user == "custom_user"
        assert backend.config.password == "custom_pass"
