"""Unit tests for storage field extraction from request parameters."""

import json
from datetime import date, datetime

import pytest

from mlb_data_platform.schema.models import FieldMetadata, SchemaMetadata
from mlb_data_platform.storage.postgres import PostgresStorageBackend


class TestFieldExtractionFromRequestParams:
    """Test extraction of fields from request parameters."""

    def test_extract_schedule_date_from_request_params(self):
        """Test extracting schedule_date from request parameters (not response)."""
        # This is the key fix - schedule_date comes from request params, not response
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)

        # Simulate combined data structure
        combined_data = {
            "totalGames": 15,
            "dates": [],
            "request_params": {
                "date": "2024-07-04",
                "sportId": 1
            }
        }

        # Schema metadata with json_path pointing to request_params
        schema = SchemaMetadata(
            endpoint="schedule",
            method="schedule",
            schema_name="schedule.schedule",
            fields=[
                FieldMetadata(
                    name="schedule_date",
                    type="date",
                    json_path="$.request_params.date",
                    is_partition_key=True,
                    nullable=False
                ),
                FieldMetadata(
                    name="sport_id",
                    type="int",
                    json_path="$.request_params.sportId"
                )
            ]
        )

        # Extract fields
        extracted = storage._extract_fields_from_data(combined_data, schema)

        # Verify extraction
        assert extracted["schedule_date"] == "2024-07-04"
        assert extracted["sport_id"] == 1

    def test_extract_game_date_from_response_data(self):
        """Test extracting game_date from nested response data."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)

        # Simulate game response data
        combined_data = {
            "gamePk": 744834,
            "gameData": {
                "datetime": {
                    "officialDate": "2024-07-04"
                },
                "game": {
                    "season": "2024",
                    "type": "R"
                },
                "status": {
                    "abstractGameState": "Final"
                },
                "teams": {
                    "home": {"id": 120},
                    "away": {"id": 121}
                },
                "venue": {"id": 3309}
            },
            "request_params": {
                "game_pk": "744834"
            }
        }

        # Schema metadata for game
        schema = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            fields=[
                FieldMetadata(
                    name="game_pk",
                    type="bigint",
                    json_path="$.gamePk",
                    nullable=False
                ),
                FieldMetadata(
                    name="game_date",
                    type="date",
                    json_path="$.gameData.datetime.officialDate",
                    is_partition_key=True,
                    nullable=False
                ),
                FieldMetadata(
                    name="season",
                    type="varchar(10)",
                    json_path="$.gameData.game.season"
                ),
                FieldMetadata(
                    name="game_type",
                    type="varchar(5)",
                    json_path="$.gameData.game.type"
                ),
                FieldMetadata(
                    name="game_state",
                    type="varchar(20)",
                    json_path="$.gameData.status.abstractGameState"
                ),
                FieldMetadata(
                    name="home_team_id",
                    type="int",
                    json_path="$.gameData.teams.home.id"
                ),
                FieldMetadata(
                    name="away_team_id",
                    type="int",
                    json_path="$.gameData.teams.away.id"
                ),
                FieldMetadata(
                    name="venue_id",
                    type="int",
                    json_path="$.gameData.venue.id"
                )
            ]
        )

        # Extract fields
        extracted = storage._extract_fields_from_data(combined_data, schema)

        # Verify all extractions
        assert extracted["game_pk"] == 744834
        assert extracted["game_date"] == "2024-07-04"
        assert extracted["season"] == "2024"
        assert extracted["game_type"] == "R"
        assert extracted["game_state"] == "Final"
        assert extracted["home_team_id"] == 120
        assert extracted["away_team_id"] == 121
        assert extracted["venue_id"] == 3309

    def test_extract_with_missing_field(self):
        """Test extraction when optional field is missing."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)

        combined_data = {
            "gamePk": 744834,
            "gameData": {
                "datetime": {
                    "officialDate": "2024-07-04"
                }
                # Missing teams, venue, etc.
            }
        }

        schema = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            fields=[
                FieldMetadata(
                    name="game_pk",
                    type="bigint",
                    json_path="$.gamePk",
                    nullable=False
                ),
                FieldMetadata(
                    name="game_date",
                    type="date",
                    json_path="$.gameData.datetime.officialDate",
                    nullable=False
                ),
                FieldMetadata(
                    name="venue_id",
                    type="int",
                    json_path="$.gameData.venue.id",
                    nullable=True  # Optional field
                )
            ]
        )

        extracted = storage._extract_fields_from_data(combined_data, schema)

        # Required fields should be present
        assert extracted["game_pk"] == 744834
        assert extracted["game_date"] == "2024-07-04"

        # Optional field should not be in extracted (None is skipped for nullable fields)
        assert "venue_id" not in extracted

    def test_extract_with_no_json_path(self):
        """Test fields without json_path are skipped."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)

        combined_data = {"some": "data"}

        schema = SchemaMetadata(
            endpoint="test",
            method="test",
            schema_name="test.test",
            fields=[
                FieldMetadata(
                    name="id",
                    type="bigserial",
                    nullable=False
                    # No json_path - should be skipped
                ),
                FieldMetadata(
                    name="captured_at",
                    type="timestamptz",
                    nullable=False
                    # No json_path - should be skipped
                )
            ]
        )

        extracted = storage._extract_fields_from_data(combined_data, schema)

        # Should return empty dict (no extractable fields)
        assert extracted == {}

    def test_extract_nested_path(self):
        """Test extraction from deeply nested paths."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)

        combined_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "deep_value": "found it!"
                        }
                    }
                }
            }
        }

        schema = SchemaMetadata(
            endpoint="test",
            method="test",
            schema_name="test.test",
            fields=[
                FieldMetadata(
                    name="deep_field",
                    type="text",
                    json_path="$.level1.level2.level3.level4.deep_value"
                )
            ]
        )

        extracted = storage._extract_fields_from_data(combined_data, schema)

        assert extracted["deep_field"] == "found it!"


class TestJsonPathExtraction:
    """Test the JSONPath extraction helper method."""

    def test_simple_path(self):
        """Test simple path extraction."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {"name": "test"}

        result = storage._extract_json_path(data, "$.name")
        assert result == "test"

    def test_nested_path(self):
        """Test nested path extraction."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {
            "game": {
                "info": {
                    "id": 123
                }
            }
        }

        result = storage._extract_json_path(data, "$.game.info.id")
        assert result == 123

    def test_missing_path_returns_none(self):
        """Test missing path returns None."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {"name": "test"}

        result = storage._extract_json_path(data, "$.missing.path")
        assert result is None

    def test_path_without_dollar_prefix(self):
        """Test path works without $ prefix."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {"name": "test"}

        # Should work without $ prefix
        result = storage._extract_json_path(data, "name")
        assert result == "test"

    def test_empty_path_returns_full_data(self):
        """Test empty path returns full data."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {"name": "test", "id": 123}

        result = storage._extract_json_path(data, "$")
        assert result == data

    def test_array_access(self):
        """Test extraction from array."""
        storage = PostgresStorageBackend.__new__(PostgresStorageBackend)
        data = {
            "items": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"}
            ]
        }

        # Note: Our current implementation doesn't support array indices
        # This test documents current behavior
        result = storage._extract_json_path(data, "$.items")
        assert isinstance(result, list)
        assert len(result) == 2


class TestCombinedDataStructure:
    """Test the combined data structure creation."""

    def test_combined_data_has_both_response_and_params(self):
        """Test combined data includes both response and request params."""
        # This tests the pattern we use in insert_raw_data
        response_data = {
            "totalGames": 15,
            "dates": []
        }

        request_params = {
            "date": "2024-07-04",
            "sportId": 1
        }

        # Combine as we do in storage backend
        combined_data = {
            **response_data,
            "request_params": request_params
        }

        # Verify structure
        assert "totalGames" in combined_data
        assert "dates" in combined_data
        assert "request_params" in combined_data
        assert combined_data["request_params"]["date"] == "2024-07-04"
        assert combined_data["request_params"]["sportId"] == 1

    def test_response_data_doesnt_override_request_params(self):
        """Test response data with 'request_params' key doesn't conflict."""
        # Edge case: what if response has a field called 'request_params'?
        response_data = {
            "request_params": "some_response_value",  # Hypothetical conflict
            "data": "test"
        }

        request_params = {
            "date": "2024-07-04"
        }

        # Our pattern: response spreads first, then we set request_params
        combined_data = {
            **response_data,
            "request_params": request_params  # This overwrites the response value
        }

        # request_params should be our dict, not the response string
        assert isinstance(combined_data["request_params"], dict)
        assert combined_data["request_params"]["date"] == "2024-07-04"
