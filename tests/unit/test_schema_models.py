"""Unit tests for schema models."""

import pytest

from mlb_data_platform.schema.models import (
    SCHEMA_METADATA_REGISTRY,
    FieldMetadata,
    RelationshipType,
    SchemaMetadata,
    SchemaRelationship,
)


class TestRelationshipType:
    """Test RelationshipType enum."""

    def test_relationship_type_values(self):
        """Test that RelationshipType has expected values."""
        assert RelationshipType.ONE_TO_ONE == "one_to_one"
        assert RelationshipType.ONE_TO_MANY == "one_to_many"
        assert RelationshipType.MANY_TO_ONE == "many_to_one"
        assert RelationshipType.MANY_TO_MANY == "many_to_many"

    def test_relationship_type_is_string_enum(self):
        """Test that RelationshipType values are strings."""
        for rel_type in RelationshipType:
            assert isinstance(rel_type.value, str)


class TestFieldMetadata:
    """Test FieldMetadata model."""

    def test_create_minimal_field(self):
        """Test creating a field with minimal required attributes."""
        field = FieldMetadata(name="test_field", type="varchar")

        assert field.name == "test_field"
        assert field.type == "varchar"
        assert field.nullable is True  # Default
        assert field.is_primary_key is False
        assert field.is_foreign_key is False
        assert field.is_partition_key is False
        assert field.is_indexed is False
        assert field.description is None
        assert field.json_path is None
        assert field.default_value is None

    def test_create_field_with_all_attributes(self):
        """Test creating a field with all attributes specified."""
        field = FieldMetadata(
            name="game_pk",
            type="bigint",
            nullable=False,
            is_primary_key=True,
            is_foreign_key=False,
            is_partition_key=False,
            is_indexed=True,
            description="Game primary key",
            json_path="$.gamePk",
            default_value=0,
        )

        assert field.name == "game_pk"
        assert field.type == "bigint"
        assert field.nullable is False
        assert field.is_primary_key is True
        assert field.is_foreign_key is False
        assert field.is_partition_key is False
        assert field.is_indexed is True
        assert field.description == "Game primary key"
        assert field.json_path == "$.gamePk"
        assert field.default_value == 0

    def test_field_with_request_params_json_path(self):
        """Test field with JSONPath referencing request_params."""
        field = FieldMetadata(
            name="sport_id",
            type="int",
            json_path="$.request_params.sportId",
        )

        assert field.json_path == "$.request_params.sportId"
        assert "request_params" in field.json_path


class TestSchemaRelationship:
    """Test SchemaRelationship model."""

    def test_create_relationship(self):
        """Test creating a schema relationship."""
        relationship = SchemaRelationship(
            from_schema="schedule.games",
            to_schema="game.live_game_v1",
            relationship_type=RelationshipType.MANY_TO_ONE,
            from_field="game_pk",
            to_field="game_pk",
            description="Games in schedule reference live game data",
        )

        assert relationship.from_schema == "schedule.games"
        assert relationship.to_schema == "game.live_game_v1"
        assert relationship.relationship_type == RelationshipType.MANY_TO_ONE
        assert relationship.from_field == "game_pk"
        assert relationship.to_field == "game_pk"
        assert relationship.description == "Games in schedule reference live game data"
        assert relationship.cascade_delete is False

    def test_relationship_with_cascade_delete(self):
        """Test relationship with cascade delete enabled."""
        relationship = SchemaRelationship(
            from_schema="game.live_game_plays",
            to_schema="game.live_game_v1",
            relationship_type=RelationshipType.MANY_TO_ONE,
            from_field="game_pk",
            to_field="game_pk",
            cascade_delete=True,
        )

        assert relationship.cascade_delete is True


class TestSchemaMetadata:
    """Test SchemaMetadata model."""

    def test_create_minimal_schema(self):
        """Test creating schema with minimal required fields."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
        )

        assert schema.endpoint == "test"
        assert schema.method == "testMethod"
        assert schema.schema_name == "test.test_method"
        assert schema.version == "v1"  # Default
        assert schema.fields == []
        assert schema.primary_keys == []
        assert schema.partition_keys == []
        assert schema.relationships == []
        assert schema.avro_schema is None
        assert schema.description is None
        assert schema.source_json_schema is None
        assert schema.created_at is None
        assert schema.updated_at is None

    def test_create_schema_with_fields(self):
        """Test creating schema with fields."""
        fields = [
            FieldMetadata(name="id", type="bigserial", is_primary_key=True, nullable=False),
            FieldMetadata(name="data", type="jsonb", nullable=False),
        ]

        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=fields,
            primary_keys=["id"],
        )

        assert len(schema.fields) == 2
        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "data"
        assert schema.primary_keys == ["id"]

    def test_get_primary_key_fields(self):
        """Test getting primary key fields."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint", is_primary_key=True),
                FieldMetadata(name="name", type="varchar"),
                FieldMetadata(name="game_pk", type="bigint", is_primary_key=True),
            ],
        )

        pk_fields = schema.get_primary_key_fields()
        assert len(pk_fields) == 2
        assert pk_fields[0].name == "id"
        assert pk_fields[1].name == "game_pk"

    def test_get_foreign_key_fields(self):
        """Test getting foreign key fields."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint", is_primary_key=True),
                FieldMetadata(name="game_pk", type="bigint", is_foreign_key=True),
                FieldMetadata(name="team_id", type="int", is_foreign_key=True),
            ],
        )

        fk_fields = schema.get_foreign_key_fields()
        assert len(fk_fields) == 2
        assert fk_fields[0].name == "game_pk"
        assert fk_fields[1].name == "team_id"

    def test_get_partition_fields(self):
        """Test getting partition key fields."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint"),
                FieldMetadata(name="game_date", type="date", is_partition_key=True),
                FieldMetadata(name="schedule_date", type="date", is_partition_key=True),
            ],
        )

        partition_fields = schema.get_partition_fields()
        assert len(partition_fields) == 2
        assert partition_fields[0].name == "game_date"
        assert partition_fields[1].name == "schedule_date"

    def test_get_relationship_by_field(self):
        """Test getting relationship for a specific field."""
        relationships = [
            SchemaRelationship(
                from_schema="test.test_method",
                to_schema="other.table",
                relationship_type=RelationshipType.MANY_TO_ONE,
                from_field="game_pk",
                to_field="game_pk",
            ),
            SchemaRelationship(
                from_schema="test.test_method",
                to_schema="another.table",
                relationship_type=RelationshipType.MANY_TO_ONE,
                from_field="team_id",
                to_field="id",
            ),
        ]

        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            relationships=relationships,
        )

        # Find relationship by field
        rel = schema.get_relationship_by_field("game_pk")
        assert rel is not None
        assert rel.from_field == "game_pk"
        assert rel.to_schema == "other.table"

        # Find non-existent relationship
        rel = schema.get_relationship_by_field("non_existent")
        assert rel is None

    def test_get_relationship_by_field_returns_first_match(self):
        """Test that get_relationship_by_field returns the first match."""
        relationships = [
            SchemaRelationship(
                from_schema="test.test_method",
                to_schema="first.table",
                relationship_type=RelationshipType.MANY_TO_ONE,
                from_field="shared_field",
                to_field="id",
            ),
            SchemaRelationship(
                from_schema="test.test_method",
                to_schema="second.table",
                relationship_type=RelationshipType.MANY_TO_ONE,
                from_field="shared_field",
                to_field="id",
            ),
        ]

        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            relationships=relationships,
        )

        rel = schema.get_relationship_by_field("shared_field")
        assert rel is not None
        assert rel.to_schema == "first.table"  # Returns first match


class TestSchemaMetadataRegistry:
    """Test SCHEMA_METADATA_REGISTRY."""

    def test_registry_is_dict(self):
        """Test that registry is a dictionary."""
        assert isinstance(SCHEMA_METADATA_REGISTRY, dict)

    def test_registry_has_expected_schemas(self):
        """Test that registry contains expected core schemas."""
        expected_schemas = [
            "schedule.schedule",
            "game.live_game_v1",
            "season.seasons",
        ]

        for schema_name in expected_schemas:
            assert schema_name in SCHEMA_METADATA_REGISTRY

    def test_schedule_schema_metadata(self):
        """Test schedule.schedule schema metadata."""
        schema = SCHEMA_METADATA_REGISTRY["schedule.schedule"]

        assert schema.endpoint == "schedule"
        assert schema.method == "schedule"
        assert schema.schema_name == "schedule.schedule"
        assert schema.description == "Daily game schedule from Schedule.schedule()"
        assert len(schema.fields) > 0
        assert "id" in schema.primary_keys
        assert "schedule_date" in schema.partition_keys

        # Verify key fields exist
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "request_params" in field_names
        assert "data" in field_names
        assert "captured_at" in field_names
        assert "schema_version" in field_names
        assert "sport_id" in field_names
        assert "schedule_date" in field_names

    def test_game_live_game_v1_schema_metadata(self):
        """Test game.live_game_v1 schema metadata."""
        schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]

        assert schema.endpoint == "game"
        assert schema.method == "liveGameV1"
        assert schema.schema_name == "game.live_game_v1"
        assert "MASTER table" in schema.description
        assert len(schema.fields) > 0
        assert "id" in schema.primary_keys
        assert "game_date" in schema.primary_keys
        assert "game_date" in schema.partition_keys

        # Verify key fields exist
        field_names = [f.name for f in schema.fields]
        assert "game_pk" in field_names
        assert "game_date" in field_names
        assert "season" in field_names
        assert "game_type" in field_names
        assert "game_state" in field_names
        assert "home_team_id" in field_names
        assert "away_team_id" in field_names
        assert "is_latest" in field_names

        # Verify relationships exist
        assert len(schema.relationships) > 0

    def test_season_seasons_schema_metadata(self):
        """Test season.seasons schema metadata."""
        schema = SCHEMA_METADATA_REGISTRY["season.seasons"]

        assert schema.endpoint == "season"
        assert schema.method == "seasons"
        assert schema.schema_name == "season.seasons"
        assert schema.description == "Season data from Season.seasons()"
        assert len(schema.fields) > 0
        assert "id" in schema.primary_keys
        assert len(schema.partition_keys) == 0  # No partitioning

        # Verify key fields exist
        field_names = [f.name for f in schema.fields]
        assert "sport_id" in field_names
        assert "season_id" in field_names

    def test_all_schemas_have_required_fields(self):
        """Test that all schemas have core required fields."""
        required_fields = [
            "id",
            "request_params",
            "source_url",
            "captured_at",
            "schema_version",
            "response_status",
            "data",
        ]

        for schema_name, schema in SCHEMA_METADATA_REGISTRY.items():
            field_names = [f.name for f in schema.fields]

            for required_field in required_fields:
                assert required_field in field_names, (
                    f"Schema {schema_name} missing required field: {required_field}"
                )

    def test_all_schemas_have_metadata_fields(self):
        """Test that all schemas have ingestion metadata fields."""
        metadata_fields = ["ingestion_timestamp", "ingestion_job_id"]

        for schema_name, schema in SCHEMA_METADATA_REGISTRY.items():
            field_names = [f.name for f in schema.fields]

            for metadata_field in metadata_fields:
                assert metadata_field in field_names, (
                    f"Schema {schema_name} missing metadata field: {metadata_field}"
                )

    def test_all_schemas_have_primary_key(self):
        """Test that all schemas have at least one primary key."""
        for schema_name, schema in SCHEMA_METADATA_REGISTRY.items():
            assert len(schema.primary_keys) > 0, f"Schema {schema_name} has no primary keys"

    def test_schemas_with_json_path_extraction(self):
        """Test that schemas have fields with JSONPath extraction."""
        # Check game.live_game_v1 has JSON paths
        game_schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]
        json_path_fields = [f for f in game_schema.fields if f.json_path is not None]

        assert len(json_path_fields) > 0

        # Verify specific extractions
        game_pk_field = next(f for f in game_schema.fields if f.name == "game_pk")
        assert game_pk_field.json_path == "$.gamePk"

        game_date_field = next(f for f in game_schema.fields if f.name == "game_date")
        assert game_date_field.json_path == "$.gameData.datetime.officialDate"

    def test_indexed_fields_configuration(self):
        """Test that critical fields are marked as indexed."""
        game_schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]

        # These fields should be indexed
        indexed_field_names = [f.name for f in game_schema.fields if f.is_indexed]

        assert "captured_at" in indexed_field_names
        assert "game_pk" in indexed_field_names
        assert "game_date" in indexed_field_names
        assert "game_state" in indexed_field_names
        assert "is_latest" in indexed_field_names

    def test_partition_key_fields_marked(self):
        """Test that partition key fields are properly marked."""
        game_schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]

        partition_fields = [f for f in game_schema.fields if f.is_partition_key]

        assert len(partition_fields) > 0
        assert partition_fields[0].name == "game_date"
        assert partition_fields[0].nullable is False

    def test_game_schema_has_relationships(self):
        """Test that game schema defines relationships."""
        game_schema = SCHEMA_METADATA_REGISTRY["game.live_game_v1"]

        assert len(game_schema.relationships) > 0

        # Check specific relationship
        rel = game_schema.relationships[0]
        assert rel.from_schema == "schedule.games"
        assert rel.to_schema == "game.live_game_v1"
        assert rel.relationship_type == RelationshipType.MANY_TO_ONE
        assert rel.from_field == "game_pk"
        assert rel.to_field == "game_pk"

    def test_nullable_constraints(self):
        """Test that critical fields are marked as non-nullable."""
        for schema_name, schema in SCHEMA_METADATA_REGISTRY.items():
            # ID should always be non-nullable
            id_field = next(f for f in schema.fields if f.name == "id")
            assert id_field.nullable is False, f"Schema {schema_name} has nullable id field"

            # Core fields should be non-nullable
            data_field = next(f for f in schema.fields if f.name == "data")
            assert data_field.nullable is False

            captured_at_field = next(f for f in schema.fields if f.name == "captured_at")
            assert captured_at_field.nullable is False
