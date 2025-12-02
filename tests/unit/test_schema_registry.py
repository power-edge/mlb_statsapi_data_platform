"""Unit tests for schema registry."""

import json
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from mlb_data_platform.schema.models import (
    FieldMetadata,
    RelationshipType,
    SchemaMetadata,
    SchemaRelationship,
)
from mlb_data_platform.schema.registry import SchemaRegistry, get_registry


class TestSchemaRegistryInitialization:
    """Test SchemaRegistry initialization."""

    def test_init_with_default_schema_dir(self):
        """Test initialization with default schema directory."""
        with patch.object(Path, "exists", return_value=False):
            registry = SchemaRegistry()

            assert registry.schema_dir is not None
            assert isinstance(registry.schema_dir, Path)
            assert str(registry.schema_dir).endswith(
                "config/schemas/registry/mlb_statsapi/v1"
            )

    def test_init_with_custom_schema_dir(self, tmp_path):
        """Test initialization with custom schema directory."""
        custom_dir = tmp_path / "custom_schemas"
        custom_dir.mkdir()

        registry = SchemaRegistry(schema_dir=custom_dir)

        assert registry.schema_dir == custom_dir

    def test_init_loads_predefined_schemas(self):
        """Test that initialization loads predefined schemas from SCHEMA_METADATA_REGISTRY."""
        with patch.object(Path, "exists", return_value=False):
            registry = SchemaRegistry()

            # Should have predefined schemas
            assert "schedule.schedule" in registry._schemas
            assert "game.live_game_v1" in registry._schemas
            assert "season.seasons" in registry._schemas

    def test_init_loads_schemas_from_disk(self, tmp_path):
        """Test that initialization loads Avro schemas from disk."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()

        # Create a mock Avro schema file
        avro_schema = {
            "type": "record",
            "name": "game.live_game_v1",
            "namespace": "com.mlb.statsapi",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "game_pk", "type": "long"},
            ],
        }

        schema_file = schema_dir / "game_live_game_v1.avsc"
        with open(schema_file, "w") as f:
            json.dump(avro_schema, f)

        registry = SchemaRegistry(schema_dir=schema_dir)

        # Should have loaded the Avro schema
        schema = registry.get_schema("game.live_game_v1")
        assert schema is not None
        assert schema.avro_schema == avro_schema

    def test_init_handles_invalid_avro_file(self, tmp_path, capsys):
        """Test that initialization handles invalid Avro schema files gracefully."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()

        # Create invalid JSON file
        invalid_file = schema_dir / "invalid.avsc"
        with open(invalid_file, "w") as f:
            f.write("{ invalid json }")

        registry = SchemaRegistry(schema_dir=schema_dir)

        # Should still initialize (with warning)
        assert registry is not None
        captured = capsys.readouterr()
        assert "Warning: Could not load schema" in captured.out

    def test_init_with_non_existent_schema_dir(self):
        """Test initialization with non-existent schema directory."""
        non_existent = Path("/non/existent/path")

        registry = SchemaRegistry(schema_dir=non_existent)

        # Should initialize without error, just won't load from disk
        assert registry.schema_dir == non_existent
        # Should still have predefined schemas
        assert len(registry._schemas) > 0


class TestSchemaRegistryGetSchema:
    """Test SchemaRegistry.get_schema method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_get_existing_schema(self, registry):
        """Test getting an existing schema."""
        schema = registry.get_schema("game.live_game_v1")

        assert schema is not None
        assert isinstance(schema, SchemaMetadata)
        assert schema.endpoint == "game"
        assert schema.method == "liveGameV1"
        assert schema.schema_name == "game.live_game_v1"

    def test_get_non_existent_schema(self, registry):
        """Test getting a non-existent schema returns None."""
        schema = registry.get_schema("non.existent_schema")

        assert schema is None

    def test_get_schema_case_sensitive(self, registry):
        """Test that schema names are case-sensitive."""
        schema_lower = registry.get_schema("game.live_game_v1")
        schema_upper = registry.get_schema("GAME.LIVE_GAME_V1")

        assert schema_lower is not None
        assert schema_upper is None  # Different case, not found


class TestSchemaRegistryGetSchemaByEndpoint:
    """Test SchemaRegistry.get_schema_by_endpoint method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_get_schema_by_endpoint_and_method(self, registry):
        """Test getting schema by endpoint and method."""
        schema = registry.get_schema_by_endpoint("game", "liveGameV1")

        assert schema is not None
        assert schema.endpoint == "game"
        assert schema.method == "liveGameV1"

    def test_get_schema_by_endpoint_camel_case_conversion(self, registry):
        """Test that camelCase method names are converted to snake_case."""
        # The method should convert liveGameV1 -> live_game_v1
        schema = registry.get_schema_by_endpoint("game", "liveGameV1")

        assert schema is not None
        assert schema.schema_name == "game.live_game_v1"

    def test_get_schema_by_endpoint_non_existent(self, registry):
        """Test getting non-existent schema by endpoint returns None."""
        schema = registry.get_schema_by_endpoint("non_existent", "method")

        assert schema is None

    def test_get_schema_by_endpoint_case_insensitive_endpoint(self, registry):
        """Test that endpoint names are lowercased."""
        schema = registry.get_schema_by_endpoint("GAME", "liveGameV1")

        # Endpoint should be lowercased
        assert schema is not None
        assert schema.endpoint == "game"


class TestSchemaRegistryRegisterSchema:
    """Test SchemaRegistry.register_schema method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_register_new_schema(self, registry):
        """Test registering a new schema."""
        new_schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            description="Test schema",
        )

        registry.register_schema(new_schema)

        # Should be retrievable
        retrieved = registry.get_schema("test.test_method")
        assert retrieved is not None
        assert retrieved.endpoint == "test"
        assert retrieved.method == "testMethod"

    def test_register_schema_overwrites_existing(self, registry):
        """Test that registering a schema with same name overwrites the existing one."""
        original_count = len(registry.list_schemas())

        # Register schema with same name as existing
        new_schema = SchemaMetadata(
            endpoint="game",
            method="liveGameV1",
            schema_name="game.live_game_v1",
            description="Updated description",
        )

        registry.register_schema(new_schema)

        # Should have same count (overwrite, not add)
        assert len(registry.list_schemas()) == original_count

        # Should have updated description
        retrieved = registry.get_schema("game.live_game_v1")
        assert retrieved.description == "Updated description"

    def test_register_multiple_schemas(self, registry):
        """Test registering multiple new schemas."""
        schemas = [
            SchemaMetadata(
                endpoint="test1",
                method="method1",
                schema_name="test1.method1",
            ),
            SchemaMetadata(
                endpoint="test2",
                method="method2",
                schema_name="test2.method2",
            ),
        ]

        for schema in schemas:
            registry.register_schema(schema)

        assert registry.get_schema("test1.method1") is not None
        assert registry.get_schema("test2.method2") is not None


class TestSchemaRegistryListSchemas:
    """Test SchemaRegistry.list_schemas method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_list_schemas_returns_all_names(self, registry):
        """Test that list_schemas returns all schema names."""
        schema_names = registry.list_schemas()

        assert isinstance(schema_names, list)
        assert len(schema_names) > 0

        # Should contain predefined schemas
        assert "schedule.schedule" in schema_names
        assert "game.live_game_v1" in schema_names
        assert "season.seasons" in schema_names

    def test_list_schemas_includes_registered(self, registry):
        """Test that list_schemas includes newly registered schemas."""
        new_schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
        )

        registry.register_schema(new_schema)

        schema_names = registry.list_schemas()
        assert "test.test_method" in schema_names

    def test_list_schemas_returns_copy(self, registry):
        """Test that list_schemas returns a new list (not reference to internal dict)."""
        schema_names = registry.list_schemas()
        original_length = len(schema_names)

        # Modifying returned list shouldn't affect registry
        schema_names.append("fake.schema")

        # Get fresh list
        new_schema_names = registry.list_schemas()
        assert len(new_schema_names) == original_length


class TestSchemaRegistryGetRelationships:
    """Test SchemaRegistry.get_relationships_for_schema method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_get_relationships_for_schema_with_outgoing(self, registry):
        """Test getting relationships for a schema that has outgoing relationships."""
        relationships = registry.get_relationships_for_schema("game.live_game_v1")

        # game.live_game_v1 has relationships defined
        assert len(relationships) > 0

    def test_get_relationships_for_schema_with_incoming(self, registry):
        """Test getting relationships for a schema that is target of relationships."""
        # Add a schema with relationship pointing to game.live_game_v1
        source_schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            relationships=[
                SchemaRelationship(
                    from_schema="test.test_method",
                    to_schema="game.live_game_v1",
                    relationship_type=RelationshipType.MANY_TO_ONE,
                    from_field="game_pk",
                    to_field="game_pk",
                )
            ],
        )

        registry.register_schema(source_schema)

        # Get relationships for game.live_game_v1 (should include incoming)
        relationships = registry.get_relationships_for_schema("game.live_game_v1")

        # Should find the incoming relationship
        incoming = [r for r in relationships if r.from_schema == "test.test_method"]
        assert len(incoming) > 0

    def test_get_relationships_for_non_existent_schema(self, registry):
        """Test getting relationships for non-existent schema returns empty list."""
        relationships = registry.get_relationships_for_schema("non.existent")

        assert relationships == []

    def test_get_relationships_includes_both_directions(self, registry):
        """Test that get_relationships includes both outgoing and incoming relationships."""
        # Create two schemas with bidirectional relationships
        schema1 = SchemaMetadata(
            endpoint="test1",
            method="method1",
            schema_name="test1.method1",
            relationships=[
                SchemaRelationship(
                    from_schema="test1.method1",
                    to_schema="test2.method2",
                    relationship_type=RelationshipType.ONE_TO_MANY,
                    from_field="id",
                    to_field="test1_id",
                )
            ],
        )

        schema2 = SchemaMetadata(
            endpoint="test2",
            method="method2",
            schema_name="test2.method2",
            relationships=[
                SchemaRelationship(
                    from_schema="test2.method2",
                    to_schema="test1.method1",
                    relationship_type=RelationshipType.MANY_TO_ONE,
                    from_field="test1_id",
                    to_field="id",
                )
            ],
        )

        registry.register_schema(schema1)
        registry.register_schema(schema2)

        # Get relationships for test1.method1
        relationships = registry.get_relationships_for_schema("test1.method1")

        # Should have both outgoing (to test2) and incoming (from test2)
        assert len(relationships) >= 2


class TestSchemaRegistryGenerateAvroSchema:
    """Test SchemaRegistry.generate_avro_schema method."""

    @pytest.fixture
    def registry(self):
        """Create a registry instance for testing."""
        with patch.object(Path, "exists", return_value=False):
            return SchemaRegistry()

    def test_generate_avro_schema_from_fields(self, registry):
        """Test generating Avro schema from field metadata."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            description="Test schema",
            fields=[
                FieldMetadata(
                    name="id",
                    type="bigserial",
                    nullable=False,
                    description="Primary key",
                ),
                FieldMetadata(
                    name="name",
                    type="varchar(100)",
                    nullable=True,
                ),
            ],
        )

        registry.register_schema(schema)

        avro_schema = registry.generate_avro_schema("test.test_method")

        assert avro_schema is not None
        assert avro_schema["type"] == "record"
        assert avro_schema["name"] == "test_test_method"
        assert avro_schema["namespace"] == "com.mlb.statsapi"
        assert avro_schema["doc"] == "Test schema"

        # Check fields
        assert len(avro_schema["fields"]) == 2

        # id field (not nullable)
        id_field = avro_schema["fields"][0]
        assert id_field["name"] == "id"
        assert id_field["type"] == "long"  # bigserial -> long
        assert id_field["doc"] == "Primary key"

        # name field (nullable)
        name_field = avro_schema["fields"][1]
        assert name_field["name"] == "name"
        assert name_field["type"] == ["null", "string"]  # nullable

    def test_generate_avro_schema_returns_existing(self, registry):
        """Test that generate_avro_schema returns existing Avro schema if present."""
        existing_avro = {
            "type": "record",
            "name": "custom_schema",
            "fields": [{"name": "custom_field", "type": "string"}],
        }

        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            avro_schema=existing_avro,
        )

        registry.register_schema(schema)

        avro_schema = registry.generate_avro_schema("test.test_method")

        # Should return the existing schema
        assert avro_schema == existing_avro

    def test_generate_avro_schema_non_existent(self, registry):
        """Test generating Avro schema for non-existent schema returns None."""
        avro_schema = registry.generate_avro_schema("non.existent")

        assert avro_schema is None

    def test_generate_avro_schema_with_default_values(self, registry):
        """Test generating Avro schema with default values."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(
                    name="is_active",
                    type="boolean",
                    nullable=False,
                    default_value=True,
                )
            ],
        )

        registry.register_schema(schema)

        avro_schema = registry.generate_avro_schema("test.test_method")

        # Check default value
        is_active_field = avro_schema["fields"][0]
        assert is_active_field["default"] is True


class TestSchemaRegistrySaveAvroSchema:
    """Test SchemaRegistry.save_avro_schema method."""

    @pytest.fixture
    def registry(self, tmp_path):
        """Create a registry instance for testing."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        return SchemaRegistry(schema_dir=schema_dir)

    def test_save_avro_schema_to_default_location(self, registry):
        """Test saving Avro schema to default location."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint", nullable=False),
            ],
        )

        registry.register_schema(schema)

        output_path = registry.save_avro_schema("test.test_method")

        # Should save to schema_dir
        assert output_path.exists()
        assert output_path.parent == registry.schema_dir
        assert output_path.name == "test_test_method.avsc"

        # Verify content
        with open(output_path) as f:
            saved_schema = json.load(f)

        assert saved_schema["type"] == "record"
        assert saved_schema["name"] == "test_test_method"

    def test_save_avro_schema_to_custom_location(self, registry, tmp_path):
        """Test saving Avro schema to custom location."""
        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint", nullable=False),
            ],
        )

        registry.register_schema(schema)

        custom_path = tmp_path / "custom" / "schema.avsc"
        custom_path.parent.mkdir(parents=True, exist_ok=True)

        output_path = registry.save_avro_schema("test.test_method", custom_path)

        assert output_path == custom_path
        assert output_path.exists()

    def test_save_avro_schema_non_existent_raises(self, registry):
        """Test that saving non-existent schema raises ValueError."""
        with pytest.raises(ValueError, match="Schema not found"):
            registry.save_avro_schema("non.existent")

    def test_save_avro_schema_creates_directory(self, registry, tmp_path):
        """Test that save_avro_schema creates schema directory if needed."""
        # Create registry with non-existent schema_dir
        new_schema_dir = tmp_path / "new_schemas"
        registry.schema_dir = new_schema_dir

        schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
            fields=[
                FieldMetadata(name="id", type="bigint", nullable=False),
            ],
        )

        registry.register_schema(schema)

        output_path = registry.save_avro_schema("test.test_method")

        # Directory should be created
        assert new_schema_dir.exists()
        assert output_path.exists()


class TestSchemaRegistryHelperMethods:
    """Test SchemaRegistry helper methods."""

    def test_method_to_table_name_conversion(self):
        """Test converting camelCase method names to snake_case."""
        assert SchemaRegistry._method_to_table_name("liveGameV1") == "live_game_v1"
        assert SchemaRegistry._method_to_table_name("schedule") == "schedule"
        assert SchemaRegistry._method_to_table_name("seasons") == "seasons"
        assert SchemaRegistry._method_to_table_name("boxscoreDiffPatch") == "boxscore_diff_patch"

    def test_sql_type_to_avro_type_integers(self):
        """Test SQL to Avro type conversion for integers."""
        assert SchemaRegistry._sql_type_to_avro_type("bigint") == "long"
        assert SchemaRegistry._sql_type_to_avro_type("bigserial") == "long"
        assert SchemaRegistry._sql_type_to_avro_type("int") == "int"
        assert SchemaRegistry._sql_type_to_avro_type("integer") == "int"
        assert SchemaRegistry._sql_type_to_avro_type("serial") == "int"

    def test_sql_type_to_avro_type_floats(self):
        """Test SQL to Avro type conversion for floats."""
        assert SchemaRegistry._sql_type_to_avro_type("float") == "float"
        assert SchemaRegistry._sql_type_to_avro_type("real") == "float"
        assert SchemaRegistry._sql_type_to_avro_type("double") == "double"
        assert SchemaRegistry._sql_type_to_avro_type("numeric") == "double"

    def test_sql_type_to_avro_type_boolean(self):
        """Test SQL to Avro type conversion for boolean."""
        assert SchemaRegistry._sql_type_to_avro_type("boolean") == "boolean"
        assert SchemaRegistry._sql_type_to_avro_type("bool") == "boolean"

    def test_sql_type_to_avro_type_temporal(self):
        """Test SQL to Avro type conversion for dates/times."""
        assert SchemaRegistry._sql_type_to_avro_type("timestamp") == "string"
        assert SchemaRegistry._sql_type_to_avro_type("timestamptz") == "string"
        assert SchemaRegistry._sql_type_to_avro_type("date") == "string"

    def test_sql_type_to_avro_type_json(self):
        """Test SQL to Avro type conversion for JSON."""
        assert SchemaRegistry._sql_type_to_avro_type("json") == "string"
        assert SchemaRegistry._sql_type_to_avro_type("jsonb") == "string"

    def test_sql_type_to_avro_type_default(self):
        """Test SQL to Avro type conversion for unknown types defaults to string."""
        assert SchemaRegistry._sql_type_to_avro_type("varchar") == "string"
        assert SchemaRegistry._sql_type_to_avro_type("text") == "string"
        assert SchemaRegistry._sql_type_to_avro_type("unknown_type") == "string"

    def test_sql_type_to_avro_type_case_insensitive(self):
        """Test that SQL type conversion is case-insensitive."""
        assert SchemaRegistry._sql_type_to_avro_type("BIGINT") == "long"
        assert SchemaRegistry._sql_type_to_avro_type("BiGiNt") == "long"


class TestGetRegistrySingleton:
    """Test get_registry singleton function."""

    def test_get_registry_returns_instance(self):
        """Test that get_registry returns a SchemaRegistry instance."""
        # Reset global registry
        import mlb_data_platform.schema.registry as registry_module

        registry_module._registry = None

        registry = get_registry()

        assert isinstance(registry, SchemaRegistry)

    def test_get_registry_returns_same_instance(self):
        """Test that get_registry returns the same instance (singleton)."""
        # Reset global registry
        import mlb_data_platform.schema.registry as registry_module

        registry_module._registry = None

        registry1 = get_registry()
        registry2 = get_registry()

        assert registry1 is registry2

    def test_get_registry_preserves_registered_schemas(self):
        """Test that singleton registry preserves registered schemas."""
        # Reset global registry
        import mlb_data_platform.schema.registry as registry_module

        registry_module._registry = None

        registry1 = get_registry()

        # Register a schema
        test_schema = SchemaMetadata(
            endpoint="test",
            method="testMethod",
            schema_name="test.test_method",
        )
        registry1.register_schema(test_schema)

        # Get registry again
        registry2 = get_registry()

        # Should have the registered schema
        assert registry2.get_schema("test.test_method") is not None
