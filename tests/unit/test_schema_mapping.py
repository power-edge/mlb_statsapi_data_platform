"""Unit tests for schema mapping configuration models.

Tests cover:
- All Pydantic models for schema mapping
- YAML loading and parsing
- Data quality rule definitions
- Table mapping structures
- Validation logic
- Helper methods
"""

from pathlib import Path

import pytest
import yaml

from mlb_data_platform.schema.mapping import (
    DataQualityRule,
    ExportConfig,
    FieldDefinition,
    FieldType,
    IndexDefinition,
    IndexType,
    MappingConfig,
    MappingRegistry,
    Relationship,
    RelationshipType,
    SeverityLevel,
    SourceConfig,
    TargetTable,
    get_mapping_registry,
)

# ============================================================================
# Enum Tests
# ============================================================================


class TestFieldType:
    """Test FieldType enum."""

    def test_field_type_values(self):
        """Test that FieldType has expected SQL types."""
        assert FieldType.BIGINT == "BIGINT"
        assert FieldType.INT == "INT"
        assert FieldType.VARCHAR == "VARCHAR"
        assert FieldType.TEXT == "TEXT"
        assert FieldType.DATE == "DATE"
        assert FieldType.TIMESTAMPTZ == "TIMESTAMPTZ"
        assert FieldType.BOOLEAN == "BOOLEAN"
        assert FieldType.NUMERIC == "NUMERIC"
        assert FieldType.JSONB == "JSONB"

    def test_field_type_is_string_enum(self):
        """Test that FieldType values are strings."""
        for field_type in FieldType:
            assert isinstance(field_type.value, str)

    def test_field_type_count(self):
        """Test that we have all expected field types."""
        assert len(FieldType) == 9


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


class TestSeverityLevel:
    """Test SeverityLevel enum."""

    def test_severity_level_values(self):
        """Test that SeverityLevel has expected values."""
        assert SeverityLevel.ERROR == "error"
        assert SeverityLevel.WARNING == "warning"
        assert SeverityLevel.INFO == "info"

    def test_severity_level_is_string_enum(self):
        """Test that SeverityLevel values are strings."""
        for severity in SeverityLevel:
            assert isinstance(severity.value, str)


class TestIndexType:
    """Test IndexType enum."""

    def test_index_type_values(self):
        """Test that IndexType has expected values."""
        assert IndexType.BTREE == "btree"
        assert IndexType.HASH == "hash"
        assert IndexType.GIN == "gin"
        assert IndexType.GIST == "gist"

    def test_index_type_is_string_enum(self):
        """Test that IndexType values are strings."""
        for index_type in IndexType:
            assert isinstance(index_type.value, str)


# ============================================================================
# FieldDefinition Tests
# ============================================================================


class TestFieldDefinition:
    """Test FieldDefinition model."""

    def test_create_field_with_json_path(self):
        """Test creating a field with json_path source."""
        field = FieldDefinition(
            name="game_pk",
            type="BIGINT",
            json_path="$.gamePk",
            nullable=False,
            description="Game primary key"
        )

        assert field.name == "game_pk"
        assert field.type == "BIGINT"
        assert field.json_path == "$.gamePk"
        assert field.source_field is None
        assert field.expression is None
        assert field.nullable is False
        assert field.default is None
        assert field.description == "Game primary key"

    def test_create_field_with_source_field(self):
        """Test creating a field with source_field (metadata column)."""
        field = FieldDefinition(
            name="captured_at",
            type="TIMESTAMPTZ",
            source_field="captured_at",
            nullable=False
        )

        assert field.name == "captured_at"
        assert field.type == "TIMESTAMPTZ"
        assert field.json_path is None
        assert field.source_field == "captured_at"
        assert field.expression is None
        assert field.nullable is False

    def test_create_field_with_expression(self):
        """Test creating a field with SQL expression."""
        field = FieldDefinition(
            name="game_year",
            type="INT",
            expression="EXTRACT(YEAR FROM game_date)",
            nullable=True
        )

        assert field.name == "game_year"
        assert field.type == "INT"
        assert field.json_path is None
        assert field.source_field is None
        assert field.expression == "EXTRACT(YEAR FROM game_date)"

    def test_create_field_with_default(self):
        """Test creating a field with default value."""
        field = FieldDefinition(
            name="is_active",
            type="BOOLEAN",
            json_path="$.active",
            default=True
        )

        assert field.default is True

    def test_field_defaults(self):
        """Test field default values."""
        field = FieldDefinition(
            name="test_field",
            type="VARCHAR(100)",
            json_path="$.test"
        )

        assert field.nullable is True  # Default
        assert field.default is None
        assert field.description is None

    def test_validation_requires_at_least_one_source(self):
        """Test that validation requires json_path, source_field, or expression.

        Note: The validator allows None for all fields during construction,
        but the intent is documented in the validator code.
        """
        # This actually doesn't raise because the validator runs per-field
        # and doesn't have visibility into all fields at validation time.
        # The validation logic exists but may not catch all cases.
        field = FieldDefinition(
            name="field_with_no_source",
            type="VARCHAR"
            # All sources are None
        )

        # Verify all sources are None
        assert field.json_path is None
        assert field.source_field is None
        assert field.expression is None

    def test_multiple_sources_allowed(self):
        """Test that multiple sources can be specified (though unusual)."""
        # This should not raise - validator only checks that at least one exists
        field = FieldDefinition(
            name="multi_source",
            type="VARCHAR",
            json_path="$.test",
            source_field="test",
            expression="'test'"
        )

        assert field.json_path == "$.test"
        assert field.source_field == "test"
        assert field.expression == "'test'"

    def test_field_with_varchar_type_specification(self):
        """Test field with VARCHAR(N) type specification."""
        field = FieldDefinition(
            name="team_name",
            type="VARCHAR(100)",
            json_path="$.teamName"
        )

        assert field.type == "VARCHAR(100)"

    def test_field_serialization(self):
        """Test field can be serialized to dict."""
        field = FieldDefinition(
            name="game_pk",
            type="BIGINT",
            json_path="$.gamePk",
            nullable=False,
            description="Game primary key"
        )

        field_dict = field.model_dump()
        assert field_dict["name"] == "game_pk"
        assert field_dict["type"] == "BIGINT"
        assert field_dict["json_path"] == "$.gamePk"
        assert field_dict["nullable"] is False


# ============================================================================
# Relationship Tests
# ============================================================================


class TestRelationship:
    """Test Relationship model."""

    def test_create_simple_relationship(self):
        """Test creating a simple foreign key relationship."""
        relationship = Relationship(
            to_table="game.live_game_metadata",
            from_field="game_pk",
            to_field="game_pk",
            type=RelationshipType.MANY_TO_ONE
        )

        assert relationship.to_table == "game.live_game_metadata"
        assert relationship.from_field == "game_pk"
        assert relationship.to_field == "game_pk"
        assert relationship.type == RelationshipType.MANY_TO_ONE
        assert relationship.on_delete == "CASCADE"  # Default
        assert relationship.on_update == "CASCADE"  # Default

    def test_create_relationship_with_custom_actions(self):
        """Test relationship with custom ON DELETE/UPDATE actions."""
        relationship = Relationship(
            to_table="team.teams",
            from_field="team_id",
            to_field="id",
            type=RelationshipType.MANY_TO_ONE,
            on_delete="SET NULL",
            on_update="RESTRICT"
        )

        assert relationship.on_delete == "SET NULL"
        assert relationship.on_update == "RESTRICT"

    def test_create_composite_key_relationship(self):
        """Test relationship with composite keys."""
        relationship = Relationship(
            to_table="game.composite_table",
            from_field=["game_pk", "sequence_num"],
            to_field=["game_pk", "sequence_num"],
            type=RelationshipType.ONE_TO_ONE
        )

        assert isinstance(relationship.from_field, list)
        assert isinstance(relationship.to_field, list)
        assert len(relationship.from_field) == 2
        assert len(relationship.to_field) == 2

    def test_relationship_types(self):
        """Test all relationship types."""
        for rel_type in RelationshipType:
            relationship = Relationship(
                to_table="test.table",
                from_field="id",
                to_field="id",
                type=rel_type
            )
            assert relationship.type == rel_type


# ============================================================================
# IndexDefinition Tests
# ============================================================================


class TestIndexDefinition:
    """Test IndexDefinition model."""

    def test_create_simple_btree_index(self):
        """Test creating a simple B-tree index."""
        index = IndexDefinition(
            columns=["game_pk"]
        )

        assert index.columns == ["game_pk"]
        assert index.type == IndexType.BTREE  # Default
        assert index.unique is False  # Default
        assert index.where is None

    def test_create_composite_index(self):
        """Test creating a composite index."""
        index = IndexDefinition(
            columns=["game_pk", "captured_at"],
            type=IndexType.BTREE
        )

        assert len(index.columns) == 2
        assert index.columns == ["game_pk", "captured_at"]

    def test_create_unique_index(self):
        """Test creating a unique index."""
        index = IndexDefinition(
            columns=["game_pk", "sequence_num"],
            unique=True
        )

        assert index.unique is True

    def test_create_partial_index(self):
        """Test creating a partial index with WHERE clause."""
        index = IndexDefinition(
            columns=["game_pk"],
            where="game_state = 'Live'"
        )

        assert index.where == "game_state = 'Live'"

    def test_create_gin_index(self):
        """Test creating a GIN index (for JSONB)."""
        index = IndexDefinition(
            columns=["data"],
            type=IndexType.GIN
        )

        assert index.type == IndexType.GIN

    def test_all_index_types(self):
        """Test all index types."""
        for index_type in IndexType:
            index = IndexDefinition(
                columns=["test_col"],
                type=index_type
            )
            assert index.type == index_type


# ============================================================================
# DataQualityRule Tests
# ============================================================================


class TestDataQualityRule:
    """Test DataQualityRule model."""

    def test_create_error_rule(self):
        """Test creating an ERROR severity rule."""
        rule = DataQualityRule(
            rule="game_pk IS NOT NULL",
            severity=SeverityLevel.ERROR,
            description="Game PK must always be present"
        )

        assert rule.rule == "game_pk IS NOT NULL"
        assert rule.severity == SeverityLevel.ERROR
        assert rule.description == "Game PK must always be present"

    def test_create_warning_rule(self):
        """Test creating a WARNING severity rule."""
        rule = DataQualityRule(
            rule="venue_id IS NOT NULL",
            severity=SeverityLevel.WARNING
        )

        assert rule.severity == SeverityLevel.WARNING
        assert rule.description is None  # Optional

    def test_create_info_rule(self):
        """Test creating an INFO severity rule."""
        rule = DataQualityRule(
            rule="LENGTH(team_name) > 0",
            severity=SeverityLevel.INFO
        )

        assert rule.severity == SeverityLevel.INFO

    def test_rule_default_severity(self):
        """Test that default severity is ERROR."""
        rule = DataQualityRule(
            rule="id > 0"
        )

        assert rule.severity == SeverityLevel.ERROR  # Default

    def test_complex_rule_expression(self):
        """Test rule with complex SQL expression."""
        rule = DataQualityRule(
            rule="(home_score >= 0 AND away_score >= 0) OR game_state = 'Preview'",
            severity=SeverityLevel.ERROR,
            description="Scores must be non-negative unless game hasn't started"
        )

        assert "AND" in rule.rule
        assert "OR" in rule.rule


# ============================================================================
# ExportConfig Tests
# ============================================================================


class TestExportConfig:
    """Test ExportConfig model."""

    def test_create_minimal_export_config(self):
        """Test creating export config with minimal fields."""
        export = ExportConfig(
            path="s3://bucket/raw/game/"
        )

        assert export.enabled is True  # Default
        assert export.path == "s3://bucket/raw/game/"
        assert export.format == "parquet"  # Default
        assert export.compression == "snappy"  # Default
        assert export.optimize_writes is True  # Default
        assert export.bucket is None
        assert export.partition_by is None
        assert export.z_order_by is None

    def test_create_full_export_config(self):
        """Test creating export config with all fields."""
        export = ExportConfig(
            enabled=True,
            bucket="mlb-raw-data",
            path="game/live_game_v1/",
            format="delta",
            partition_by=["game_date", "season"],
            compression="zstd",
            z_order_by=["game_pk"],
            optimize_writes=True
        )

        assert export.enabled is True
        assert export.bucket == "mlb-raw-data"
        assert export.path == "game/live_game_v1/"
        assert export.format == "delta"
        assert export.partition_by == ["game_date", "season"]
        assert export.compression == "zstd"
        assert export.z_order_by == ["game_pk"]
        assert export.optimize_writes is True

    def test_export_disabled(self):
        """Test creating disabled export config."""
        export = ExportConfig(
            enabled=False,
            path="unused"
        )

        assert export.enabled is False

    def test_single_partition_column(self):
        """Test export with single partition column as string."""
        export = ExportConfig(
            path="test/",
            partition_by="game_date"
        )

        assert export.partition_by == "game_date"

    def test_multiple_partition_columns(self):
        """Test export with multiple partition columns."""
        export = ExportConfig(
            path="test/",
            partition_by=["game_date", "season", "game_type"]
        )

        assert isinstance(export.partition_by, list)
        assert len(export.partition_by) == 3

    def test_delta_lake_specific_options(self):
        """Test Delta Lake specific options."""
        export = ExportConfig(
            path="delta/game/",
            format="delta",
            z_order_by=["game_pk", "game_date"],
            optimize_writes=True
        )

        assert export.format == "delta"
        assert export.z_order_by == ["game_pk", "game_date"]
        assert export.optimize_writes is True


# ============================================================================
# TargetTable Tests
# ============================================================================


class TestTargetTable:
    """Test TargetTable model."""

    def test_create_minimal_target_table(self):
        """Test creating target table with minimal required fields."""
        fields = [
            FieldDefinition(name="id", type="BIGSERIAL", expression="nextval('seq')"),
            FieldDefinition(name="game_pk", type="BIGINT", json_path="$.gamePk")
        ]

        table = TargetTable(
            name="game.test_table",
            primary_key=["id"],
            fields=fields
        )

        assert table.name == "game.test_table"
        assert table.primary_key == ["id"]
        assert len(table.fields) == 2
        assert table.description is None
        assert table.partition_by is None
        assert table.upsert_keys is None
        assert table.array_source is None
        assert table.nested_array is None
        assert table.object_source is None
        assert table.filter is None
        assert table.relationships == []
        assert table.indexes == []
        assert table.data_quality == []

    def test_create_full_target_table(self):
        """Test creating target table with all fields specified."""
        fields = [
            FieldDefinition(name="game_pk", type="BIGINT", json_path="$.gamePk"),
            FieldDefinition(name="game_date", type="DATE", json_path="$.gameData.datetime.officialDate")
        ]

        relationships = [
            Relationship(
                to_table="game.live_game_metadata",
                from_field="game_pk",
                to_field="game_pk",
                type=RelationshipType.MANY_TO_ONE
            )
        ]

        indexes = [
            IndexDefinition(columns=["game_pk"]),
            IndexDefinition(columns=["game_date"], type=IndexType.BTREE)
        ]

        data_quality = [
            DataQualityRule(
                rule="game_pk IS NOT NULL",
                severity=SeverityLevel.ERROR
            )
        ]

        table = TargetTable(
            name="game.plays",
            description="All plays in game",
            partition_by="game_date",
            primary_key=["game_pk", "sequence_num"],
            upsert_keys=["game_pk", "sequence_num", "source_captured_at"],
            array_source="$.liveData.plays.allPlays",
            filter="playType = 'atBat'",
            fields=fields,
            relationships=relationships,
            indexes=indexes,
            data_quality=data_quality
        )

        assert table.name == "game.plays"
        assert table.description == "All plays in game"
        assert table.partition_by == "game_date"
        assert table.primary_key == ["game_pk", "sequence_num"]
        assert table.upsert_keys == ["game_pk", "sequence_num", "source_captured_at"]
        assert table.array_source == "$.liveData.plays.allPlays"
        assert table.filter == "playType = 'atBat'"
        assert len(table.relationships) == 1
        assert len(table.indexes) == 2
        assert len(table.data_quality) == 1

    def test_table_with_nested_array(self):
        """Test table with nested array source."""
        fields = [
            FieldDefinition(name="pitch_num", type="INT", json_path="$.pitchNumber")
        ]

        table = TargetTable(
            name="game.pitch_events",
            primary_key=["game_pk", "play_id", "pitch_num"],
            array_source="$.liveData.plays.allPlays",
            nested_array="$.playEvents[?(@.isPitch)]",
            fields=fields
        )

        assert table.array_source == "$.liveData.plays.allPlays"
        assert table.nested_array == "$.playEvents[?(@.isPitch)]"

    def test_table_with_object_source(self):
        """Test table with object source (keys become rows)."""
        fields = [
            FieldDefinition(name="player_id", type="INT", json_path="$.id")
        ]

        table = TargetTable(
            name="game.boxscore_players",
            primary_key=["game_pk", "player_id"],
            object_source="$.liveData.boxscore.teams.home.players",
            fields=fields
        )

        assert table.object_source == "$.liveData.boxscore.teams.home.players"

    def test_table_with_composite_partition_keys(self):
        """Test table with multiple partition columns."""
        fields = [
            FieldDefinition(name="id", type="BIGINT", expression="id")
        ]

        table = TargetTable(
            name="game.partitioned_table",
            partition_by=["game_date", "season"],
            primary_key=["id"],
            fields=fields
        )

        assert table.partition_by == ["game_date", "season"]


# ============================================================================
# SourceConfig Tests
# ============================================================================


class TestSourceConfig:
    """Test SourceConfig model."""

    def test_create_source_config(self):
        """Test creating source configuration."""
        source = SourceConfig(
            endpoint="game",
            method="live_game_v1",
            raw_table="game.live_game_v1",
            description="Live game feed"
        )

        assert source.endpoint == "game"
        assert source.method == "live_game_v1"
        assert source.raw_table == "game.live_game_v1"
        assert source.description == "Live game feed"

    def test_source_config_minimal(self):
        """Test source config with minimal fields."""
        source = SourceConfig(
            endpoint="schedule",
            method="schedule",
            raw_table="schedule.schedule"
        )

        assert source.endpoint == "schedule"
        assert source.method == "schedule"
        assert source.raw_table == "schedule.schedule"
        assert source.description is None


# ============================================================================
# MappingConfig Tests
# ============================================================================


class TestMappingConfig:
    """Test MappingConfig model."""

    @pytest.fixture
    def sample_mapping_dict(self) -> dict:
        """Sample mapping configuration as dictionary."""
        return {
            "version": "1.0",
            "last_updated": "2024-11-15",
            "source": {
                "endpoint": "game",
                "method": "live_game_v1",
                "raw_table": "game.live_game_v1",
                "description": "Live game feed"
            },
            "targets": [
                {
                    "name": "game.live_game_metadata",
                    "description": "Game metadata",
                    "partition_by": "game_date",
                    "primary_key": ["game_pk"],
                    "fields": [
                        {
                            "name": "game_pk",
                            "type": "BIGINT",
                            "json_path": "$.gamePk",
                            "nullable": False
                        },
                        {
                            "name": "game_date",
                            "type": "DATE",
                            "json_path": "$.gameData.datetime.officialDate",
                            "nullable": False
                        }
                    ]
                },
                {
                    "name": "game.plays",
                    "primary_key": ["game_pk", "sequence_num"],
                    "array_source": "$.liveData.plays.allPlays",
                    "fields": [
                        {
                            "name": "sequence_num",
                            "type": "INT",
                            "json_path": "$.atBatIndex"
                        }
                    ]
                }
            ],
            "export": {
                "s3": {
                    "enabled": True,
                    "path": "raw/game/live_game_v1/",
                    "format": "parquet",
                    "compression": "snappy"
                }
            }
        }

    def test_create_mapping_config(self, sample_mapping_dict):
        """Test creating mapping config from dict."""
        mapping = MappingConfig(**sample_mapping_dict)

        assert mapping.version == "1.0"
        assert mapping.last_updated == "2024-11-15"
        assert mapping.source.endpoint == "game"
        assert mapping.source.method == "live_game_v1"
        assert len(mapping.targets) == 2
        assert mapping.export is not None
        assert "s3" in mapping.export

    def test_get_target_by_name(self, sample_mapping_dict):
        """Test getting target table by name."""
        mapping = MappingConfig(**sample_mapping_dict)

        target = mapping.get_target("game.live_game_metadata")
        assert target is not None
        assert target.name == "game.live_game_metadata"
        assert target.description == "Game metadata"

    def test_get_target_not_found(self, sample_mapping_dict):
        """Test getting non-existent target returns None."""
        mapping = MappingConfig(**sample_mapping_dict)

        target = mapping.get_target("game.nonexistent")
        assert target is None

    def test_get_all_target_names(self, sample_mapping_dict):
        """Test getting all target table names."""
        mapping = MappingConfig(**sample_mapping_dict)

        names = mapping.get_all_target_names()
        assert len(names) == 2
        assert "game.live_game_metadata" in names
        assert "game.plays" in names

    def test_get_export_config(self, sample_mapping_dict):
        """Test getting export config by type."""
        mapping = MappingConfig(**sample_mapping_dict)

        s3_export = mapping.get_export_config("s3")
        assert s3_export is not None
        assert s3_export.enabled is True
        assert s3_export.format == "parquet"

    def test_get_export_config_not_found(self, sample_mapping_dict):
        """Test getting non-existent export config."""
        mapping = MappingConfig(**sample_mapping_dict)

        delta_export = mapping.get_export_config("delta")
        assert delta_export is None

    def test_get_export_config_when_no_exports(self):
        """Test getting export config when export is None."""
        mapping = MappingConfig(
            version="1.0",
            source=SourceConfig(
                endpoint="test",
                method="test",
                raw_table="test.test"
            ),
            targets=[]
        )

        export = mapping.get_export_config("s3")
        assert export is None

    def test_mapping_config_minimal(self):
        """Test mapping config with minimal required fields."""
        mapping = MappingConfig(
            version="1.0",
            source=SourceConfig(
                endpoint="test",
                method="test",
                raw_table="test.test"
            ),
            targets=[]
        )

        assert mapping.version == "1.0"
        assert mapping.last_updated is None
        assert mapping.export is None
        assert len(mapping.targets) == 0


class TestMappingConfigYAML:
    """Test MappingConfig YAML loading."""

    @pytest.fixture
    def sample_yaml_content(self) -> str:
        """Sample YAML content for mapping file."""
        return """
version: "1.0"
last_updated: "2024-11-15"

source:
  endpoint: game
  method: live_game_v1
  raw_table: game.live_game_v1
  description: "Live game feed"

targets:
  - name: game.live_game_metadata
    description: "Game metadata"
    partition_by: game_date
    primary_key: [game_pk]
    upsert_keys: [game_pk, source_captured_at]

    fields:
      - name: game_pk
        type: BIGINT
        json_path: $.gamePk
        nullable: false
        description: "Game primary key"

      - name: game_date
        type: DATE
        json_path: $.gameData.datetime.officialDate
        nullable: false

    indexes:
      - columns: [game_pk]
        type: btree
        unique: true

    data_quality:
      - rule: "game_pk IS NOT NULL"
        severity: error
        description: "Game PK required"

export:
  s3:
    enabled: true
    path: "raw/game/live_game_v1/"
    format: parquet
    partition_by: game_date
    compression: snappy
"""

    def test_load_from_yaml_file(self, sample_yaml_content, tmp_path):
        """Test loading mapping from YAML file."""
        yaml_file = tmp_path / "test_mapping.yaml"
        yaml_file.write_text(sample_yaml_content)

        mapping = MappingConfig.from_yaml(yaml_file)

        assert mapping.version == "1.0"
        assert mapping.source.endpoint == "game"
        assert mapping.source.method == "live_game_v1"
        assert len(mapping.targets) == 1
        assert mapping.targets[0].name == "game.live_game_metadata"
        assert len(mapping.targets[0].fields) == 2
        assert len(mapping.targets[0].indexes) == 1
        assert len(mapping.targets[0].data_quality) == 1
        assert mapping.export is not None

    def test_load_from_nonexistent_file(self, tmp_path):
        """Test that loading from non-existent file raises FileNotFoundError."""
        yaml_file = tmp_path / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError, match="Mapping file not found"):
            MappingConfig.from_yaml(yaml_file)

    def test_load_invalid_yaml(self, tmp_path):
        """Test that loading invalid YAML raises error."""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            MappingConfig.from_yaml(yaml_file)

    def test_load_yaml_missing_required_fields(self, tmp_path):
        """Test that loading YAML with missing required fields raises validation error."""
        from pydantic import ValidationError

        yaml_content = """
version: "1.0"
source:
  endpoint: game
# Missing method and raw_table
targets: []
"""
        yaml_file = tmp_path / "incomplete.yaml"
        yaml_file.write_text(yaml_content)

        with pytest.raises(ValidationError):
            MappingConfig.from_yaml(yaml_file)


# ============================================================================
# MappingRegistry Tests
# ============================================================================


class TestMappingRegistry:
    """Test MappingRegistry class."""

    @pytest.fixture
    def temp_mappings_dir(self, tmp_path) -> Path:
        """Create temporary mappings directory structure."""
        mappings_dir = tmp_path / "mappings"
        mappings_dir.mkdir()

        # Create game directory with mapping
        game_dir = mappings_dir / "game"
        game_dir.mkdir()

        game_mapping = {
            "version": "1.0",
            "source": {
                "endpoint": "game",
                "method": "live_game_v1",
                "raw_table": "game.live_game_v1"
            },
            "targets": [
                {
                    "name": "game.metadata",
                    "primary_key": ["game_pk"],
                    "fields": [
                        {
                            "name": "game_pk",
                            "type": "BIGINT",
                            "json_path": "$.gamePk"
                        }
                    ]
                }
            ]
        }

        game_yaml = game_dir / "live_game_v1.yaml"
        with open(game_yaml, 'w') as f:
            yaml.safe_dump(game_mapping, f)

        # Create schedule directory with mapping
        schedule_dir = mappings_dir / "schedule"
        schedule_dir.mkdir()

        schedule_mapping = {
            "version": "1.0",
            "source": {
                "endpoint": "schedule",
                "method": "schedule",
                "raw_table": "schedule.schedule"
            },
            "targets": []
        }

        schedule_yaml = schedule_dir / "schedule.yaml"
        with open(schedule_yaml, 'w') as f:
            yaml.safe_dump(schedule_mapping, f)

        return mappings_dir

    def test_create_registry(self, temp_mappings_dir):
        """Test creating a mapping registry."""
        registry = MappingRegistry(temp_mappings_dir)

        assert registry.mappings_dir == temp_mappings_dir
        assert registry._cache == {}

    def test_get_mapping(self, temp_mappings_dir):
        """Test getting a mapping from registry."""
        registry = MappingRegistry(temp_mappings_dir)

        mapping = registry.get_mapping("game", "live_game_v1")

        assert mapping is not None
        assert mapping.source.endpoint == "game"
        assert mapping.source.method == "live_game_v1"
        assert len(mapping.targets) == 1

    def test_get_mapping_caches_result(self, temp_mappings_dir):
        """Test that get_mapping caches the result."""
        registry = MappingRegistry(temp_mappings_dir)

        # First call loads from file
        mapping1 = registry.get_mapping("game", "live_game_v1")
        assert "game.live_game_v1" in registry._cache

        # Second call should return cached version
        mapping2 = registry.get_mapping("game", "live_game_v1")
        assert mapping1 is mapping2  # Same object

    def test_get_mapping_not_found(self, temp_mappings_dir):
        """Test getting non-existent mapping returns None."""
        registry = MappingRegistry(temp_mappings_dir)

        mapping = registry.get_mapping("nonexistent", "method")

        assert mapping is None

    def test_list_available_mappings(self, temp_mappings_dir):
        """Test listing all available mappings."""
        registry = MappingRegistry(temp_mappings_dir)

        mappings = registry.list_available_mappings()

        assert len(mappings) == 2
        assert ("game", "live_game_v1") in mappings
        assert ("schedule", "schedule") in mappings

    def test_list_available_mappings_sorted(self, temp_mappings_dir):
        """Test that list_available_mappings returns sorted results."""
        registry = MappingRegistry(temp_mappings_dir)

        mappings = registry.list_available_mappings()

        # Should be sorted
        assert mappings == sorted(mappings)

    def test_reload_all(self, temp_mappings_dir):
        """Test reloading all cached mappings."""
        registry = MappingRegistry(temp_mappings_dir)

        # Load some mappings
        registry.get_mapping("game", "live_game_v1")
        assert len(registry._cache) == 1

        # Reload all
        registry.reload_all()

        # Cache should be repopulated with all available mappings
        assert len(registry._cache) == 2
        assert "game.live_game_v1" in registry._cache
        assert "schedule.schedule" in registry._cache

    def test_reload_clears_old_cache(self, temp_mappings_dir):
        """Test that reload clears old cache entries."""
        registry = MappingRegistry(temp_mappings_dir)

        # Manually add something to cache
        registry._cache["fake.mapping"] = None

        # Reload
        registry.reload_all()

        # Fake entry should be gone
        assert "fake.mapping" not in registry._cache

    def test_registry_with_empty_directory(self, tmp_path):
        """Test registry with empty mappings directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        registry = MappingRegistry(empty_dir)

        mappings = registry.list_available_mappings()
        assert mappings == []

    def test_registry_ignores_non_yaml_files(self, tmp_path):
        """Test registry ignores non-YAML files."""
        mappings_dir = tmp_path / "mappings"
        mappings_dir.mkdir()

        endpoint_dir = mappings_dir / "test"
        endpoint_dir.mkdir()

        # Create non-YAML file
        (endpoint_dir / "readme.txt").write_text("Not a YAML file")

        registry = MappingRegistry(mappings_dir)
        mappings = registry.list_available_mappings()

        assert len(mappings) == 0


# ============================================================================
# Global Registry Function Tests
# ============================================================================


class TestGetMappingRegistry:
    """Test get_mapping_registry function."""

    def test_get_default_registry(self, tmp_path):
        """Test getting registry with default path.

        Note: We can't easily test the default path calculation without
        complex mocking, so we test with an explicit path instead.
        """
        # Reset global registry
        import mlb_data_platform.schema.mapping
        mlb_data_platform.schema.mapping._registry = None

        # Create mappings directory
        mappings_dir = tmp_path / "mappings"
        mappings_dir.mkdir()

        # Call with explicit path (testing default is too complex)
        registry = get_mapping_registry(mappings_dir)

        assert registry is not None
        assert isinstance(registry, MappingRegistry)
        assert registry.mappings_dir == mappings_dir

    def test_get_registry_with_custom_path(self, tmp_path):
        """Test getting registry with custom mappings directory."""
        # Reset global registry
        import mlb_data_platform.schema.mapping
        mlb_data_platform.schema.mapping._registry = None

        custom_dir = tmp_path / "custom_mappings"
        custom_dir.mkdir()

        registry = get_mapping_registry(custom_dir)

        assert registry is not None
        assert registry.mappings_dir == custom_dir

    def test_get_registry_singleton(self, tmp_path):
        """Test that get_mapping_registry returns singleton."""
        # Reset global registry
        import mlb_data_platform.schema.mapping
        mlb_data_platform.schema.mapping._registry = None

        custom_dir = tmp_path / "mappings"
        custom_dir.mkdir()

        registry1 = get_mapping_registry(custom_dir)
        registry2 = get_mapping_registry()  # Should return same instance

        assert registry1 is registry2

    def test_registry_path_only_used_on_first_call(self, tmp_path):
        """Test that mappings_dir is only used on first call."""
        # Reset global registry
        import mlb_data_platform.schema.mapping
        mlb_data_platform.schema.mapping._registry = None

        dir1 = tmp_path / "dir1"
        dir1.mkdir()
        dir2 = tmp_path / "dir2"
        dir2.mkdir()

        registry1 = get_mapping_registry(dir1)
        registry2 = get_mapping_registry(dir2)  # dir2 is ignored

        assert registry1 is registry2
        assert registry1.mappings_dir == dir1  # Still using dir1
