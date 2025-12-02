"""Unit tests for DDL generator."""

from pathlib import Path

import pytest

from mlb_data_platform.schema.generator import DDLGenerator, Field, Table


class TestDDLGenerator:
    """Test DDL generation from YAML schema mappings."""

    @pytest.fixture
    def generator(self):
        """Create DDL generator instance."""
        return DDLGenerator()

    def test_generate_simple_table(self, generator):
        """Test generating DDL for a simple table."""
        table = Table(
            name="test.simple_table",
            description="A simple test table",
            primary_key=["id"],
            fields=[
                Field(name="id", type="BIGINT", nullable=False, description="Primary key"),
                Field(name="name", type="VARCHAR(100)", nullable=True, description="Name field"),
                Field(name="created_at", type="TIMESTAMPTZ", nullable=False),
            ],
        )

        ddl = generator._generate_table_ddl(table)

        # Check table creation
        assert "CREATE TABLE IF NOT EXISTS test.simple_table" in ddl
        assert "id BIGINT NOT NULL" in ddl
        assert "name VARCHAR(100)" in ddl
        assert "created_at TIMESTAMPTZ NOT NULL" in ddl
        assert "PRIMARY KEY (id)" in ddl

        # Check comments
        assert "COMMENT ON TABLE test.simple_table IS 'A simple test table'" in ddl
        assert "COMMENT ON COLUMN test.simple_table.id IS 'Primary key'" in ddl

    def test_generate_partitioned_table(self, generator):
        """Test generating DDL for a table with partition_by hint.

        Note: Generator outputs TODO comments about partitioning because PostgreSQL
        requires partition columns to be part of the primary key.
        """
        table = Table(
            name="game.live_game_metadata",
            description="Game metadata",
            primary_key=["game_pk"],
            fields=[
                Field(name="game_pk", type="BIGINT", nullable=False),
                Field(name="game_date", type="DATE", nullable=False),
                Field(name="home_team_id", type="INT", nullable=True),
            ],
            partition_by="game_date",
        )

        ddl = generator._generate_table_ddl(table)

        # Check partitioning hint (TODO comment, not inline PARTITION BY)
        assert "TODO: Add partitioning by game_date" in ddl
        assert "Requires adding game_date to primary key" in ddl

        # Check partition index (should still be created for the column)
        assert "CREATE INDEX IF NOT EXISTS idx_live_game_metadata_game_date" in ddl

    def test_generate_table_without_partition_column(self, generator):
        """Test generating DDL for table with partition_by but missing column.

        The generator outputs TODO hints regardless of whether the partition column
        exists in the table (column may be added when partitioning is implemented).
        No index is created for the non-existent column.
        """
        table = Table(
            name="game.test_table",
            description="Test table",
            primary_key=["id"],
            fields=[
                Field(name="id", type="BIGINT", nullable=False),
                Field(name="value", type="VARCHAR(100)", nullable=True),
            ],
            partition_by="missing_column",  # Column doesn't exist in fields
        )

        ddl = generator._generate_table_ddl(table)

        # Should NOT have inline PARTITION BY (never does now)
        assert "PARTITION BY RANGE" not in ddl

        # Should still have TODO hint (generator doesn't validate column existence)
        assert "TODO: Add partitioning by missing_column" in ddl

        # Should NOT have index on missing column (can't index non-existent column)
        assert "idx_test_table_missing_column" not in ddl

    def test_generate_multi_column_primary_key(self, generator):
        """Test generating DDL for table with composite primary key."""
        table = Table(
            name="game.live_game_plays",
            description="Game plays",
            primary_key=["game_pk", "play_id"],
            fields=[
                Field(name="game_pk", type="BIGINT", nullable=False),
                Field(name="play_id", type="VARCHAR(50)", nullable=False),
                Field(name="description", type="TEXT", nullable=True),
            ],
        )

        ddl = generator._generate_table_ddl(table)

        # Check composite primary key
        assert "PRIMARY KEY (game_pk, play_id)" in ddl

        # Check composite index
        assert "CREATE INDEX IF NOT EXISTS idx_live_game_plays_pk" in ddl
        assert "ON game.live_game_plays (game_pk, play_id)" in ddl

    def test_index_generation(self, generator):
        """Test index generation."""
        table = Table(
            name="game.test_table",
            description="Test table",
            primary_key=["id"],
            fields=[
                Field(name="id", type="BIGINT", nullable=False),
                Field(name="game_pk", type="BIGINT", nullable=False),
                Field(name="captured_at", type="TIMESTAMPTZ", nullable=False),
            ],
            partition_by="captured_at",
        )

        indexes = generator._generate_indexes(table)

        # Should have partition index
        assert any("captured_at" in idx for idx in indexes)

        # Should have game_pk index (not in primary key)
        assert any("game_pk" in idx for idx in indexes)

    def test_generate_from_yaml(self, generator):
        """Test generating DDL from YAML file."""
        yaml_path = Path("config/schemas/mappings/game/live_game_v1.yaml")

        if not yaml_path.exists():
            pytest.skip("Schema mapping file not found")

        ddl = generator.generate_from_yaml(yaml_path)

        # Check header
        assert "MLB Stats API Data Platform - Schema Migration" in ddl
        assert "Endpoint: game" in ddl
        assert "Method: live_game_v1" in ddl

        # Check schema creation
        assert "CREATE SCHEMA IF NOT EXISTS game" in ddl

        # Check some expected tables
        assert "CREATE TABLE IF NOT EXISTS game.live_game_metadata" in ddl
        assert "CREATE TABLE IF NOT EXISTS game.live_game_plays" in ddl
        assert "CREATE TABLE IF NOT EXISTS game.live_game_pitch_events" in ddl
        assert "CREATE TABLE IF NOT EXISTS game.live_game_play_actions" in ddl
        assert "CREATE TABLE IF NOT EXISTS game.live_game_fielding_credits" in ddl

    def test_extract_schema_name(self, generator):
        """Test extracting schema name from full table name."""
        assert generator._extract_schema_name("game.live_game_metadata") == "game"
        assert generator._extract_schema_name("schedule.schedule") == "schedule"
        assert generator._extract_schema_name("simple_table") == "public"

    def test_generate_index_name(self, generator):
        """Test index name generation."""
        assert (
            generator._generate_index_name("game.live_game_metadata", "captured_at")
            == "idx_live_game_metadata_captured_at"
        )
        assert (
            generator._generate_index_name("schedule.schedule", "date") == "idx_schedule_date"
        )
        assert generator._generate_index_name("simple_table", "pk") == "idx_simple_table_pk"

    def test_field_nullability(self, generator):
        """Test nullable vs NOT NULL field generation."""
        table = Table(
            name="test.nullable_test",
            description="Test nullable fields",
            primary_key=["id"],
            fields=[
                Field(name="id", type="BIGINT", nullable=False),
                Field(name="required_field", type="VARCHAR(100)", nullable=False),
                Field(name="optional_field", type="VARCHAR(100)", nullable=True),
            ],
        )

        ddl = generator._generate_table_ddl(table)

        # Check NOT NULL constraints
        assert "id BIGINT NOT NULL" in ddl
        assert "required_field VARCHAR(100) NOT NULL" in ddl

        # Optional field should NOT have NOT NULL
        assert "optional_field VARCHAR(100) NOT NULL" not in ddl
        assert "optional_field VARCHAR(100)" in ddl

    def test_upsert_keys_in_comment(self, generator):
        """Test that upsert keys appear in table comments."""
        table = Table(
            name="game.live_game_metadata",
            description="Game metadata",
            primary_key=["game_pk"],
            upsert_keys=["game_pk", "source_captured_at"],
            fields=[
                Field(name="game_pk", type="BIGINT", nullable=False),
                Field(name="source_captured_at", type="TIMESTAMPTZ", nullable=False),
            ],
        )

        ddl = generator._generate_table_ddl(table)

        # Check upsert keys in comment
        assert "Upsert Keys: (game_pk, source_captured_at)" in ddl
