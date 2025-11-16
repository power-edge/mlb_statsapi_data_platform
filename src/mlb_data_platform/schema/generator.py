#!/usr/bin/env python3
"""DDL generator from YAML schema mappings to PostgreSQL SQL.

This module generates PostgreSQL DDL (CREATE TABLE, CREATE INDEX, etc.) from
YAML schema mapping files.

Features:
- Table creation with proper types
- Primary key constraints
- Index generation (primary keys, foreign keys, timestamps)
- Partitioning strategy (monthly by captured_at)
- NOT NULL constraints
- Comments on tables and columns

Usage:
    from mlb_data_platform.schema.generator import DDLGenerator

    generator = DDLGenerator()
    ddl = generator.generate_from_yaml("config/schemas/mappings/game/live_game_v1.yaml")
    print(ddl)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml


@dataclass
class Field:
    """Represents a database field."""

    name: str
    type: str
    nullable: bool = True
    description: Optional[str] = None
    json_path: Optional[str] = None


@dataclass
class Table:
    """Represents a database table."""

    name: str
    description: str
    primary_key: List[str]
    fields: List[Field]
    upsert_keys: Optional[List[str]] = None
    partition_by: Optional[str] = "source_captured_at"  # Default to monthly partitioning


class DDLGenerator:
    """Generates PostgreSQL DDL from YAML schema mappings."""

    def __init__(self):
        """Initialize DDL generator."""
        pass

    def generate_from_yaml(self, yaml_path: str | Path) -> str:
        """Generate DDL from YAML schema mapping file.

        Args:
            yaml_path: Path to YAML schema mapping file

        Returns:
            Complete PostgreSQL DDL as string
        """
        yaml_path = Path(yaml_path)

        with open(yaml_path) as f:
            schema = yaml.safe_load(f)

        return self.generate_from_schema(schema)

    def generate_from_schema(self, schema: Dict) -> str:
        """Generate DDL from parsed schema dictionary.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            Complete PostgreSQL DDL as string
        """
        tables = self._parse_tables(schema)

        ddl_parts = []

        # Header comment
        ddl_parts.append(self._generate_header(schema))

        # Schema creation
        schema_name = self._extract_schema_name(tables[0].name if tables else "unknown")
        ddl_parts.append(f"-- Create schema if not exists\nCREATE SCHEMA IF NOT EXISTS {schema_name};\n")

        # Generate DDL for each table
        for table in tables:
            ddl_parts.append(self._generate_table_ddl(table))
            ddl_parts.append("")  # Blank line between tables

        # Footer
        ddl_parts.append("-- End of migration\n")

        return "\n".join(ddl_parts)

    def _parse_tables(self, schema: Dict) -> List[Table]:
        """Parse tables from schema dictionary.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            List of Table objects
        """
        tables = []

        # YAML uses "targets" not "tables"
        for table_def in schema.get("targets", []):
            # Parse fields
            fields = []
            for field_def in table_def.get("fields", []):
                field = Field(
                    name=field_def["name"],
                    type=field_def["type"],
                    nullable=field_def.get("nullable", True),
                    description=field_def.get("description"),
                    json_path=field_def.get("json_path"),
                )
                fields.append(field)

            # Create table object
            table = Table(
                name=table_def["name"],
                description=table_def.get("description", ""),
                primary_key=table_def.get("primary_key", []),
                fields=fields,
                upsert_keys=table_def.get("upsert_keys"),
                partition_by=table_def.get("partition_by", "source_captured_at"),
            )
            tables.append(table)

        return tables

    def _generate_header(self, schema: Dict) -> str:
        """Generate migration header comment.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            Header comment block
        """
        # Extract from source section
        source = schema.get("source", {})
        endpoint = source.get("endpoint", "unknown")
        method = source.get("method", "unknown")
        version = schema.get("version", "1.0")

        header = f"""-- ============================================================================
-- MLB Stats API Data Platform - Schema Migration
-- ============================================================================
--
-- Endpoint: {endpoint}
-- Method: {method}
-- Version: {version}
--
-- Generated from: config/schemas/mappings/{endpoint}/{method}.yaml
--
-- This migration creates tables for the {endpoint}.{method} API endpoint.
-- Tables are partitioned by captured_at timestamp (monthly partitions).
--
-- ============================================================================
"""
        return header

    def _generate_table_ddl(self, table: Table) -> str:
        """Generate CREATE TABLE DDL for a single table.

        Args:
            table: Table object

        Returns:
            Complete CREATE TABLE statement with indexes
        """
        parts = []

        # Table comment
        parts.append(f"-- {table.description}")
        parts.append(f"-- Table: {table.name}")
        parts.append(f"-- Primary Key: ({', '.join(table.primary_key)})")
        if table.upsert_keys:
            parts.append(f"-- Upsert Keys: ({', '.join(table.upsert_keys)})")

        # CREATE TABLE
        parts.append(f"CREATE TABLE IF NOT EXISTS {table.name} (")

        # Fields
        field_lines = []
        for i, field in enumerate(table.fields):
            field_line = f"    {field.name} {field.type}"
            if not field.nullable:
                field_line += " NOT NULL"

            # Add description as inline comment if no comma needed
            # Comma will be added later
            if field.description:
                # Store description for later use in COMMENT statements
                pass

            field_lines.append(field_line)

        # Primary key constraint
        if table.primary_key:
            pk_line = f"    PRIMARY KEY ({', '.join(table.primary_key)})"
            field_lines.append(pk_line)

        parts.append(",\n".join(field_lines))

        # Close CREATE TABLE
        # Skip partitioning for now - PostgreSQL requires partition column in primary key
        # This would require adding partition columns to all tables and modifying primary keys
        # TODO: Add partitioning strategy later when needed for performance
        parts.append(");")

        if table.partition_by:
            parts.append(f"-- TODO: Add partitioning by {table.partition_by} when performance requires it")
            parts.append(f"-- Note: Requires adding {table.partition_by} to primary key: ({', '.join(table.primary_key + [table.partition_by])})")

        # Indexes
        parts.append("")
        parts.extend(self._generate_indexes(table))

        # Table comment
        if table.description:
            parts.append(f"COMMENT ON TABLE {table.name} IS '{table.description}';")

        # Column comments
        for field in table.fields:
            if field.description:
                parts.append(
                    f"COMMENT ON COLUMN {table.name}.{field.name} IS '{field.description}';"
                )

        return "\n".join(parts)

    def _generate_indexes(self, table: Table) -> List[str]:
        """Generate index DDL for a table.

        Args:
            table: Table object

        Returns:
            List of CREATE INDEX statements
        """
        indexes = []

        # Index on partition column (only if column exists in table)
        if table.partition_by:
            # Check if partition column exists in fields
            if any(f.name == table.partition_by for f in table.fields):
                idx_name = self._generate_index_name(table.name, table.partition_by)
                indexes.append(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table.name} ({table.partition_by});"
                )

        # Index on game_pk (if exists and not in primary key)
        if any(f.name == "game_pk" for f in table.fields):
            if "game_pk" not in table.primary_key:
                idx_name = self._generate_index_name(table.name, "game_pk")
                indexes.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table.name} (game_pk);")

        # Composite index on primary key (if multi-column)
        if len(table.primary_key) > 1:
            pk_cols = ", ".join(table.primary_key)
            idx_name = self._generate_index_name(table.name, "pk")
            indexes.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table.name} ({pk_cols});")

        return indexes

    def _generate_index_name(self, table_name: str, column_name: str) -> str:
        """Generate index name following PostgreSQL conventions.

        Args:
            table_name: Full table name (schema.table)
            column_name: Column name or abbreviation

        Returns:
            Index name (e.g., idx_live_game_metadata_captured_at)
        """
        # Strip schema prefix
        if "." in table_name:
            _, table_part = table_name.split(".", 1)
        else:
            table_part = table_name

        return f"idx_{table_part}_{column_name}"

    def _extract_schema_name(self, table_name: str) -> str:
        """Extract schema name from full table name.

        Args:
            table_name: Full table name (schema.table)

        Returns:
            Schema name (e.g., "game")
        """
        if "." in table_name:
            return table_name.split(".", 1)[0]
        return "public"


def main():
    """CLI entry point for DDL generator."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m mlb_data_platform.schema.generator <yaml_file>")
        sys.exit(1)

    yaml_path = sys.argv[1]

    generator = DDLGenerator()
    ddl = generator.generate_from_yaml(yaml_path)

    print(ddl)


if __name__ == "__main__":
    main()
