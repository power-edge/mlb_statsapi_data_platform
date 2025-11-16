#!/usr/bin/env python3
"""ORM model generator from YAML schema mappings to SQLModel classes.

This module generates SQLModel (SQLAlchemy 2.0 + Pydantic) ORM classes from
YAML schema mapping files.

Features:
- Type-safe SQLModel classes with Pydantic validation
- Proper type hints (Mapped[T])
- Relationships between tables
- Table-specific schemas (game, schedule, etc.)
- Async support ready
- Compatible with Alembic migrations

Usage:
    from mlb_data_platform.schema.orm_generator import ORMGenerator

    generator = ORMGenerator()
    code = generator.generate_from_yaml("config/schemas/mappings/game/live_game_v1.yaml")
    print(code)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml


# Type mapping from PostgreSQL to Python/SQLModel
PG_TO_PYTHON_TYPES = {
    "BIGINT": "int",
    "INT": "int",
    "INTEGER": "int",
    "SMALLINT": "int",
    "VARCHAR": "str",
    "TEXT": "str",
    "CHAR": "str",
    "DATE": "date",
    "TIMESTAMP": "datetime",
    "TIMESTAMPTZ": "datetime",
    "BOOLEAN": "bool",
    "NUMERIC": "Decimal",
    "DECIMAL": "Decimal",
    "FLOAT": "float",
    "REAL": "float",
    "DOUBLE PRECISION": "float",
    "JSON": "dict",
    "JSONB": "dict",
}

# SQLAlchemy type mapping
PG_TO_SQLALCHEMY_TYPES = {
    "BIGINT": "BigInteger",
    "INT": "Integer",
    "INTEGER": "Integer",
    "SMALLINT": "SmallInteger",
    "VARCHAR": "String",
    "TEXT": "Text",
    "CHAR": "String",
    "DATE": "Date",
    "TIMESTAMP": "DateTime",
    "TIMESTAMPTZ": "DateTime",
    "BOOLEAN": "Boolean",
    "NUMERIC": "Numeric",
    "DECIMAL": "Numeric",
    "FLOAT": "Float",
    "REAL": "Float",
    "DOUBLE PRECISION": "Float",
    "JSON": "JSON",
    "JSONB": "JSONB",
}


@dataclass
class FieldDef:
    """Represents a model field."""

    name: str
    pg_type: str  # PostgreSQL type (e.g., "VARCHAR(100)")
    python_type: str  # Python type (e.g., "str")
    sqlalchemy_type: str  # SQLAlchemy type (e.g., "String(100)")
    nullable: bool = True
    description: Optional[str] = None
    json_path: Optional[str] = None


@dataclass
class ModelDef:
    """Represents an ORM model."""

    name: str  # Python class name
    table_name: str  # Database table name
    schema_name: str  # Database schema (e.g., "game")
    description: str
    primary_key: List[str]
    fields: List[FieldDef]
    upsert_keys: Optional[List[str]] = None
    partition_by: Optional[str] = None


class ORMGenerator:
    """Generates SQLModel ORM classes from YAML schema mappings."""

    def __init__(self):
        """Initialize ORM generator."""
        pass

    def generate_from_yaml(self, yaml_path: str | Path) -> str:
        """Generate ORM models from YAML schema mapping file.

        Args:
            yaml_path: Path to YAML schema mapping file

        Returns:
            Complete Python module code with SQLModel classes
        """
        yaml_path = Path(yaml_path)

        with open(yaml_path) as f:
            schema = yaml.safe_load(f)

        return self.generate_from_schema(schema)

    def generate_from_schema(self, schema: Dict) -> str:
        """Generate ORM models from parsed schema dictionary.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            Complete Python module code
        """
        models = self._parse_models(schema)

        code_parts = []

        # Module header
        code_parts.append(self._generate_header(schema))

        # Imports
        code_parts.append(self._generate_imports())

        # Base class
        code_parts.append(self._generate_base_class())

        # Generate each model
        for model in models:
            code_parts.append(self._generate_model_class(model))
            code_parts.append("")  # Blank line between models

        # Footer
        code_parts.append(self._generate_footer())

        return "\n".join(code_parts)

    def _parse_models(self, schema: Dict) -> List[ModelDef]:
        """Parse models from schema dictionary.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            List of ModelDef objects
        """
        models = []

        for table_def in schema.get("targets", []):
            # Parse fields
            fields = []
            for field_def in table_def.get("fields", []):
                python_type, sqlalchemy_type = self._map_types(field_def["type"])

                field = FieldDef(
                    name=field_def["name"],
                    pg_type=field_def["type"],
                    python_type=python_type,
                    sqlalchemy_type=sqlalchemy_type,
                    nullable=field_def.get("nullable", True),
                    description=field_def.get("description"),
                    json_path=field_def.get("json_path"),
                )
                fields.append(field)

            # Extract schema and table name
            full_table_name = table_def["name"]
            if "." in full_table_name:
                schema_name, table_name = full_table_name.split(".", 1)
            else:
                schema_name = "public"
                table_name = full_table_name

            # Generate Python class name from table name
            class_name = self._table_to_class_name(table_name)

            # Create model object
            model = ModelDef(
                name=class_name,
                table_name=table_name,
                schema_name=schema_name,
                description=table_def.get("description", ""),
                primary_key=table_def.get("primary_key", []),
                fields=fields,
                upsert_keys=table_def.get("upsert_keys"),
                partition_by=table_def.get("partition_by"),
            )
            models.append(model)

        return models

    def _map_types(self, pg_type: str) -> tuple[str, str]:
        """Map PostgreSQL type to Python and SQLAlchemy types.

        Args:
            pg_type: PostgreSQL type (e.g., "VARCHAR(100)")

        Returns:
            Tuple of (python_type, sqlalchemy_type)
        """
        # Extract base type (remove size/precision)
        base_type = pg_type.split("(")[0].strip().upper()

        # Get Python type
        python_type = PG_TO_PYTHON_TYPES.get(base_type, "Any")

        # Get SQLAlchemy type
        sa_base = PG_TO_SQLALCHEMY_TYPES.get(base_type, "String")

        # Preserve size/precision for SQLAlchemy
        if "(" in pg_type:
            # Extract size (e.g., "100" from "VARCHAR(100)")
            size = pg_type.split("(")[1].split(")")[0]
            sqlalchemy_type = f"{sa_base}({size})"
        else:
            sqlalchemy_type = sa_base

        return python_type, sqlalchemy_type

    def _table_to_class_name(self, table_name: str) -> str:
        """Convert table name to Python class name.

        Args:
            table_name: Database table name (e.g., "live_game_metadata")

        Returns:
            Python class name (e.g., "LiveGameMetadata")
        """
        # Split on underscores, capitalize each word, join
        parts = table_name.split("_")
        return "".join(word.capitalize() for word in parts)

    def _generate_header(self, schema: Dict) -> str:
        """Generate module header docstring.

        Args:
            schema: Parsed YAML schema dictionary

        Returns:
            Module docstring
        """
        source = schema.get("source", {})
        endpoint = source.get("endpoint", "unknown")
        method = source.get("method", "unknown")
        version = schema.get("version", "1.0")

        header = f'''"""SQLModel ORM models for {endpoint}.{method} API endpoint.

**Auto-generated from YAML schema mapping**

Endpoint: {endpoint}
Method: {method}
Version: {version}

DO NOT EDIT THIS FILE MANUALLY!
Regenerate using: uv run python -m mlb_data_platform.schema.orm_generator

These models use SQLModel (SQLAlchemy 2.0 + Pydantic) for:
- Type-safe database access
- Pydantic validation
- Async support
- Alembic migrations
"""'''
        return header

    def _generate_imports(self) -> str:
        """Generate import statements.

        Returns:
            Import block
        """
        imports = """
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel
"""
        return imports

    def _generate_base_class(self) -> str:
        """Generate base class for all models.

        Returns:
            Base class definition
        """
        base = '''

class BaseModel(SQLModel):
    """Base model for all ORM classes."""

    class Config:
        """Pydantic config."""

        arbitrary_types_allowed = True
        from_attributes = True
'''
        return base

    def _generate_model_class(self, model: ModelDef) -> str:
        """Generate SQLModel class for a single model.

        Args:
            model: ModelDef object

        Returns:
            Complete class definition
        """
        parts = []

        # Class docstring
        parts.append(f"class {model.name}(BaseModel, table=True):")
        parts.append(f'    """{model.description}"""')
        parts.append("")

        # Table metadata
        parts.append("    __tablename__ = " + f'"{model.table_name}"')
        parts.append(f'    __table_args__ = {{"schema": "{model.schema_name}"}}')
        parts.append("")

        # Fields
        for field in model.fields:
            parts.append(self._generate_field(field, model.primary_key))

        return "\n".join(parts)

    def _generate_field(self, field: FieldDef, primary_keys: List[str]) -> str:
        """Generate field definition.

        Args:
            field: FieldDef object
            primary_keys: List of primary key column names

        Returns:
            Field definition line
        """
        # Determine if primary key
        is_primary = field.name in primary_keys

        # Build type annotation
        if field.nullable and not is_primary:
            type_annotation = f"Optional[{field.python_type}]"
        else:
            type_annotation = field.python_type

        # Build Field() arguments
        field_args = []

        # Add SQLAlchemy type
        field_args.append(f"sa_type={field.sqlalchemy_type}")

        # Add primary_key
        if is_primary:
            field_args.append("primary_key=True")

        # Add nullable
        if not field.nullable:
            field_args.append("nullable=False")

        # Add description
        if field.description:
            # Escape quotes in description
            desc = field.description.replace('"', '\\"')
            field_args.append(f'description="{desc}"')

        # Default for Optional fields
        if field.nullable and not is_primary:
            field_args.append("default=None")

        field_str = f"    {field.name}: {type_annotation} = Field({', '.join(field_args)})"

        # Add inline comment with JSON path if available
        if field.json_path:
            field_str += f"  # {field.json_path}"

        return field_str

    def _generate_footer(self) -> str:
        """Generate module footer.

        Returns:
            Footer comment
        """
        footer = """
# End of auto-generated models
"""
        return footer


def main():
    """CLI entry point for ORM generator."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m mlb_data_platform.schema.orm_generator <yaml_file>")
        sys.exit(1)

    yaml_path = sys.argv[1]

    generator = ORMGenerator()
    code = generator.generate_from_yaml(yaml_path)

    print(code)


if __name__ == "__main__":
    main()
