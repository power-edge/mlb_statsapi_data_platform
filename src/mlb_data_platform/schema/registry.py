"""Schema registry for managing Avro schemas and relationships."""

import json
from pathlib import Path
from typing import Optional

from .models import SCHEMA_METADATA_REGISTRY, SchemaMetadata


class SchemaRegistry:
    """Central registry for managing schemas and their relationships."""

    def __init__(self, schema_dir: Optional[Path] = None):
        """Initialize schema registry.

        Args:
            schema_dir: Optional directory containing Avro schema files.
                       Defaults to config/schemas/registry/mlb_statsapi/v1/
        """
        if schema_dir is None:
            # Default to project config directory
            schema_dir = Path(__file__).parents[3] / "config" / "schemas" / "registry" / "mlb_statsapi" / "v1"

        self.schema_dir = schema_dir
        self._schemas: dict[str, SchemaMetadata] = SCHEMA_METADATA_REGISTRY.copy()
        self._load_schemas_from_disk()

    def _load_schemas_from_disk(self) -> None:
        """Load Avro schemas from disk if they exist."""
        if not self.schema_dir.exists():
            return

        for avro_file in self.schema_dir.glob("*.avsc"):
            try:
                with open(avro_file) as f:
                    avro_schema = json.load(f)

                # Extract schema name from Avro schema
                schema_name = avro_schema.get("name", avro_file.stem)

                # If we have metadata for this schema, update the Avro schema
                if schema_name in self._schemas:
                    self._schemas[schema_name].avro_schema = avro_schema

            except Exception as e:
                print(f"Warning: Could not load schema from {avro_file}: {e}")

    def get_schema(self, schema_name: str) -> Optional[SchemaMetadata]:
        """Get schema metadata by name.

        Args:
            schema_name: Full schema name (e.g., 'game.live_game_v1')

        Returns:
            SchemaMetadata if found, None otherwise
        """
        return self._schemas.get(schema_name)

    def get_schema_by_endpoint(self, endpoint: str, method: str) -> Optional[SchemaMetadata]:
        """Get schema metadata by endpoint and method.

        Args:
            endpoint: Endpoint name (e.g., 'game')
            method: Method name (e.g., 'liveGameV1')

        Returns:
            SchemaMetadata if found, None otherwise
        """
        # Generate expected schema name
        schema_name = f"{endpoint.lower()}.{self._method_to_table_name(method)}"
        return self.get_schema(schema_name)

    def register_schema(self, schema: SchemaMetadata) -> None:
        """Register a new schema or update existing one.

        Args:
            schema: SchemaMetadata to register
        """
        self._schemas[schema.schema_name] = schema

    def list_schemas(self) -> list[str]:
        """List all registered schema names."""
        return list(self._schemas.keys())

    def get_relationships_for_schema(self, schema_name: str) -> list:
        """Get all relationships involving a schema.

        Args:
            schema_name: Schema name to find relationships for

        Returns:
            List of SchemaRelationship objects
        """
        schema = self.get_schema(schema_name)
        if not schema:
            return []

        relationships = schema.relationships.copy()

        # Also find relationships where this schema is the target
        for other_schema in self._schemas.values():
            for rel in other_schema.relationships:
                if rel.to_schema == schema_name:
                    relationships.append(rel)

        return relationships

    def generate_avro_schema(self, schema_name: str) -> Optional[dict]:
        """Generate Avro schema from metadata.

        Args:
            schema_name: Schema name to generate Avro schema for

        Returns:
            Avro schema as dict, or None if schema not found
        """
        schema = self.get_schema(schema_name)
        if not schema:
            return None

        # If Avro schema already exists, return it
        if schema.avro_schema:
            return schema.avro_schema

        # Generate basic Avro schema from field metadata
        avro_fields = []

        for field in schema.fields:
            avro_field = {
                "name": field.name,
                "type": self._sql_type_to_avro_type(field.type),
            }

            if field.nullable:
                avro_field["type"] = ["null", avro_field["type"]]

            if field.default_value is not None:
                avro_field["default"] = field.default_value

            if field.description:
                avro_field["doc"] = field.description

            avro_fields.append(avro_field)

        avro_schema = {
            "type": "record",
            "name": schema.schema_name.replace(".", "_"),
            "namespace": "com.mlb.statsapi",
            "doc": schema.description or f"Schema for {schema.endpoint}.{schema.method}",
            "fields": avro_fields,
        }

        return avro_schema

    def save_avro_schema(self, schema_name: str, output_path: Optional[Path] = None) -> Path:
        """Save Avro schema to disk.

        Args:
            schema_name: Schema name to save
            output_path: Optional output path. If not provided, uses schema_dir.

        Returns:
            Path where schema was saved

        Raises:
            ValueError: If schema not found
        """
        avro_schema = self.generate_avro_schema(schema_name)
        if not avro_schema:
            raise ValueError(f"Schema not found: {schema_name}")

        if output_path is None:
            self.schema_dir.mkdir(parents=True, exist_ok=True)
            output_path = self.schema_dir / f"{schema_name.replace('.', '_')}.avsc"

        with open(output_path, "w") as f:
            json.dump(avro_schema, f, indent=2)

        return output_path

    @staticmethod
    def _method_to_table_name(method: str) -> str:
        """Convert camelCase method name to snake_case table name.

        Args:
            method: Method name (e.g., 'liveGameV1')

        Returns:
            Snake case table name (e.g., 'live_game_v1')
        """
        import re

        return re.sub(r"(?<!^)(?=[A-Z])", "_", method).lower()

    @staticmethod
    def _sql_type_to_avro_type(sql_type: str) -> str:
        """Convert SQL type to Avro type.

        Args:
            sql_type: SQL type string

        Returns:
            Avro type string
        """
        sql_type_lower = sql_type.lower()

        # Integer types
        if "bigint" in sql_type_lower or "bigserial" in sql_type_lower:
            return "long"
        if "int" in sql_type_lower or "serial" in sql_type_lower:
            return "int"

        # Float types
        if "float" in sql_type_lower or "real" in sql_type_lower:
            return "float"
        if "double" in sql_type_lower or "numeric" in sql_type_lower:
            return "double"

        # Boolean
        if "bool" in sql_type_lower:
            return "boolean"

        # Dates/times (stored as string in Avro, can use logical types)
        if "timestamp" in sql_type_lower or "date" in sql_type_lower:
            return "string"  # Or use Avro logical type

        # JSON
        if "jsonb" in sql_type_lower or "json" in sql_type_lower:
            return "string"  # JSON as string

        # Default to string
        return "string"


# Global schema registry instance
_registry: Optional[SchemaRegistry] = None


def get_registry() -> SchemaRegistry:
    """Get global schema registry instance (singleton)."""
    global _registry
    if _registry is None:
        _registry = SchemaRegistry()
    return _registry
