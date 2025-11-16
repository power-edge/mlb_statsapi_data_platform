"""Schema mapping configuration models.

These models define the structure of YAML files in config/schemas/mappings/
which describe how to transform raw JSONB tables into normalized relational tables.
"""

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator


class FieldType(str, Enum):
    """SQL field types."""

    BIGINT = "BIGINT"
    INT = "INT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    DATE = "DATE"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    BOOLEAN = "BOOLEAN"
    NUMERIC = "NUMERIC"
    JSONB = "JSONB"


class RelationshipType(str, Enum):
    """Relationship types between tables."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class SeverityLevel(str, Enum):
    """Data quality rule severity levels."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class IndexType(str, Enum):
    """Database index types."""

    BTREE = "btree"
    HASH = "hash"
    GIN = "gin"
    GIST = "gist"


class FieldDefinition(BaseModel):
    """Definition of a single field in a target table."""

    name: str = Field(description="Column name in target table")
    type: str = Field(description="SQL data type (e.g., BIGINT, VARCHAR(100))")
    json_path: Optional[str] = Field(
        None, description="JSONPath expression to extract from source data"
    )
    source_field: Optional[str] = Field(
        None, description="Field from raw table metadata (not JSON)"
    )
    expression: Optional[str] = Field(
        None, description="SQL expression to compute value"
    )
    nullable: bool = Field(True, description="Whether field can be NULL")
    default: Any = Field(None, description="Default value if NULL")
    description: Optional[str] = Field(None, description="Field documentation")

    @field_validator("json_path", "source_field", "expression")
    @classmethod
    def at_least_one_source(cls, v, info):
        """Ensure at least one of json_path, source_field, or expression is provided."""
        values = info.data
        if not v and not values.get("source_field") and not values.get("expression"):
            raise ValueError(
                "Must provide at least one of: json_path, source_field, expression"
            )
        return v


class Relationship(BaseModel):
    """Foreign key relationship to another table."""

    to_table: str = Field(description="Target table name (schema.table)")
    from_field: Union[str, List[str]] = Field(
        description="Column(s) in this table that reference target"
    )
    to_field: Union[str, List[str]] = Field(
        description="Column(s) in target table"
    )
    type: RelationshipType = Field(description="Relationship cardinality")
    on_delete: str = Field("CASCADE", description="ON DELETE action")
    on_update: str = Field("CASCADE", description="ON UPDATE action")


class IndexDefinition(BaseModel):
    """Database index definition."""

    columns: List[str] = Field(description="Columns to include in index")
    type: IndexType = Field(IndexType.BTREE, description="Index type")
    unique: bool = Field(False, description="Whether index enforces uniqueness")
    where: Optional[str] = Field(None, description="Partial index WHERE clause")


class DataQualityRule(BaseModel):
    """Data quality validation rule."""

    rule: str = Field(description="SQL expression that must evaluate to true")
    severity: SeverityLevel = Field(
        SeverityLevel.ERROR, description="Severity if rule fails"
    )
    description: Optional[str] = Field(None, description="Rule documentation")


class ExportConfig(BaseModel):
    """Export configuration for S3/Delta Lake."""

    enabled: bool = Field(True, description="Whether to export this table")
    bucket: Optional[str] = Field(None, description="S3 bucket name")
    path: str = Field(description="Path within bucket")
    format: str = Field("parquet", description="File format (parquet, delta, avro)")
    partition_by: Optional[Union[str, List[str]]] = Field(
        None, description="Partition column(s)"
    )
    compression: str = Field("snappy", description="Compression codec")
    z_order_by: Optional[List[str]] = Field(
        None, description="Z-order columns (Delta Lake only)"
    )
    optimize_writes: bool = Field(True, description="Enable Delta Lake optimizations")


class TargetTable(BaseModel):
    """Definition of a target normalized table."""

    name: str = Field(description="Target table name (schema.table)")
    description: Optional[str] = Field(None, description="Table documentation")

    # Partitioning and keys
    partition_by: Optional[Union[str, List[str]]] = Field(
        None, description="Partition column(s)"
    )
    primary_key: List[str] = Field(description="Primary key column(s)")
    upsert_keys: Optional[List[str]] = Field(
        None,
        description="Columns to check for conflicts in upsert (includes timestamp)",
    )

    # Data extraction
    array_source: Optional[str] = Field(
        None, description="JSONPath to array in source data (for array explosion)"
    )
    nested_array: Optional[str] = Field(
        None, description="Nested array path (for multi-level explosion)"
    )
    object_source: Optional[str] = Field(
        None, description="JSONPath to object in source data (keys become rows)"
    )
    filter: Optional[str] = Field(
        None, description="Filter expression for array elements"
    )

    # Schema definition
    fields: List[FieldDefinition] = Field(description="Column definitions")
    relationships: List[Relationship] = Field(
        default_factory=list, description="Foreign key relationships"
    )
    indexes: List[IndexDefinition] = Field(
        default_factory=list, description="Index definitions"
    )
    data_quality: List[DataQualityRule] = Field(
        default_factory=list, description="Data quality rules"
    )


class SourceConfig(BaseModel):
    """Configuration for source raw table."""

    endpoint: str = Field(description="MLB API endpoint name")
    method: str = Field(description="MLB API method name")
    raw_table: str = Field(description="Raw table name (schema.table)")
    description: Optional[str] = Field(None, description="Source documentation")


class MappingConfig(BaseModel):
    """Complete schema mapping configuration.

    This is the top-level model that represents a YAML mapping file.
    """

    version: str = Field(description="Mapping version")
    last_updated: Optional[str] = Field(None, description="Last update date")

    source: SourceConfig = Field(description="Source raw table configuration")
    targets: List[TargetTable] = Field(description="Target normalized tables")

    export: Optional[Dict[str, ExportConfig]] = Field(
        None, description="Export configurations (s3, delta, etc.)"
    )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "MappingConfig":
        """Load mapping configuration from YAML file.

        Args:
            yaml_path: Path to YAML mapping file

        Returns:
            Parsed MappingConfig instance

        Raises:
            FileNotFoundError: If YAML file doesn't exist
            ValueError: If YAML is invalid
        """
        if not yaml_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {yaml_path}")

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        return cls(**data)

    def get_target(self, table_name: str) -> Optional[TargetTable]:
        """Get target table configuration by name.

        Args:
            table_name: Full table name (schema.table)

        Returns:
            TargetTable if found, None otherwise
        """
        for target in self.targets:
            if target.name == table_name:
                return target
        return None

    def get_all_target_names(self) -> List[str]:
        """Get list of all target table names.

        Returns:
            List of table names
        """
        return [target.name for target in self.targets]

    def get_export_config(self, export_type: str) -> Optional[ExportConfig]:
        """Get export configuration by type.

        Args:
            export_type: Export type (s3, delta, etc.)

        Returns:
            ExportConfig if found, None otherwise
        """
        if not self.export:
            return None
        return self.export.get(export_type)


class MappingRegistry:
    """Registry for loading and caching schema mappings."""

    def __init__(self, mappings_dir: Path):
        """Initialize mapping registry.

        Args:
            mappings_dir: Directory containing mapping YAML files
                         (e.g., config/schemas/mappings/)
        """
        self.mappings_dir = mappings_dir
        self._cache: Dict[str, MappingConfig] = {}

    def get_mapping(self, endpoint: str, method: str) -> Optional[MappingConfig]:
        """Get mapping configuration for endpoint/method.

        Args:
            endpoint: MLB API endpoint name
            method: MLB API method name

        Returns:
            MappingConfig if found, None otherwise
        """
        cache_key = f"{endpoint}.{method}"

        # Check cache first
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Try to load from file
        yaml_path = self.mappings_dir / endpoint / f"{method}.yaml"

        if not yaml_path.exists():
            return None

        mapping = MappingConfig.from_yaml(yaml_path)
        self._cache[cache_key] = mapping

        return mapping

    def list_available_mappings(self) -> List[tuple[str, str]]:
        """List all available endpoint/method mappings.

        Returns:
            List of (endpoint, method) tuples
        """
        mappings = []

        for endpoint_dir in self.mappings_dir.iterdir():
            if not endpoint_dir.is_dir():
                continue

            endpoint = endpoint_dir.name

            for yaml_file in endpoint_dir.glob("*.yaml"):
                method = yaml_file.stem
                mappings.append((endpoint, method))

        return sorted(mappings)

    def reload_all(self) -> None:
        """Reload all cached mappings from disk."""
        self._cache.clear()

        for endpoint, method in self.list_available_mappings():
            self.get_mapping(endpoint, method)


# Global registry instance (initialized on first import)
_registry: Optional[MappingRegistry] = None


def get_mapping_registry(mappings_dir: Optional[Path] = None) -> MappingRegistry:
    """Get global mapping registry instance.

    Args:
        mappings_dir: Directory containing mapping YAML files
                     (only used on first call)

    Returns:
        MappingRegistry singleton
    """
    global _registry

    if _registry is None:
        if mappings_dir is None:
            # Default to config/schemas/mappings relative to project root
            from pathlib import Path

            project_root = Path(__file__).parent.parent.parent.parent
            mappings_dir = project_root / "config" / "schemas" / "mappings"

        _registry = MappingRegistry(mappings_dir)

    return _registry
