"""Base transformation class for schema-driven PySpark transformations.

This module provides the abstract base class that all transformations inherit from.
It handles both batch and streaming transformations using the same code logic.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..schema.mapping import (
    FieldDefinition,
    MappingConfig,
    TargetTable,
    get_mapping_registry,
)
from .upsert import defensive_upsert_postgres, UpsertMetrics


class TransformMode(str, Enum):
    """Transformation execution mode."""

    BATCH = "batch"
    STREAMING = "streaming"


class BaseTransformation(ABC):
    """Base class for all PySpark transformations.

    This class provides the core framework for transforming raw JSONB data
    into normalized relational tables. It handles:
    - Reading from source (batch or streaming)
    - JSON path extraction
    - Array/object explosion
    - Schema validation
    - Defensive upserts
    - Export to S3/Delta Lake

    Subclasses implement specific transformation logic.
    """

    def __init__(
        self,
        spark: SparkSession,
        endpoint: str,
        method: str,
        mode: TransformMode = TransformMode.BATCH,
        mappings_dir: Optional[Path] = None,
        enable_defensive_upsert: bool = True,
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
    ):
        """Initialize transformation.

        Args:
            spark: SparkSession instance
            endpoint: MLB API endpoint name
            method: MLB API method name
            mode: Batch or streaming mode
            mappings_dir: Directory containing mapping YAML files
            enable_defensive_upsert: Use timestamp-based defensive upserts (default: True)
            jdbc_url: PostgreSQL JDBC URL (optional, defaults to localhost)
            jdbc_properties: JDBC connection properties (user, password, etc.)
        """
        self.spark = spark
        self.endpoint = endpoint
        self.method = method
        self.mode = mode
        self.enable_defensive_upsert = enable_defensive_upsert
        self.jdbc_url = jdbc_url or "jdbc:postgresql://localhost:5432/mlb_games"
        self.jdbc_properties = jdbc_properties or {
            "user": "mlb_admin",
            "password": "mlb_admin_password",
            "driver": "org.postgresql.Driver",
        }

        # Load mapping configuration
        registry = get_mapping_registry(mappings_dir)
        self.config = registry.get_mapping(endpoint, method)

        if not self.config:
            raise ValueError(
                f"No mapping configuration found for {endpoint}.{method}"
            )

        # Validate configuration
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate mapping configuration."""
        if not self.config.targets:
            raise ValueError("Mapping configuration has no target tables defined")

        for target in self.config.targets:
            if not target.fields:
                raise ValueError(f"Target table {target.name} has no fields defined")

    def read_source(
        self,
        filter_condition: Optional[str] = None,
        partition_filter: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        """Read source data from raw table.

        Args:
            filter_condition: SQL WHERE clause to filter data
            partition_filter: Dict of partition column values

        Returns:
            DataFrame with raw JSONB data
        """
        table_name = self.config.source.raw_table

        if self.mode == TransformMode.STREAMING:
            df = (
                self.spark.readStream.format("postgresql")
                .option("table", table_name)
                .load()
            )
        else:
            df = self.spark.read.format("postgresql").option("table", table_name).load()

        # Apply filters
        if filter_condition:
            df = df.filter(filter_condition)

        if partition_filter:
            for col, value in partition_filter.items():
                df = df.filter(F.col(col) == value)

        return df

    def transform(
        self, source_df: DataFrame, target_tables: Optional[List[str]] = None
    ) -> Dict[str, DataFrame]:
        """Transform source DataFrame into target DataFrames.

        Args:
            source_df: Source DataFrame from read_source()
            target_tables: List of specific target tables to generate
                         (None = all targets)

        Returns:
            Dict mapping table name to DataFrame
        """
        target_dfs = {}

        targets_to_process = self.config.targets
        if target_tables:
            targets_to_process = [
                t for t in self.config.targets if t.name in target_tables
            ]

        for target in targets_to_process:
            df = self._transform_target(source_df, target)
            target_dfs[target.name] = df

        return target_dfs

    def _transform_target(
        self, source_df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Transform source to single target table.

        Args:
            source_df: Source DataFrame
            target: Target table configuration

        Returns:
            Transformed DataFrame
        """
        # Handle different extraction patterns
        if target.array_source:
            df = self._explode_array(source_df, target)
        elif target.object_source:
            df = self._explode_object(source_df, target)
        else:
            df = self._map_fields_direct(source_df, target)

        # Apply filters
        if target.filter:
            df = df.filter(target.filter)

        # Add metadata columns
        df = self._add_metadata_columns(df, target)

        # Validate schema
        df = self._validate_and_cast_schema(df, target)

        return df

    def _map_fields_direct(
        self, source_df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Direct field mapping (no array explosion).

        Args:
            source_df: Source DataFrame
            target: Target table configuration

        Returns:
            DataFrame with extracted fields
        """
        select_exprs = []

        for field in target.fields:
            expr = self._build_field_expression(field, source_df)
            select_exprs.append(expr.alias(field.name))

        return source_df.select(*select_exprs)

    def _explode_array(
        self, source_df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Explode JSON array into rows.

        Args:
            source_df: Source DataFrame
            target: Target table configuration with array_source

        Returns:
            DataFrame with one row per array element
        """
        # Extract array from JSON
        array_path = target.array_source.lstrip("$.").replace(".", ".`")
        array_col = F.get_json_object(F.col("data"), f"$.{array_path}")

        # Parse as JSON array and explode
        df = source_df.withColumn("_array", F.from_json(array_col, "array<string>"))
        df = df.withColumn("_element", F.explode(F.col("_array")))

        # Handle nested arrays
        if target.nested_array:
            nested_path = target.nested_array.lstrip("$.").replace(".", ".`")
            nested_col = F.get_json_object(F.col("_element"), f"$.{nested_path}")
            df = df.withColumn("_nested", F.from_json(nested_col, "array<string>"))
            df = df.withColumn("_element", F.explode(F.col("_nested")))

        # Extract fields from each element
        select_exprs = []
        for field in target.fields:
            if field.json_path and "parent." in field.json_path:
                # Field from parent context (not array element)
                path = field.json_path.replace("parent.", "").lstrip("$.").replace(".", ".`")
                expr = F.get_json_object(F.col("data"), f"$.{path}")
            else:
                # Field from array element
                expr = self._build_field_expression(field, df, json_col="_element")

            select_exprs.append(expr.alias(field.name))

        return df.select(*select_exprs)

    def _explode_object(
        self, source_df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Explode JSON object into rows (keys become row values).

        Args:
            source_df: Source DataFrame
            target: Target table configuration with object_source

        Returns:
            DataFrame with one row per object key
        """
        # Extract object from JSON
        object_path = target.object_source.lstrip("$.").replace(".", ".`")
        object_col = F.get_json_object(F.col("data"), f"$.{object_path}")

        # Parse as map and explode
        df = source_df.withColumn(
            "_map", F.from_json(object_col, "map<string,string>")
        )
        df = df.select("*", F.explode(F.col("_map")).alias("_key", "_value"))

        # Extract fields
        select_exprs = []
        for field in target.fields:
            expr = self._build_field_expression(field, df, json_col="_value")
            select_exprs.append(expr.alias(field.name))

        return df.select(*select_exprs)

    def _build_field_expression(
        self,
        field: FieldDefinition,
        df: DataFrame,
        json_col: str = "data",
    ) -> F.Column:
        """Build Spark column expression for field extraction.

        Args:
            field: Field definition
            df: Source DataFrame
            json_col: Column containing JSON data

        Returns:
            Spark Column expression
        """
        if field.source_field:
            # Field from raw table metadata (not JSON)
            return F.col(field.source_field)

        elif field.expression:
            # SQL expression
            return F.expr(field.expression)

        elif field.json_path:
            # JSON path extraction
            path = field.json_path.lstrip("$.").replace(".", ".`")
            expr = F.get_json_object(F.col(json_col), f"$.{path}")

            # Cast to appropriate type
            expr = self._cast_to_type(expr, field.type)

            # Handle default values
            if field.default is not None:
                expr = F.coalesce(expr, F.lit(field.default))

            # Handle nullability
            if not field.nullable:
                expr = F.when(expr.isNull(), F.lit(field.default)).otherwise(expr)

            return expr

        else:
            raise ValueError(f"Field {field.name} has no source definition")

    def _cast_to_type(self, expr: F.Column, type_str: str) -> F.Column:
        """Cast expression to SQL type.

        Args:
            expr: Expression to cast
            type_str: SQL type string (e.g., "BIGINT", "VARCHAR(100)")

        Returns:
            Casted expression
        """
        type_str_upper = type_str.upper()

        if type_str_upper == "BIGINT":
            return expr.cast(LongType())
        elif type_str_upper == "INT":
            return expr.cast(IntegerType())
        elif type_str_upper.startswith("VARCHAR") or type_str_upper == "TEXT":
            return expr.cast(StringType())
        elif type_str_upper == "DATE":
            return expr.cast(DateType())
        elif type_str_upper == "TIMESTAMPTZ":
            return expr.cast(TimestampType())
        elif type_str_upper == "BOOLEAN":
            return expr.cast(BooleanType())
        elif type_str_upper.startswith("NUMERIC"):
            # Extract precision and scale if specified
            return expr.cast("decimal")
        else:
            return expr

    def _add_metadata_columns(
        self, df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Add standard metadata columns.

        Args:
            df: DataFrame to augment
            target: Target table configuration

        Returns:
            DataFrame with metadata columns added
        """
        # Add transform timestamp if not already present
        if "transform_timestamp" not in df.columns:
            df = df.withColumn("transform_timestamp", F.current_timestamp())

        return df

    def _validate_and_cast_schema(
        self, df: DataFrame, target: TargetTable
    ) -> DataFrame:
        """Validate DataFrame schema matches target definition.

        Args:
            df: DataFrame to validate
            target: Target table configuration

        Returns:
            DataFrame with validated schema

        Raises:
            ValueError: If schema validation fails
        """
        # Build expected schema
        expected_fields = []
        for field in target.fields:
            spark_type = self._sql_type_to_spark(field.type)
            expected_fields.append(StructField(field.name, spark_type, field.nullable))

        expected_schema = StructType(expected_fields)

        # Check if all expected fields are present
        df_columns = set(df.columns)
        expected_columns = set([f.name for f in expected_fields])

        missing = expected_columns - df_columns
        if missing:
            raise ValueError(
                f"Missing columns in {target.name}: {missing}"
            )

        # Select and order columns to match expected schema
        df = df.select(*[f.name for f in expected_fields])

        return df

    def _sql_type_to_spark(self, sql_type: str):
        """Convert SQL type string to Spark DataType.

        Args:
            sql_type: SQL type string

        Returns:
            Spark DataType
        """
        sql_type_upper = sql_type.upper()

        if sql_type_upper == "BIGINT":
            return LongType()
        elif sql_type_upper == "INT":
            return IntegerType()
        elif sql_type_upper.startswith("VARCHAR") or sql_type_upper == "TEXT":
            return StringType()
        elif sql_type_upper == "DATE":
            return DateType()
        elif sql_type_upper == "TIMESTAMPTZ":
            return TimestampType()
        elif sql_type_upper == "BOOLEAN":
            return BooleanType()
        else:
            return StringType()  # Default to string

    def write_targets(
        self,
        target_dfs: Dict[str, DataFrame],
        checkpoint_location: Optional[str] = None,
        mode: str = "append",
    ) -> Dict[str, Any]:
        """Write target DataFrames to storage using defensive upserts.

        Args:
            target_dfs: Dict mapping table name to DataFrame
            checkpoint_location: Checkpoint directory for streaming
            mode: Write mode (append, overwrite, etc.)

        Returns:
            Dict of write metrics including inserted/updated/skipped counts
        """
        metrics = {}

        for table_name, df in target_dfs.items():
            target = self.config.get_target(table_name)

            if self.mode == TransformMode.STREAMING:
                query = self._write_streaming(df, target, checkpoint_location)
                metrics[table_name] = {"stream_query": query, "status": "running"}
            else:
                write_metrics = self._write_batch(df, target, mode)
                metrics[table_name] = {**write_metrics, "status": "completed"}

        return metrics

    def _write_batch(
        self, df: DataFrame, target: TargetTable, mode: str = "append"
    ) -> Dict[str, Any]:
        """Write DataFrame in batch mode using defensive upserts.

        Args:
            df: DataFrame to write
            target: Target table configuration
            mode: Write mode (only used if defensive_upsert is disabled)

        Returns:
            Dict with write metrics (rows written, inserted, updated, etc.)
        """
        if self.enable_defensive_upsert and target.upsert_keys:
            # Use defensive upsert (timestamp-based MERGE)
            metrics = defensive_upsert_postgres(
                spark=self.spark,
                source_df=df,
                target_table=target.name,
                primary_keys=target.primary_key,
                timestamp_column="captured_at",  # Standard timestamp column
                jdbc_url=self.jdbc_url,
                jdbc_properties=self.jdbc_properties,
            )

            return {
                "rows_written": metrics.total_written,
                "inserted_rows": metrics.inserted_rows,
                "updated_rows": metrics.updated_rows,
                "skipped_rows": metrics.skipped_rows,
                "error_rows": metrics.error_rows,
            }
        else:
            # Fallback: regular JDBC write (not defensive)
            df.write.format("jdbc").option("url", self.jdbc_url).option(
                "dbtable", target.name
            ).options(**self.jdbc_properties).mode(mode).save()

            row_count = df.count()
            return {
                "rows_written": row_count,
                "inserted_rows": row_count if mode == "append" else 0,
                "updated_rows": 0,
                "skipped_rows": 0,
                "error_rows": 0,
            }

    def _write_streaming(
        self, df: DataFrame, target: TargetTable, checkpoint_location: str
    ):
        """Write DataFrame in streaming mode.

        Args:
            df: Streaming DataFrame
            target: Target table configuration
            checkpoint_location: Checkpoint directory

        Returns:
            StreamingQuery instance
        """
        query = (
            df.writeStream.foreachBatch(
                lambda batch_df, batch_id: self._write_batch(batch_df, target)
            )
            .option("checkpointLocation", f"{checkpoint_location}/{target.name}")
            .trigger(processingTime="30 seconds")
            .start()
        )

        return query

    def _get_jdbc_url(self) -> str:
        """Get JDBC URL for PostgreSQL connection.

        Returns:
            JDBC URL string
        """
        # TODO: Get from configuration
        return "jdbc:postgresql://localhost:5432/mlb_games"

    @abstractmethod
    def __call__(self, **kwargs) -> Dict[str, Any]:
        """Execute the transformation pipeline.

        This makes transformations callable like functions:
            transform = GameLiveV1Transformation(spark, mode=BATCH)
            metrics = transform(game_pks=[747175, 747176])

        Subclasses implement this to define their specific workflow.

        Args:
            **kwargs: Transformation-specific parameters
                     (e.g., game_pks, start_date, end_date, filters, etc.)

        Returns:
            Dict of execution metrics

        Example:
            >>> transform = GameLiveV1Transformation(spark, mode=BATCH)
            >>> metrics = transform(
            ...     game_pks=[747175],
            ...     validate=True,
            ...     export_to_s3=True
            ... )
            >>> print(f"Processed {metrics['rows_written']} rows")
        """
        pass
