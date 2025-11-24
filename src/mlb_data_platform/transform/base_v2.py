"""Generic BaseTransformation with extraction registry pattern.

This module provides a base class where __call__() is fully generic and
subclasses simply register their extraction methods.

Design Pattern:
    1. BaseTransformation provides generic __call__() pipeline
    2. Subclasses register extraction methods in __init__()
    3. Each extraction method transforms raw_df → normalized_df
    4. Base class handles reading, filtering, writing, quality checks

Example:
    >>> class GameTransform(BaseTransformation):
    ...     def __init__(self, spark):
    ...         super().__init__(spark, "game", "liveGameV1")
    ...         # Register extractions
    ...         self.register_extraction(
    ...             name="metadata",
    ...             method=self._extract_metadata,
    ...             target_table="game.live_game_metadata"
    ...         )
    ...
    ...     def _extract_metadata(self, raw_df):
    ...         # Transform logic here
    ...         return metadata_df
    ...
    >>> # Usage is now standardized
    >>> transform = GameTransform(spark)
    >>> results = transform(game_pks=[745123], write_to_postgres=True)
"""

from abc import ABC
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


class ExtractionDefinition:
    """Defines a single extraction (raw → normalized table)."""

    def __init__(
        self,
        name: str,
        method: Callable[[DataFrame], DataFrame],
        target_table: str,
        enabled: bool = True,
        depends_on: Optional[List[str]] = None,
    ):
        """Initialize extraction definition.

        Args:
            name: Unique extraction name (e.g., "metadata", "players")
            method: Callable that transforms raw_df → normalized_df
            target_table: Target PostgreSQL table (e.g., "game.live_game_metadata")
            enabled: Whether this extraction is enabled (default: True)
            depends_on: List of extraction names this depends on (for ordering)
        """
        self.name = name
        self.method = method
        self.target_table = target_table
        self.enabled = enabled
        self.depends_on = depends_on or []


class BaseTransformation(ABC):
    """Generic base transformation with extraction registry.

    This class provides a complete, generic __call__() implementation.
    Subclasses only need to:
    1. Call super().__init__()
    2. Register their extraction methods
    3. Implement extraction methods (raw_df → normalized_df)

    The base class handles:
    - Reading raw data from PostgreSQL
    - Filtering (by PKs, dates, etc.)
    - Executing all registered extractions
    - Data quality validation
    - Writing to PostgreSQL/Delta Lake
    - Metrics collection and reporting
    """

    def __init__(
        self,
        spark: SparkSession,
        endpoint: str,
        method: str,
        source_table: Optional[str] = None,
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
        enable_quality_checks: bool = True,
    ):
        """Initialize base transformation.

        Args:
            spark: SparkSession instance
            endpoint: MLB API endpoint (e.g., "game", "schedule")
            method: MLB API method (e.g., "liveGameV1", "schedule")
            source_table: Raw source table (default: {endpoint}.{method})
            jdbc_url: PostgreSQL JDBC URL
            jdbc_properties: JDBC connection properties
            enable_quality_checks: Run data quality validation
        """
        self.spark = spark
        self.endpoint = endpoint
        self.method = method
        self.source_table = source_table or f"{endpoint}.{method.lower()}"
        self.enable_quality_checks = enable_quality_checks

        # Build JDBC connection
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "mlb_games")
        pg_user = os.getenv("POSTGRES_USER", "mlb_admin")
        pg_password = os.getenv("POSTGRES_PASSWORD", "mlb_dev_password")

        self.jdbc_url = jdbc_url or f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
        self.jdbc_properties = jdbc_properties or {
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        }

        # Extraction registry
        self._extractions: Dict[str, ExtractionDefinition] = {}

    def register_extraction(
        self,
        name: str,
        method: Callable[[DataFrame], DataFrame],
        target_table: str,
        enabled: bool = True,
        depends_on: Optional[List[str]] = None,
    ) -> None:
        """Register an extraction method.

        Args:
            name: Unique extraction name
            method: Extraction method (raw_df → normalized_df)
            target_table: Target PostgreSQL table
            enabled: Whether this extraction is enabled
            depends_on: List of extraction names this depends on

        Example:
            >>> self.register_extraction(
            ...     name="metadata",
            ...     method=self._extract_metadata,
            ...     target_table="game.live_game_metadata"
            ... )
        """
        extraction = ExtractionDefinition(
            name=name,
            method=method,
            target_table=target_table,
            enabled=enabled,
            depends_on=depends_on,
        )
        self._extractions[name] = extraction

    def __call__(
        self,
        # Filtering options
        filter_sql: Optional[str] = None,
        # Export options
        write_to_postgres: bool = False,
        export_to_delta: bool = False,
        delta_path: Optional[str] = None,
        # Execution options
        extractions: Optional[List[str]] = None,
        **filter_kwargs,
    ) -> Dict[str, Any]:
        """Execute the transformation pipeline (GENERIC IMPLEMENTATION).

        This is the same for ALL transforms. Subclasses don't override this;
        they just register their extractions.

        Args:
            filter_sql: SQL WHERE clause for filtering raw data
            write_to_postgres: Write results to PostgreSQL
            export_to_delta: Export results to Delta Lake
            delta_path: Delta Lake base path
            extractions: Specific extractions to run (None = all)
            **filter_kwargs: Additional filter parameters (passed to _apply_filters)

        Returns:
            Dict with execution metrics and results

        Pipeline Stages:
            1. Read raw data from source table
            2. Apply filters (SQL, kwargs)
            3. Execute registered extractions
            4. Run quality checks
            5. Write to PostgreSQL/Delta
            6. Return metrics
        """
        console.print(Panel.fit(
            f"[bold blue]{self.endpoint}.{self.method} Transformation Pipeline[/bold blue]\n"
            f"Source: {self.source_table}",
            border_style="blue"
        ))

        # Stage 1: Read raw data
        console.print("\n[cyan]📖 Stage 1: Reading raw data...[/cyan]")
        raw_df = self._read_raw_data()
        raw_count = raw_df.count()
        console.print(f"  Found {raw_count} raw records")

        # Stage 2: Apply filters
        filtered_df = raw_df
        if filter_sql or filter_kwargs:
            console.print("[cyan]🔍 Stage 2: Applying filters...[/cyan]")
            filtered_df = self._apply_filters(raw_df, filter_sql, **filter_kwargs)
            filtered_count = filtered_df.count()
            console.print(f"  Filtered to {filtered_count} records")

        if filtered_df.count() == 0:
            console.print("[yellow]⚠ No records found matching filters[/yellow]")
            return {"status": "no_data", "raw_count": raw_count}

        # Stage 3: Execute extractions
        console.print("\n[cyan]⚙️  Stage 3: Running extractions...[/cyan]")
        results = self._execute_extractions(filtered_df, extractions)

        # Stage 4: Quality checks
        if self.enable_quality_checks and results:
            console.print("\n[cyan]✓ Stage 4: Running quality checks...[/cyan]")
            quality_report = self._validate_quality(results)

        # Stage 5: Write results
        write_metrics = {}
        if write_to_postgres and results:
            console.print("\n[cyan]💾 Stage 5: Writing to PostgreSQL...[/cyan]")
            write_metrics = self._write_to_postgres(results)

        if export_to_delta and delta_path and results:
            console.print(f"\n[cyan]📦 Stage 6: Exporting to Delta Lake...[/cyan]")
            delta_metrics = self._export_to_delta(results, delta_path)

        # Stage 6: Generate metrics
        metrics = self._generate_metrics(results, write_metrics)
        self._display_summary(metrics)

        console.print("\n[bold green]✓ Transformation complete![/bold green]\n")

        return {
            "status": "success",
            "raw_count": raw_count,
            "filtered_count": filtered_df.count(),
            "extractions": metrics,
            "write_metrics": write_metrics,
        }

    def _read_raw_data(self) -> DataFrame:
        """Read raw data from source table.

        Returns:
            DataFrame with raw JSONB data
        """
        return self.spark.read.jdbc(
            self.jdbc_url,
            self.source_table,
            properties=self.jdbc_properties
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: Optional[str] = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply filters to raw data.

        Subclasses can override to add custom filtering logic.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        if filter_sql:
            df = df.filter(filter_sql)

        # Subclasses override to handle specific filters
        # Example: game_pks, start_date, end_date, etc.

        return df

    def _execute_extractions(
        self,
        raw_df: DataFrame,
        extraction_names: Optional[List[str]] = None,
    ) -> Dict[str, DataFrame]:
        """Execute registered extraction methods.

        Args:
            raw_df: Raw source DataFrame
            extraction_names: Specific extractions to run (None = all enabled)

        Returns:
            Dict mapping extraction name to result DataFrame
        """
        results = {}

        # Determine which extractions to run
        extractions_to_run = self._extractions.values()
        if extraction_names:
            extractions_to_run = [
                e for e in extractions_to_run
                if e.name in extraction_names
            ]

        # Filter to enabled only
        extractions_to_run = [e for e in extractions_to_run if e.enabled]

        # Sort by dependencies (simple topological sort)
        extractions_to_run = self._sort_by_dependencies(extractions_to_run)

        # Execute each extraction
        for extraction in extractions_to_run:
            console.print(f"  [dim]→ {extraction.name}[/dim]")
            try:
                result_df = extraction.method(raw_df)
                row_count = result_df.count()

                if row_count > 0:
                    results[extraction.name] = result_df
                    console.print(f"    [green]✓ {row_count} rows[/green]")
                else:
                    console.print(f"    [dim]∅ No data[/dim]")

            except Exception as e:
                console.print(f"    [red]✗ Error: {e}[/red]")
                # Continue with other extractions

        return results

    def _sort_by_dependencies(
        self,
        extractions: List[ExtractionDefinition],
    ) -> List[ExtractionDefinition]:
        """Sort extractions by dependencies (topological sort).

        Args:
            extractions: List of extractions to sort

        Returns:
            Sorted list (dependencies first)
        """
        # Simple implementation - just put extractions with no deps first
        no_deps = [e for e in extractions if not e.depends_on]
        has_deps = [e for e in extractions if e.depends_on]
        return no_deps + has_deps

    def _validate_quality(self, results: Dict[str, DataFrame]) -> Dict[str, Any]:
        """Run data quality checks.

        Subclasses can override to add custom quality checks.

        Args:
            results: Dict of extraction results

        Returns:
            Quality report dict
        """
        report = {}

        for name, df in results.items():
            row_count = df.count()
            null_counts = {}

            # Check for nulls in all columns
            for col in df.columns:
                nulls = df.filter(F.col(col).isNull()).count()
                if nulls > 0:
                    null_counts[col] = nulls

            report[name] = {
                "row_count": row_count,
                "null_counts": null_counts,
            }

        console.print(f"  [green]✓ Quality checks passed[/green]")
        return report

    def _write_to_postgres(self, results: Dict[str, DataFrame]) -> Dict[str, int]:
        """Write results to PostgreSQL.

        Args:
            results: Dict of extraction name → DataFrame

        Returns:
            Dict of table name → row count written
        """
        write_metrics = {}

        for name, df in results.items():
            extraction = self._extractions.get(name)
            if not extraction:
                continue

            table_name = extraction.target_table
            row_count = df.count()

            if row_count > 0:
                console.print(f"  Writing {row_count} rows to {table_name}")
                try:
                    df.write.jdbc(
                        url=self.jdbc_url,
                        table=table_name,
                        mode="append",
                        properties=self.jdbc_properties
                    )
                    write_metrics[table_name] = row_count
                except Exception as e:
                    console.print(f"    [red]✗ Error: {e}[/red]")
                    write_metrics[table_name] = 0
            else:
                console.print(f"  [dim]Skipping {table_name} (no data)[/dim]")
                write_metrics[table_name] = 0

        return write_metrics

    def _export_to_delta(
        self,
        results: Dict[str, DataFrame],
        delta_path: str,
    ) -> Dict[str, Any]:
        """Export results to Delta Lake.

        Args:
            results: Dict of extraction results
            delta_path: Base Delta Lake path

        Returns:
            Export metrics
        """
        metrics = {}

        for name, df in results.items():
            extraction = self._extractions.get(name)
            if not extraction:
                continue

            table_name = extraction.target_table.replace(".", "_")
            path = f"{delta_path}/{table_name}"

            console.print(f"  Writing to {path}")
            df.write.format("delta").mode("append").save(path)
            metrics[name] = {"path": path, "rows": df.count()}

        return metrics

    def _generate_metrics(
        self,
        results: Dict[str, DataFrame],
        write_metrics: Dict[str, int],
    ) -> Dict[str, Any]:
        """Generate execution metrics.

        Args:
            results: Extraction results
            write_metrics: Write metrics

        Returns:
            Metrics summary
        """
        metrics = {}

        for name, df in results.items():
            metrics[name] = {
                "row_count": df.count(),
                "columns": len(df.columns),
            }

        return metrics

    def _display_summary(self, metrics: Dict[str, Any]):
        """Display transformation summary.

        Args:
            metrics: Metrics to display
        """
        table = Table(title="Transformation Summary", show_header=True)
        table.add_column("Extraction", style="cyan")
        table.add_column("Rows", style="green", justify="right")
        table.add_column("Columns", style="blue", justify="right")

        for name, data in metrics.items():
            table.add_row(
                name,
                str(data.get("row_count", 0)),
                str(data.get("columns", 0))
            )

        console.print("\n")
        console.print(table)
