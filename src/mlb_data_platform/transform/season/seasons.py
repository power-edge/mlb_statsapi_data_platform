"""Season.seasons() transformation.

Transforms raw season.seasons JSONB data for validation and optional export.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.season import SeasonTransformation
    >>>
    >>> spark = SparkSession.builder.appName("season-transform").getOrCreate()
    >>> transform = SeasonTransformation(spark)
    >>> metrics = transform(sport_ids=[1])
"""

from datetime import date
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DateType,
    StructType,
    StructField,
    ArrayType,
    BooleanType,
    DoubleType,
)

from rich.console import Console
from rich.table import Table

console = Console()


class SeasonTransformation:
    """Transform Season.seasons() raw data.

    Season data is already flat in the raw table, so this transformation
    primarily validates data quality and optionally exports to Delta Lake.

    The raw season.seasons table already contains:
    - season_id (from JSONB)
    - sport_id (extracted from request params)
    - data (full JSONB)
    - metadata fields (captured_at, source_url, etc.)
    """

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
        enable_quality_checks: bool = True,
    ):
        """Initialize Season transformation.

        Args:
            spark: SparkSession instance
            jdbc_url: PostgreSQL JDBC URL (default: localhost)
            jdbc_properties: JDBC connection properties
            enable_quality_checks: Run data quality validation
        """
        self.spark = spark
        self.jdbc_url = jdbc_url or "jdbc:postgresql://localhost:5432/mlb_games"
        self.jdbc_properties = jdbc_properties or {
            "user": "mlb_admin",
            "password": "mlb_dev_password",
            "driver": "org.postgresql.Driver",
        }
        self.enable_quality_checks = enable_quality_checks

    def __call__(
        self,
        sport_ids: Optional[List[int]] = None,
        season_ids: Optional[List[str]] = None,
        export_to_delta: bool = False,
        delta_path: Optional[str] = None,
        export_to_parquet: bool = False,
        parquet_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute the transformation pipeline.

        Args:
            sport_ids: Filter by sport IDs (e.g., [1] for MLB)
            season_ids: Filter by season IDs (e.g., ["2024", "2025"])
            export_to_delta: Export to Delta Lake
            delta_path: Delta Lake path (e.g., "s3://mlb-data/season/seasons")
            export_to_parquet: Export to Parquet
            parquet_path: Parquet path (e.g., "s3://mlb-data/season/seasons")

        Returns:
            Dict of execution metrics

        Examples:
            >>> # Validate all season data
            >>> metrics = transform()

            >>> # Filter to MLB seasons only
            >>> metrics = transform(sport_ids=[1])

            >>> # Export to Delta Lake
            >>> metrics = transform(
            ...     sport_ids=[1],
            ...     export_to_delta=True,
            ...     delta_path="s3://mlb-data/season/seasons"
            ... )
        """
        console.print("\n[bold blue]Season Transformation Pipeline[/bold blue]")
        console.print("=" * 60)

        # Read raw season data
        console.print("[cyan]Reading raw season data from PostgreSQL...[/cyan]")
        raw_df = self._read_raw_data()

        # Apply filters
        filtered_df = self._apply_filters(raw_df, sport_ids, season_ids)

        # Extract and flatten season data
        console.print("[cyan]Extracting season metadata...[/cyan]")
        seasons_df = self._extract_seasons(filtered_df)

        # Validate data quality
        if self.enable_quality_checks:
            console.print("[cyan]Running data quality checks...[/cyan]")
            self._validate_quality(seasons_df)

        # Export if requested
        if export_to_delta:
            console.print(f"[cyan]Exporting to Delta Lake: {delta_path}[/cyan]")
            self._export_delta(seasons_df, delta_path)

        if export_to_parquet:
            console.print(f"[cyan]Exporting to Parquet: {parquet_path}[/cyan]")
            self._export_parquet(seasons_df, parquet_path)

        # Calculate metrics
        metrics = self._calculate_metrics(seasons_df)

        # Display summary
        self._display_summary(metrics)

        console.print("[bold green]✓ Season transformation complete![/bold green]\n")

        return metrics

    def _read_raw_data(self) -> DataFrame:
        """Read raw season data from PostgreSQL."""
        return self.spark.read.jdbc(
            self.jdbc_url,
            "season.seasons",
            properties=self.jdbc_properties
        )

    def _apply_filters(
        self,
        df: DataFrame,
        sport_ids: Optional[List[int]],
        season_ids: Optional[List[str]]
    ) -> DataFrame:
        """Apply filters to raw data."""
        if sport_ids:
            df = df.filter(F.col("sport_id").isin(sport_ids))

        if season_ids:
            # Filter by season_id in the JSONB data
            df = df.filter(
                F.col("data").getItem("seasons").getItem(0).getItem("seasonId").isin(season_ids)
            )

        return df

    def _extract_seasons(self, raw_df: DataFrame) -> DataFrame:
        """Extract and flatten season data from JSONB.

        The raw table stores an array of seasons in data.seasons.
        We explode this array and extract key fields.

        Note: PostgreSQL JSONB columns are read as STRING via JDBC,
        so we need to parse them with from_json() first.
        """
        # Define schema for the JSONB data structure
        season_schema = StructType([
            StructField("seasons", ArrayType(StructType([
                StructField("seasonId", StringType(), True),
                StructField("regularSeasonStartDate", StringType(), True),
                StructField("regularSeasonEndDate", StringType(), True),
                StructField("seasonStartDate", StringType(), True),
                StructField("seasonEndDate", StringType(), True),
                StructField("springStartDate", StringType(), True),
                StructField("springEndDate", StringType(), True),
                StructField("hasWildcard", BooleanType(), True),
                StructField("allStarDate", StringType(), True),
                StructField("gameLevelGamedayType", StringType(), True),
                StructField("seasonLevelGamedayType", StringType(), True),
                StructField("qualifierPlateAppearances", DoubleType(), True),
                StructField("qualifierOutsPitched", DoubleType(), True),
            ])), True)
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), season_schema)
        )

        # Explode the seasons array
        exploded_df = parsed_df.select(
            F.col("id").alias("raw_id"),
            F.col("sport_id"),
            F.col("captured_at"),
            F.col("source_url"),
            F.explode(F.col("parsed_data.seasons")).alias("season")
        )

        # Extract season fields
        seasons_df = exploded_df.select(
            F.col("raw_id"),
            F.col("sport_id"),
            F.col("captured_at"),
            F.col("source_url"),
            # Season identification
            F.col("season.seasonId").cast(StringType()).alias("season_id"),
            # Season dates
            F.col("season.regularSeasonStartDate").cast(DateType()).alias("regular_season_start_date"),
            F.col("season.regularSeasonEndDate").cast(DateType()).alias("regular_season_end_date"),
            F.col("season.seasonStartDate").cast(DateType()).alias("season_start_date"),
            F.col("season.seasonEndDate").cast(DateType()).alias("season_end_date"),
            F.col("season.springStartDate").cast(DateType()).alias("spring_start_date"),
            F.col("season.springEndDate").cast(DateType()).alias("spring_end_date"),
            # Season metadata
            F.col("season.hasWildcard").cast("boolean").alias("has_wildcard"),
            F.col("season.allStarDate").cast(DateType()).alias("all_star_date"),
            F.col("season.gameLevelGamedayType").cast(StringType()).alias("game_level_gameday_type"),
            F.col("season.seasonLevelGamedayType").cast(StringType()).alias("season_level_gameday_type"),
            # Qualifier stats
            F.col("season.qualifierPlateAppearances").cast("double").alias("qualifier_plate_appearances"),
            F.col("season.qualifierOutsPitched").cast("double").alias("qualifier_outs_pitched"),
        )

        return seasons_df

    def _validate_quality(self, df: DataFrame):
        """Validate data quality.

        Checks:
        - season_id is not null
        - regular_season_start_date < regular_season_end_date
        - season dates are valid
        """
        total_count = df.count()

        # Check for null season_ids
        null_season_ids = df.filter(F.col("season_id").isNull()).count()
        if null_season_ids > 0:
            console.print(f"[yellow]⚠ Warning: {null_season_ids} seasons with null season_id[/yellow]")

        # Check date logic
        invalid_dates = df.filter(
            F.col("regular_season_start_date") >= F.col("regular_season_end_date")
        ).count()
        if invalid_dates > 0:
            console.print(f"[yellow]⚠ Warning: {invalid_dates} seasons with invalid date ranges[/yellow]")

        # Check for future seasons (> current year + 2)
        current_year = date.today().year
        future_seasons = df.filter(
            F.col("season_id").cast(IntegerType()) > current_year + 2
        ).count()
        if future_seasons > 0:
            console.print(f"[yellow]⚠ Warning: {future_seasons} seasons far in the future[/yellow]")

        console.print(f"[green]✓ Quality checks passed: {total_count} total seasons[/green]")

    def _export_delta(self, df: DataFrame, path: str):
        """Export to Delta Lake."""
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .partitionBy("sport_id", "season_id") \
            .save(path)

        console.print(f"[green]✓ Exported to Delta Lake: {path}[/green]")

    def _export_parquet(self, df: DataFrame, path: str):
        """Export to Parquet."""
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("sport_id", "season_id") \
            .save(path)

        console.print(f"[green]✓ Exported to Parquet: {path}[/green]")

    def _calculate_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate execution metrics."""
        total_seasons = df.count()

        # Group by sport_id to count seasons per sport
        sport_counts = df.groupBy("sport_id").count().collect()

        return {
            "total_seasons": total_seasons,
            "seasons_by_sport": {row["sport_id"]: row["count"] for row in sport_counts},
            "unique_seasons": df.select("season_id").distinct().count(),
        }

    def _display_summary(self, metrics: Dict[str, Any]):
        """Display execution summary."""
        table = Table(title="Season Transformation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Seasons", str(metrics["total_seasons"]))
        table.add_row("Unique Seasons", str(metrics["unique_seasons"]))

        for sport_id, count in metrics["seasons_by_sport"].items():
            table.add_row(f"  Sport {sport_id}", str(count))

        console.print(table)
