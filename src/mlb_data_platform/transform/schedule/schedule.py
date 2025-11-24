"""Schedule.schedule() transformation.

Transforms raw schedule JSONB data into normalized tables.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.schedule import ScheduleTransformation
    >>>
    >>> spark = SparkSession.builder.appName("schedule-transform").getOrCreate()
    >>> transform = ScheduleTransformation(spark)
    >>> metrics = transform(
    ...     start_date="2024-07-01",
    ...     end_date="2024-07-31",
    ...     sport_ids=[1]
    ... )
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
    StructType,
    StructField,
    ArrayType,
)

from rich.console import Console
from rich.table import Table

console = Console()


class ScheduleTransformation:
    """Transform Schedule.schedule() raw data.

    Schedule data contains nested arrays that need to be exploded:
    - data.dates[] - array of dates
    - data.dates[].games[] - array of games for each date

    This transformation creates two normalized tables:
    - schedule.schedule_metadata - top-level schedule info
    - schedule.games - individual games extracted from nested arrays
    """

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
        enable_quality_checks: bool = True,
    ):
        """Initialize Schedule transformation.

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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sport_ids: Optional[List[int]] = None,
        export_to_delta: bool = False,
        delta_path: Optional[str] = None,
        export_to_parquet: bool = False,
        parquet_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute the transformation pipeline.

        Args:
            start_date: Filter schedules >= this date (YYYY-MM-DD)
            end_date: Filter schedules <= this date (YYYY-MM-DD)
            sport_ids: Filter by sport IDs (e.g., [1] for MLB)
            export_to_delta: Export to Delta Lake
            delta_path: Delta Lake path (e.g., "s3://mlb-data/schedule/games")
            export_to_parquet: Export to Parquet
            parquet_path: Parquet path (e.g., "s3://mlb-data/schedule/games")

        Returns:
            Dict of execution metrics

        Examples:
            >>> # Transform all schedule data
            >>> metrics = transform()

            >>> # Filter to July 2024 MLB games
            >>> metrics = transform(
            ...     start_date="2024-07-01",
            ...     end_date="2024-07-31",
            ...     sport_ids=[1]
            ... )

            >>> # Export to Delta Lake
            >>> metrics = transform(
            ...     sport_ids=[1],
            ...     export_to_delta=True,
            ...     delta_path="s3://mlb-data/schedule/games"
            ... )
        """
        console.print("\n[bold blue]Schedule Transformation Pipeline[/bold blue]")
        console.print("=" * 60)

        # Read raw schedule data
        console.print("[cyan]Reading raw schedule data from PostgreSQL...[/cyan]")
        raw_df = self._read_raw_data()

        # Apply filters
        filtered_df = self._apply_filters(raw_df, start_date, end_date, sport_ids)

        # Extract schedule metadata
        console.print("[cyan]Extracting schedule metadata...[/cyan]")
        metadata_df = self._extract_metadata(filtered_df)

        # Extract games from nested arrays
        console.print("[cyan]Extracting games from nested arrays...[/cyan]")
        games_df = self._extract_games(filtered_df)

        # Validate data quality
        if self.enable_quality_checks:
            console.print("[cyan]Running data quality checks...[/cyan]")
            self._validate_quality(metadata_df, games_df)

        # Export if requested
        if export_to_delta:
            console.print(f"[cyan]Exporting to Delta Lake: {delta_path}[/cyan]")
            self._export_delta(games_df, delta_path)

        if export_to_parquet:
            console.print(f"[cyan]Exporting to Parquet: {parquet_path}[/cyan]")
            self._export_parquet(games_df, parquet_path)

        # Calculate metrics
        metrics = self._calculate_metrics(metadata_df, games_df)

        # Display summary
        self._display_summary(metrics)

        console.print("[bold green]✓ Schedule transformation complete![/bold green]\n")

        return metrics

    def _read_raw_data(self) -> DataFrame:
        """Read raw schedule data from PostgreSQL."""
        return self.spark.read.jdbc(
            self.jdbc_url, "schedule.schedule", properties=self.jdbc_properties
        )

    def _apply_filters(
        self,
        df: DataFrame,
        start_date: Optional[str],
        end_date: Optional[str],
        sport_ids: Optional[List[int]],
    ) -> DataFrame:
        """Apply filters to raw data."""
        if start_date:
            df = df.filter(F.col("schedule_date") >= start_date)

        if end_date:
            df = df.filter(F.col("schedule_date") <= end_date)

        if sport_ids:
            df = df.filter(F.col("sport_id").isin(sport_ids))

        return df

    def _extract_metadata(self, raw_df: DataFrame) -> DataFrame:
        """Extract top-level schedule metadata.

        This creates the schedule.schedule_metadata table with aggregated info.

        Note: PostgreSQL JSONB columns are read as STRING via JDBC,
        so we need to parse them with from_json() first.
        """
        # Define schema for top-level metadata
        metadata_schema = StructType([
            StructField("totalGames", IntegerType(), True),
            StructField("totalEvents", IntegerType(), True),
            StructField("totalItems", IntegerType(), True),
            StructField("dates", ArrayType(StructType()), True),  # Minimal schema for counting
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), metadata_schema)
        )

        metadata_df = parsed_df.select(
            F.col("id").alias("raw_id"),
            F.col("sport_id"),
            F.col("schedule_date"),
            F.col("captured_at"),
            F.col("source_url"),
            # Extract top-level fields
            F.col("parsed_data.totalGames").alias("total_games"),
            F.col("parsed_data.totalEvents").alias("total_events"),
            F.col("parsed_data.totalItems").alias("total_items"),
            F.size(F.col("parsed_data.dates")).alias("num_dates"),
        )

        return metadata_df

    def _extract_games(self, raw_df: DataFrame) -> DataFrame:
        """Extract and flatten games from nested arrays.

        The schedule data structure is:
        - data.dates[] - array of dates
        - data.dates[].games[] - array of games for each date

        We need to:
        1. Parse JSONB string to structured data
        2. Explode the dates array
        3. Explode the games array within each date
        4. Extract game fields including nested team/venue data

        Note: PostgreSQL JSONB columns are read as STRING via JDBC.
        """
        # Define comprehensive schema for schedule data
        game_schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameGuid", StringType(), True),
            StructField("gameType", StringType(), True),
            StructField("season", StringType(), True),
            StructField("gameDate", StringType(), True),
            StructField("officialDate", StringType(), True),
            StructField("status", StructType([
                StructField("statusCode", StringType(), True),
                StructField("detailedState", StringType(), True),
                StructField("abstractGameState", StringType(), True),
                StructField("codedGameState", StringType(), True),
            ]), True),
            StructField("teams", StructType([
                StructField("away", StructType([
                    StructField("team", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                    StructField("leagueRecord", StructType([
                        StructField("wins", IntegerType(), True),
                        StructField("losses", IntegerType(), True),
                    ]), True),
                    StructField("score", IntegerType(), True),
                ]), True),
                StructField("home", StructType([
                    StructField("team", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                    StructField("leagueRecord", StructType([
                        StructField("wins", IntegerType(), True),
                        StructField("losses", IntegerType(), True),
                    ]), True),
                    StructField("score", IntegerType(), True),
                ]), True),
            ]), True),
            StructField("venue", StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]), True),
            StructField("doubleHeader", StringType(), True),
            StructField("dayNight", StringType(), True),
            StructField("scheduledInnings", IntegerType(), True),
            StructField("gamesInSeries", IntegerType(), True),
            StructField("seriesGameNumber", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("gameNumber", IntegerType(), True),
            StructField("ifNecessary", StringType(), True),
            StructField("tiebreaker", StringType(), True),
        ])

        schedule_schema = StructType([
            StructField("dates", ArrayType(StructType([
                StructField("date", StringType(), True),
                StructField("games", ArrayType(game_schema), True),
            ])), True)
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), schedule_schema)
        )

        # Explode the dates array
        dates_df = parsed_df.select(
            F.col("id").alias("raw_id"),
            F.col("sport_id"),
            F.col("schedule_date"),
            F.col("captured_at"),
            F.col("source_url"),
            F.explode(F.col("parsed_data.dates")).alias("date_obj"),
        )

        # Explode the games array within each date
        games_exploded_df = dates_df.select(
            F.col("raw_id"),
            F.col("sport_id"),
            F.col("schedule_date"),
            F.col("captured_at"),
            F.col("source_url"),
            F.col("date_obj.date").cast(DateType()).alias("game_date"),
            F.explode(F.col("date_obj.games")).alias("game"),
        )

        # Extract game fields
        games_df = games_exploded_df.select(
            F.col("raw_id"),
            F.col("sport_id"),
            F.col("schedule_date"),
            F.col("captured_at"),
            F.col("source_url"),
            F.col("game_date"),
            # Game identification
            F.col("game.gamePk").cast(IntegerType()).alias("game_pk"),
            F.col("game.gameGuid").cast(StringType()).alias("game_guid"),
            # Game classification
            F.col("game.gameType").cast(StringType()).alias("game_type"),
            F.col("game.season").cast(StringType()).alias("season"),
            # Game timing
            F.col("game.gameDate").cast(TimestampType()).alias("game_datetime"),
            F.col("game.officialDate").cast(DateType()).alias("official_date"),
            # Game status
            F.col("game.status.statusCode").cast(StringType()).alias("status_code"),
            F.col("game.status.detailedState")
            .cast(StringType())
            .alias("detailed_state"),
            F.col("game.status.abstractGameState")
            .cast(StringType())
            .alias("abstract_game_state"),
            F.col("game.status.codedGameState")
            .cast(StringType())
            .alias("coded_game_state"),
            # Teams
            F.col("game.teams.away.team.id").cast(IntegerType()).alias("away_team_id"),
            F.col("game.teams.away.team.name")
            .cast(StringType())
            .alias("away_team_name"),
            F.col("game.teams.away.leagueRecord.wins")
            .cast(IntegerType())
            .alias("away_wins"),
            F.col("game.teams.away.leagueRecord.losses")
            .cast(IntegerType())
            .alias("away_losses"),
            F.col("game.teams.away.score").cast(IntegerType()).alias("away_score"),
            F.col("game.teams.home.team.id").cast(IntegerType()).alias("home_team_id"),
            F.col("game.teams.home.team.name")
            .cast(StringType())
            .alias("home_team_name"),
            F.col("game.teams.home.leagueRecord.wins")
            .cast(IntegerType())
            .alias("home_wins"),
            F.col("game.teams.home.leagueRecord.losses")
            .cast(IntegerType())
            .alias("home_losses"),
            F.col("game.teams.home.score").cast(IntegerType()).alias("home_score"),
            # Venue
            F.col("game.venue.id").cast(IntegerType()).alias("venue_id"),
            F.col("game.venue.name").cast(StringType()).alias("venue_name"),
            # Flags
            F.col("game.doubleHeader").cast(StringType()).alias("double_header"),
            F.col("game.dayNight").cast(StringType()).alias("day_night"),
            F.col("game.scheduledInnings").cast(IntegerType()).alias("scheduled_innings"),
            F.col("game.gamesInSeries").cast(IntegerType()).alias("games_in_series"),
            F.col("game.seriesGameNumber").cast(IntegerType()).alias("series_game_number"),
            # Weather/field conditions
            F.col("game.description").cast(StringType()).alias("description"),
            F.col("game.gameNumber").cast(IntegerType()).alias("game_number"),
            F.col("game.ifNecessary").cast(StringType()).alias("if_necessary"),
            F.col("game.tiebreaker").cast(StringType()).alias("tiebreaker"),
        )

        return games_df

    def _validate_quality(self, metadata_df: DataFrame, games_df: DataFrame):
        """Validate data quality.

        Checks:
        - metadata: total_games >= 0, num_dates > 0
        - games: game_pk is not null, teams present, valid dates
        """
        # Metadata checks
        metadata_count = metadata_df.count()
        console.print(f"[cyan]Metadata records: {metadata_count}[/cyan]")

        invalid_totals = metadata_df.filter(F.col("total_games") < 0).count()
        if invalid_totals > 0:
            console.print(
                f"[yellow]⚠ Warning: {invalid_totals} schedules with negative total_games[/yellow]"
            )

        # Games checks
        games_count = games_df.count()
        console.print(f"[cyan]Games extracted: {games_count}[/cyan]")

        null_game_pks = games_df.filter(F.col("game_pk").isNull()).count()
        if null_game_pks > 0:
            console.print(
                f"[yellow]⚠ Warning: {null_game_pks} games with null game_pk[/yellow]"
            )

        # Check for missing teams
        missing_teams = games_df.filter(
            F.col("home_team_id").isNull() | F.col("away_team_id").isNull()
        ).count()
        if missing_teams > 0:
            console.print(
                f"[yellow]⚠ Warning: {missing_teams} games with missing team data[/yellow]"
            )

        # Check for invalid dates (game_date in the past but > 5 years ago)
        current_date = date.today()
        very_old_games = games_df.filter(
            F.datediff(F.lit(current_date), F.col("game_date")) > 365 * 5
        ).count()
        if very_old_games > 0:
            console.print(
                f"[yellow]⚠ Warning: {very_old_games} games more than 5 years old[/yellow]"
            )

        console.print(
            f"[green]✓ Quality checks passed: {metadata_count} schedules, {games_count} games[/green]"
        )

    def _export_delta(self, df: DataFrame, path: str):
        """Export to Delta Lake."""
        df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).partitionBy("sport_id", "game_date").save(path)

        console.print(f"[green]✓ Exported to Delta Lake: {path}[/green]")

    def _export_parquet(self, df: DataFrame, path: str):
        """Export to Parquet."""
        df.write.format("parquet").mode("overwrite").partitionBy(
            "sport_id", "game_date"
        ).save(path)

        console.print(f"[green]✓ Exported to Parquet: {path}[/green]")

    def _calculate_metrics(
        self, metadata_df: DataFrame, games_df: DataFrame
    ) -> Dict[str, Any]:
        """Calculate execution metrics."""
        total_schedules = metadata_df.count()
        total_games = games_df.count()

        # Games by sport
        sport_counts = (
            games_df.groupBy("sport_id").count().collect() if total_games > 0 else []
        )

        # Games by date
        date_counts = (
            games_df.groupBy("game_date")
            .count()
            .orderBy(F.desc("count"))
            .limit(5)
            .collect()
            if total_games > 0
            else []
        )

        # Games by status
        status_counts = (
            games_df.groupBy("status_code").count().collect() if total_games > 0 else []
        )

        return {
            "total_schedules": total_schedules,
            "total_games": total_games,
            "games_by_sport": {row["sport_id"]: row["count"] for row in sport_counts},
            "top_dates": [
                (str(row["game_date"]), row["count"]) for row in date_counts
            ],
            "games_by_status": {
                row["status_code"]: row["count"] for row in status_counts
            },
        }

    def _display_summary(self, metrics: Dict[str, Any]):
        """Display execution summary."""
        table = Table(title="Schedule Transformation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Schedules", str(metrics["total_schedules"]))
        table.add_row("Total Games", str(metrics["total_games"]))

        for sport_id, count in metrics["games_by_sport"].items():
            table.add_row(f"  Sport {sport_id}", str(count))

        if metrics["top_dates"]:
            table.add_row("", "")
            table.add_row("Top Dates", "")
            for game_date, count in metrics["top_dates"]:
                table.add_row(f"  {game_date}", str(count))

        if metrics["games_by_status"]:
            table.add_row("", "")
            table.add_row("By Status", "")
            for status, count in metrics["games_by_status"].items():
                table.add_row(f"  {status}", str(count))

        console.print(table)
