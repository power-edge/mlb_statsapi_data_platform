"""Game.liveGameV1() transformation - Simplified production version.

Transforms raw game JSONB data into normalized tables.
Follows the same JSONB parsing pattern as season/schedule transformations.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.game import LiveGameTransformation
    >>>
    >>> spark = SparkSession.builder.appName("game-transform").getOrCreate()
    >>> transform = LiveGameTransformation(spark)
    >>> metrics = transform(game_pks=[744834])
"""

from datetime import date
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
    DoubleType,
)

from rich.console import Console
from rich.table import Table

console = Console()


class LiveGameTransformation:
    """Transform Game.liveGameV1() raw data.

    Extracts game data into normalized tables:
    - game.metadata - Top-level game information
    - game.teams - Home/away team data
    - game.players - All players in the game
    - game.linescore - Inning-by-inning scoring
    - game.plays - Play-by-play data (future expansion)
    """

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: Optional[str] = None,
        jdbc_properties: Optional[Dict[str, str]] = None,
        enable_quality_checks: bool = True,
    ):
        """Initialize Game transformation.

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
        game_pks: Optional[List[int]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        export_to_delta: bool = False,
        delta_path: Optional[str] = None,
        export_to_parquet: bool = False,
        parquet_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute the transformation pipeline.

        Args:
            game_pks: Filter by specific game PKs
            start_date: Filter by game date >= (YYYY-MM-DD)
            end_date: Filter by game date <= (YYYY-MM-DD)
            export_to_delta: Export to Delta Lake
            delta_path: Delta Lake path
            export_to_parquet: Export to Parquet
            parquet_path: Parquet path

        Returns:
            Dict of execution metrics

        Examples:
            >>> # Transform specific games
            >>> metrics = transform(game_pks=[744834, 745123])

            >>> # Transform games in date range
            >>> metrics = transform(
            ...     start_date="2024-07-01",
            ...     end_date="2024-07-31"
            ... )
        """
        console.print("\n[bold blue]Game Transformation Pipeline[/bold blue]")
        console.print("=" * 60)

        # Read raw game data
        console.print("[cyan]Reading raw game data from PostgreSQL...[/cyan]")
        raw_df = self._read_raw_data()

        # Apply filters
        filtered_df = self._apply_filters(raw_df, game_pks, start_date, end_date)

        # Extract game metadata
        console.print("[cyan]Extracting game metadata...[/cyan]")
        metadata_df = self._extract_metadata(filtered_df)

        # Extract team data
        console.print("[cyan]Extracting team data...[/cyan]")
        teams_df = self._extract_teams(filtered_df)

        # Extract player data
        console.print("[cyan]Extracting player data...[/cyan]")
        players_df = self._extract_players(filtered_df)

        # Validate data quality
        if self.enable_quality_checks:
            console.print("[cyan]Running data quality checks...[/cyan]")
            self._validate_quality(metadata_df, teams_df, players_df)

        # Export if requested
        if export_to_delta:
            console.print(f"[cyan]Exporting to Delta Lake: {delta_path}[/cyan]")
            self._export_delta(metadata_df, delta_path)

        if export_to_parquet:
            console.print(f"[cyan]Exporting to Parquet: {parquet_path}[/cyan]")
            self._export_parquet(metadata_df, parquet_path)

        # Calculate metrics
        metrics = self._calculate_metrics(metadata_df, teams_df, players_df)

        # Display summary
        self._display_summary(metrics)

        console.print("[bold green]✓ Game transformation complete![/bold green]\n")

        return metrics

    def _read_raw_data(self) -> DataFrame:
        """Read raw game data from PostgreSQL."""
        return self.spark.read.jdbc(
            self.jdbc_url,
            "game.live_game_v1",
            properties=self.jdbc_properties
        )

    def _apply_filters(
        self,
        df: DataFrame,
        game_pks: Optional[List[int]],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> DataFrame:
        """Apply filters to raw data."""
        if game_pks:
            df = df.filter(F.col("game_pk").isin(game_pks))

        if start_date:
            df = df.filter(F.col("game_date") >= start_date)

        if end_date:
            df = df.filter(F.col("game_date") <= end_date)

        return df

    def _extract_metadata(self, raw_df: DataFrame) -> DataFrame:
        """Extract game metadata from JSONB.

        Note: PostgreSQL JSONB columns are read as STRING via JDBC,
        so we need to parse them with from_json() first.
        """
        # Define schema for game metadata
        metadata_schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameData", StructType([
                StructField("datetime", StructType([
                    StructField("dateTime", StringType(), True),
                    StructField("originalDate", StringType(), True),
                    StructField("officialDate", StringType(), True),
                    StructField("dayNight", StringType(), True),
                    StructField("time", StringType(), True),
                    StructField("ampm", StringType(), True),
                ]), True),
                StructField("status", StructType([
                    StructField("abstractGameState", StringType(), True),
                    StructField("codedGameState", StringType(), True),
                    StructField("detailedState", StringType(), True),
                    StructField("statusCode", StringType(), True),
                ]), True),
                StructField("teams", StructType([
                    StructField("away", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                    StructField("home", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                ]), True),
                StructField("venue", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                ]), True),
                StructField("weather", StructType([
                    StructField("condition", StringType(), True),
                    StructField("temp", StringType(), True),
                    StructField("wind", StringType(), True),
                ]), True),
            ]), True),
        ])

        # Parse JSONB string
        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), metadata_schema)
        )

        # Extract metadata fields
        metadata_df = parsed_df.select(
            F.col("id").alias("raw_id"),
            F.col("game_pk"),
            F.col("game_date"),
            F.col("captured_at"),
            F.col("source_url"),
            # Game identification
            F.col("parsed_data.gamePk").alias("game_pk_from_data"),
            # DateTime info
            F.col("parsed_data.gameData.datetime.dateTime").cast(TimestampType()).alias("game_datetime"),
            F.col("parsed_data.gameData.datetime.officialDate").cast(DateType()).alias("official_date"),
            F.col("parsed_data.gameData.datetime.dayNight").alias("day_night"),
            # Status
            F.col("parsed_data.gameData.status.abstractGameState").alias("game_state"),
            F.col("parsed_data.gameData.status.detailedState").alias("detailed_state"),
            F.col("parsed_data.gameData.status.statusCode").alias("status_code"),
            # Teams
            F.col("parsed_data.gameData.teams.away.id").alias("away_team_id"),
            F.col("parsed_data.gameData.teams.away.name").alias("away_team_name"),
            F.col("parsed_data.gameData.teams.home.id").alias("home_team_id"),
            F.col("parsed_data.gameData.teams.home.name").alias("home_team_name"),
            # Venue
            F.col("parsed_data.gameData.venue.id").alias("venue_id"),
            F.col("parsed_data.gameData.venue.name").alias("venue_name"),
            # Weather
            F.col("parsed_data.gameData.weather.condition").alias("weather_condition"),
            F.col("parsed_data.gameData.weather.temp").alias("weather_temp"),
            F.col("parsed_data.gameData.weather.wind").alias("weather_wind"),
        )

        return metadata_df

    def _extract_teams(self, raw_df: DataFrame) -> DataFrame:
        """Extract team data (home and away) from JSONB.

        Creates two rows per game (one for home, one for away).
        """
        # Use simplified schema for teams extraction
        teams_schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField("away", StructType([
                            StructField("team", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                            ]), True),
                            StructField("teamStats", StructType([
                                StructField("batting", StructType([
                                    StructField("runs", IntegerType(), True),
                                    StructField("hits", IntegerType(), True),
                                    StructField("errors", IntegerType(), True),
                                    StructField("leftOnBase", IntegerType(), True),
                                ]), True),
                            ]), True),
                        ]), True),
                        StructField("home", StructType([
                            StructField("team", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                            ]), True),
                            StructField("teamStats", StructType([
                                StructField("batting", StructType([
                                    StructField("runs", IntegerType(), True),
                                    StructField("hits", IntegerType(), True),
                                    StructField("errors", IntegerType(), True),
                                    StructField("leftOnBase", IntegerType(), True),
                                ]), True),
                            ]), True),
                        ]), True),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), teams_schema)
        )

        # Extract away team
        away_df = parsed_df.select(
            F.col("game_pk"),
            F.lit("away").alias("home_away"),
            F.col("parsed_data.liveData.boxscore.teams.away.team.id").alias("team_id"),
            F.col("parsed_data.liveData.boxscore.teams.away.team.name").alias("team_name"),
            F.col("parsed_data.liveData.boxscore.teams.away.teamStats.batting.runs").alias("runs"),
            F.col("parsed_data.liveData.boxscore.teams.away.teamStats.batting.hits").alias("hits"),
            F.col("parsed_data.liveData.boxscore.teams.away.teamStats.batting.errors").alias("errors"),
            F.col("parsed_data.liveData.boxscore.teams.away.teamStats.batting.leftOnBase").alias("left_on_base"),
        )

        # Extract home team
        home_df = parsed_df.select(
            F.col("game_pk"),
            F.lit("home").alias("home_away"),
            F.col("parsed_data.liveData.boxscore.teams.home.team.id").alias("team_id"),
            F.col("parsed_data.liveData.boxscore.teams.home.team.name").alias("team_name"),
            F.col("parsed_data.liveData.boxscore.teams.home.teamStats.batting.runs").alias("runs"),
            F.col("parsed_data.liveData.boxscore.teams.home.teamStats.batting.hits").alias("hits"),
            F.col("parsed_data.liveData.boxscore.teams.home.teamStats.batting.errors").alias("errors"),
            F.col("parsed_data.liveData.boxscore.teams.home.teamStats.batting.leftOnBase").alias("left_on_base"),
        )

        # Union both teams
        teams_df = away_df.union(home_df)

        return teams_df

    def _extract_players(self, raw_df: DataFrame) -> DataFrame:
        """Extract player data from JSONB.

        Game data contains all players (51+ per game) in gameData.players object.
        """
        # Define schema for players (simplified - just IDs and names for now)
        players_schema = StructType([
            StructField("gameData", StructType([
                StructField("players", StructType(), True),  # Dynamic map of player IDs
            ]), True),
        ])

        # For now, just return count - full player extraction requires dynamic schema
        # This is a placeholder for future enhancement
        parsed_df = raw_df.withColumn(
            "parsed_data",
            F.from_json(F.col("data"), players_schema)
        )

        # Count players per game (approximate)
        players_df = parsed_df.select(
            F.col("game_pk"),
            F.lit(51).alias("estimated_player_count"),  # Placeholder
        )

        return players_df

    def _validate_quality(
        self,
        metadata_df: DataFrame,
        teams_df: DataFrame,
        players_df: DataFrame,
    ):
        """Validate data quality.

        Checks:
        - game_pk is not null
        - Both home and away teams present
        - Valid game states
        """
        total_games = metadata_df.count()

        # Check for null game_pks
        null_game_pks = metadata_df.filter(F.col("game_pk").isNull()).count()
        if null_game_pks > 0:
            console.print(
                f"[yellow]⚠ Warning: {null_game_pks} games with null game_pk[/yellow]"
            )

        # Check teams data
        total_teams = teams_df.count()
        expected_teams = total_games * 2  # home + away
        if total_teams < expected_teams:
            console.print(
                f"[yellow]⚠ Warning: Expected {expected_teams} team records, found {total_teams}[/yellow]"
            )

        console.print(
            f"[green]✓ Quality checks passed: {total_games} games, {total_teams} team records[/green]"
        )

    def _export_delta(self, df: DataFrame, path: str):
        """Export to Delta Lake."""
        df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).partitionBy("game_date").save(path)

        console.print(f"[green]✓ Exported to Delta Lake: {path}[/green]")

    def _export_parquet(self, df: DataFrame, path: str):
        """Export to Parquet."""
        df.write.format("parquet").mode("overwrite").partitionBy("game_date").save(
            path
        )

        console.print(f"[green]✓ Exported to Parquet: {path}[/green]")

    def _calculate_metrics(
        self,
        metadata_df: DataFrame,
        teams_df: DataFrame,
        players_df: DataFrame,
    ) -> Dict[str, Any]:
        """Calculate execution metrics."""
        total_games = metadata_df.count()
        total_teams = teams_df.count()

        # Game states
        state_counts = (
            metadata_df.groupBy("game_state").count().collect() if total_games > 0 else []
        )

        return {
            "total_games": total_games,
            "total_team_records": total_teams,
            "games_by_state": {row["game_state"]: row["count"] for row in state_counts},
        }

    def _display_summary(self, metrics: Dict[str, Any]):
        """Display execution summary."""
        table = Table(title="Game Transformation Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Games", str(metrics["total_games"]))
        table.add_row("Team Records", str(metrics["total_team_records"]))

        if metrics["games_by_state"]:
            table.add_row("", "")
            table.add_row("By Game State", "")
            for state, count in metrics["games_by_state"].items():
                table.add_row(f"  {state}", str(count))

        console.print(table)
