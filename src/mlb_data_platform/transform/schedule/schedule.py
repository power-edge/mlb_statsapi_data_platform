"""Schedule.schedule() transformation.

Uses the extraction registry pattern from base.py.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.schedule import ScheduleTransform
    >>>
    >>> spark = SparkSession.builder.appName("schedule-transform").getOrCreate()
    >>> transform = ScheduleTransform(spark)
    >>> results = transform(
    ...     start_date="2024-07-01",
    ...     end_date="2024-07-31",
    ...     sport_ids=[1],
    ...     write_to_postgres=True
    ... )
"""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..base import BaseTransformation


class ScheduleTransform(BaseTransformation):
    """Schedule transformation using extraction registry pattern.

    Transforms raw schedule.schedule JSONB data into normalized tables:
    - schedule.schedule_metadata - Top-level schedule info
    - schedule.games - Individual games extracted from nested arrays
    """

    def __init__(
        self,
        spark: SparkSession,
        enable_quality_checks: bool = True,
    ):
        """Initialize schedule transformation with extraction registry.

        Args:
            spark: SparkSession instance
            enable_quality_checks: Run data quality validation
        """
        # Initialize base with endpoint/method info
        super().__init__(
            spark=spark,
            endpoint="schedule",
            method="schedule",
            source_table="schedule.schedule",
            enable_quality_checks=enable_quality_checks,
        )

        # Register extractions
        self.register_extraction(
            name="metadata",
            method=self._extract_metadata,
            target_table="schedule.schedule_metadata",
        )
        self.register_extraction(
            name="games",
            method=self._extract_games,
            target_table="schedule.games",
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sport_ids: Optional[List[int]] = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply schedule-specific filters.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            start_date: Filter by schedule date >= (YYYY-MM-DD)
            end_date: Filter by schedule date <= (YYYY-MM-DD)
            sport_ids: Filter by sport IDs (e.g., [1] for MLB)
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        # Apply SQL filter first if provided
        if filter_sql:
            df = df.filter(filter_sql)

        # Apply date range filters
        if start_date:
            df = df.filter(F.col("schedule_date") >= start_date)
        if end_date:
            df = df.filter(F.col("schedule_date") <= end_date)

        # Apply sport_ids filter
        if sport_ids:
            df = df.filter(F.col("sport_id").isin(sport_ids))

        return df

    def _extract_metadata(self, raw_df: DataFrame) -> DataFrame:
        """Extract top-level schedule metadata.

        Schema: schedule.schedule_metadata
        - raw_id, sport_id, schedule_date
        - total_games, total_events, total_items, num_dates
        - captured_at, source_url
        """
        # Define schema for top-level metadata
        metadata_schema = StructType([
            StructField("totalGames", IntegerType(), True),
            StructField("totalEvents", IntegerType(), True),
            StructField("totalItems", IntegerType(), True),
            StructField("dates", ArrayType(StructType()), True),  # Minimal schema for counting
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn("parsed_data", F.from_json(F.col("data"), metadata_schema))

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

        Schema: schedule.games
        - game_pk, game_guid, game_type, season
        - game_date, game_datetime, official_date
        - status_code, detailed_state, abstract_game_state, coded_game_state
        - home/away team info (id, name, wins, losses, score)
        - venue_id, venue_name
        - double_header, day_night, scheduled_innings
        - games_in_series, series_game_number
        """
        # Define comprehensive schema for schedule data
        game_schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameGuid", StringType(), True),
            StructField("gameType", StringType(), True),
            StructField("season", StringType(), True),
            StructField("gameDate", StringType(), True),
            StructField("officialDate", StringType(), True),
            StructField(
                "status",
                StructType([
                    StructField("statusCode", StringType(), True),
                    StructField("detailedState", StringType(), True),
                    StructField("abstractGameState", StringType(), True),
                    StructField("codedGameState", StringType(), True),
                ]),
                True,
            ),
            StructField(
                "teams",
                StructType([
                    StructField(
                        "away",
                        StructType([
                            StructField(
                                "team",
                                StructType([
                                    StructField("id", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                ]),
                                True,
                            ),
                            StructField(
                                "leagueRecord",
                                StructType([
                                    StructField("wins", IntegerType(), True),
                                    StructField("losses", IntegerType(), True),
                                ]),
                                True,
                            ),
                            StructField("score", IntegerType(), True),
                        ]),
                        True,
                    ),
                    StructField(
                        "home",
                        StructType([
                            StructField(
                                "team",
                                StructType([
                                    StructField("id", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                ]),
                                True,
                            ),
                            StructField(
                                "leagueRecord",
                                StructType([
                                    StructField("wins", IntegerType(), True),
                                    StructField("losses", IntegerType(), True),
                                ]),
                                True,
                            ),
                            StructField("score", IntegerType(), True),
                        ]),
                        True,
                    ),
                ]),
                True,
            ),
            StructField(
                "venue",
                StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                ]),
                True,
            ),
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
            StructField(
                "dates",
                ArrayType(
                    StructType([
                        StructField("date", StringType(), True),
                        StructField("games", ArrayType(game_schema), True),
                    ])
                ),
                True,
            )
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn("parsed_data", F.from_json(F.col("data"), schedule_schema))

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
            F.col("game.status.detailedState").cast(StringType()).alias("detailed_state"),
            F.col("game.status.abstractGameState").cast(StringType()).alias("abstract_game_state"),
            F.col("game.status.codedGameState").cast(StringType()).alias("coded_game_state"),
            # Teams
            F.col("game.teams.away.team.id").cast(IntegerType()).alias("away_team_id"),
            F.col("game.teams.away.team.name").cast(StringType()).alias("away_team_name"),
            F.col("game.teams.away.leagueRecord.wins").cast(IntegerType()).alias("away_wins"),
            F.col("game.teams.away.leagueRecord.losses").cast(IntegerType()).alias("away_losses"),
            F.col("game.teams.away.score").cast(IntegerType()).alias("away_score"),
            F.col("game.teams.home.team.id").cast(IntegerType()).alias("home_team_id"),
            F.col("game.teams.home.team.name").cast(StringType()).alias("home_team_name"),
            F.col("game.teams.home.leagueRecord.wins").cast(IntegerType()).alias("home_wins"),
            F.col("game.teams.home.leagueRecord.losses").cast(IntegerType()).alias("home_losses"),
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
