"""Complete Game.liveGameV1() transformation - Refactored with base_v2.

This is the refactored version using the extraction registry pattern from base_v2.py.
The key improvement: subclass only needs to register extractions, not implement __call__().

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.game.live_game_v2 import LiveGameTransformV2
    >>>
    >>> spark = SparkSession.builder.appName("game-transform").getOrCreate()
    >>> transform = LiveGameTransformV2(spark)
    >>> results = transform(game_pks=[744834], write_to_postgres=True)
    >>> print(f"Transformed {results['filtered_count']} games")
"""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..base_v2 import BaseTransformation


class LiveGameTransformV2(BaseTransformation):
    """Refactored game transformation using extraction registry pattern.

    Transforms raw game.live_game_v1 JSONB data into 19 normalized tables:

    **Core Metadata (3 tables):**
    - live_game_metadata
    - live_game_venue_details
    - live_game_umpires

    **Player Lists (9 tables):**
    - live_game_players
    - live_game_home_batting_order, live_game_home_bench, live_game_home_bullpen, live_game_home_pitchers
    - live_game_away_batting_order, live_game_away_bench, live_game_away_bullpen, live_game_away_pitchers

    **Scoring (1 table):**
    - live_game_linescore_innings

    **Play-by-Play (6 tables):**
    - live_game_plays
    - live_game_play_actions
    - live_game_pitch_events
    - live_game_runners
    - live_game_scoring_plays
    - live_game_fielding_credits
    """

    def __init__(
        self,
        spark: SparkSession,
        enable_quality_checks: bool = True,
    ):
        """Initialize game transformation with extraction registry.

        Args:
            spark: SparkSession instance
            enable_quality_checks: Run data quality validation
        """
        # Initialize base with endpoint/method info
        super().__init__(
            spark=spark,
            endpoint="game",
            method="liveGameV1",
            source_table="game.live_game_v1",
            enable_quality_checks=enable_quality_checks,
        )

        # Register all extractions (Group 1: Core Metadata)
        self.register_extraction(
            name="metadata",
            method=self._extract_metadata,
            target_table="game.live_game_metadata",
        )
        self.register_extraction(
            name="venue_details",
            method=self._extract_venue_details,
            target_table="game.live_game_venue_details",
        )
        self.register_extraction(
            name="umpires",
            method=self._extract_umpires,
            target_table="game.live_game_umpires",
        )

        # Group 2: Player Lists (9 tables)
        self.register_extraction(
            name="players",
            method=self._extract_all_players,
            target_table="game.live_game_players",
        )
        self.register_extraction(
            name="home_batting_order",
            method=lambda df: self._extract_batting_order(df, "home"),
            target_table="game.live_game_home_batting_order",
        )
        self.register_extraction(
            name="home_bench",
            method=lambda df: self._extract_bench(df, "home"),
            target_table="game.live_game_home_bench",
        )
        self.register_extraction(
            name="home_bullpen",
            method=lambda df: self._extract_bullpen(df, "home"),
            target_table="game.live_game_home_bullpen",
        )
        self.register_extraction(
            name="home_pitchers",
            method=lambda df: self._extract_pitchers(df, "home"),
            target_table="game.live_game_home_pitchers",
        )
        self.register_extraction(
            name="away_batting_order",
            method=lambda df: self._extract_batting_order(df, "away"),
            target_table="game.live_game_away_batting_order",
        )
        self.register_extraction(
            name="away_bench",
            method=lambda df: self._extract_bench(df, "away"),
            target_table="game.live_game_away_bench",
        )
        self.register_extraction(
            name="away_bullpen",
            method=lambda df: self._extract_bullpen(df, "away"),
            target_table="game.live_game_away_bullpen",
        )
        self.register_extraction(
            name="away_pitchers",
            method=lambda df: self._extract_pitchers(df, "away"),
            target_table="game.live_game_away_pitchers",
        )

        # Group 3: Scoring
        self.register_extraction(
            name="linescore_innings",
            method=self._extract_linescore,
            target_table="game.live_game_linescore_innings",
        )

        # Group 4: Play-by-Play (6 tables)
        self.register_extraction(
            name="plays",
            method=self._extract_plays,
            target_table="game.live_game_plays",
        )
        self.register_extraction(
            name="play_actions",
            method=self._extract_play_actions,
            target_table="game.live_game_play_actions",
        )
        self.register_extraction(
            name="pitch_events",
            method=self._extract_pitch_events,
            target_table="game.live_game_pitch_events",
        )
        self.register_extraction(
            name="runners",
            method=self._extract_runners,
            target_table="game.live_game_runners",
        )
        self.register_extraction(
            name="scoring_plays",
            method=self._extract_scoring_plays,
            target_table="game.live_game_scoring_plays",
        )
        self.register_extraction(
            name="fielding_credits",
            method=self._extract_fielding_credits,
            target_table="game.live_game_fielding_credits",
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: Optional[str] = None,
        game_pks: Optional[List[int]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply game-specific filters.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            game_pks: Filter by specific game PKs
            start_date: Filter by game date >= (YYYY-MM-DD)
            end_date: Filter by game date <= (YYYY-MM-DD)
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        # Apply SQL filter first if provided
        if filter_sql:
            df = df.filter(filter_sql)

        # Apply game_pks filter
        if game_pks:
            df = df.filter(F.col("game_pk").isin(game_pks))

        # Apply date range filters
        if start_date:
            df = df.filter(F.col("game_date") >= start_date)
        if end_date:
            df = df.filter(F.col("game_date") <= end_date)

        return df

    # ========================================================================
    # EXTRACTION METHODS - Core Metadata (3 tables)
    # ========================================================================

    def _extract_metadata(self, raw_df: DataFrame) -> DataFrame:
        """Extract core game metadata.

        Schema: game.live_game_metadata
        - game_pk, game_date, season, game_type
        - home/away team info, scores
        - status, venue, weather
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameData", StructType([
                StructField("datetime", StructType([
                    StructField("officialDate", StringType(), True),
                    StructField("dateTime", StringType(), True),
                ]), True),
                StructField("game", StructType([
                    StructField("season", StringType(), True),
                    StructField("type", StringType(), True),
                ]), True),
                StructField("teams", StructType([
                    StructField("home", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                    StructField("away", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]), True),
                ]), True),
                StructField("venue", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                ]), True),
                StructField("status", StructType([
                    StructField("statusCode", StringType(), True),
                    StructField("detailedState", StringType(), True),
                ]), True),
                StructField("weather", StructType([
                    StructField("temp", StringType(), True),
                    StructField("condition", StringType(), True),
                    StructField("wind", StringType(), True),
                ]), True),
            ]), True),
            StructField("liveData", StructType([
                StructField("linescore", StructType([
                    StructField("teams", StructType([
                        StructField("home", StructType([
                            StructField("runs", IntegerType(), True),
                            StructField("hits", IntegerType(), True),
                            StructField("errors", IntegerType(), True),
                        ]), True),
                        StructField("away", StructType([
                            StructField("runs", IntegerType(), True),
                            StructField("hits", IntegerType(), True),
                            StructField("errors", IntegerType(), True),
                        ]), True),
                    ]), True),
                    StructField("currentInning", IntegerType(), True),
                    StructField("inningState", StringType(), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        metadata_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.col("parsed.gameData.datetime.officialDate").alias("game_date"),
            F.col("parsed.gameData.datetime.dateTime").alias("game_datetime"),
            F.col("parsed.gameData.game.season").alias("season"),
            F.col("parsed.gameData.game.type").alias("game_type"),
            F.col("parsed.gameData.teams.home.id").alias("home_team_id"),
            F.col("parsed.gameData.teams.home.name").alias("home_team_name"),
            F.col("parsed.gameData.teams.away.id").alias("away_team_id"),
            F.col("parsed.gameData.teams.away.name").alias("away_team_name"),
            F.col("parsed.liveData.linescore.teams.home.runs").alias("home_score"),
            F.col("parsed.liveData.linescore.teams.away.runs").alias("away_score"),
            F.col("parsed.liveData.linescore.teams.home.hits").alias("home_hits"),
            F.col("parsed.liveData.linescore.teams.away.hits").alias("away_hits"),
            F.col("parsed.liveData.linescore.teams.home.errors").alias("home_errors"),
            F.col("parsed.liveData.linescore.teams.away.errors").alias("away_errors"),
            F.col("parsed.liveData.linescore.currentInning").alias("current_inning"),
            F.col("parsed.liveData.linescore.inningState").alias("inning_state"),
            F.col("parsed.gameData.venue.id").alias("venue_id"),
            F.col("parsed.gameData.venue.name").alias("venue_name"),
            F.col("parsed.gameData.status.statusCode").alias("status_code"),
            F.col("parsed.gameData.status.detailedState").alias("detailed_state"),
            F.col("parsed.gameData.weather.temp").alias("weather_temp"),
            F.col("parsed.gameData.weather.condition").alias("weather_condition"),
            F.col("parsed.gameData.weather.wind").alias("weather_wind"),
            F.col("captured_at").alias("source_captured_at"),
        )

        return metadata_df

    def _extract_venue_details(self, raw_df: DataFrame) -> DataFrame:
        """Extract venue details (placeholder)."""
        # TODO: Implement venue extraction
        return self.spark.createDataFrame([], "game_pk INT, venue_id INT")

    def _extract_umpires(self, raw_df: DataFrame) -> DataFrame:
        """Extract umpire assignments (placeholder)."""
        # TODO: Implement umpire extraction
        return self.spark.createDataFrame([], "game_pk INT, umpire_id INT")

    # ========================================================================
    # EXTRACTION METHODS - Player Lists (9 tables)
    # ========================================================================

    def _extract_all_players(self, raw_df: DataFrame) -> DataFrame:
        """Extract all players registry (placeholder)."""
        # TODO: Implement player registry extraction
        return self.spark.createDataFrame([], "game_pk INT, player_id INT")

    def _extract_batting_order(self, raw_df: DataFrame, side: str) -> DataFrame:
        """Extract batting order (1-9) for home or away team.

        Args:
            raw_df: Raw game data
            side: "home" or "away"

        Returns:
            DataFrame with columns: game_pk, batting_position, player_id
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField(
                            side,
                            StructType([
                                StructField("batters", ArrayType(IntegerType()), True),
                            ]),
                            True,
                        ),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        batters_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.posexplode(F.col(f"parsed.liveData.boxscore.teams.{side}.batters")).alias(
                "position", "player_id"
            ),
        )

        # Filter to batting order only (positions 0-8)
        batting_order_df = batters_df.filter(F.col("position") < 9).select(
            F.col("game_pk"),
            (F.col("position") + 1).alias("batting_position"),
            F.col("player_id"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return batting_order_df

    def _extract_bench(self, raw_df: DataFrame, side: str) -> DataFrame:
        """Extract bench players for home or away team.

        Args:
            raw_df: Raw game data
            side: "home" or "away"

        Returns:
            DataFrame with columns: game_pk, player_id
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField(
                            side,
                            StructType([
                                StructField("bench", ArrayType(IntegerType()), True),
                            ]),
                            True,
                        ),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        bench_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col(f"parsed.liveData.boxscore.teams.{side}.bench")).alias("player_id"),
        ).select(
            F.col("game_pk"),
            F.col("player_id"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return bench_df

    def _extract_bullpen(self, raw_df: DataFrame, side: str) -> DataFrame:
        """Extract bullpen pitchers for home or away team.

        Args:
            raw_df: Raw game data
            side: "home" or "away"

        Returns:
            DataFrame with columns: game_pk, player_id
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField(
                            side,
                            StructType([
                                StructField("bullpen", ArrayType(IntegerType()), True),
                            ]),
                            True,
                        ),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        bullpen_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col(f"parsed.liveData.boxscore.teams.{side}.bullpen")).alias("player_id"),
        ).select(
            F.col("game_pk"),
            F.col("player_id"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return bullpen_df

    def _extract_pitchers(self, raw_df: DataFrame, side: str) -> DataFrame:
        """Extract all pitchers for home or away team.

        Args:
            raw_df: Raw game data
            side: "home" or "away"

        Returns:
            DataFrame with columns: game_pk, player_id
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField(
                            side,
                            StructType([
                                StructField("pitchers", ArrayType(IntegerType()), True),
                            ]),
                            True,
                        ),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        pitchers_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col(f"parsed.liveData.boxscore.teams.{side}.pitchers")).alias("player_id"),
        ).select(
            F.col("game_pk"),
            F.col("player_id"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return pitchers_df

    # ========================================================================
    # EXTRACTION METHODS - Scoring (1 table)
    # ========================================================================

    def _extract_linescore(self, raw_df: DataFrame) -> DataFrame:
        """Extract linescore innings (placeholder)."""
        # TODO: Implement linescore extraction
        return self.spark.createDataFrame([], "game_pk INT, inning INT")

    # ========================================================================
    # EXTRACTION METHODS - Play-by-Play (6 tables)
    # ========================================================================

    def _extract_plays(self, raw_df: DataFrame) -> DataFrame:
        """Extract all plays/at-bats (placeholder)."""
        # TODO: Implement plays extraction
        return self.spark.createDataFrame([], "game_pk INT, play_id STRING")

    def _extract_play_actions(self, raw_df: DataFrame) -> DataFrame:
        """Extract play actions (placeholder)."""
        # TODO: Implement play actions extraction
        return self.spark.createDataFrame([], "game_pk INT, play_id STRING")

    def _extract_pitch_events(self, raw_df: DataFrame) -> DataFrame:
        """Extract pitch events with Statcast data (placeholder)."""
        # TODO: Implement pitch events extraction
        return self.spark.createDataFrame([], "game_pk INT, pitch_number INT")

    def _extract_runners(self, raw_df: DataFrame) -> DataFrame:
        """Extract runner movements (placeholder)."""
        # TODO: Implement runners extraction
        return self.spark.createDataFrame([], "game_pk INT, runner_id INT")

    def _extract_scoring_plays(self, raw_df: DataFrame) -> DataFrame:
        """Extract scoring plays (placeholder)."""
        # TODO: Implement scoring plays extraction
        return self.spark.createDataFrame([], "game_pk INT, play_id STRING")

    def _extract_fielding_credits(self, raw_df: DataFrame) -> DataFrame:
        """Extract fielding credits (placeholder)."""
        # TODO: Implement fielding credits extraction
        return self.spark.createDataFrame([], "game_pk INT, player_id INT")
