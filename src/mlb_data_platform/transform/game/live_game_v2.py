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
        """Extract detailed venue information including field dimensions."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameData", StructType([
                StructField("venue", StructType([
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("location", StructType([
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("stateAbbrev", StringType(), True),
                    ]), True),
                    StructField("timeZone", StructType([
                        StructField("id", StringType(), True),
                        StructField("offset", IntegerType(), True),
                        StructField("tz", StringType(), True),
                    ]), True),
                    StructField("fieldInfo", StructType([
                        StructField("capacity", IntegerType(), True),
                        StructField("turfType", StringType(), True),
                        StructField("roofType", StringType(), True),
                        StructField("leftLine", IntegerType(), True),
                        StructField("leftCenter", IntegerType(), True),
                        StructField("center", IntegerType(), True),
                        StructField("rightCenter", IntegerType(), True),
                        StructField("rightLine", IntegerType(), True),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        venue_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.col("parsed.gameData.venue.id").alias("venue_id"),
            F.col("parsed.gameData.venue.name").alias("venue_name"),
            F.col("parsed.gameData.venue.location.city").alias("city"),
            F.col("parsed.gameData.venue.location.state").alias("state"),
            F.col("parsed.gameData.venue.location.stateAbbrev").alias("state_abbrev"),
            F.col("parsed.gameData.venue.timeZone.id").alias("timezone_id"),
            F.col("parsed.gameData.venue.timeZone.offset").alias("timezone_offset"),
            F.col("parsed.gameData.venue.timeZone.tz").alias("timezone_tz"),
            F.col("parsed.gameData.venue.fieldInfo.capacity").alias("capacity"),
            F.col("parsed.gameData.venue.fieldInfo.turfType").alias("turf_type"),
            F.col("parsed.gameData.venue.fieldInfo.roofType").alias("roof_type"),
            F.col("parsed.gameData.venue.fieldInfo.leftLine").alias("left_line"),
            F.col("parsed.gameData.venue.fieldInfo.leftCenter").alias("left_center"),
            F.col("parsed.gameData.venue.fieldInfo.center").alias("center"),
            F.col("parsed.gameData.venue.fieldInfo.rightCenter").alias("right_center"),
            F.col("parsed.gameData.venue.fieldInfo.rightLine").alias("right_line"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return venue_df

    def _extract_umpires(self, raw_df: DataFrame) -> DataFrame:
        """Extract umpire assignments for all positions."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("officials", ArrayType(StructType([
                        StructField("official", StructType([
                            StructField("id", IntegerType(), True),
                            StructField("fullName", StringType(), True),
                        ]), True),
                        StructField("officialType", StringType(), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        umpires_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.boxscore.officials")).alias("umpire")
        )

        umpires_extracted = umpires_df.select(
            F.col("game_pk"),
            F.col("umpire.official.id").alias("umpire_id"),
            F.col("umpire.official.fullName").alias("full_name"),
            F.col("umpire.officialType").alias("position"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return umpires_extracted

    # ========================================================================
    # EXTRACTION METHODS - Player Lists (9 tables)
    # ========================================================================

    def _extract_all_players(self, raw_df: DataFrame) -> DataFrame:
        """Extract all players registry with stats from both teams."""
        from pyspark.sql.types import MapType

        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("boxscore", StructType([
                    StructField("teams", StructType([
                        StructField("home", StructType([
                            StructField("players", MapType(StringType(), StructType([
                                StructField("person", StructType([
                                    StructField("id", IntegerType(), True),
                                    StructField("fullName", StringType(), True),
                                ]), True),
                                StructField("jerseyNumber", StringType(), True),
                                StructField("position", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("type", StringType(), True),
                                    StructField("abbreviation", StringType(), True),
                                ]), True),
                                StructField("status", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("description", StringType(), True),
                                ]), True),
                            ])), True),
                        ]), True),
                        StructField("away", StructType([
                            StructField("players", MapType(StringType(), StructType([
                                StructField("person", StructType([
                                    StructField("id", IntegerType(), True),
                                    StructField("fullName", StringType(), True),
                                ]), True),
                                StructField("jerseyNumber", StringType(), True),
                                StructField("position", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("type", StringType(), True),
                                    StructField("abbreviation", StringType(), True),
                                ]), True),
                                StructField("status", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("description", StringType(), True),
                                ]), True),
                            ])), True),
                        ]), True),
                    ]), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        # Extract home players
        home_players = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.lit("home").alias("team_side"),
            F.explode(F.col("parsed.liveData.boxscore.teams.home.players")).alias("player_key", "player")
        )

        # Extract away players
        away_players = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.lit("away").alias("team_side"),
            F.explode(F.col("parsed.liveData.boxscore.teams.away.players")).alias("player_key", "player")
        )

        # Union and extract fields
        all_players = home_players.union(away_players).select(
            F.col("game_pk"),
            F.col("team_side"),
            F.col("player.person.id").alias("player_id"),
            F.col("player.person.fullName").alias("full_name"),
            F.col("player.jerseyNumber").alias("jersey_number"),
            F.col("player.position.code").alias("position_code"),
            F.col("player.position.name").alias("position_name"),
            F.col("player.position.type").alias("position_type"),
            F.col("player.position.abbreviation").alias("position_abbrev"),
            F.col("player.status.code").alias("status_code"),
            F.col("player.status.description").alias("status_description"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return all_players

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
        """Extract inning-by-inning linescore."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("linescore", StructType([
                    StructField("innings", ArrayType(StructType([
                        StructField("num", IntegerType(), True),
                        StructField("ordinalNum", StringType(), True),
                        StructField("home", StructType([
                            StructField("runs", IntegerType(), True),
                            StructField("hits", IntegerType(), True),
                            StructField("errors", IntegerType(), True),
                            StructField("leftOnBase", IntegerType(), True),
                        ]), True),
                        StructField("away", StructType([
                            StructField("runs", IntegerType(), True),
                            StructField("hits", IntegerType(), True),
                            StructField("errors", IntegerType(), True),
                            StructField("leftOnBase", IntegerType(), True),
                        ]), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        innings_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.linescore.innings")).alias("inning")
        )

        linescore_df = innings_df.select(
            F.col("game_pk"),
            F.col("inning.num").alias("inning_number"),
            F.col("inning.ordinalNum").alias("inning_ordinal"),
            F.col("inning.home.runs").alias("home_runs"),
            F.col("inning.home.hits").alias("home_hits"),
            F.col("inning.home.errors").alias("home_errors"),
            F.col("inning.home.leftOnBase").alias("home_left_on_base"),
            F.col("inning.away.runs").alias("away_runs"),
            F.col("inning.away.hits").alias("away_hits"),
            F.col("inning.away.errors").alias("away_errors"),
            F.col("inning.away.leftOnBase").alias("away_left_on_base"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return linescore_df

    # ========================================================================
    # EXTRACTION METHODS - Play-by-Play (6 tables)
    # ========================================================================

    def _extract_plays(self, raw_df: DataFrame) -> DataFrame:
        """Extract all plays/at-bats from the game.

        Each play represents an at-bat with matchup info (batter vs pitcher),
        result (event type, runs, outs), and metadata.

        Args:
            raw_df: Raw game data

        Returns:
            DataFrame with columns: game_pk, at_bat_index, inning, half_inning,
                batter_id, pitcher_id, event_type, description, rbi, etc.
        """
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                            StructField("inning", IntegerType(), True),
                            StructField("halfInning", StringType(), True),
                            StructField("isTopInning", BooleanType(), True),
                            StructField("startTime", StringType(), True),
                            StructField("endTime", StringType(), True),
                            StructField("isComplete", BooleanType(), True),
                            StructField("isScoringPlay", BooleanType(), True),
                            StructField("hasOut", BooleanType(), True),
                            StructField("hasReview", BooleanType(), True),
                            StructField("captivatingIndex", IntegerType(), True),
                        ]), True),
                        StructField("result", StructType([
                            StructField("type", StringType(), True),
                            StructField("event", StringType(), True),
                            StructField("eventType", StringType(), True),
                            StructField("description", StringType(), True),
                            StructField("rbi", IntegerType(), True),
                            StructField("awayScore", IntegerType(), True),
                            StructField("homeScore", IntegerType(), True),
                            StructField("isOut", BooleanType(), True),
                        ]), True),
                        StructField("matchup", StructType([
                            StructField("batter", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("fullName", StringType(), True),
                            ]), True),
                            StructField("pitcher", StructType([
                                StructField("id", IntegerType(), True),
                                StructField("fullName", StringType(), True),
                            ]), True),
                            StructField("batSide", StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]), True),
                            StructField("pitchHand", StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]), True),
                            StructField("splits", StructType([
                                StructField("batter", StringType(), True),
                                StructField("pitcher", StringType(), True),
                                StructField("menOnBase", StringType(), True),
                            ]), True),
                        ]), True),
                        StructField("count", StructType([
                            StructField("balls", IntegerType(), True),
                            StructField("strikes", IntegerType(), True),
                            StructField("outs", IntegerType(), True),
                        ]), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        # Explode allPlays array
        plays_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
        )

        # Extract play fields
        plays_extracted = plays_df.select(
            F.col("game_pk"),
            # About
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.col("play.about.inning").alias("inning"),
            F.col("play.about.halfInning").alias("half_inning"),
            F.col("play.about.isTopInning").alias("is_top_inning"),
            F.col("play.about.startTime").cast(TimestampType()).alias("start_time"),
            F.col("play.about.endTime").cast(TimestampType()).alias("end_time"),
            F.col("play.about.isComplete").alias("is_complete"),
            F.col("play.about.isScoringPlay").alias("is_scoring_play"),
            F.col("play.about.hasOut").alias("has_out"),
            F.col("play.about.hasReview").alias("has_review"),
            F.col("play.about.captivatingIndex").alias("captivating_index"),
            # Result
            F.col("play.result.type").alias("result_type"),
            F.col("play.result.event").alias("event"),
            F.col("play.result.eventType").alias("event_type"),
            F.col("play.result.description").alias("description"),
            F.col("play.result.rbi").alias("rbi"),
            F.col("play.result.awayScore").alias("away_score"),
            F.col("play.result.homeScore").alias("home_score"),
            F.col("play.result.isOut").alias("is_out"),
            # Matchup
            F.col("play.matchup.batter.id").alias("batter_id"),
            F.col("play.matchup.batter.fullName").alias("batter_name"),
            F.col("play.matchup.pitcher.id").alias("pitcher_id"),
            F.col("play.matchup.pitcher.fullName").alias("pitcher_name"),
            F.col("play.matchup.batSide.code").alias("bat_side"),
            F.col("play.matchup.pitchHand.code").alias("pitch_hand"),
            F.col("play.matchup.splits.batter").alias("split_batter"),
            F.col("play.matchup.splits.pitcher").alias("split_pitcher"),
            F.col("play.matchup.splits.menOnBase").alias("men_on_base"),
            # Count at end of at-bat
            F.col("play.count.balls").alias("final_balls"),
            F.col("play.count.strikes").alias("final_strikes"),
            F.col("play.count.outs").alias("final_outs"),
            # Metadata
            F.current_timestamp().alias("source_captured_at"),
        )

        return plays_extracted

    def _extract_play_actions(self, raw_df: DataFrame) -> DataFrame:
        """Extract play actions (non-pitch events like delays, reviews, substitutions)."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                        ]), True),
                        StructField("playEvents", ArrayType(StructType([
                            StructField("isPitch", BooleanType(), True),
                            StructField("type", StringType(), True),
                            StructField("index", IntegerType(), True),
                            StructField("startTime", StringType(), True),
                            StructField("endTime", StringType(), True),
                            StructField("details", StructType([
                                StructField("event", StringType(), True),
                                StructField("eventType", StringType(), True),
                                StructField("description", StringType(), True),
                                StructField("awayScore", IntegerType(), True),
                                StructField("homeScore", IntegerType(), True),
                            ]), True),
                            StructField("player", StructType([
                                StructField("id", IntegerType(), True),
                            ]), True),
                        ])), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        plays_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
        )

        actions_df = plays_df.select(
            F.col("game_pk"),
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.explode(F.col("play.playEvents")).alias("event")
        ).filter(F.col("event.isPitch") == False)

        actions_extracted = actions_df.select(
            F.col("game_pk"),
            F.col("at_bat_index"),
            F.col("event.index").alias("event_index"),
            F.col("event.type").alias("event_type"),
            F.col("event.startTime").cast(TimestampType()).alias("start_time"),
            F.col("event.endTime").cast(TimestampType()).alias("end_time"),
            F.col("event.details.event").alias("event"),
            F.col("event.details.eventType").alias("event_type_code"),
            F.col("event.details.description").alias("description"),
            F.col("event.details.awayScore").alias("away_score"),
            F.col("event.details.homeScore").alias("home_score"),
            F.col("event.player.id").alias("player_id"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return actions_extracted

    def _extract_pitch_events(self, raw_df: DataFrame) -> DataFrame:
        """Extract every pitch thrown with complete Statcast data.

        This includes both pitchData (velocity, spin, movement) and hitData
        (exit velocity, launch angle, distance) when ball is put in play.

        Args:
            raw_df: Raw game data

        Returns:
            DataFrame with columns: game_pk, play_id, pitch_number,
                pitch_type, start_speed, end_speed, spin_rate,
                launch_speed, launch_angle, total_distance, trajectory, hardness, etc.
        """
        # Schema for play events (pitches only)
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                            StructField("inning", IntegerType(), True),
                            StructField("halfInning", StringType(), True),
                        ]), True),
                        StructField("playEvents", ArrayType(StructType([
                            StructField("isPitch", BooleanType(), True),
                            StructField("type", StringType(), True),
                            StructField("playId", StringType(), True),
                            StructField("pitchNumber", IntegerType(), True),
                            StructField("startTime", StringType(), True),
                            StructField("endTime", StringType(), True),
                            StructField("details", StructType([
                                StructField("call", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("description", StringType(), True),
                                ]), True),
                                StructField("code", StringType(), True),
                                StructField("type", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("description", StringType(), True),
                                ]), True),
                                StructField("description", StringType(), True),
                                StructField("isInPlay", BooleanType(), True),
                                StructField("isStrike", BooleanType(), True),
                                StructField("isBall", BooleanType(), True),
                            ]), True),
                            StructField("count", StructType([
                                StructField("balls", IntegerType(), True),
                                StructField("strikes", IntegerType(), True),
                                StructField("outs", IntegerType(), True),
                            ]), True),
                            # Pitch tracking data
                            StructField("pitchData", StructType([
                                StructField("startSpeed", DoubleType(), True),
                                StructField("endSpeed", DoubleType(), True),
                                StructField("zone", IntegerType(), True),
                                StructField("extension", DoubleType(), True),
                                StructField("plateTime", DoubleType(), True),
                                StructField("strikeZoneTop", DoubleType(), True),
                                StructField("strikeZoneBottom", DoubleType(), True),
                                StructField("breaks", StructType([
                                    StructField("spinRate", IntegerType(), True),
                                    StructField("spinDirection", IntegerType(), True),
                                    StructField("breakAngle", DoubleType(), True),
                                    StructField("breakLength", DoubleType(), True),
                                    StructField("breakY", DoubleType(), True),
                                    StructField("breakVertical", DoubleType(), True),
                                    StructField("breakVerticalInduced", DoubleType(), True),
                                    StructField("breakHorizontal", DoubleType(), True),
                                ]), True),
                                StructField("coordinates", StructType([
                                    StructField("x", DoubleType(), True),
                                    StructField("y", DoubleType(), True),
                                    StructField("pX", DoubleType(), True),
                                    StructField("pZ", DoubleType(), True),
                                    StructField("pfxX", DoubleType(), True),
                                    StructField("pfxZ", DoubleType(), True),
                                    StructField("x0", DoubleType(), True),
                                    StructField("y0", DoubleType(), True),
                                    StructField("z0", DoubleType(), True),
                                    StructField("vX0", DoubleType(), True),
                                    StructField("vY0", DoubleType(), True),
                                    StructField("vZ0", DoubleType(), True),
                                    StructField("aX", DoubleType(), True),
                                    StructField("aY", DoubleType(), True),
                                    StructField("aZ", DoubleType(), True),
                                ]), True),
                            ]), True),
                            # Hit tracking data (Statcast)
                            StructField("hitData", StructType([
                                StructField("launchSpeed", DoubleType(), True),
                                StructField("launchAngle", DoubleType(), True),
                                StructField("totalDistance", DoubleType(), True),
                                StructField("trajectory", StringType(), True),
                                StructField("hardness", StringType(), True),
                                StructField("location", StringType(), True),
                                StructField("coordinates", StructType([
                                    StructField("coordX", DoubleType(), True),
                                    StructField("coordY", DoubleType(), True),
                                ]), True),
                            ]), True),
                        ])), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        # Explode plays
        plays_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
        )

        # Explode play events and filter to pitches only
        pitch_events_df = plays_df.select(
            F.col("game_pk"),
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.col("play.about.inning").alias("inning"),
            F.col("play.about.halfInning").alias("half_inning"),
            F.explode(F.col("play.playEvents")).alias("event")
        ).filter(F.col("event.isPitch") == True)

        # Extract all pitch fields
        pitches_df = pitch_events_df.select(
            F.col("game_pk"),
            F.col("event.playId").alias("play_id"),
            F.col("at_bat_index"),
            F.col("inning"),
            F.col("half_inning"),
            F.col("event.pitchNumber").alias("pitch_number"),
            F.col("event.startTime").cast(TimestampType()).alias("start_time"),
            F.col("event.endTime").cast(TimestampType()).alias("end_time"),
            # Pitch classification
            F.col("event.details.type.code").alias("pitch_type"),
            F.col("event.details.type.description").alias("pitch_type_desc"),
            F.col("event.details.code").alias("call_code"),
            F.col("event.details.call.description").alias("call_description"),
            F.col("event.details.description").alias("description"),
            # Pitch outcome flags
            F.col("event.details.isInPlay").alias("is_in_play"),
            F.col("event.details.isStrike").alias("is_strike"),
            F.col("event.details.isBall").alias("is_ball"),
            # Count
            F.col("event.count.balls").alias("balls"),
            F.col("event.count.strikes").alias("strikes"),
            F.col("event.count.outs").alias("outs"),
            # Pitch tracking (velocity, spin, movement)
            F.col("event.pitchData.startSpeed").alias("start_speed"),
            F.col("event.pitchData.endSpeed").alias("end_speed"),
            F.col("event.pitchData.zone").alias("zone"),
            F.col("event.pitchData.extension").alias("extension"),
            F.col("event.pitchData.plateTime").alias("plate_time"),
            F.col("event.pitchData.strikeZoneTop").alias("strike_zone_top"),
            F.col("event.pitchData.strikeZoneBottom").alias("strike_zone_bottom"),
            # Spin data
            F.col("event.pitchData.breaks.spinRate").alias("spin_rate"),
            F.col("event.pitchData.breaks.spinDirection").alias("spin_direction"),
            F.col("event.pitchData.breaks.breakAngle").alias("break_angle"),
            F.col("event.pitchData.breaks.breakLength").alias("break_length"),
            F.col("event.pitchData.breaks.breakY").alias("break_y"),
            F.col("event.pitchData.breaks.breakVertical").alias("break_vertical"),
            F.col("event.pitchData.breaks.breakVerticalInduced").alias("break_vertical_induced"),
            F.col("event.pitchData.breaks.breakHorizontal").alias("break_horizontal"),
            # Pitch location
            F.col("event.pitchData.coordinates.x").alias("coord_x"),
            F.col("event.pitchData.coordinates.y").alias("coord_y"),
            F.col("event.pitchData.coordinates.pX").alias("px"),
            F.col("event.pitchData.coordinates.pZ").alias("pz"),
            F.col("event.pitchData.coordinates.pfxX").alias("pfx_x"),
            F.col("event.pitchData.coordinates.pfxZ").alias("pfx_z"),
            # Release point
            F.col("event.pitchData.coordinates.x0").alias("x0"),
            F.col("event.pitchData.coordinates.y0").alias("y0"),
            F.col("event.pitchData.coordinates.z0").alias("z0"),
            # Velocity vectors
            F.col("event.pitchData.coordinates.vX0").alias("vx0"),
            F.col("event.pitchData.coordinates.vY0").alias("vy0"),
            F.col("event.pitchData.coordinates.vZ0").alias("vz0"),
            # Acceleration vectors
            F.col("event.pitchData.coordinates.aX").alias("ax"),
            F.col("event.pitchData.coordinates.aY").alias("ay"),
            F.col("event.pitchData.coordinates.aZ").alias("az"),
            # Hit tracking (Statcast) - only populated when ball is put in play
            F.col("event.hitData.launchSpeed").alias("launch_speed"),
            F.col("event.hitData.launchAngle").alias("launch_angle"),
            F.col("event.hitData.totalDistance").alias("total_distance"),
            F.col("event.hitData.trajectory").alias("trajectory"),
            F.col("event.hitData.hardness").alias("hardness"),
            F.col("event.hitData.location").alias("hit_location"),
            F.col("event.hitData.coordinates.coordX").alias("hit_coord_x"),
            F.col("event.hitData.coordinates.coordY").alias("hit_coord_y"),
            # Metadata
            F.current_timestamp().alias("source_captured_at"),
        )

        return pitches_df

    def _extract_runners(self, raw_df: DataFrame) -> DataFrame:
        """Extract base runner movements and scoring events."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                        ]), True),
                        StructField("runners", ArrayType(StructType([
                            StructField("movement", StructType([
                                StructField("originBase", StringType(), True),
                                StructField("start", StringType(), True),
                                StructField("end", StringType(), True),
                                StructField("outBase", StringType(), True),
                                StructField("isOut", BooleanType(), True),
                                StructField("outNumber", IntegerType(), True),
                            ]), True),
                            StructField("details", StructType([
                                StructField("event", StringType(), True),
                                StructField("eventType", StringType(), True),
                                StructField("runner", StructType([
                                    StructField("id", IntegerType(), True),
                                    StructField("fullName", StringType(), True),
                                ]), True),
                                StructField("responsiblePitcher", StructType([
                                    StructField("id", IntegerType(), True),
                                ]), True),
                                StructField("isScoringEvent", BooleanType(), True),
                                StructField("rbi", BooleanType(), True),
                                StructField("earned", BooleanType(), True),
                                StructField("teamUnearned", BooleanType(), True),
                            ]), True),
                        ])), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        plays_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
        )

        runners_df = plays_df.select(
            F.col("game_pk"),
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.explode(F.col("play.runners")).alias("runner")
        )

        runners_extracted = runners_df.select(
            F.col("game_pk"),
            F.col("at_bat_index"),
            F.col("runner.details.runner.id").alias("runner_id"),
            F.col("runner.details.runner.fullName").alias("runner_name"),
            F.col("runner.movement.originBase").alias("origin_base"),
            F.col("runner.movement.start").alias("start_base"),
            F.col("runner.movement.end").alias("end_base"),
            F.col("runner.movement.outBase").alias("out_base"),
            F.col("runner.movement.isOut").alias("is_out"),
            F.col("runner.movement.outNumber").alias("out_number"),
            F.col("runner.details.event").alias("event"),
            F.col("runner.details.eventType").alias("event_type"),
            F.col("runner.details.responsiblePitcher.id").alias("responsible_pitcher_id"),
            F.col("runner.details.isScoringEvent").alias("is_scoring_event"),
            F.col("runner.details.rbi").alias("rbi"),
            F.col("runner.details.earned").alias("earned"),
            F.col("runner.details.teamUnearned").alias("team_unearned"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return runners_extracted

    def _extract_scoring_plays(self, raw_df: DataFrame) -> DataFrame:
        """Extract plays that resulted in runs scored."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("scoringPlays", ArrayType(IntegerType()), True),
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                            StructField("inning", IntegerType(), True),
                            StructField("halfInning", StringType(), True),
                        ]), True),
                        StructField("result", StructType([
                            StructField("event", StringType(), True),
                            StructField("description", StringType(), True),
                            StructField("rbi", IntegerType(), True),
                            StructField("awayScore", IntegerType(), True),
                            StructField("homeScore", IntegerType(), True),
                        ]), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        # Get scoring play indices
        scoring_indices = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.scoringPlays")).alias("scoring_index")
        )

        # Get all plays
        all_plays = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.posexplode(F.col("parsed.liveData.plays.allPlays")).alias("idx", "play")
        )

        # Join to get only scoring plays
        scoring_plays = scoring_indices.join(
            all_plays,
            (scoring_indices.game_pk == all_plays.game_pk) &
            (scoring_indices.scoring_index == all_plays.idx)
        ).select(
            all_plays.game_pk,
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.col("play.about.inning").alias("inning"),
            F.col("play.about.halfInning").alias("half_inning"),
            F.col("play.result.event").alias("event"),
            F.col("play.result.description").alias("description"),
            F.col("play.result.rbi").alias("rbi"),
            F.col("play.result.awayScore").alias("away_score"),
            F.col("play.result.homeScore").alias("home_score"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return scoring_plays

    def _extract_fielding_credits(self, raw_df: DataFrame) -> DataFrame:
        """Extract defensive credits (putouts, assists, errors)."""
        schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("liveData", StructType([
                StructField("plays", StructType([
                    StructField("allPlays", ArrayType(StructType([
                        StructField("about", StructType([
                            StructField("atBatIndex", IntegerType(), True),
                        ]), True),
                        StructField("runners", ArrayType(StructType([
                            StructField("credits", ArrayType(StructType([
                                StructField("player", StructType([
                                    StructField("id", IntegerType(), True),
                                ]), True),
                                StructField("position", StructType([
                                    StructField("code", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("abbreviation", StringType(), True),
                                ]), True),
                                StructField("credit", StringType(), True),
                            ])), True),
                        ])), True),
                    ])), True),
                ]), True),
            ]), True),
        ])

        parsed_df = raw_df.withColumn("parsed", F.from_json(F.col("data"), schema))

        plays_df = parsed_df.select(
            F.col("parsed.gamePk").alias("game_pk"),
            F.explode(F.col("parsed.liveData.plays.allPlays")).alias("play")
        )

        runners_df = plays_df.select(
            F.col("game_pk"),
            F.col("play.about.atBatIndex").alias("at_bat_index"),
            F.explode(F.col("play.runners")).alias("runner")
        )

        credits_df = runners_df.select(
            F.col("game_pk"),
            F.col("at_bat_index"),
            F.explode(F.col("runner.credits")).alias("credit")
        )

        credits_extracted = credits_df.select(
            F.col("game_pk"),
            F.col("at_bat_index"),
            F.col("credit.player.id").alias("player_id"),
            F.col("credit.position.code").alias("position_code"),
            F.col("credit.position.name").alias("position_name"),
            F.col("credit.position.abbreviation").alias("position_abbrev"),
            F.col("credit.credit").alias("credit_type"),
            F.current_timestamp().alias("source_captured_at"),
        )

        return credits_extracted
