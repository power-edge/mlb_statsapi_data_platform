"""Season.seasons() transformation - Refactored with base_v2.

This is the refactored version using the extraction registry pattern from base_v2.py.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.season.seasons_v2 import SeasonTransformV2
    >>>
    >>> spark = SparkSession.builder.appName("season-transform").getOrCreate()
    >>> transform = SeasonTransformV2(spark)
    >>> results = transform(sport_ids=[1], write_to_postgres=True)
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
    StringType,
    StructField,
    StructType,
)

from ..base_v2 import BaseTransformation


class SeasonTransformV2(BaseTransformation):
    """Refactored season transformation using extraction registry pattern.

    Transforms raw season.seasons JSONB data into normalized table:
    - season.seasons_normalized - Flattened season metadata with dates
    """

    def __init__(
        self,
        spark: SparkSession,
        enable_quality_checks: bool = True,
    ):
        """Initialize season transformation with extraction registry.

        Args:
            spark: SparkSession instance
            enable_quality_checks: Run data quality validation
        """
        # Initialize base with endpoint/method info
        super().__init__(
            spark=spark,
            endpoint="season",
            method="seasons",
            source_table="season.seasons",
            enable_quality_checks=enable_quality_checks,
        )

        # Register single extraction
        self.register_extraction(
            name="seasons",
            method=self._extract_seasons,
            target_table="season.seasons_normalized",
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: Optional[str] = None,
        sport_ids: Optional[List[int]] = None,
        season_ids: Optional[List[str]] = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply season-specific filters.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            sport_ids: Filter by sport IDs (e.g., [1] for MLB)
            season_ids: Filter by season IDs (e.g., ["2024", "2025"])
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        # Apply SQL filter first if provided
        if filter_sql:
            df = df.filter(filter_sql)

        # Apply sport_ids filter
        if sport_ids:
            df = df.filter(F.col("sport_id").isin(sport_ids))

        # Apply season_ids filter (need to parse JSONB)
        if season_ids:
            # For season_ids, we need to check inside the JSONB array
            # This is more complex, so we'll do it in the extraction step
            pass

        return df

    def _extract_seasons(self, raw_df: DataFrame) -> DataFrame:
        """Extract and flatten season data from JSONB.

        The raw table stores an array of seasons in data.seasons.
        We explode this array and extract key fields.

        Schema: season.seasons_normalized
        - season_id, sport_id
        - regular_season_start_date, regular_season_end_date
        - season_start_date, season_end_date
        - spring_start_date, spring_end_date
        - has_wildcard, all_star_date
        - game_level_gameday_type, season_level_gameday_type
        - qualifier_plate_appearances, qualifier_outs_pitched
        """
        # Define schema for the JSONB data structure
        season_schema = StructType([
            StructField(
                "seasons",
                ArrayType(
                    StructType([
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
                    ])
                ),
                True,
            )
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn("parsed_data", F.from_json(F.col("data"), season_schema))

        # Explode the seasons array
        exploded_df = parsed_df.select(
            F.col("id").alias("raw_id"),
            F.col("sport_id"),
            F.col("captured_at"),
            F.col("source_url"),
            F.explode(F.col("parsed_data.seasons")).alias("season"),
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
