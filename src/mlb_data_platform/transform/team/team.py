"""Team.teams() transformation.

Uses the extraction registry pattern from base.py.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.team import TeamTransform
    >>>
    >>> spark = SparkSession.builder.appName("team-transform").getOrCreate()
    >>> transform = TeamTransform(spark)
    >>> results = transform(team_ids=[119], write_to_postgres=True)
"""


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ..base import BaseTransformation


class TeamTransform(BaseTransformation):
    """Team transformation using extraction registry pattern.

    Transforms raw team.team_raw JSONB data into normalized table:
    - team.team - Core team information and affiliations
    """

    def __init__(
        self,
        spark: SparkSession,
        enable_quality_checks: bool = True,
    ):
        """Initialize team transformation with extraction registry.

        Args:
            spark: SparkSession instance
            enable_quality_checks: Run data quality validation
        """
        # Initialize base with endpoint/method info
        super().__init__(
            spark=spark,
            endpoint="team",
            method="teams",
            source_table="team.team_raw",
            enable_quality_checks=enable_quality_checks,
        )

        # Register extractions
        self.register_extraction(
            name="team",
            method=self._extract_team,
            target_table="team.team",
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: str | None = None,
        team_ids: list[int] | None = None,
        sport_ids: list[int] | None = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply team-specific filters.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            team_ids: Filter by team IDs
            sport_ids: Filter by sport IDs (e.g., [1] for MLB)
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        # Apply SQL filter first if provided
        if filter_sql:
            df = df.filter(filter_sql)

        # Apply team_ids filter
        if team_ids:
            df = df.filter(F.col("team_id").isin(team_ids))

        return df

    def _extract_team(self, raw_df: DataFrame) -> DataFrame:
        """Extract and flatten team data from JSONB.

        The raw table stores an array of teams in data.teams.
        We explode this array and extract key fields.

        Schema: team.team_info
        - team_id, name, abbreviation, team_name, team_code, file_code
        - location_name, short_name, franchise_name, club_name
        - first_year_of_play, active, season
        - venue_id, venue_name
        - spring_venue_id
        - spring_league_id, spring_league_name, spring_league_abbrev
        - league_id, league_name
        - division_id, division_name
        - sport_id, sport_name
        - all_star_status
        """
        # Define schema for the JSONB data structure
        team_schema = StructType([
            StructField(
                "teams",
                ArrayType(
                    StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                        StructField("link", StringType(), True),
                        StructField("season", IntegerType(), True),
                        StructField("abbreviation", StringType(), True),
                        StructField("teamName", StringType(), True),
                        StructField("teamCode", StringType(), True),
                        StructField("fileCode", StringType(), True),
                        StructField("locationName", StringType(), True),
                        StructField("shortName", StringType(), True),
                        StructField("franchiseName", StringType(), True),
                        StructField("clubName", StringType(), True),
                        StructField("firstYearOfPlay", StringType(), True),
                        StructField("active", BooleanType(), True),
                        StructField("allStarStatus", StringType(), True),
                        StructField(
                            "venue",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "springVenue",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "springLeague",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("abbreviation", StringType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "league",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "division",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "sport",
                            StructType([
                                StructField("id", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("link", StringType(), True),
                            ]),
                            True,
                        ),
                    ])
                ),
                True,
            )
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn("parsed_data", F.from_json(F.col("data"), team_schema))

        # Explode the teams array
        exploded_df = parsed_df.select(
            F.col("team_id").alias("raw_team_id"),
            F.col("captured_at"),
            F.col("url").alias("source_url"),
            F.explode(F.col("parsed_data.teams")).alias("team"),
        )

        # Extract team fields
        team_df = exploded_df.select(
            F.col("raw_team_id"),
            F.col("captured_at"),
            F.col("source_url"),
            # Team identification
            F.col("team.id").cast(IntegerType()).alias("team_id"),
            F.col("team.name").cast(StringType()).alias("name"),
            F.col("team.abbreviation").cast(StringType()).alias("abbreviation"),
            F.col("team.teamName").cast(StringType()).alias("team_name"),
            F.col("team.teamCode").cast(StringType()).alias("team_code"),
            F.col("team.fileCode").cast(StringType()).alias("file_code"),
            # Location info
            F.col("team.locationName").cast(StringType()).alias("location_name"),
            F.col("team.shortName").cast(StringType()).alias("short_name"),
            F.col("team.franchiseName").cast(StringType()).alias("franchise_name"),
            F.col("team.clubName").cast(StringType()).alias("club_name"),
            # Season and status
            F.col("team.season").cast(IntegerType()).alias("season"),
            F.col("team.firstYearOfPlay").cast(StringType()).alias("first_year_of_play"),
            F.col("team.active").cast("boolean").alias("active"),
            F.col("team.allStarStatus").cast(StringType()).alias("all_star_status"),
            # Venue
            F.col("team.venue.id").cast(IntegerType()).alias("venue_id"),
            F.col("team.venue.name").cast(StringType()).alias("venue_name"),
            # Spring venue
            F.col("team.springVenue.id").cast(IntegerType()).alias("spring_venue_id"),
            # Spring league
            F.col("team.springLeague.id").cast(IntegerType()).alias("spring_league_id"),
            F.col("team.springLeague.name").cast(StringType()).alias("spring_league_name"),
            F.col("team.springLeague.abbreviation").cast(StringType()).alias("spring_league_abbrev"),
            # League
            F.col("team.league.id").cast(IntegerType()).alias("league_id"),
            F.col("team.league.name").cast(StringType()).alias("league_name"),
            # Division
            F.col("team.division.id").cast(IntegerType()).alias("division_id"),
            F.col("team.division.name").cast(StringType()).alias("division_name"),
            # Sport
            F.col("team.sport.id").cast(IntegerType()).alias("sport_id"),
            F.col("team.sport.name").cast(StringType()).alias("sport_name"),
        )

        return team_df
