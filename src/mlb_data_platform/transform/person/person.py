"""Person.person() transformation.

Uses the extraction registry pattern from base.py.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.person import PersonTransform
    >>>
    >>> spark = SparkSession.builder.appName("person-transform").getOrCreate()
    >>> transform = PersonTransform(spark)
    >>> results = transform(person_ids=[660271], write_to_postgres=True)
"""


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

from ..base import BaseTransformation


class PersonTransform(BaseTransformation):
    """Person transformation using extraction registry pattern.

    Transforms raw person.person_raw JSONB data into normalized table:
    - person.person - Core biographical and position information
    """

    def __init__(
        self,
        spark: SparkSession,
        enable_quality_checks: bool = True,
    ):
        """Initialize person transformation with extraction registry.

        Args:
            spark: SparkSession instance
            enable_quality_checks: Run data quality validation
        """
        # Initialize base with endpoint/method info
        super().__init__(
            spark=spark,
            endpoint="person",
            method="person",
            source_table="person.person_raw",
            enable_quality_checks=enable_quality_checks,
        )

        # Register extractions
        self.register_extraction(
            name="person",
            method=self._extract_person,
            target_table="person.person",
        )

    def _apply_filters(
        self,
        df: DataFrame,
        filter_sql: str | None = None,
        person_ids: list[int] | None = None,
        **filter_kwargs,
    ) -> DataFrame:
        """Apply person-specific filters.

        Args:
            df: DataFrame to filter
            filter_sql: SQL WHERE clause
            person_ids: Filter by person IDs
            **filter_kwargs: Additional filter parameters

        Returns:
            Filtered DataFrame
        """
        # Apply SQL filter first if provided
        if filter_sql:
            df = df.filter(filter_sql)

        # Apply person_ids filter
        if person_ids:
            df = df.filter(F.col("person_id").isin(person_ids))

        return df

    def _extract_person(self, raw_df: DataFrame) -> DataFrame:
        """Extract and flatten person data from JSONB.

        The raw table stores an array of people in data.people.
        We explode this array and extract key fields.

        Schema: person.person_info
        - person_id, full_name, first_name, last_name
        - birth_date, birth_city, birth_country, current_age
        - height, weight, active, is_player, is_verified
        - primary_position_code, primary_position_name, primary_position_abbrev
        - bat_side_code, bat_side_desc, pitch_hand_code, pitch_hand_desc
        - mlb_debut_date, pronunciation, gender
        - use_name, box_score_name, nick_name
        - strike_zone_top, strike_zone_bottom
        """
        # Define schema for the JSONB data structure
        person_schema = StructType([
            StructField(
                "people",
                ArrayType(
                    StructType([
                        StructField("id", IntegerType(), True),
                        StructField("fullName", StringType(), True),
                        StructField("link", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("primaryNumber", StringType(), True),
                        StructField("birthDate", StringType(), True),
                        StructField("currentAge", IntegerType(), True),
                        StructField("birthCity", StringType(), True),
                        StructField("birthStateProvince", StringType(), True),
                        StructField("birthCountry", StringType(), True),
                        StructField("height", StringType(), True),
                        StructField("weight", IntegerType(), True),
                        StructField("active", BooleanType(), True),
                        StructField("isPlayer", BooleanType(), True),
                        StructField("isVerified", BooleanType(), True),
                        StructField("useName", StringType(), True),
                        StructField("useLastName", StringType(), True),
                        StructField("boxscoreName", StringType(), True),
                        StructField("nickName", StringType(), True),
                        StructField("gender", StringType(), True),
                        StructField("pronunciation", StringType(), True),
                        StructField("mlbDebutDate", StringType(), True),
                        StructField("nameSlug", StringType(), True),
                        StructField("strikeZoneTop", DoubleType(), True),
                        StructField("strikeZoneBottom", DoubleType(), True),
                        StructField(
                            "primaryPosition",
                            StructType([
                                StructField("code", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("abbreviation", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "batSide",
                            StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]),
                            True,
                        ),
                        StructField(
                            "pitchHand",
                            StructType([
                                StructField("code", StringType(), True),
                                StructField("description", StringType(), True),
                            ]),
                            True,
                        ),
                    ])
                ),
                True,
            )
        ])

        # Parse JSONB string to structured data
        parsed_df = raw_df.withColumn("parsed_data", F.from_json(F.col("data"), person_schema))

        # Explode the people array
        exploded_df = parsed_df.select(
            F.col("person_id").alias("raw_person_id"),
            F.col("captured_at"),
            F.col("url").alias("source_url"),
            F.explode(F.col("parsed_data.people")).alias("person"),
        )

        # Extract person fields
        person_df = exploded_df.select(
            F.col("raw_person_id"),
            F.col("captured_at"),
            F.col("source_url"),
            # Person identification
            F.col("person.id").cast(IntegerType()).alias("person_id"),
            F.col("person.fullName").cast(StringType()).alias("full_name"),
            F.col("person.firstName").cast(StringType()).alias("first_name"),
            F.col("person.lastName").cast(StringType()).alias("last_name"),
            F.col("person.primaryNumber").cast(StringType()).alias("primary_number"),
            F.col("person.nameSlug").cast(StringType()).alias("name_slug"),
            # Biographical info
            F.col("person.birthDate").cast(DateType()).alias("birth_date"),
            F.col("person.currentAge").cast(IntegerType()).alias("current_age"),
            F.col("person.birthCity").cast(StringType()).alias("birth_city"),
            F.col("person.birthStateProvince").cast(StringType()).alias("birth_state_province"),
            F.col("person.birthCountry").cast(StringType()).alias("birth_country"),
            F.col("person.height").cast(StringType()).alias("height"),
            F.col("person.weight").cast(IntegerType()).alias("weight"),
            F.col("person.gender").cast(StringType()).alias("gender"),
            # Status flags
            F.col("person.active").cast("boolean").alias("active"),
            F.col("person.isPlayer").cast("boolean").alias("is_player"),
            F.col("person.isVerified").cast("boolean").alias("is_verified"),
            # Names
            F.col("person.useName").cast(StringType()).alias("use_name"),
            F.col("person.useLastName").cast(StringType()).alias("use_last_name"),
            F.col("person.boxscoreName").cast(StringType()).alias("boxscore_name"),
            F.col("person.nickName").cast(StringType()).alias("nick_name"),
            F.col("person.pronunciation").cast(StringType()).alias("pronunciation"),
            # Career info
            F.col("person.mlbDebutDate").cast(DateType()).alias("mlb_debut_date"),
            # Primary position
            F.col("person.primaryPosition.code").cast(StringType()).alias("primary_position_code"),
            F.col("person.primaryPosition.name").cast(StringType()).alias("primary_position_name"),
            F.col("person.primaryPosition.type").cast(StringType()).alias("primary_position_type"),
            F.col("person.primaryPosition.abbreviation").cast(StringType()).alias(
                "primary_position_abbrev"
            ),
            # Bat/pitch handedness
            F.col("person.batSide.code").cast(StringType()).alias("bat_side_code"),
            F.col("person.batSide.description").cast(StringType()).alias("bat_side_desc"),
            F.col("person.pitchHand.code").cast(StringType()).alias("pitch_hand_code"),
            F.col("person.pitchHand.description").cast(StringType()).alias("pitch_hand_desc"),
            # Strike zone
            F.col("person.strikeZoneTop").cast("double").alias("strike_zone_top"),
            F.col("person.strikeZoneBottom").cast("double").alias("strike_zone_bottom"),
        )

        return person_df
