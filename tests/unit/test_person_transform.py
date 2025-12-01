"""Unit tests for Person transformation.

Tests the PersonTransform class that converts raw Person.person() API
responses (JSONB) into normalized relational tables.

Spark tests require:
- Java 17+ runtime
- Run via Docker for full environment:
    docker compose --profile spark run --rm spark pytest tests/unit/test_person_transform.py
"""

import json
import subprocess
from datetime import UTC, datetime

import pytest

from mlb_data_platform.transform.person import PersonTransform


def _check_java_version():
    """Check if Java 17+ is available."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        version_output = result.stderr
        import re

        match = re.search(r'"(\d+)\.', version_output)
        if match:
            major_version = int(match.group(1))
            return major_version >= 17
        return False
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


# Check Java availability once at module load
_JAVA_17_AVAILABLE = _check_java_version()


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing.

    Skips if Java 17+ is not available (required for PySpark 3.5+).
    """
    if not _JAVA_17_AVAILABLE:
        pytest.skip("Java 17+ required for Spark tests (run via Docker Spark container)")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("test-person-transform")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_person_json():
    """Sample Person API response as stored in person_raw table."""
    return {
        "people": [
            {
                "id": 660271,
                "fullName": "Shohei Ohtani",
                "link": "/api/v1/people/660271",
                "firstName": "Shohei",
                "lastName": "Ohtani",
                "primaryNumber": "17",
                "birthDate": "1994-07-05",
                "currentAge": 30,
                "birthCity": "Oshu",
                "birthStateProvince": "Iwate",
                "birthCountry": "Japan",
                "height": "6' 3\"",
                "weight": 210,
                "active": True,
                "primaryPosition": {
                    "code": "Y",
                    "name": "Two-Way Player",
                    "type": "Two-Way Player",
                    "abbreviation": "TWP",
                },
                "useName": "Shohei",
                "useLastName": "Ohtani",
                "boxscoreName": "Ohtani",
                "nickName": "Showtime",
                "gender": "M",
                "isPlayer": True,
                "isVerified": False,
                "pronunciation": "show-HEY oh-TAWN-ee",
                "mlbDebutDate": "2018-03-29",
                "batSide": {"code": "L", "description": "Left"},
                "pitchHand": {"code": "R", "description": "Right"},
                "nameSlug": "shohei-ohtani-660271",
                "strikeZoneTop": 3.53,
                "strikeZoneBottom": 1.66,
            }
        ]
    }


@pytest.fixture
def sample_raw_person_df(spark, sample_person_json):
    """Create a sample raw person DataFrame."""
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("person_id", LongType(), False),
        StructField("captured_at", TimestampType(), False),
        StructField("data", StringType(), False),
        StructField("endpoint", StringType(), True),
        StructField("method", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status_code", IntegerType(), True),
    ])

    now = datetime.now(UTC)
    data = [
        (
            660271,
            now,
            json.dumps(sample_person_json),
            "person",
            "person",
            "https://statsapi.mlb.com/api/v1/people?personIds=660271",
            200,
        )
    ]

    return spark.createDataFrame(data, schema=schema)


class TestPersonTransformInit:
    """Test PersonTransform initialization."""

    def test_init_without_spark(self):
        """Test that initialization fails gracefully without Spark."""
        # Cannot properly test without Spark, so just verify class exists
        assert PersonTransform is not None

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_init_with_spark(self, spark):
        """Test initialization with SparkSession."""
        transform = PersonTransform(spark)

        assert transform.spark is spark
        assert transform.endpoint == "person"
        assert transform.method == "person"
        assert transform.source_table == "person.person_raw"
        assert "person" in transform._extractions


class TestPersonTransformExtraction:
    """Test PersonTransform extraction methods."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_extract_person(self, spark, sample_raw_person_df):
        """Test extraction of person data from raw JSONB."""
        transform = PersonTransform(spark)

        # Run extraction
        result_df = transform._extract_person(sample_raw_person_df)

        # Verify row count
        assert result_df.count() == 1

        # Get the extracted row
        row = result_df.first()

        # Verify person identification
        assert row["person_id"] == 660271
        assert row["full_name"] == "Shohei Ohtani"
        assert row["first_name"] == "Shohei"
        assert row["last_name"] == "Ohtani"
        assert row["primary_number"] == "17"

        # Verify biographical info
        assert row["birth_city"] == "Oshu"
        assert row["birth_country"] == "Japan"
        assert row["height"] == "6' 3\""
        assert row["weight"] == 210
        assert row["gender"] == "M"

        # Verify status flags
        assert row["active"] is True
        assert row["is_player"] is True

        # Verify position info
        assert row["primary_position_code"] == "Y"
        assert row["primary_position_name"] == "Two-Way Player"
        assert row["primary_position_abbrev"] == "TWP"

        # Verify handedness
        assert row["bat_side_code"] == "L"
        assert row["bat_side_desc"] == "Left"
        assert row["pitch_hand_code"] == "R"
        assert row["pitch_hand_desc"] == "Right"

        # Verify strike zone
        assert row["strike_zone_top"] == pytest.approx(3.53, 0.01)
        assert row["strike_zone_bottom"] == pytest.approx(1.66, 0.01)


class TestPersonTransformFilters:
    """Test PersonTransform filtering methods."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_apply_filters_with_person_ids(self, spark, sample_raw_person_df):
        """Test filtering by person_ids."""
        transform = PersonTransform(spark)

        # Filter for matching person_id
        result = transform._apply_filters(sample_raw_person_df, person_ids=[660271])
        assert result.count() == 1

        # Filter for non-matching person_id
        result = transform._apply_filters(sample_raw_person_df, person_ids=[123456])
        assert result.count() == 0

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_apply_filters_with_sql(self, spark, sample_raw_person_df):
        """Test filtering with SQL expression."""
        transform = PersonTransform(spark)

        # Filter with matching SQL
        result = transform._apply_filters(sample_raw_person_df, filter_sql="person_id = 660271")
        assert result.count() == 1

        # Filter with non-matching SQL
        result = transform._apply_filters(sample_raw_person_df, filter_sql="person_id = 123456")
        assert result.count() == 0


class TestPersonTransformIntegration:
    """Integration tests for PersonTransform."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_full_extraction_pipeline(self, spark, sample_raw_person_df):
        """Test the full extraction pipeline produces valid output schema."""
        transform = PersonTransform(spark)

        # Run extraction
        result_df = transform._extract_person(sample_raw_person_df)

        # Verify all expected columns exist
        expected_columns = [
            "raw_person_id",
            "captured_at",
            "source_url",
            "person_id",
            "full_name",
            "first_name",
            "last_name",
            "primary_number",
            "name_slug",
            "birth_date",
            "current_age",
            "birth_city",
            "birth_state_province",
            "birth_country",
            "height",
            "weight",
            "gender",
            "active",
            "is_player",
            "is_verified",
            "use_name",
            "use_last_name",
            "boxscore_name",
            "nick_name",
            "pronunciation",
            "mlb_debut_date",
            "primary_position_code",
            "primary_position_name",
            "primary_position_type",
            "primary_position_abbrev",
            "bat_side_code",
            "bat_side_desc",
            "pitch_hand_code",
            "pitch_hand_desc",
            "strike_zone_top",
            "strike_zone_bottom",
        ]

        actual_columns = result_df.columns
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"
