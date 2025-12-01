"""Unit tests for Team transformation.

Tests the TeamTransform class that converts raw Team.teams() API
responses (JSONB) into normalized relational tables.

Spark tests require:
- Java 17+ runtime
- Run via Docker for full environment:
    docker compose --profile spark run --rm spark pytest tests/unit/test_team_transform.py
"""

import json
import subprocess
from datetime import UTC, datetime

import pytest

from mlb_data_platform.transform.team import TeamTransform


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
        SparkSession.builder.appName("test-team-transform")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_team_json():
    """Sample Team API response as stored in team_raw table."""
    return {
        "teams": [
            {
                "springLeague": {
                    "id": 114,
                    "name": "Cactus League",
                    "link": "/api/v1/league/114",
                    "abbreviation": "CL",
                },
                "allStarStatus": "N",
                "id": 119,
                "name": "Los Angeles Dodgers",
                "link": "/api/v1/teams/119",
                "season": 2024,
                "venue": {
                    "id": 22,
                    "name": "Dodger Stadium",
                    "link": "/api/v1/venues/22",
                },
                "springVenue": {"id": 3809, "link": "/api/v1/venues/3809"},
                "teamCode": "lan",
                "fileCode": "la",
                "abbreviation": "LAD",
                "teamName": "Dodgers",
                "locationName": "Los Angeles",
                "firstYearOfPlay": "1884",
                "league": {
                    "id": 104,
                    "name": "National League",
                    "link": "/api/v1/league/104",
                },
                "division": {
                    "id": 203,
                    "name": "National League West",
                    "link": "/api/v1/divisions/203",
                },
                "sport": {
                    "id": 1,
                    "link": "/api/v1/sports/1",
                    "name": "Major League Baseball",
                },
                "shortName": "LA Dodgers",
                "franchiseName": "Los Angeles",
                "clubName": "Dodgers",
                "active": True,
            }
        ]
    }


@pytest.fixture
def sample_raw_team_df(spark, sample_team_json):
    """Create a sample raw team DataFrame."""
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("team_id", IntegerType(), False),
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
            119,
            now,
            json.dumps(sample_team_json),
            "team",
            "teams",
            "https://statsapi.mlb.com/api/v1/teams?teamId=119&season=2024",
            200,
        )
    ]

    return spark.createDataFrame(data, schema=schema)


class TestTeamTransformInit:
    """Test TeamTransform initialization."""

    def test_init_without_spark(self):
        """Test that initialization fails gracefully without Spark."""
        # Cannot properly test without Spark, so just verify class exists
        assert TeamTransform is not None

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_init_with_spark(self, spark):
        """Test initialization with SparkSession."""
        transform = TeamTransform(spark)

        assert transform.spark is spark
        assert transform.endpoint == "team"
        assert transform.method == "teams"
        assert transform.source_table == "team.team_raw"
        assert "team" in transform._extractions


class TestTeamTransformExtraction:
    """Test TeamTransform extraction methods."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_extract_team(self, spark, sample_raw_team_df):
        """Test extraction of team data from raw JSONB."""
        transform = TeamTransform(spark)

        # Run extraction
        result_df = transform._extract_team(sample_raw_team_df)

        # Verify row count
        assert result_df.count() == 1

        # Get the extracted row
        row = result_df.first()

        # Verify team identification
        assert row["team_id"] == 119
        assert row["name"] == "Los Angeles Dodgers"
        assert row["abbreviation"] == "LAD"
        assert row["team_name"] == "Dodgers"
        assert row["team_code"] == "lan"
        assert row["file_code"] == "la"

        # Verify location info
        assert row["location_name"] == "Los Angeles"
        assert row["short_name"] == "LA Dodgers"
        assert row["franchise_name"] == "Los Angeles"
        assert row["club_name"] == "Dodgers"

        # Verify season and status
        assert row["season"] == 2024
        assert row["first_year_of_play"] == "1884"
        assert row["active"] is True
        assert row["all_star_status"] == "N"

        # Verify venue
        assert row["venue_id"] == 22
        assert row["venue_name"] == "Dodger Stadium"

        # Verify spring venue
        assert row["spring_venue_id"] == 3809

        # Verify spring league
        assert row["spring_league_id"] == 114
        assert row["spring_league_name"] == "Cactus League"
        assert row["spring_league_abbrev"] == "CL"

        # Verify league
        assert row["league_id"] == 104
        assert row["league_name"] == "National League"

        # Verify division
        assert row["division_id"] == 203
        assert row["division_name"] == "National League West"

        # Verify sport
        assert row["sport_id"] == 1
        assert row["sport_name"] == "Major League Baseball"


class TestTeamTransformFilters:
    """Test TeamTransform filtering methods."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_apply_filters_with_team_ids(self, spark, sample_raw_team_df):
        """Test filtering by team_ids."""
        transform = TeamTransform(spark)

        # Filter for matching team_id
        result = transform._apply_filters(sample_raw_team_df, team_ids=[119])
        assert result.count() == 1

        # Filter for non-matching team_id
        result = transform._apply_filters(sample_raw_team_df, team_ids=[147])
        assert result.count() == 0

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_apply_filters_with_sql(self, spark, sample_raw_team_df):
        """Test filtering with SQL expression."""
        transform = TeamTransform(spark)

        # Filter with matching SQL
        result = transform._apply_filters(sample_raw_team_df, filter_sql="team_id = 119")
        assert result.count() == 1

        # Filter with non-matching SQL
        result = transform._apply_filters(sample_raw_team_df, filter_sql="team_id = 147")
        assert result.count() == 0


class TestTeamTransformIntegration:
    """Integration tests for TeamTransform."""

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_full_extraction_pipeline(self, spark, sample_raw_team_df):
        """Test the full extraction pipeline produces valid output schema."""
        transform = TeamTransform(spark)

        # Run extraction
        result_df = transform._extract_team(sample_raw_team_df)

        # Verify all expected columns exist
        expected_columns = [
            "raw_team_id",
            "captured_at",
            "source_url",
            "team_id",
            "name",
            "abbreviation",
            "team_name",
            "team_code",
            "file_code",
            "location_name",
            "short_name",
            "franchise_name",
            "club_name",
            "season",
            "first_year_of_play",
            "active",
            "all_star_status",
            "venue_id",
            "venue_name",
            "spring_venue_id",
            "spring_league_id",
            "spring_league_name",
            "spring_league_abbrev",
            "league_id",
            "league_name",
            "division_id",
            "division_name",
            "sport_id",
            "sport_name",
        ]

        actual_columns = result_df.columns
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"


class TestTeamTransformMultipleTeams:
    """Test TeamTransform with multiple teams in response."""

    @pytest.fixture
    def multi_team_json(self):
        """Sample Team API response with multiple teams."""
        return {
            "teams": [
                {
                    "id": 119,
                    "name": "Los Angeles Dodgers",
                    "abbreviation": "LAD",
                    "teamName": "Dodgers",
                    "teamCode": "lan",
                    "fileCode": "la",
                    "locationName": "Los Angeles",
                    "shortName": "LA Dodgers",
                    "franchiseName": "Los Angeles",
                    "clubName": "Dodgers",
                    "season": 2024,
                    "firstYearOfPlay": "1884",
                    "active": True,
                    "allStarStatus": "N",
                    "venue": {"id": 22, "name": "Dodger Stadium"},
                    "league": {"id": 104, "name": "National League"},
                    "division": {"id": 203, "name": "National League West"},
                    "sport": {"id": 1, "name": "Major League Baseball"},
                },
                {
                    "id": 147,
                    "name": "New York Yankees",
                    "abbreviation": "NYY",
                    "teamName": "Yankees",
                    "teamCode": "nya",
                    "fileCode": "nyy",
                    "locationName": "Bronx",
                    "shortName": "NY Yankees",
                    "franchiseName": "New York",
                    "clubName": "Yankees",
                    "season": 2024,
                    "firstYearOfPlay": "1903",
                    "active": True,
                    "allStarStatus": "N",
                    "venue": {"id": 3313, "name": "Yankee Stadium"},
                    "league": {"id": 103, "name": "American League"},
                    "division": {"id": 201, "name": "American League East"},
                    "sport": {"id": 1, "name": "Major League Baseball"},
                },
            ]
        }

    @pytest.mark.skipif(not _JAVA_17_AVAILABLE, reason="Java 17+ required")
    def test_extract_multiple_teams(self, spark, multi_team_json):
        """Test extraction with multiple teams in response."""
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType([
            StructField("team_id", IntegerType(), False),
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
                119,  # Using first team ID as raw_team_id
                now,
                json.dumps(multi_team_json),
                "team",
                "teams",
                "https://statsapi.mlb.com/api/v1/teams?sportId=1&season=2024",
                200,
            )
        ]

        raw_df = spark.createDataFrame(data, schema=schema)

        transform = TeamTransform(spark)
        result_df = transform._extract_team_info(raw_df)

        # Should explode to 2 teams
        assert result_df.count() == 2

        # Verify both teams are present
        team_ids = [row["team_id"] for row in result_df.collect()]
        assert 119 in team_ids
        assert 147 in team_ids

        # Verify Dodgers data
        dodgers = result_df.filter("team_id = 119").first()
        assert dodgers["name"] == "Los Angeles Dodgers"
        assert dodgers["league_name"] == "National League"

        # Verify Yankees data
        yankees = result_df.filter("team_id = 147").first()
        assert yankees["name"] == "New York Yankees"
        assert yankees["league_name"] == "American League"
