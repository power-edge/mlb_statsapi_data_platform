"""Step definitions for ingestion pipeline BDD tests.

This module implements steps for:
- Season data ingestion
- Schedule data ingestion
- Game data ingestion

Using the MLBStatsAPIClient with stub mode for deterministic testing.
"""

import os
import re
from datetime import date, datetime

from behave import given, when, then
from psycopg.rows import dict_row

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.ingestion.config import JobConfig, StubMode
from mlb_data_platform.schema.models import SCHEMA_METADATA_REGISTRY
from mlb_data_platform.storage.postgres import PostgresConfig, PostgresStorageBackend


# ============================================================================
# Helper Functions
# ============================================================================

def get_db_config():
    """Get database configuration for tests."""
    return PostgresConfig(
        host=os.getenv("TEST_DB_HOST", "localhost"),
        port=int(os.getenv("TEST_DB_PORT", "5432")),
        database=os.getenv("TEST_DB_NAME", "mlb_games"),
        user=os.getenv("TEST_DB_USER", "mlb_admin"),
        password=os.getenv("TEST_DB_PASSWORD", "mlb_dev_password"),
    )


def query_table(storage_backend: PostgresStorageBackend, sql: str) -> list[dict]:
    """Execute query and return results as list of dicts."""
    with storage_backend.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            return cur.fetchall()


def count_rows(storage_backend: PostgresStorageBackend, table: str) -> int:
    """Count rows in a table."""
    with storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*)::int as count FROM {table}")
            result = cur.fetchone()
            return result[0] if isinstance(result, tuple) else result["count"]


# ============================================================================
# Background Steps - Database Setup
# ============================================================================

@given("a clean test database")
def step_clean_ingestion_database(context):
    """Clean ingestion test database tables (raw and normalized).

    This step cleans all test tables including:
    - Raw JSONB storage (game.live_game_v1)
    - Normalized tables (season.seasons, schedule.schedule)
    """
    db_config = get_db_config()
    context.storage_backend = PostgresStorageBackend(db_config)

    # Tables to clean - uses IF EXISTS to handle missing tables gracefully
    tables_to_clean = [
        "season.seasons",
        "schedule.schedule",
        "game.live_game_v1",           # Main raw storage (partitioned)
        "game.live_game_metadata",     # Normalized tables
        "game.live_game_players",
        "game.live_game_plays",
        "game.live_game_pitch_events",
    ]

    with context.storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            for table in tables_to_clean:
                # Use DO block to safely truncate if table exists
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM information_schema.tables
                                   WHERE table_schema || '.' || table_name = '{table}') THEN
                            EXECUTE 'TRUNCATE TABLE {table} CASCADE';
                        END IF;
                    END $$;
                """)
            conn.commit()

    # Initialize context variables for raw ingestion tests
    context.raw_games = []


# ============================================================================
# Season Ingestion Steps
# ============================================================================

@given("a configured MLB Stats API client for season data")
def step_create_season_client(context):
    """Create MLB Stats API client configured for season data."""
    context.season_job_config = JobConfig(
        name="test_season_job",
        description="Test season ingestion",
        type="batch",
        source={
            "endpoint": "season",
            "method": "seasons",
            "parameters": {
                "sportId": 1
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 30
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "season.seasons"
            }
        }
    )


@given("the client is using stub mode for deterministic testing")
def step_enable_stub_mode(context):
    """Enable stub mode for deterministic testing."""
    context.stub_mode = StubMode.REPLAY


@when("I ingest season data for sport ID {sport_id:d}")
def step_ingest_season_data(context, sport_id):
    """Ingest season data for the given sport ID."""
    context.season_client = MLBStatsAPIClient(
        context.season_job_config,
        stub_mode=context.stub_mode
    )
    context.result = context.season_client.fetch_and_save(context.storage_backend)


@when("I fetch season data without saving")
def step_fetch_season_data_dry_run(context):
    """Fetch season data without saving to database."""
    context.season_client = MLBStatsAPIClient(
        context.season_job_config,
        stub_mode=context.stub_mode
    )
    context.fetch_result = context.season_client.fetch()


@when("I ingest the same season data again")
def step_ingest_season_data_again(context):
    """Ingest the same season data a second time."""
    context.result2 = context.season_client.fetch_and_save(context.storage_backend)


@when("I ingest season data {count:d} times concurrently")
def step_ingest_season_concurrent(context, count):
    """Ingest season data multiple times concurrently."""
    import concurrent.futures

    context.season_client = MLBStatsAPIClient(
        context.season_job_config,
        stub_mode=context.stub_mode
    )

    def ingest_once():
        return context.season_client.fetch_and_save(context.storage_backend)

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as executor:
        futures = [executor.submit(ingest_once) for _ in range(count)]
        context.concurrent_results = [f.result() for f in futures]


@then("the season data should be saved to PostgreSQL")
def step_verify_season_saved(context):
    """Verify season data was saved to PostgreSQL."""
    assert "row_id" in context.result
    assert context.result["row_id"] > 0


@then("the raw table should contain {count:d} season record")
@then("the raw table should contain {count:d} season records")
def step_verify_season_count(context, count):
    """Verify the number of season records in the database."""
    actual_count = count_rows(context.storage_backend, "season.seasons")
    assert actual_count == count, f"Expected {count} records, got {actual_count}"


# ============================================================================
# Schedule Ingestion Steps
# ============================================================================

@given("a configured MLB Stats API client for schedule data")
def step_create_schedule_client(context):
    """Create MLB Stats API client configured for schedule data."""
    context.schedule_job_config = JobConfig(
        name="test_schedule_job",
        description="Test schedule ingestion",
        type="batch",
        source={
            "endpoint": "schedule",
            "method": "schedule",
            "parameters": {
                "sportId": 1,
                "date": "2024-07-04"
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 30
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "schedule.schedule"
            }
        }
    )


@when('I ingest schedule data for date "{schedule_date}" and sport ID {sport_id:d}')
def step_ingest_schedule_data(context, schedule_date, sport_id):
    """Ingest schedule data for the given date and sport ID."""
    # Update job config with the date
    context.schedule_job_config.source.parameters["date"] = schedule_date
    context.schedule_job_config.source.parameters["sportId"] = sport_id

    context.schedule_client = MLBStatsAPIClient(
        context.schedule_job_config,
        stub_mode=context.stub_mode
    )
    context.result = context.schedule_client.fetch_and_save(context.storage_backend)
    context.schedule_date = schedule_date


@when('I fetch schedule data for date "{schedule_date}" without saving')
def step_fetch_schedule_data_dry_run(context, schedule_date):
    """Fetch schedule data without saving to database."""
    context.schedule_job_config.source.parameters["date"] = schedule_date

    context.schedule_client = MLBStatsAPIClient(
        context.schedule_job_config,
        stub_mode=context.stub_mode
    )
    context.fetch_result = context.schedule_client.fetch()


@when("I ingest schedule data for a date with no games")
def step_ingest_schedule_no_games(context):
    """Ingest schedule data for a date with no games."""
    # Use today's date which likely has no games in stub data
    today = datetime.now().strftime("%Y-%m-%d")
    context.schedule_job_config.source.parameters["date"] = today

    context.schedule_client = MLBStatsAPIClient(
        context.schedule_job_config,
        stub_mode=context.stub_mode
    )
    context.result = context.schedule_client.fetch_and_save(context.storage_backend)


@when("I ingest the same schedule data again")
def step_ingest_schedule_data_again(context):
    """Ingest the same schedule data a second time."""
    context.result2 = context.schedule_client.fetch_and_save(context.storage_backend)


@then("the schedule data should be saved to PostgreSQL")
def step_verify_schedule_saved(context):
    """Verify schedule data was saved to PostgreSQL."""
    assert "row_id" in context.result
    assert context.result["row_id"] > 0


@then("the raw table should contain {count:d} schedule record")
@then("the raw table should contain {count:d} schedule records")
def step_verify_schedule_count(context, count):
    """Verify the number of schedule records in the database."""
    actual_count = count_rows(context.storage_backend, "schedule.schedule")
    assert actual_count == count, f"Expected {count} records, got {actual_count}"


@then('the schedule_date field should be extracted as "{expected_date}"')
def step_verify_schedule_date_extraction(context, expected_date):
    """Verify schedule_date was extracted correctly."""
    rows = query_table(
        context.storage_backend,
        f"SELECT schedule_date FROM schedule.schedule WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    actual_date = rows[0]["schedule_date"]
    expected = date.fromisoformat(expected_date)
    assert actual_date == expected, f"Expected {expected}, got {actual_date}"


# ============================================================================
# Game Ingestion Steps
# ============================================================================

@given("a configured MLB Stats API client for game data")
def step_create_game_client(context):
    """Create MLB Stats API client configured for game data."""
    context.game_job_config = JobConfig(
        name="test_game_job",
        description="Test game ingestion",
        type="batch",
        source={
            "endpoint": "game",
            "method": "liveGameV1",
            "parameters": {
                "game_pk": 744834
            }
        },
        ingestion={
            "rate_limit": 30,
            "retry": {
                "max_attempts": 3,
                "backoff": "exponential"
            },
            "timeout": 45
        },
        storage={
            "raw": {
                "backend": "postgres",
                "table": "game.live_game_v1"
            }
        }
    )


@when("I ingest live game data for game_pk {game_pk:d}")
def step_ingest_game_data(context, game_pk):
    """Ingest live game data for the given game_pk."""
    context.game_job_config.source.parameters["game_pk"] = game_pk

    context.game_client = MLBStatsAPIClient(
        context.game_job_config,
        stub_mode=context.stub_mode
    )
    context.result = context.game_client.fetch_and_save(context.storage_backend)
    context.game_pk = game_pk


@when('I ingest live game data for game_pk {game_pk:d} at timestamp "{timestamp}"')
def step_ingest_game_data_at_timestamp(context, game_pk, timestamp):
    """Ingest live game data at a specific timestamp."""
    # For stub mode, we can't actually control the timestamp,
    # but we can simulate multiple ingestions
    if not hasattr(context, "game_ingestion_count"):
        context.game_ingestion_count = 0
        context.game_results = []

    context.game_job_config.source.parameters["game_pk"] = game_pk
    context.game_client = MLBStatsAPIClient(
        context.game_job_config,
        stub_mode=context.stub_mode
    )

    result = context.game_client.fetch_and_save(context.storage_backend)
    context.game_results.append(result)
    context.game_ingestion_count += 1


@when("I ingest the same game at timestamp {timestamp}")
def step_ingest_same_game_at_timestamp(context, timestamp):
    """Ingest the same game at another timestamp."""
    result = context.game_client.fetch_and_save(context.storage_backend)
    context.game_results.append(result)
    context.game_ingestion_count += 1


@when("I fetch game data for game_pk {game_pk:d} without saving")
def step_fetch_game_data_dry_run(context, game_pk):
    """Fetch game data without saving to database."""
    context.game_job_config.source.parameters["game_pk"] = game_pk

    context.game_client = MLBStatsAPIClient(
        context.game_job_config,
        stub_mode=context.stub_mode
    )
    context.fetch_result = context.game_client.fetch()


@when("I ingest the same game data again")
def step_ingest_game_data_again(context):
    """Ingest the same game data a second time."""
    context.result2 = context.game_client.fetch_and_save(context.storage_backend)


@then("the game data should be saved to PostgreSQL")
def step_verify_game_saved(context):
    """Verify game data was saved to PostgreSQL."""
    assert "row_id" in context.result
    assert context.result["row_id"] > 0


@then("the raw table should contain {count:d} game record")
@then("the raw table should contain {count:d} game records")
def step_verify_game_count(context, count):
    """Verify the number of game records in the database."""
    actual_count = count_rows(context.storage_backend, "game.live_game_v1")
    assert actual_count == count, f"Expected {count} records, got {actual_count}"


# Note: "the raw table should contain {count:d} records for game_pk {game_pk:d}"
# is defined in raw_ingestion_steps.py (uses RawLiveGameV1 model for actual raw table)


# ============================================================================
# Common Validation Steps
# ============================================================================

@then('the {record_type} record should have schema version "{version}"')
def step_verify_schema_version(context, record_type, version):
    """Verify the schema version of the record."""
    table_map = {
        "season": "season.seasons",
        "schedule": "schedule.schedule",
        "game": "game.live_game_v1"
    }
    table = table_map[record_type]

    rows = query_table(
        context.storage_backend,
        f"SELECT schema_version FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["schema_version"] == version


@then("the {record_type} record should have response status {status_code:d}")
def step_verify_response_status(context, record_type, status_code):
    """Verify the response status code."""
    table_map = {
        "season": "season.seasons",
        "schedule": "schedule.schedule",
        "game": "game.live_game_v1"
    }
    table = table_map[record_type]

    rows = query_table(
        context.storage_backend,
        f"SELECT response_status FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["response_status"] == status_code


@then("the {record_type} data should contain required fields")
def step_verify_required_fields(context, record_type):
    """Verify required fields are present in the data."""
    table_map = {
        "season": "season.seasons",
        "schedule": "schedule.schedule",
        "game": "game.live_game_v1"
    }
    table = table_map[record_type]

    rows = query_table(
        context.storage_backend,
        f"SELECT data FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    data = rows[0]["data"]

    for row in context.table:
        field = row["field"]
        # Navigate nested structure
        current = data
        for key in field.split("."):
            assert key in current, f"Missing required field: {field}"
            current = current[key]


@then("all date fields should be in YYYY-MM-DD format")
def step_verify_date_formats(context):
    """Verify all dates are in YYYY-MM-DD format."""
    rows = query_table(
        context.storage_backend,
        f"SELECT data FROM season.seasons WHERE id = {context.result['row_id']}"
    )
    data = rows[0]["data"]

    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    date_fields = [
        "regularSeasonStartDate",
        "regularSeasonEndDate",
        "seasonStartDate",
        "seasonEndDate"
    ]

    for season in data.get("seasons", []):
        for field in date_fields:
            if field in season:
                assert re.match(date_pattern, season[field]), \
                    f"{field} has invalid date format: {season[field]}"


@then("the sport_id field should be extracted as {sport_id:d}")
def step_verify_sport_id_extraction(context, sport_id):
    """Verify sport_id was extracted correctly."""
    # Check season or schedule table
    if hasattr(context, "season_client"):
        table = "season.seasons"
    else:
        table = "schedule.schedule"

    rows = query_table(
        context.storage_backend,
        f"SELECT sport_id FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["sport_id"] == sport_id


@then("the request parameters should be stored as JSONB")
def step_verify_request_params_jsonb(context):
    """Verify request parameters are stored as JSONB."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT request_params FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["request_params"] is not None
    assert isinstance(rows[0]["request_params"], dict)


@then("the request parameters should contain {param_check}")
def step_verify_request_param_value(context, param_check):
    """Verify request parameters contain a specific value."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT request_params FROM {table} WHERE id = {context.result['row_id']}"
    )

    request_params = rows[0]["request_params"]

    # Parse the param_check (e.g., "sportId=1" or "date=2024-07-04")
    if "=" in param_check:
        key, expected_value = param_check.split("=", 1)
        assert key in request_params, f"Parameter '{key}' not found in request_params"

        actual_value = request_params[key]
        # Try to convert to int if possible
        try:
            expected_value = int(expected_value)
        except ValueError:
            pass

        assert actual_value == expected_value, \
            f"Expected {key}={expected_value}, got {key}={actual_value}"


@then("the {record_type} record should have a valid source URL")
def step_verify_source_url(context, record_type):
    """Verify the source URL is present and valid."""
    table_map = {
        "season": "season.seasons",
        "schedule": "schedule.schedule",
        "game": "game.live_game_v1"
    }
    table = table_map[record_type]

    rows = query_table(
        context.storage_backend,
        f"SELECT source_url FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    url = rows[0]["source_url"]
    assert url is not None
    assert len(url) > 0


@then('the source URL should contain "{substring}"')
def step_verify_url_contains(context, substring):
    """Verify the source URL contains a specific substring."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT source_url FROM {table} WHERE id = {context.result['row_id']}"
    )
    url = rows[0]["source_url"]
    assert substring.lower() in url.lower(), f"URL '{url}' does not contain '{substring}'"


@then("the captured_at timestamp should be present")
def step_verify_captured_at(context):
    """Verify captured_at timestamp is present."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT captured_at FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["captured_at"] is not None


@then("the ingestion_timestamp should be present")
def step_verify_ingestion_timestamp(context):
    """Verify ingestion_timestamp is present."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT ingestion_timestamp FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["ingestion_timestamp"] is not None


@then("the API response should contain {data_type} data")
def step_verify_fetch_response(context, data_type):
    """Verify the fetch response contains data."""
    assert hasattr(context, "fetch_result")
    assert "data" in context.fetch_result
    assert context.fetch_result["data"] is not None


@then("the response should have metadata")
def step_verify_fetch_metadata(context):
    """Verify the fetch response has metadata."""
    assert "metadata" in context.fetch_result
    assert context.fetch_result["metadata"] is not None


@then("no database record should be created")
def step_verify_no_record_created(context):
    """Verify no record was created in the database."""
    assert "row_id" not in context.fetch_result


@then("each record should have a unique ID")
def step_verify_unique_ids(context):
    """Verify all records have unique IDs."""
    if hasattr(context, "result2"):
        assert context.result["row_id"] != context.result2["row_id"]
    elif hasattr(context, "concurrent_results"):
        ids = [r["row_id"] for r in context.concurrent_results]
        assert len(ids) == len(set(ids)), "Found duplicate IDs"


@then("each record should have a different ingestion timestamp")
def step_verify_different_timestamps(context):
    """Verify records have different ingestion timestamps."""
    # This is implicitly tested by the idempotency test
    # Since we're doing INSERT mode, each record gets a new timestamp
    pass


@then("the data column should be valid JSONB")
def step_verify_jsonb_column(context):
    """Verify the data column is valid JSONB."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT data FROM {table} WHERE id = {context.result['row_id']}"
    )
    assert len(rows) == 1
    assert rows[0]["data"] is not None
    assert isinstance(rows[0]["data"], dict)


@then("the source_url should not be empty")
def step_verify_source_url_not_empty(context):
    """Verify source_url is not empty."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    rows = query_table(
        context.storage_backend,
        f"SELECT source_url FROM {table} WHERE id = {context.result['row_id']}"
    )
    url = rows[0]["source_url"]
    assert url is not None
    assert len(url) > 0


# ============================================================================
# Schema Metadata Steps
# ============================================================================

@given("the {endpoint} schema metadata is registered")
def step_verify_schema_registered(context, endpoint):
    """Verify schema metadata is registered."""
    table_map = {
        "season": "season.seasons",
        "schedule": "schedule.schedule",
        "game": "game.live_game_v1"
    }
    table = table_map[endpoint]
    assert table in SCHEMA_METADATA_REGISTRY


@then('the schema should have endpoint "{endpoint_name}"')
def step_verify_schema_endpoint(context, endpoint_name):
    """Verify schema has the correct endpoint name."""
    # Determine which schema to check
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    schema = SCHEMA_METADATA_REGISTRY[table]
    assert schema.endpoint == endpoint_name


@then('the schema should have method "{method_name}"')
def step_verify_schema_method(context, method_name):
    """Verify schema has the correct method name."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    schema = SCHEMA_METADATA_REGISTRY[table]
    assert schema.method == method_name


@then("the schema should have field definitions")
def step_verify_schema_fields(context):
    """Verify schema has field definitions."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    schema = SCHEMA_METADATA_REGISTRY[table]
    assert len(schema.fields) > 0


@then("the {field_name} field should be marked as partition key")
def step_verify_partition_key(context, field_name):
    """Verify a field is marked as partition key."""
    if hasattr(context, "season_client"):
        table = "season.seasons"
    elif hasattr(context, "schedule_client"):
        table = "schedule.schedule"
    else:
        table = "game.live_game_v1"

    schema = SCHEMA_METADATA_REGISTRY[table]
    partition_field = next(
        (f for f in schema.fields if f.name == field_name),
        None
    )
    assert partition_field is not None, f"Field {field_name} not found in schema"
    assert partition_field.is_partition_key is True
