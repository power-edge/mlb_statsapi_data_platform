"""Common step definitions shared across BDD tests.

This module contains shared steps and utilities used by multiple feature files:
- Database setup and cleanup
- Storage backend utilities
- Common query helpers
"""

import os
from datetime import date

from behave import given
from psycopg.rows import dict_row

from mlb_data_platform.storage.postgres import PostgresConfig, PostgresStorageBackend


# =============================================================================
# Helper Functions (shared utilities)
# =============================================================================


def get_db_config() -> PostgresConfig:
    """Get database configuration for tests.

    Returns:
        PostgresConfig with test database settings from environment or defaults.
    """
    return PostgresConfig(
        host=os.getenv("TEST_DB_HOST", "localhost"),
        port=int(os.getenv("TEST_DB_PORT", "5432")),
        database=os.getenv("TEST_DB_NAME", "mlb_games"),
        user=os.getenv("TEST_DB_USER", "mlb_admin"),
        password=os.getenv("TEST_DB_PASSWORD", "mlb_dev_password"),
    )


def query_table(storage_backend: PostgresStorageBackend, sql: str) -> list[dict]:
    """Execute query and return results as list of dicts.

    Args:
        storage_backend: The PostgresStorageBackend to use for the query.
        sql: The SQL query to execute.

    Returns:
        List of dicts with query results.
    """
    with storage_backend.pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            return cur.fetchall()


def count_rows(storage_backend: PostgresStorageBackend, table: str) -> int:
    """Count rows in a table.

    Args:
        storage_backend: The PostgresStorageBackend to use for the query.
        table: The fully qualified table name (schema.table).

    Returns:
        Number of rows in the table.
    """
    with storage_backend.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*)::int as count FROM {table}")
            result = cur.fetchone()
            return result[0] if isinstance(result, tuple) else result["count"]


# =============================================================================
# Shared Given Steps
# =============================================================================


@given("a clean test database")
def step_clean_test_database(context):
    """Clean test database tables (raw and normalized).

    This step cleans all test tables including:
    - Raw JSONB storage (game.live_game_v1)
    - Normalized tables (season.seasons, schedule.schedule)

    It can be used with or without a real database connection.
    When no database is available, it sets a flag for mock testing.
    """
    # Check if we're in mock mode or real database mode
    use_real_db = os.getenv("BDD_USE_REAL_DB", "false").lower() == "true"

    if use_real_db:
        # Real database cleanup
        db_config = get_db_config()
        context.storage_backend = PostgresStorageBackend(db_config)

        # Tables to clean - uses IF EXISTS to handle missing tables gracefully
        tables_to_clean = [
            "season.seasons",
            "schedule.schedule",
            "game.live_game_v1",
            "game.live_game_v1_raw",
            "game.live_game_metadata",
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
    else:
        # Mock mode - just set a flag
        context.storage_backend = None

    # Initialize common context variables
    context.db_clean = True
    context.raw_games = []
