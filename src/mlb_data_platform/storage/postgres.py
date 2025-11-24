"""PostgreSQL storage backend for MLB data platform.

This module provides:
- Connection pooling with psycopg
- Insert/upsert operations for raw JSONB data
- Automatic partition management for time-series data
- Transaction management and error handling
- Integration with job configuration
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from ..schema.models import SchemaMetadata

logger = logging.getLogger(__name__)


class PostgresConfig(BaseModel):
    """PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "mlb_games"
    user: str = "mlb_admin"
    password: str = "mlb_admin_password"
    min_connections: int = 2
    max_connections: int = 10
    connection_timeout: int = 30


class PostgresStorageBackend:
    """PostgreSQL storage backend with connection pooling and partition management.

    Features:
    - Connection pooling for efficient resource usage
    - Automatic partition creation for date-partitioned tables
    - Upsert logic based on primary keys
    - JSONB storage for raw API responses
    - Transaction management with rollback
    """

    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL storage backend.

        Args:
            config: PostgreSQL connection configuration
        """
        self.config = config
        self.pool: Optional[ConnectionPool] = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize connection pool."""
        conninfo = (
            f"host={self.config.host} "
            f"port={self.config.port} "
            f"dbname={self.config.database} "
            f"user={self.config.user} "
            f"password={self.config.password}"
        )

        try:
            self.pool = ConnectionPool(
                conninfo=conninfo,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
                timeout=self.config.connection_timeout,
                kwargs={"row_factory": dict_row},
            )
            logger.info(
                f"Initialized PostgreSQL connection pool: "
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            self.pool.close()
            logger.info("Closed PostgreSQL connection pool")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def insert_raw_data(
        self,
        table_name: str,
        data: dict[str, Any],
        metadata: dict[str, Any],
        schema_metadata: Optional[SchemaMetadata] = None,
        partition_date: Optional[date] = None,
    ) -> int:
        """Insert raw JSONB data into specified table.

        Args:
            table_name: Full table name (schema.table)
            data: JSON data to store (API response)
            metadata: Request/response metadata
            schema_metadata: Optional schema metadata for field extraction
            partition_date: Date for partitioning (defaults to today)

        Returns:
            Inserted row ID

        Raises:
            Exception: If insert fails
        """
        if partition_date is None:
            partition_date = date.today()

        # Ensure partition exists
        self._ensure_partition_exists(table_name, partition_date)

        # Extract fields if schema metadata provided
        extracted_fields = {}
        if schema_metadata:
            # Build combined data structure for extraction (includes response + request params)
            request_params = metadata.get("request", {}).get("query_params", {})
            combined_data = {
                **data,
                "request_params": request_params
            }
            extracted_fields = self._extract_fields_from_data(combined_data, schema_metadata)

        # Build INSERT statement
        insert_sql = self._build_insert_sql(table_name, extracted_fields)

        # Prepare values
        values = {
            "data": json.dumps(data),
            "captured_at": datetime.now(),
            "schema_version": metadata.get("schema_version", "v1"),
            "source_url": metadata.get("request", {}).get("url", ""),
            "request_params": json.dumps(metadata.get("request", {}).get("query_params", {})),
            **extracted_fields,
        }

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, values)
                    row_id = cur.fetchone()["id"]
                    conn.commit()
                    logger.info(f"Inserted row {row_id} into {table_name}")
                    return row_id
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to insert data into {table_name}: {e}")
                raise

    def upsert_raw_data(
        self,
        table_name: str,
        data: dict[str, Any],
        metadata: dict[str, Any],
        schema_metadata: SchemaMetadata,
        partition_date: Optional[date] = None,
    ) -> int:
        """Upsert raw JSONB data based on primary keys.

        If a row with the same primary key values exists, updates it.
        Otherwise inserts a new row.

        Args:
            table_name: Full table name (schema.table)
            data: JSON data to store
            metadata: Request/response metadata
            schema_metadata: Schema metadata with primary key definitions
            partition_date: Date for partitioning (defaults to today)

        Returns:
            Upserted row ID
        """
        if partition_date is None:
            partition_date = date.today()

        # Ensure partition exists
        self._ensure_partition_exists(table_name, partition_date)

        # Extract fields (include request params for extraction)
        request_params = metadata.get("request", {}).get("query_params", {})
        combined_data = {
            **data,
            "request_params": request_params
        }
        extracted_fields = self._extract_fields_from_data(combined_data, schema_metadata)

        # Build UPSERT statement
        upsert_sql = self._build_upsert_sql(table_name, schema_metadata, extracted_fields)

        # Prepare values
        values = {
            "data": json.dumps(data),
            "captured_at": datetime.now(),
            "schema_version": metadata.get("schema_version", "v1"),
            "source_url": metadata.get("request", {}).get("url", ""),
            "request_params": json.dumps(metadata.get("request", {}).get("query_params", {})),
            **extracted_fields,
        }

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(upsert_sql, values)
                    row_id = cur.fetchone()["id"]
                    conn.commit()
                    logger.info(f"Upserted row {row_id} into {table_name}")
                    return row_id
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to upsert data into {table_name}: {e}")
                raise

    def _extract_fields_from_data(
        self, data: dict[str, Any], schema_metadata: SchemaMetadata
    ) -> dict[str, Any]:
        """Extract fields from JSON data based on schema metadata.

        Args:
            data: JSON data to extract from
            schema_metadata: Schema metadata with field definitions

        Returns:
            Dictionary of field_name -> extracted_value
        """
        extracted = {}

        for field in schema_metadata.fields:
            if not field.json_path:
                continue

            # Extract value using JSONPath
            value = self._extract_json_path(data, field.json_path)

            # Skip if null and field is nullable
            if value is None and field.nullable:
                continue

            extracted[field.name] = value

        return extracted

    @staticmethod
    def _extract_json_path(data: dict, json_path: str) -> Any:
        """Extract value from nested dict using JSONPath.

        Args:
            data: Dictionary to extract from
            json_path: JSONPath like '$.gameData.game.season'

        Returns:
            Extracted value or None if not found
        """
        # Strip leading $ and .
        path = json_path.lstrip("$").lstrip(".")
        if not path:
            return data

        # Navigate nested structure
        current = data
        for key in path.split("."):
            if isinstance(current, dict):
                current = current.get(key)
                if current is None:
                    return None
            else:
                return None

        return current

    def _build_insert_sql(self, table_name: str, extracted_fields: dict[str, Any]) -> str:
        """Build INSERT SQL statement.

        Args:
            table_name: Full table name
            extracted_fields: Fields to include in INSERT

        Returns:
            SQL INSERT statement
        """
        # Base columns
        columns = ["data", "captured_at", "schema_version", "source_url", "request_params"]

        # Add extracted field columns
        columns.extend(extracted_fields.keys())

        # Build SQL
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            RETURNING id
        """

        return sql

    def _build_upsert_sql(
        self, table_name: str, schema_metadata: SchemaMetadata, extracted_fields: dict[str, Any]
    ) -> str:
        """Build UPSERT (INSERT ... ON CONFLICT) SQL statement.

        Args:
            table_name: Full table name
            schema_metadata: Schema metadata with primary key info
            extracted_fields: Fields to include

        Returns:
            SQL UPSERT statement
        """
        # Base columns
        columns = ["data", "captured_at", "schema_version", "source_url", "request_params"]
        columns.extend(extracted_fields.keys())

        # Build SQL
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        # Conflict columns (primary keys excluding auto-increment id)
        conflict_columns = [pk for pk in schema_metadata.primary_keys if pk != "id"]
        conflict_str = ", ".join(conflict_columns)

        # Update columns (exclude primary keys)
        update_columns = [col for col in columns if col not in schema_metadata.primary_keys]
        update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

        sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}
            RETURNING id
        """

        return sql

    def _ensure_partition_exists(self, table_name: str, partition_date: date) -> None:
        """Ensure partition exists for given date.

        Args:
            table_name: Parent table name
            partition_date: Date to check partition for
        """
        # Generate partition name (e.g., schedule_schedule_2024_11)
        schema, table = table_name.split(".")
        year = partition_date.year
        month = partition_date.month
        partition_name = f"{schema}.{table}_{year}_{month:02d}"

        # Check if partition exists
        check_sql = """
            SELECT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            )
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(check_sql, (schema, f"{table}_{year}_{month:02d}"))
                exists = cur.fetchone()["exists"]

                if not exists:
                    # Create partition
                    logger.info(f"Creating partition {partition_name} for {partition_date}")
                    self._create_partition(table_name, partition_date)

    def _create_partition(self, table_name: str, partition_date: date) -> None:
        """Create partition for given month.

        Args:
            table_name: Parent table name
            partition_date: Date in the month to partition
        """
        schema, table = table_name.split(".")
        year = partition_date.year
        month = partition_date.month

        # Calculate date range
        from calendar import monthrange

        _, last_day = monthrange(year, month)
        start_date = date(year, month, 1)
        end_date = date(year, month, last_day)

        partition_name = f"{table}_{year}_{month:02d}"

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
            PARTITION OF {table_name}
            FOR VALUES FROM ('{start_date}') TO ('{end_date}');
        """

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                    conn.commit()
                    logger.info(f"Created partition {schema}.{partition_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to create partition {partition_name}: {e}")
                # Don't raise - partition might already exist from concurrent process

    def query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        fetch_one: bool = False,
    ) -> list[dict] | dict | None:
        """Execute SELECT query.

        Args:
            sql: SQL SELECT statement
            params: Query parameters
            fetch_one: If True, return single row

        Returns:
            List of dicts (rows) or single dict if fetch_one=True
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})

                if fetch_one:
                    return cur.fetchone()
                else:
                    return cur.fetchall()

    def execute(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> int:
        """Execute non-SELECT statement.

        Args:
            sql: SQL statement
            params: Query parameters

        Returns:
            Number of affected rows
        """
        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params or {})
                    affected = cur.rowcount
                    conn.commit()
                    return affected
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to execute SQL: {e}")
                raise

    def get_latest_captured_at(self, table_name: str) -> Optional[datetime]:
        """Get latest captured_at timestamp from table.

        Useful for incremental ingestion (fetch only new data).

        Args:
            table_name: Full table name

        Returns:
            Latest captured_at timestamp or None if table empty
        """
        sql = f"SELECT MAX(captured_at) as max_ts FROM {table_name}"

        result = self.query(sql, fetch_one=True)
        return result["max_ts"] if result else None

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists.

        Args:
            table_name: Full table name (schema.table)

        Returns:
            True if table exists
        """
        schema, table = table_name.split(".")

        sql = """
            SELECT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            )
        """

        result = self.query(sql, {"schema": schema, "table": table}, fetch_one=True)
        return result["exists"] if result else False

    def refresh_materialized_view(self, view_name: str, concurrently: bool = True) -> None:
        """Refresh materialized view.

        Args:
            view_name: Full view name (schema.view)
            concurrently: If True, use CONCURRENTLY (requires UNIQUE index)
        """
        concurrent_str = "CONCURRENTLY" if concurrently else ""
        sql = f"REFRESH MATERIALIZED VIEW {concurrent_str} {view_name}"

        logger.info(f"Refreshing materialized view {view_name}")
        self.execute(sql)

    def get_table_row_count(self, table_name: str) -> int:
        """Get total row count for table.

        Args:
            table_name: Full table name

        Returns:
            Row count
        """
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.query(sql, fetch_one=True)
        return result["count"] if result else 0


def create_postgres_backend(
    host: str = "localhost",
    port: int = 5432,
    database: str = "mlb_games",
    user: str = "mlb_admin",
    password: str = "mlb_admin_password",
) -> PostgresStorageBackend:
    """Create PostgreSQL storage backend with default configuration.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        Configured PostgresStorageBackend instance
    """
    config = PostgresConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    return PostgresStorageBackend(config)
