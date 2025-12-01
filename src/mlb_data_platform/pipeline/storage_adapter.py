"""Storage adapter for pipeline orchestrator.

Bridges the PipelineOrchestrator callback interface with PostgresStorageBackend.

Usage:
    >>> from mlb_data_platform.storage import create_postgres_backend
    >>> from mlb_data_platform.pipeline.storage_adapter import StorageAdapter
    >>>
    >>> backend = create_postgres_backend(host="localhost", port=5432)
    >>> adapter = StorageAdapter(backend)
    >>>
    >>> # Use with orchestrator
    >>> orchestrator = PipelineOrchestrator(storage_callback=adapter.store)
"""

import logging
import re
from datetime import date, datetime
from typing import Any

from mlb_data_platform.schema.registry import get_registry
from mlb_data_platform.storage.postgres import PostgresStorageBackend

logger = logging.getLogger(__name__)


class StorageAdapter:
    """Adapts PipelineOrchestrator storage callback to PostgresStorageBackend.

    The orchestrator calls: callback(endpoint, method, data, metadata)
    The backend expects: insert_raw_data(table_name, data, metadata, schema_metadata)

    This adapter:
    1. Maps endpoint/method to table names
    2. Looks up schema metadata from the registry
    3. Formats metadata for the backend
    4. Handles upsert vs insert based on configuration
    """

    def __init__(
        self,
        backend: PostgresStorageBackend,
        upsert: bool = False,  # Default to INSERT - existing schema lacks upsert-friendly constraints
        partition_date: date | None = None,
    ):
        """Initialize storage adapter.

        Args:
            backend: PostgreSQL storage backend
            upsert: If True, use upsert; otherwise insert
            partition_date: Override partition date (default: today)
        """
        self.backend = backend
        self.upsert = upsert
        self.partition_date = partition_date
        self.registry = get_registry()

        # Statistics
        self.stats = {
            "inserts": 0,
            "upserts": 0,
            "errors": 0,
            "by_table": {},
        }

    def store(
        self,
        endpoint: str,
        method: str,
        data: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> int | None:
        """Store data from pipeline orchestrator.

        This is the callback signature expected by PipelineOrchestrator.

        Args:
            endpoint: API endpoint name (e.g., "season", "game")
            method: API method name (e.g., "seasons", "liveGameV1")
            data: API response data
            metadata: Request/pipeline metadata

        Returns:
            Row ID if successful, None if failed
        """
        metadata = metadata or {}

        # Map to table name
        table_name = self._get_table_name(endpoint, method)

        # Get schema metadata (may be None for endpoints without schemas)
        schema_meta = self.registry.get_schema_by_endpoint(endpoint, method)

        # Format metadata for backend
        backend_metadata = self._format_metadata(endpoint, method, metadata)

        # Determine partition date
        partition_date = self.partition_date or self._extract_partition_date(metadata)

        try:
            if self.upsert and schema_meta:
                row_id = self.backend.upsert_raw_data(
                    table_name=table_name,
                    data=data,
                    metadata=backend_metadata,
                    schema_metadata=schema_meta,
                    partition_date=partition_date,
                )
                self.stats["upserts"] += 1
            else:
                row_id = self.backend.insert_raw_data(
                    table_name=table_name,
                    data=data,
                    metadata=backend_metadata,
                    schema_metadata=schema_meta,
                    partition_date=partition_date,
                )
                self.stats["inserts"] += 1

            # Track by table
            self.stats["by_table"][table_name] = (
                self.stats["by_table"].get(table_name, 0) + 1
            )

            logger.info(f"Stored data in {table_name} (row_id: {row_id})")
            return row_id

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to store data in {table_name}: {e}")
            return None

    # Special table name mappings where API method doesn't match table name
    TABLE_NAME_OVERRIDES = {
        ("game", "liveTimestampv11"): "game.live_game_timestamps",
        ("game", "liveGameDiffPatchV1"): "game.live_game_v1",  # Diffs go to same table
    }

    def _get_table_name(self, endpoint: str, method: str) -> str:
        """Convert endpoint/method to PostgreSQL table name.

        Args:
            endpoint: API endpoint (e.g., "Game")
            method: API method (e.g., "liveGameV1")

        Returns:
            Full table name (e.g., "game.live_game_v1")
        """
        # Check for special mappings first
        key = (endpoint.lower(), method)
        if key in self.TABLE_NAME_OVERRIDES:
            return self.TABLE_NAME_OVERRIDES[key]

        schema_name = endpoint.lower()
        table_name = self._method_to_snake_case(method)
        return f"{schema_name}.{table_name}"

    @staticmethod
    def _method_to_snake_case(method: str) -> str:
        """Convert camelCase method name to snake_case.

        Args:
            method: Method name (e.g., "liveGameV1", "seasons")

        Returns:
            Snake case name (e.g., "live_game_v1", "seasons")
        """
        return re.sub(r"(?<!^)(?=[A-Z])", "_", method).lower()

    def _format_metadata(
        self,
        endpoint: str,
        method: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """Format metadata for PostgresStorageBackend.

        Args:
            endpoint: API endpoint
            method: API method
            metadata: Raw metadata from orchestrator

        Returns:
            Formatted metadata dict
        """
        # Build request info from available metadata
        request_info = {
            "url": f"https://statsapi.mlb.com/api/v1/{endpoint.lower()}/{method}",
            "timestamp": datetime.now().isoformat(),
            "query_params": {},
        }

        # Extract known parameters from metadata
        param_mapping = {
            "sport_id": "sportId",
            "season": "season",
            "schedule_date": "date",
            "game_pk": "gamePk",
            "timecode": "timecode",
            "person_id": "personIds",
            "team_id": "teamId",
        }

        for meta_key, param_key in param_mapping.items():
            if meta_key in metadata:
                request_info["query_params"][param_key] = metadata[meta_key]

        return {
            "schema_version": "v1",
            "request": request_info,
            "response": {
                "status_code": 200,
                "captured_at": datetime.now().isoformat(),
            },
            "pipeline": {
                "endpoint": endpoint,
                "method": method,
                "original_metadata": metadata,
            },
        }

    def _extract_partition_date(self, metadata: dict[str, Any]) -> date:
        """Extract partition date from metadata.

        Uses schedule_date if available, otherwise today.

        Args:
            metadata: Pipeline metadata

        Returns:
            Date for partitioning
        """
        if "schedule_date" in metadata:
            schedule_date = metadata["schedule_date"]
            if isinstance(schedule_date, str):
                return date.fromisoformat(schedule_date)
            elif isinstance(schedule_date, date):
                return schedule_date

        return date.today()

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.

        Returns:
            Dict with insert/upsert/error counts
        """
        return self.stats.copy()

    def reset_stats(self) -> None:
        """Reset storage statistics."""
        self.stats = {
            "inserts": 0,
            "upserts": 0,
            "errors": 0,
            "by_table": {},
        }


def create_storage_callback(
    host: str = "localhost",
    port: int = 5432,
    database: str = "mlb_games",
    user: str = "mlb_admin",
    password: str = "mlb_admin_password",
    upsert: bool = False,  # Default to INSERT - existing schema lacks upsert-friendly constraints
) -> tuple[StorageAdapter, PostgresStorageBackend]:
    """Create storage adapter with backend for pipeline use.

    Convenience function that creates both the backend and adapter.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: Database user
        password: Database password
        upsert: Use upsert instead of insert

    Returns:
        Tuple of (StorageAdapter, PostgresStorageBackend)
        Use adapter.store as the storage_callback for PipelineOrchestrator.
        Remember to call backend.close() when done.

    Example:
        >>> adapter, backend = create_storage_callback(host="localhost")
        >>> orchestrator = PipelineOrchestrator(storage_callback=adapter.store)
        >>> result = orchestrator.run_daily()
        >>> print(adapter.get_stats())
        >>> backend.close()
    """
    from mlb_data_platform.storage import create_postgres_backend

    backend = create_postgres_backend(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    adapter = StorageAdapter(backend, upsert=upsert)

    return adapter, backend
