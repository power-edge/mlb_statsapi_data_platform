#!/usr/bin/env python3
"""Quick script to ingest season data for testing transformations."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mlb_data_platform.ingestion.client import MLBStatsAPIClient
from mlb_data_platform.ingestion.config import (
    JobConfig,
    SourceConfig,
    StorageConfig,
    RawStorageConfig,
    StubMode,
)
from mlb_data_platform.storage.postgres import PostgresStorageBackend, PostgresConfig

def main():
    """Ingest season data using stub mode."""
    print("Ingesting season data...")

    # Configure PostgreSQL
    db_config = PostgresConfig(
        host="localhost",
        port=5432,
        database="mlb_games",
        user="mlb_admin",
        password="mlb_dev_password",
    )

    # Create storage backend
    storage = PostgresStorageBackend(db_config)

    # Create client with stub mode
    job_config = JobConfig(
        name="test_season",
        description="Test season ingestion",
        type="batch",
        source=SourceConfig(
            endpoint="season",
            method="seasons",
            parameters={"sportId": 1}
        ),
        storage=StorageConfig(
            raw=RawStorageConfig(
                backend="postgres",
                table="season.seasons",
                format="jsonb"
            )
        )
    )

    client = MLBStatsAPIClient(job_config, stub_mode=StubMode.REPLAY)

    # Fetch and save
    result = client.fetch_and_save(storage)

    print(f"Ingested season data:")
    print(f"  Endpoint: {result.get('endpoint')}")
    print(f"  Method: {result.get('method')}")
    print(f"  Response status: {result.get('response', {}).get('status_code')}")
    print(f"  Saved: {result.get('saved', False)}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
