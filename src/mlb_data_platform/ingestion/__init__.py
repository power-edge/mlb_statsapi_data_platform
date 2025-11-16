"""Ingestion layer for MLB Stats API data.

Provides:
- MLBStatsAPIClient: Wrapper around pymlb_statsapi with metadata capture
- RawStorageClient: Store API responses in PostgreSQL raw tables
- JobConfig: Configuration models for ingestion jobs
"""

from .client import MLBStatsAPIClient
from .config import JobConfig, load_job_config
from .raw_storage import RawStorageClient

__all__ = [
    "MLBStatsAPIClient",
    "JobConfig",
    "load_job_config",
    "RawStorageClient",
]
