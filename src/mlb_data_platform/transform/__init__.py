"""Transformation layer for MLB Stats API data.

Transforms raw JSONB data from PostgreSQL into normalized relational tables.

Design Principles:
- Schema-driven: YAML configs define transformations
- Incremental: Process only new data since last checkpoint
- Defensive: Upserts to handle duplicate/late-arriving data
- PySpark: For scalability and complex nested JSON flattening

Architecture:
    Raw Layer (JSONB) → Transform Jobs → Normalized Layer (Relational)

    Uses checkpoints to track what has been transformed, enabling
    incremental processing and replay capability.

Usage:
    from mlb_data_platform.transform import LiveGameTransform

    transform = LiveGameTransform(spark_session)
    transform.run(incremental=True)  # Process only new data
    transform.run(incremental=False, start_date='2024-01-01')  # Full backfill
"""

from mlb_data_platform.transform.base import BaseTransform
from mlb_data_platform.transform.live_game import LiveGameTransform

__all__ = [
    "BaseTransform",
    "LiveGameTransform",
]
