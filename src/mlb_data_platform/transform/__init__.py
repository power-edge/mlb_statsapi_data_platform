"""Transformation layer for MLB Stats API data.

Transforms raw JSONB data from PostgreSQL into normalized relational tables.

Design Principles:
- Schema-driven: Extraction registry pattern defines transformations
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
    results = transform(game_pks=[744834], write_to_postgres=True)
"""

from mlb_data_platform.transform.base import BaseTransformation
from mlb_data_platform.transform.season import SeasonTransform
from mlb_data_platform.transform.schedule import ScheduleTransform
from mlb_data_platform.transform.game import LiveGameTransform
from mlb_data_platform.transform.person import PersonTransform
from mlb_data_platform.transform.team import TeamTransform

__all__ = [
    "BaseTransformation",
    "SeasonTransform",
    "ScheduleTransform",
    "LiveGameTransform",
    "PersonTransform",
    "TeamTransform",
]
