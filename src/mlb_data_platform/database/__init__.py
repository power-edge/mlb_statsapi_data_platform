"""Database utilities with context managers and configuration.

This module provides:
- Database configuration from environment variables
- Context manager for database sessions
- Connection pooling
- Transaction management

Usage:
    from mlb_data_platform.database import get_session, DatabaseConfig

    # Using context manager (recommended)
    with get_session() as session:
        games = session.exec(select(LiveGameMetadata)).all()

    # Custom configuration
    config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="mlb_games",
        user="mlb_admin",
        password="mlb_dev_password"
    )
    with get_session(config) as session:
        ...
"""

from mlb_data_platform.database.config import DatabaseConfig
from mlb_data_platform.database.session import (
    dispose_engines,
    get_engine,
    get_read_only_session,
    get_session,
)

__all__ = [
    "DatabaseConfig",
    "dispose_engines",
    "get_engine",
    "get_read_only_session",
    "get_session",
]
