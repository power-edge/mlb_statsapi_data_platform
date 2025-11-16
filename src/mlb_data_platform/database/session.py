"""Database session management with context managers.

Provides:
- Thread-safe engine creation with connection pooling
- Context manager for automatic session cleanup
- Transaction management
- Error handling
"""

from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy.pool import QueuePool
from sqlmodel import Session, create_engine

from mlb_data_platform.database.config import DEFAULT_CONFIG, DatabaseConfig

# Global engine cache (one engine per unique connection URL)
_engines = {}


def get_engine(config: Optional[DatabaseConfig] = None):
    """Get or create a SQLAlchemy engine.

    Engines are cached per connection URL to enable connection pooling.
    Thread-safe and reuses engines across multiple get_session() calls.

    Args:
        config: Database configuration (uses DEFAULT_CONFIG if None)

    Returns:
        SQLAlchemy Engine instance
    """
    if config is None:
        config = DEFAULT_CONFIG

    connection_url = config.get_connection_url()

    # Return cached engine if exists
    if connection_url in _engines:
        return _engines[connection_url]

    # Create new engine with connection pooling
    engine = create_engine(
        connection_url,
        echo=config.echo,
        echo_pool=config.echo_pool,
        poolclass=QueuePool,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout,
        pool_recycle=config.pool_recycle,
        pool_pre_ping=True,  # Verify connections before using
    )

    # Cache engine
    _engines[connection_url] = engine

    return engine


@contextmanager
def get_session(
    config: Optional[DatabaseConfig] = None,
    autocommit: bool = False,
    autoflush: bool = True,
) -> Generator[Session, None, None]:
    """Context manager for database sessions.

    Automatically handles:
    - Session creation
    - Transaction commit on success
    - Rollback on exception
    - Session cleanup

    Usage:
        with get_session() as session:
            game = session.exec(select(LiveGameMetadata).where(...)).first()
            session.add(new_game)
            # Automatically commits on exit

        # With custom config
        config = DatabaseConfig(host="prod-db.example.com")
        with get_session(config) as session:
            ...

    Args:
        config: Database configuration (uses DEFAULT_CONFIG if None)
        autocommit: Disable transaction management (default: False)
        autoflush: Auto-flush changes before queries (default: True)

    Yields:
        SQLModel Session instance

    Raises:
        Any database exceptions (after rollback)
    """
    engine = get_engine(config)

    session = Session(
        engine,
        autocommit=autocommit,
        autoflush=autoflush,
    )

    try:
        yield session

        # Commit transaction if not autocommit
        if not autocommit:
            session.commit()

    except Exception:
        # Rollback on any exception
        session.rollback()
        raise

    finally:
        # Always close session
        session.close()


@contextmanager
def get_read_only_session(
    config: Optional[DatabaseConfig] = None,
) -> Generator[Session, None, None]:
    """Context manager for read-only database sessions.

    Similar to get_session() but:
    - No commits (read-only)
    - No autoflush
    - Useful for queries that don't modify data

    Usage:
        with get_read_only_session() as session:
            games = session.exec(select(LiveGameMetadata)).all()
            # No commit on exit

    Args:
        config: Database configuration (uses DEFAULT_CONFIG if None)

    Yields:
        SQLModel Session instance (read-only)
    """
    engine = get_engine(config)

    session = Session(
        engine,
        autocommit=False,
        autoflush=False,
    )

    try:
        yield session
        # No commit for read-only

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


def dispose_engines():
    """Dispose all cached engines.

    Useful for:
    - Application shutdown
    - Testing cleanup
    - Switching configurations

    Warning: Closes all connection pools.
    """
    for engine in _engines.values():
        engine.dispose()

    _engines.clear()
