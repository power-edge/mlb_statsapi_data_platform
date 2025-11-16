"""Database configuration management.

Supports configuration from:
1. Environment variables (DATABASE_URL or individual params)
2. Explicit parameters
3. Defaults for local development
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database connection configuration.

    Supports multiple initialization methods:
    - From environment variables
    - From explicit parameters
    - From connection URL
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "mlb_games"
    user: str = "mlb_admin"
    password: str = "mlb_dev_password"
    driver: str = "psycopg"  # SQLAlchemy driver (psycopg, psycopg2, asyncpg)

    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600  # Recycle connections after 1 hour

    # Additional settings
    echo: bool = False  # Log all SQL queries
    echo_pool: bool = False  # Log connection pool events

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create configuration from environment variables.

        Environment variables:
        - DATABASE_URL: Full connection URL (takes precedence)
        - POSTGRES_HOST: Database host
        - POSTGRES_PORT: Database port
        - POSTGRES_DB: Database name
        - POSTGRES_USER: Database user
        - POSTGRES_PASSWORD: Database password
        - DB_POOL_SIZE: Connection pool size
        - DB_ECHO: Enable SQL query logging (true/false)

        Returns:
            DatabaseConfig instance
        """
        # If DATABASE_URL is set, parse it (handled by SQLAlchemy)
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            # We'll use this directly in get_connection_url()
            return cls()

        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "mlb_games"),
            user=os.getenv("POSTGRES_USER", "mlb_admin"),
            password=os.getenv("POSTGRES_PASSWORD", "mlb_dev_password"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            echo=os.getenv("DB_ECHO", "false").lower() == "true",
        )

    def get_connection_url(self) -> str:
        """Get SQLAlchemy connection URL.

        Returns:
            Connection URL string (e.g., postgresql+psycopg://user:pass@host:port/db)
        """
        # Check for DATABASE_URL environment variable first
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url

        # Build URL from components
        return (
            f"postgresql+{self.driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def get_async_connection_url(self) -> str:
        """Get async SQLAlchemy connection URL (for asyncpg).

        Returns:
            Async connection URL string
        """
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def __repr__(self) -> str:
        """String representation (hides password)."""
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, "
            f"database={self.database}, user={self.user}, "
            f"password=*****, pool_size={self.pool_size})"
        )


# Default configuration for local development
DEFAULT_CONFIG = DatabaseConfig()
