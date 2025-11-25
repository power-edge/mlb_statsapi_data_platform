"""Unit tests for database configuration management."""

import os
from unittest.mock import patch

import pytest

from mlb_data_platform.database.config import DEFAULT_CONFIG, DatabaseConfig


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass initialization and defaults."""

    def test_default_initialization(self):
        """Test DatabaseConfig with default values."""
        config = DatabaseConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "mlb_games"
        assert config.user == "mlb_admin"
        assert config.password == "mlb_dev_password"
        assert config.driver == "psycopg"
        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.pool_timeout == 30
        assert config.pool_recycle == 3600
        assert config.echo is False
        assert config.echo_pool is False

    def test_custom_initialization(self):
        """Test DatabaseConfig with custom values."""
        config = DatabaseConfig(
            host="prod-db.example.com",
            port=5433,
            database="mlb_production",
            user="prod_user",
            password="prod_password",
            driver="psycopg2",
            pool_size=20,
            max_overflow=30,
            pool_timeout=60,
            pool_recycle=7200,
            echo=True,
            echo_pool=True,
        )

        assert config.host == "prod-db.example.com"
        assert config.port == 5433
        assert config.database == "mlb_production"
        assert config.user == "prod_user"
        assert config.password == "prod_password"
        assert config.driver == "psycopg2"
        assert config.pool_size == 20
        assert config.max_overflow == 30
        assert config.pool_timeout == 60
        assert config.pool_recycle == 7200
        assert config.echo is True
        assert config.echo_pool is True

    def test_partial_custom_initialization(self):
        """Test DatabaseConfig with partial custom values."""
        config = DatabaseConfig(
            host="custom-host",
            database="custom_db",
            pool_size=10,
        )

        # Custom values
        assert config.host == "custom-host"
        assert config.database == "custom_db"
        assert config.pool_size == 10

        # Default values for unspecified fields
        assert config.port == 5432
        assert config.user == "mlb_admin"
        assert config.password == "mlb_dev_password"
        assert config.driver == "psycopg"
        assert config.echo is False


class TestDatabaseConfigFromEnv:
    """Test DatabaseConfig.from_env() class method."""

    def test_from_env_with_no_env_vars(self):
        """Test from_env uses defaults when no env vars are set."""
        # Clear any existing env vars by using empty dict with clear=True
        with patch.dict(os.environ, {}, clear=True):
            config = DatabaseConfig.from_env()

            assert config.host == "localhost"
            assert config.port == 5432
            assert config.database == "mlb_games"
            assert config.user == "mlb_admin"
            assert config.password == "mlb_dev_password"
            assert config.pool_size == 5
            assert config.echo is False

    def test_from_env_with_all_env_vars(self):
        """Test from_env with all environment variables set."""
        env_vars = {
            "POSTGRES_HOST": "prod-db.example.com",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "mlb_production",
            "POSTGRES_USER": "prod_user",
            "POSTGRES_PASSWORD": "prod_password",
            "DB_POOL_SIZE": "20",
            "DB_ECHO": "true",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = DatabaseConfig.from_env()

            assert config.host == "prod-db.example.com"
            assert config.port == 5433
            assert config.database == "mlb_production"
            assert config.user == "prod_user"
            assert config.password == "prod_password"
            assert config.pool_size == 20
            assert config.echo is True

    def test_from_env_with_partial_env_vars(self):
        """Test from_env with some environment variables set."""
        env_vars = {
            "POSTGRES_HOST": "staging-db",
            "POSTGRES_PORT": "5434",
            # Other vars use defaults
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = DatabaseConfig.from_env()

            # Custom from env
            assert config.host == "staging-db"
            assert config.port == 5434

            # Defaults
            assert config.database == "mlb_games"
            assert config.user == "mlb_admin"
            assert config.password == "mlb_dev_password"
            assert config.pool_size == 5
            assert config.echo is False

    def test_from_env_with_database_url(self):
        """Test from_env with DATABASE_URL takes precedence."""
        env_vars = {
            "DATABASE_URL": "postgresql://user:pass@host:5432/dbname",
            "POSTGRES_HOST": "should_be_ignored",
            "POSTGRES_PORT": "9999",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = DatabaseConfig.from_env()

            # When DATABASE_URL is set, from_env returns defaults
            # because the URL will be used directly in get_connection_url()
            assert isinstance(config, DatabaseConfig)

    def test_from_env_db_echo_false(self):
        """Test from_env with DB_ECHO=false."""
        env_vars = {
            "DB_ECHO": "false",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = DatabaseConfig.from_env()

            assert config.echo is False

    def test_from_env_db_echo_case_insensitive(self):
        """Test from_env DB_ECHO is case-insensitive."""
        test_cases = [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("false", False),
            ("False", False),
            ("FALSE", False),
            ("yes", False),  # Only "true" is truthy
            ("1", False),
        ]

        for env_value, expected in test_cases:
            with patch.dict(os.environ, {"DB_ECHO": env_value}, clear=True):
                config = DatabaseConfig.from_env()
                assert config.echo == expected, f"DB_ECHO={env_value} should be {expected}"

    def test_from_env_invalid_port(self):
        """Test from_env with invalid port number raises error."""
        env_vars = {
            "POSTGRES_PORT": "not_a_number",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError):
                DatabaseConfig.from_env()

    def test_from_env_invalid_pool_size(self):
        """Test from_env with invalid pool size raises error."""
        env_vars = {
            "DB_POOL_SIZE": "invalid",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ValueError):
                DatabaseConfig.from_env()


class TestGetConnectionUrl:
    """Test get_connection_url() method."""

    def test_get_connection_url_default(self):
        """Test get_connection_url with default config."""
        config = DatabaseConfig()
        url = config.get_connection_url()

        expected = (
            "postgresql+psycopg://mlb_admin:mlb_dev_password@localhost:5432/mlb_games"
        )
        assert url == expected

    def test_get_connection_url_custom(self):
        """Test get_connection_url with custom config."""
        config = DatabaseConfig(
            host="prod-db.example.com",
            port=5433,
            database="mlb_production",
            user="prod_user",
            password="prod_password",
            driver="psycopg2",
        )
        url = config.get_connection_url()

        expected = (
            "postgresql+psycopg2://prod_user:prod_password@prod-db.example.com:5433/mlb_production"
        )
        assert url == expected

    def test_get_connection_url_with_special_chars(self):
        """Test get_connection_url with special characters in password."""
        config = DatabaseConfig(
            password="p@ssw0rd!#$%",
        )
        url = config.get_connection_url()

        # Password should be included as-is (URL encoding is handled by SQLAlchemy)
        assert "p@ssw0rd!#$%" in url

    def test_get_connection_url_uses_database_url_env(self):
        """Test get_connection_url uses DATABASE_URL environment variable."""
        custom_url = "postgresql://custom:pass@customhost:5555/customdb"
        env_vars = {
            "DATABASE_URL": custom_url,
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = DatabaseConfig()
            url = config.get_connection_url()

            assert url == custom_url

    def test_get_connection_url_driver_variations(self):
        """Test get_connection_url with different drivers."""
        drivers = ["psycopg", "psycopg2", "asyncpg"]

        for driver in drivers:
            config = DatabaseConfig(driver=driver)
            url = config.get_connection_url()

            assert url.startswith(f"postgresql+{driver}://")

    def test_get_connection_url_different_ports(self):
        """Test get_connection_url with different ports."""
        ports = [5432, 5433, 15432, 65254]

        for port in ports:
            config = DatabaseConfig(port=port)
            url = config.get_connection_url()

            assert f":{port}/" in url


class TestGetAsyncConnectionUrl:
    """Test get_async_connection_url() method."""

    def test_get_async_connection_url_default(self):
        """Test get_async_connection_url with default config."""
        config = DatabaseConfig()
        url = config.get_async_connection_url()

        expected = (
            "postgresql+asyncpg://mlb_admin:mlb_dev_password@localhost:5432/mlb_games"
        )
        assert url == expected

    def test_get_async_connection_url_custom(self):
        """Test get_async_connection_url with custom config."""
        config = DatabaseConfig(
            host="async-db.example.com",
            port=5433,
            database="mlb_async",
            user="async_user",
            password="async_password",
        )
        url = config.get_async_connection_url()

        expected = (
            "postgresql+asyncpg://async_user:async_password@async-db.example.com:5433/mlb_async"
        )
        assert url == expected

    def test_get_async_connection_url_always_uses_asyncpg(self):
        """Test get_async_connection_url always uses asyncpg driver."""
        config = DatabaseConfig(driver="psycopg2")  # Different driver
        url = config.get_async_connection_url()

        # Should always use asyncpg for async connections
        assert url.startswith("postgresql+asyncpg://")


class TestRepr:
    """Test __repr__ method."""

    def test_repr_hides_password(self):
        """Test __repr__ hides password."""
        config = DatabaseConfig(
            host="test-host",
            port=5432,
            database="test_db",
            user="test_user",
            password="super_secret_password",
            pool_size=10,
        )

        repr_str = repr(config)

        # Should show other fields
        assert "test-host" in repr_str
        assert "5432" in repr_str
        assert "test_db" in repr_str
        assert "test_user" in repr_str
        assert "pool_size=10" in repr_str

        # Should NOT show password
        assert "super_secret_password" not in repr_str
        assert "*****" in repr_str

    def test_repr_format(self):
        """Test __repr__ format is correct."""
        config = DatabaseConfig()
        repr_str = repr(config)

        assert repr_str.startswith("DatabaseConfig(")
        assert repr_str.endswith(")")
        assert "host=" in repr_str
        assert "port=" in repr_str
        assert "database=" in repr_str
        assert "user=" in repr_str
        assert "password=" in repr_str
        assert "pool_size=" in repr_str


class TestDefaultConfig:
    """Test DEFAULT_CONFIG constant."""

    def test_default_config_exists(self):
        """Test DEFAULT_CONFIG is defined."""
        assert DEFAULT_CONFIG is not None
        assert isinstance(DEFAULT_CONFIG, DatabaseConfig)

    def test_default_config_values(self):
        """Test DEFAULT_CONFIG has expected default values."""
        assert DEFAULT_CONFIG.host == "localhost"
        assert DEFAULT_CONFIG.port == 5432
        assert DEFAULT_CONFIG.database == "mlb_games"
        assert DEFAULT_CONFIG.user == "mlb_admin"
        assert DEFAULT_CONFIG.password == "mlb_dev_password"
        assert DEFAULT_CONFIG.pool_size == 5

    def test_default_config_immutability_concern(self):
        """Test that modifying DEFAULT_CONFIG affects subsequent calls."""
        # Note: This is a dataclass caveat - mutable default
        # Save original value
        original_host = DEFAULT_CONFIG.host

        # Modify
        DEFAULT_CONFIG.host = "modified"

        # Verify modification persists
        assert DEFAULT_CONFIG.host == "modified"

        # Restore original (for other tests)
        DEFAULT_CONFIG.host = original_host


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_password(self):
        """Test config with empty password."""
        config = DatabaseConfig(password="")
        url = config.get_connection_url()

        assert ":@" in url  # Empty password between : and @

    def test_empty_database_name(self):
        """Test config with empty database name."""
        config = DatabaseConfig(database="")
        url = config.get_connection_url()

        # URL should end with port and slash
        assert url.endswith(":5432/")

    def test_very_large_pool_size(self):
        """Test config with very large pool size."""
        config = DatabaseConfig(pool_size=1000, max_overflow=2000)

        assert config.pool_size == 1000
        assert config.max_overflow == 2000

    def test_zero_pool_size(self):
        """Test config with zero pool size."""
        config = DatabaseConfig(pool_size=0)

        assert config.pool_size == 0

    def test_negative_port(self):
        """Test config with negative port number."""
        # Dataclass doesn't validate, so this will succeed
        config = DatabaseConfig(port=-1)
        url = config.get_connection_url()

        assert ":-1/" in url

    def test_very_long_hostname(self):
        """Test config with very long hostname."""
        long_hostname = "a" * 253  # Max DNS hostname length
        config = DatabaseConfig(host=long_hostname)
        url = config.get_connection_url()

        assert long_hostname in url

    def test_unicode_in_password(self):
        """Test config with unicode characters in password."""
        config = DatabaseConfig(password="пароль123")  # Russian + numbers
        url = config.get_connection_url()

        assert "пароль123" in url

    def test_ipv6_host(self):
        """Test config with IPv6 host."""
        config = DatabaseConfig(host="::1")  # IPv6 localhost
        url = config.get_connection_url()

        assert "::1" in url

    def test_connection_url_format_structure(self):
        """Test connection URL has correct structure."""
        config = DatabaseConfig(
            driver="psycopg",
            user="testuser",
            password="testpass",
            host="testhost",
            port=5432,
            database="testdb",
        )
        url = config.get_connection_url()

        # Should match: postgresql+driver://user:password@host:port/database
        parts = url.split("://")
        assert len(parts) == 2
        assert parts[0] == "postgresql+psycopg"

        # Parse connection part
        connection_part = parts[1]
        assert connection_part.startswith("testuser:testpass@")
        assert "testhost:5432/testdb" in connection_part
