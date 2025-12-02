"""Unit tests for database session management.

Uses mocking to test session management without requiring a live database.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from sqlalchemy.pool import QueuePool
from sqlmodel import Session

from mlb_data_platform.database.config import DEFAULT_CONFIG, DatabaseConfig
from mlb_data_platform.database.session import (
    _engines,
    dispose_engines,
    get_engine,
    get_read_only_session,
    get_session,
)


@pytest.fixture(autouse=True)
def clear_engine_cache():
    """Clear the global engine cache before each test."""
    _engines.clear()
    yield
    _engines.clear()


class TestGetEngine:
    """Test get_engine() function."""

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_creates_engine(self, mock_create_engine):
        """Test get_engine creates a new engine."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        result = get_engine()

        assert result == mock_engine
        mock_create_engine.assert_called_once()

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_uses_default_config(self, mock_create_engine):
        """Test get_engine uses DEFAULT_CONFIG when no config provided."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        get_engine()

        # Should be called with DEFAULT_CONFIG's connection URL
        expected_url = DEFAULT_CONFIG.get_connection_url()
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == expected_url

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_uses_custom_config(self, mock_create_engine):
        """Test get_engine uses custom config when provided."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        custom_config = DatabaseConfig(
            host="custom-host",
            database="custom_db",
            user="custom_user",
            password="custom_pass",
        )

        get_engine(custom_config)

        # Should be called with custom config's connection URL
        expected_url = custom_config.get_connection_url()
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == expected_url

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_caches_engines(self, mock_create_engine):
        """Test get_engine caches engines by connection URL."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # First call should create engine
        engine1 = get_engine()
        assert mock_create_engine.call_count == 1

        # Second call should return cached engine
        engine2 = get_engine()
        assert mock_create_engine.call_count == 1  # Not called again
        assert engine1 is engine2  # Same object

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_different_configs_create_different_engines(self, mock_create_engine):
        """Test get_engine creates different engines for different configs."""
        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        mock_create_engine.side_effect = [mock_engine1, mock_engine2]

        config1 = DatabaseConfig(database="db1")
        config2 = DatabaseConfig(database="db2")

        # Different configs should create different engines
        engine1 = get_engine(config1)
        engine2 = get_engine(config2)

        assert mock_create_engine.call_count == 2
        assert engine1 is mock_engine1
        assert engine2 is mock_engine2
        assert engine1 is not engine2

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_configuration_parameters(self, mock_create_engine):
        """Test get_engine passes correct configuration parameters."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(
            echo=True,
            echo_pool=True,
            pool_size=20,
            max_overflow=30,
            pool_timeout=60,
            pool_recycle=7200,
        )

        get_engine(config)

        # Verify create_engine was called with correct parameters
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["echo"] is True
        assert call_kwargs["echo_pool"] is True
        assert call_kwargs["poolclass"] == QueuePool
        assert call_kwargs["pool_size"] == 20
        assert call_kwargs["max_overflow"] == 30
        assert call_kwargs["pool_timeout"] == 60
        assert call_kwargs["pool_recycle"] == 7200
        assert call_kwargs["pool_pre_ping"] is True

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_enables_pool_pre_ping(self, mock_create_engine):
        """Test get_engine enables pool_pre_ping for connection health checks."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        get_engine()

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["pool_pre_ping"] is True

    @patch("mlb_data_platform.database.session.create_engine")
    def test_get_engine_cache_key_is_connection_url(self, mock_create_engine):
        """Test engine cache uses connection URL as key."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DatabaseConfig(host="test-host", database="test_db")
        get_engine(config)

        # Check cache contains the connection URL
        expected_key = config.get_connection_url()
        assert expected_key in _engines
        assert _engines[expected_key] is mock_engine


class TestGetSession:
    """Test get_session() context manager."""

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_creates_session(self, mock_session_class, mock_get_engine):
        """Test get_session creates a Session."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session_instance = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session_instance

        with get_session() as session:
            assert session is mock_session_instance

        mock_session_class.assert_called_once_with(
            mock_engine,
            autocommit=False,
            autoflush=True,
        )

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_commits_on_success(self, mock_session_class, mock_get_engine):
        """Test get_session commits transaction on successful exit."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_session() as session:
            pass  # No exception

        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_not_called()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_rollback_on_exception(self, mock_session_class, mock_get_engine):
        """Test get_session rolls back transaction on exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError):
            with get_session() as session:
                raise ValueError("Test error")

        mock_session.commit.assert_not_called()
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_always_closes(self, mock_session_class, mock_get_engine):
        """Test get_session always closes session even on exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        # Test with exception
        with pytest.raises(RuntimeError):
            with get_session() as session:
                raise RuntimeError("Test error")

        mock_session.close.assert_called_once()

        # Reset and test without exception
        mock_session.reset_mock()

        with get_session() as session:
            pass

        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_with_custom_config(self, mock_session_class, mock_get_engine):
        """Test get_session with custom DatabaseConfig."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        custom_config = DatabaseConfig(host="custom-host")

        with get_session(custom_config) as session:
            pass

        # Verify get_engine was called with custom config
        mock_get_engine.assert_called_once_with(custom_config)

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_autocommit_parameter(self, mock_session_class, mock_get_engine):
        """Test get_session with autocommit=True."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_session(autocommit=True) as session:
            pass

        # Should be created with autocommit=True
        mock_session_class.assert_called_once_with(
            mock_engine,
            autocommit=True,
            autoflush=True,
        )

        # Should not commit when autocommit=True
        mock_session.commit.assert_not_called()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_autoflush_parameter(self, mock_session_class, mock_get_engine):
        """Test get_session with autoflush=False."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_session(autoflush=False) as session:
            pass

        # Should be created with autoflush=False
        mock_session_class.assert_called_once_with(
            mock_engine,
            autocommit=False,
            autoflush=False,
        )

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_yields_session(self, mock_session_class, mock_get_engine):
        """Test get_session yields the session object."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        # Verify we can use the session within the context
        with get_session() as session:
            assert session is mock_session
            # Simulate some work with session
            session.query()

        mock_session.query.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_session_exception_propagates(self, mock_session_class, mock_get_engine):
        """Test get_session propagates exceptions after rollback."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        class CustomError(Exception):
            pass

        with pytest.raises(CustomError, match="Custom error"):
            with get_session() as session:
                raise CustomError("Custom error")

        # Verify rollback was called before exception propagated
        mock_session.rollback.assert_called_once()


class TestGetReadOnlySession:
    """Test get_read_only_session() context manager."""

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_creates_session(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session creates a Session."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_read_only_session() as session:
            assert session is mock_session

        mock_session_class.assert_called_once_with(
            mock_engine,
            autocommit=False,
            autoflush=False,
        )

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_no_commit(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session does not commit."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_read_only_session() as session:
            pass

        # Read-only sessions should never commit
        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_rollback_on_exception(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session rolls back on exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with pytest.raises(RuntimeError):
            with get_read_only_session() as session:
                raise RuntimeError("Read error")

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_with_custom_config(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session with custom config."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        custom_config = DatabaseConfig(host="read-replica")

        with get_read_only_session(custom_config) as session:
            pass

        mock_get_engine.assert_called_once_with(custom_config)

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_disables_autoflush(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session has autoflush disabled."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        with get_read_only_session() as session:
            pass

        # Verify autoflush is disabled
        call_kwargs = mock_session_class.call_args[1]
        assert call_kwargs["autoflush"] is False

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_get_read_only_session_always_closes(self, mock_session_class, mock_get_engine):
        """Test get_read_only_session always closes session."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session_class.return_value = mock_session

        # Test normal exit
        with get_read_only_session() as session:
            pass

        mock_session.close.assert_called_once()

        # Reset and test with exception
        mock_session.reset_mock()

        with pytest.raises(ValueError):
            with get_read_only_session() as session:
                raise ValueError("Test error")

        mock_session.close.assert_called_once()


class TestDisposeEngines:
    """Test dispose_engines() function."""

    @patch("mlb_data_platform.database.session.create_engine")
    def test_dispose_engines_disposes_all_engines(self, mock_create_engine):
        """Test dispose_engines disposes all cached engines."""
        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        mock_create_engine.side_effect = [mock_engine1, mock_engine2]

        # Create two engines
        config1 = DatabaseConfig(database="db1")
        config2 = DatabaseConfig(database="db2")
        get_engine(config1)
        get_engine(config2)

        # Verify engines are cached
        assert len(_engines) == 2

        # Dispose all engines
        dispose_engines()

        # Verify all engines were disposed
        mock_engine1.dispose.assert_called_once()
        mock_engine2.dispose.assert_called_once()

        # Verify cache is cleared
        assert len(_engines) == 0

    @patch("mlb_data_platform.database.session.create_engine")
    def test_dispose_engines_clears_cache(self, mock_create_engine):
        """Test dispose_engines clears the engine cache."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Create an engine
        get_engine()

        # Verify cache has engine
        assert len(_engines) == 1

        # Dispose engines
        dispose_engines()

        # Verify cache is empty
        assert len(_engines) == 0

    def test_dispose_engines_safe_when_empty(self):
        """Test dispose_engines is safe to call when no engines exist."""
        # Clear cache
        _engines.clear()

        # Should not raise exception
        dispose_engines()

        assert len(_engines) == 0

    @patch("mlb_data_platform.database.session.create_engine")
    def test_dispose_engines_allows_recreation(self, mock_create_engine):
        """Test engines can be recreated after dispose_engines."""
        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        mock_create_engine.side_effect = [mock_engine1, mock_engine2]

        # Create engine
        engine1 = get_engine()
        assert engine1 is mock_engine1
        assert len(_engines) == 1

        # Dispose
        dispose_engines()
        assert len(_engines) == 0

        # Create again - should create new engine
        engine2 = get_engine()
        assert engine2 is mock_engine2
        assert len(_engines) == 1
        assert engine2 is not engine1  # Different engine


class TestContextManagerBehavior:
    """Test context manager behavior and patterns."""

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_nested_sessions(self, mock_session_class, mock_get_engine):
        """Test nested session contexts."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        # Create two different mock sessions
        mock_session1 = MagicMock(spec=Session)
        mock_session2 = MagicMock(spec=Session)
        mock_session_class.side_effect = [mock_session1, mock_session2]

        with get_session() as session1:
            with get_session() as session2:
                assert session1 is mock_session1
                assert session2 is mock_session2
                assert session1 is not session2

        # Both should be committed and closed
        mock_session1.commit.assert_called_once()
        mock_session1.close.assert_called_once()
        mock_session2.commit.assert_called_once()
        mock_session2.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_multiple_sequential_sessions(self, mock_session_class, mock_get_engine):
        """Test multiple sequential session contexts."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        sessions = [MagicMock(spec=Session) for _ in range(3)]
        mock_session_class.side_effect = sessions

        # Use sessions sequentially
        for i in range(3):
            with get_session() as session:
                assert session is sessions[i]

        # All should be committed and closed
        for mock_session in sessions:
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_session_reuse_pattern(self, mock_session_class, mock_get_engine):
        """Test that sessions are not reused across contexts."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_session1 = MagicMock(spec=Session)
        mock_session2 = MagicMock(spec=Session)
        mock_session_class.side_effect = [mock_session1, mock_session2]

        with get_session() as session1:
            pass

        with get_session() as session2:
            pass

        # Different session objects
        assert mock_session1 is not mock_session2

        # First session should be closed before second is created
        assert mock_session1.close.called
        assert mock_session2.close.called


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_commit_raises_exception(self, mock_session_class, mock_get_engine):
        """Test handling when commit itself raises an exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session.commit.side_effect = RuntimeError("Commit failed")
        mock_session_class.return_value = mock_session

        with pytest.raises(RuntimeError, match="Commit failed"):
            with get_session() as session:
                pass

        # Rollback should be called after commit fails
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_rollback_raises_exception(self, mock_session_class, mock_get_engine):
        """Test handling when rollback raises an exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session.rollback.side_effect = RuntimeError("Rollback failed")
        mock_session_class.return_value = mock_session

        # When rollback raises, it can suppress or replace the original exception
        # depending on Python's exception chaining behavior
        with pytest.raises((ValueError, RuntimeError)):
            with get_session() as session:
                raise ValueError("Original error")

        # rollback was called even though it raised
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    @patch("mlb_data_platform.database.session.Session")
    def test_close_raises_exception(self, mock_session_class, mock_get_engine):
        """Test handling when close raises an exception."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session = MagicMock(spec=Session)
        mock_session.close.side_effect = RuntimeError("Close failed")
        mock_session_class.return_value = mock_session

        # Close exception should propagate after successful commit
        with pytest.raises(RuntimeError, match="Close failed"):
            with get_session() as session:
                pass

        mock_session.commit.assert_called_once()

    @patch("mlb_data_platform.database.session.get_engine")
    def test_get_engine_raises_exception(self, mock_get_engine):
        """Test handling when get_engine raises an exception."""
        mock_get_engine.side_effect = RuntimeError("Engine creation failed")

        with pytest.raises(RuntimeError, match="Engine creation failed"):
            with get_session() as session:
                pass

    @patch("mlb_data_platform.database.session.create_engine")
    def test_engine_cache_with_none_config(self, mock_create_engine):
        """Test engine caching when config is None."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Multiple calls with None should use cached engine
        engine1 = get_engine(None)
        engine2 = get_engine(None)

        assert engine1 is engine2
        assert mock_create_engine.call_count == 1
