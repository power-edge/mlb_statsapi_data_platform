"""Unit tests for pipeline live_poller.

Tests the LiveGamePoller class that polls MLB API for live game updates
using timestamp-based diffs.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from mlb_data_platform.pipeline.live_poller import (
    LiveGamePoller,
    LiveGameState,
    TimestampedSnapshot,
)


class TestTimestampedSnapshot:
    """Test TimestampedSnapshot dataclass."""

    def test_create_full_snapshot(self):
        """Test creating a full payload snapshot."""
        snapshot = TimestampedSnapshot(
            game_pk=745123,
            timestamp="20241027_230500",
            captured_at=datetime(2024, 10, 27, 23, 5, 0),
            data={"gameData": {"game": {"pk": 745123}}},
            is_diff=False,
        )

        assert snapshot.game_pk == 745123
        assert snapshot.timestamp == "20241027_230500"
        assert snapshot.is_diff is False

    def test_create_diff_snapshot(self):
        """Test creating a diff/patch snapshot."""
        snapshot = TimestampedSnapshot(
            game_pk=745123,
            timestamp="20241027_230600",
            captured_at=datetime(2024, 10, 27, 23, 6, 0),
            data=[{"path": "/liveData/plays", "op": "add"}],
            is_diff=True,
        )

        assert snapshot.is_diff is True


class TestLiveGameState:
    """Test LiveGameState dataclass."""

    def test_is_complete_when_final(self):
        """Test is_complete property for final game."""
        state = LiveGameState(
            game_pk=745123,
            abstract_state="Final",
        )

        assert state.is_complete is True
        assert state.is_active is False

    def test_is_active_when_live(self):
        """Test is_active property for live game."""
        state = LiveGameState(
            game_pk=745123,
            abstract_state="Live",
        )

        assert state.is_complete is False
        assert state.is_active is True

    def test_is_active_when_preview(self):
        """Test is_active property for preview game."""
        state = LiveGameState(
            game_pk=745123,
            abstract_state="Preview",
        )

        assert state.is_complete is False
        assert state.is_active is True

    def test_initial_state(self):
        """Test initial state values."""
        state = LiveGameState(game_pk=745123)

        assert state.last_timestamp is None
        assert state.abstract_state is None
        assert state.snapshots == []
        assert state.poll_count == 0
        assert state.started_at is None
        assert state.finished_at is None


class TestLiveGamePoller:
    """Test LiveGamePoller class."""

    @pytest.fixture
    def mock_api(self):
        """Create a mock StatsAPI instance."""
        return MagicMock()

    @pytest.fixture
    def poller(self, mock_api):
        """Create a LiveGamePoller with mock API."""
        return LiveGamePoller(api=mock_api, poll_interval_seconds=30)

    def test_init_default(self):
        """Test default initialization."""
        with patch("mlb_data_platform.pipeline.live_poller.StatsAPI") as MockAPI:
            MockAPI.return_value = MagicMock()
            poller = LiveGamePoller()

            assert poller.poll_interval == 30
            MockAPI.assert_called_once()

    def test_init_custom_interval(self, mock_api):
        """Test initialization with custom poll interval."""
        poller = LiveGamePoller(api=mock_api, poll_interval_seconds=60)

        assert poller.poll_interval == 60

    def test_get_all_timestamps(self, poller, mock_api):
        """Test getting all timestamps for a game."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            "20241027_200800",
            "20241027_201000",
            "20241027_202000",
        ]
        mock_api.Game.liveTimestampv11.return_value = mock_response

        timestamps = poller.get_all_timestamps(745123)

        assert timestamps == [
            "20241027_200800",
            "20241027_201000",
            "20241027_202000",
        ]
        mock_api.Game.liveTimestampv11.assert_called_once_with(game_pk=745123)

    def test_get_all_timestamps_non_list_response(self, poller, mock_api):
        """Test handling non-list response from timestamps endpoint."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}  # Non-list response
        mock_api.Game.liveTimestampv11.return_value = mock_response

        timestamps = poller.get_all_timestamps(745123)

        assert timestamps == []

    def test_get_snapshot_no_timestamp(self, poller, mock_api):
        """Test getting snapshot without specific timestamp (latest)."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metaData": {"timeStamp": "20241027_230500"},
            "gameData": {
                "game": {"pk": 745123},
                "status": {"abstractGameState": "Live"},
            },
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        snapshot = poller.get_snapshot(745123)

        assert snapshot.game_pk == 745123
        assert snapshot.timestamp == "20241027_230500"
        assert snapshot.is_diff is False
        mock_api.Game.liveGameV1.assert_called_once_with(game_pk=745123)

    def test_get_snapshot_with_timestamp(self, poller, mock_api):
        """Test getting snapshot at specific timestamp."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metaData": {"timeStamp": "20241027_200800"},
            "gameData": {"game": {"pk": 745123}},
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        snapshot = poller.get_snapshot(745123, timestamp="20241027_200800")

        mock_api.Game.liveGameV1.assert_called_once_with(
            game_pk=745123, timecode="20241027_200800"
        )
        assert snapshot.timestamp == "20241027_200800"

    def test_get_diff(self, poller, mock_api):
        """Test getting diff since timestamp."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "metaData": {"timeStamp": "20241027_230600"},
                "path": "/liveData/plays/allPlays/75",
                "op": "add",
                "value": {"result": {"type": "hit"}},
            }
        ]
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_response

        diff = poller.get_diff(745123, start_timecode="20241027_230500")

        assert diff is not None
        assert diff.game_pk == 745123
        assert diff.timestamp == "20241027_230600"
        assert diff.is_diff is True
        mock_api.Game.liveGameDiffPatchV1.assert_called_once_with(
            game_pk=745123, startTimecode="20241027_230500"
        )

    def test_get_diff_with_end_timecode(self, poller, mock_api):
        """Test getting diff with end timecode."""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"path": "/test", "op": "replace"}]
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_response

        poller.get_diff(
            745123,
            start_timecode="20241027_230500",
            end_timecode="20241027_231000",
        )

        mock_api.Game.liveGameDiffPatchV1.assert_called_once_with(
            game_pk=745123,
            startTimecode="20241027_230500",
            endTimecode="20241027_231000",
        )

    def test_get_diff_no_changes(self, poller, mock_api):
        """Test getting diff when no changes available."""
        mock_response = MagicMock()
        mock_response.json.return_value = []  # Empty diff
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_response

        diff = poller.get_diff(745123, start_timecode="20241027_230500")

        assert diff is None

    def test_poll_game_initial(self, poller, mock_api):
        """Test initial poll gets full payload."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metaData": {"timeStamp": "20241027_200800"},
            "gameData": {
                "game": {"pk": 745123},
                "status": {"abstractGameState": "Live"},
            },
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        state = poller.poll_game(745123)

        assert state.game_pk == 745123
        assert state.last_timestamp == "20241027_200800"
        assert state.abstract_state == "Live"
        assert state.poll_count == 1
        assert len(state.snapshots) == 1
        assert state.snapshots[0].is_diff is False

    def test_poll_game_subsequent(self, poller, mock_api):
        """Test subsequent poll gets diff."""
        # Setup existing state
        existing_state = LiveGameState(
            game_pk=745123,
            last_timestamp="20241027_200800",
            abstract_state="Live",
            started_at=datetime.now(),
            poll_count=1,
        )

        # Mock diff response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "metaData": {"timeStamp": "20241027_201000"},
                "path": "/liveData/plays",
                "op": "add",
            }
        ]
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_response

        state = poller.poll_game(745123, state=existing_state)

        assert state.poll_count == 2
        assert state.last_timestamp == "20241027_201000"
        assert len(state.snapshots) == 1
        assert state.snapshots[0].is_diff is True

    def test_poll_game_no_changes(self, poller, mock_api):
        """Test poll with no changes since last timestamp."""
        existing_state = LiveGameState(
            game_pk=745123,
            last_timestamp="20241027_200800",
            abstract_state="Live",
            poll_count=1,
        )

        mock_response = MagicMock()
        mock_response.json.return_value = []  # No changes
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_response

        state = poller.poll_game(745123, state=existing_state)

        assert state.poll_count == 2
        assert state.last_timestamp == "20241027_200800"  # Unchanged
        assert len(state.snapshots) == 0  # No new snapshots

    def test_get_game_state(self, poller):
        """Test getting tracked game state."""
        # Initially no state
        assert poller.get_game_state(745123) is None

        # Add state via poll
        poller._game_states[745123] = LiveGameState(game_pk=745123)

        state = poller.get_game_state(745123)
        assert state is not None
        assert state.game_pk == 745123

    def test_clear_game_state(self, poller):
        """Test clearing tracked game state."""
        poller._game_states[745123] = LiveGameState(game_pk=745123)

        poller.clear_game_state(745123)

        assert poller.get_game_state(745123) is None

    def test_clear_game_state_nonexistent(self, poller):
        """Test clearing non-existent state doesn't raise."""
        # Should not raise
        poller.clear_game_state(999999)

    def test_backfill_game(self, poller, mock_api):
        """Test backfilling all snapshots for a game."""
        # Mock timestamps response
        timestamps_response = MagicMock()
        timestamps_response.json.return_value = [
            "20241027_200800",
            "20241027_201000",
        ]
        mock_api.Game.liveTimestampv11.return_value = timestamps_response

        # Mock game responses for each timestamp
        game_responses = []
        for ts in ["20241027_200800", "20241027_201000", None]:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {
                "metaData": {"timeStamp": ts or "20241027_230000"},
                "gameData": {"game": {"pk": 745123}},
                "liveData": {},
            }
            game_responses.append(mock_resp)

        mock_api.Game.liveGameV1.side_effect = game_responses

        snapshots = poller.backfill_game(745123)

        # Should have 2 timestamped + 1 final snapshot
        assert len(snapshots) == 3
        assert all(s.game_pk == 745123 for s in snapshots)
        assert all(s.is_diff is False for s in snapshots)

    def test_backfill_game_handles_errors(self, poller, mock_api):
        """Test backfill continues on individual snapshot errors."""
        timestamps_response = MagicMock()
        timestamps_response.json.return_value = ["20241027_200800"]
        mock_api.Game.liveTimestampv11.return_value = timestamps_response

        # First call fails, second succeeds
        mock_api.Game.liveGameV1.side_effect = [
            Exception("API Error"),
            MagicMock(
                json=MagicMock(
                    return_value={
                        "metaData": {"timeStamp": "20241027_230000"},
                        "gameData": {"game": {"pk": 745123}},
                        "liveData": {},
                    }
                )
            ),
        ]

        snapshots = poller.backfill_game(745123)

        # Should still get the final snapshot
        assert len(snapshots) == 1


class TestLiveGamePollerStateTracking:
    """Test state tracking across multiple polls."""

    @pytest.fixture
    def mock_api(self):
        """Create mock API."""
        return MagicMock()

    @pytest.fixture
    def poller(self, mock_api):
        """Create poller with mock API."""
        return LiveGamePoller(api=mock_api)

    def test_state_persists_between_polls(self, poller, mock_api):
        """Test that game state persists between poll calls."""
        # First poll
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metaData": {"timeStamp": "20241027_200800"},
            "gameData": {
                "game": {"pk": 745123},
                "status": {"abstractGameState": "Live"},
            },
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        state1 = poller.poll_game(745123)

        # Second poll without passing state
        mock_diff = MagicMock()
        mock_diff.json.return_value = [
            {"metaData": {"timeStamp": "20241027_201000"}, "op": "add"}
        ]
        mock_api.Game.liveGameDiffPatchV1.return_value = mock_diff

        state2 = poller.poll_game(745123)

        # State should be preserved
        assert state2.poll_count == 2
        assert state2 is state1  # Same state object

    def test_multiple_games_tracked_independently(self, poller, mock_api):
        """Test tracking multiple games independently."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metaData": {"timeStamp": "20241027_200800"},
            "gameData": {
                "game": {"pk": 745123},
                "status": {"abstractGameState": "Live"},
            },
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        state1 = poller.poll_game(745123)
        state2 = poller.poll_game(745124)

        assert state1.game_pk == 745123
        assert state2.game_pk == 745124
        assert poller.get_game_state(745123) is not poller.get_game_state(745124)
