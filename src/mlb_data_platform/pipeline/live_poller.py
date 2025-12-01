"""Live game polling using timestamp diffs.

Implements the MLB Stats API recommended polling pattern:
1. Get initial full payload (no timecode)
2. Extract latest timestamp from response
3. Poll with startTimecode to get diffs
4. Store timestamped snapshots for replay capability

Key Endpoints:
- liveTimestampv11: Get all available timestamps (for backfill)
- liveGameDiffPatchV1: Get changes since last timestamp (for live polling)
- liveGameV1: Get full payload at specific timecode (for snapshots)
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pymlb_statsapi import StatsAPI

logger = logging.getLogger(__name__)


@dataclass
class TimestampedSnapshot:
    """A game snapshot at a specific timestamp."""

    game_pk: int
    timestamp: str  # YYYYMMDD_HHMMSS format
    captured_at: datetime
    data: dict[str, Any]
    is_diff: bool = False  # True if this is a diff patch, False if full payload


@dataclass
class LiveGameState:
    """Tracks the state of a live game being polled."""

    game_pk: int
    last_timestamp: str | None = None
    abstract_state: str | None = None  # Preview, Live, Final
    snapshots: list[TimestampedSnapshot] = field(default_factory=list)
    poll_count: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def is_complete(self) -> bool:
        """Check if game has reached final state."""
        return self.abstract_state == "Final"

    @property
    def is_active(self) -> bool:
        """Check if game should continue to be polled."""
        return self.abstract_state in ("Preview", "Live")


class LiveGamePoller:
    """Polls live games for timestamp-based updates.

    Usage:
        >>> api = StatsAPI()
        >>> poller = LiveGamePoller(api)
        >>>
        >>> # Get all timestamps for a game (backfill)
        >>> timestamps = poller.get_all_timestamps(game_pk=745123)
        >>>
        >>> # Poll for updates (live tracking)
        >>> state = poller.poll_game(game_pk=745123)
        >>> while state.is_active:
        >>>     time.sleep(30)
        >>>     state = poller.poll_game(game_pk=745123, state=state)
        >>>
        >>> # Get snapshot at specific timestamp (replay)
        >>> snapshot = poller.get_snapshot(game_pk=745123, timestamp="20241027_230000")
    """

    def __init__(
        self,
        api: StatsAPI | None = None,
        poll_interval_seconds: int = 30,
    ):
        """Initialize the live game poller.

        Args:
            api: StatsAPI instance (creates new one if None)
            poll_interval_seconds: Recommended wait between polls
        """
        self.api = api or StatsAPI()
        self.poll_interval = poll_interval_seconds
        self._game_states: dict[int, LiveGameState] = {}

    def get_all_timestamps(self, game_pk: int) -> list[str]:
        """Get all available timestamps for a game.

        This is useful for backfilling historical games.

        Args:
            game_pk: Game primary key

        Returns:
            List of timestamp strings (YYYYMMDD_HHMMSS format)
        """
        response = self.api.Game.liveTimestampv11(game_pk=game_pk)
        data = response.json()

        # Response is a list of timestamps
        if isinstance(data, list):
            return data
        return []

    def get_snapshot(
        self,
        game_pk: int,
        timestamp: str | None = None,
    ) -> TimestampedSnapshot:
        """Get a game snapshot at a specific timestamp.

        Args:
            game_pk: Game primary key
            timestamp: Specific timestamp (None for latest)

        Returns:
            TimestampedSnapshot with full game data
        """
        if timestamp:
            response = self.api.Game.liveGameV1(game_pk=game_pk, timecode=timestamp)
        else:
            response = self.api.Game.liveGameV1(game_pk=game_pk)

        data = response.json()
        actual_timestamp = data.get("metaData", {}).get("timeStamp", timestamp or "")

        return TimestampedSnapshot(
            game_pk=game_pk,
            timestamp=actual_timestamp,
            captured_at=datetime.now(),
            data=data,
            is_diff=False,
        )

    def get_diff(
        self,
        game_pk: int,
        start_timecode: str,
        end_timecode: str | None = None,
    ) -> TimestampedSnapshot | None:
        """Get diff/patch data since a timestamp.

        Args:
            game_pk: Game primary key
            start_timecode: Get changes since this timestamp
            end_timecode: Optional end timestamp for snapshot

        Returns:
            TimestampedSnapshot with diff data, or None if no changes
        """
        kwargs = {"game_pk": game_pk, "startTimecode": start_timecode}
        if end_timecode:
            kwargs["endTimecode"] = end_timecode

        response = self.api.Game.liveGameDiffPatchV1(**kwargs)
        data = response.json()

        # Empty response means no changes
        if not data or data == []:
            return None

        # Extract new timestamp from diff
        new_timestamp = None
        if isinstance(data, list) and len(data) > 0:
            # Diff format varies, try to find timestamp
            for item in data:
                if isinstance(item, dict) and "metaData" in item:
                    new_timestamp = item.get("metaData", {}).get("timeStamp")
                    break

        return TimestampedSnapshot(
            game_pk=game_pk,
            timestamp=new_timestamp or start_timecode,
            captured_at=datetime.now(),
            data=data,
            is_diff=True,
        )

    def poll_game(
        self,
        game_pk: int,
        state: LiveGameState | None = None,
    ) -> LiveGameState:
        """Poll a game for updates.

        First call gets full payload, subsequent calls get diffs.

        Args:
            game_pk: Game primary key
            state: Existing game state (None for initial poll)

        Returns:
            Updated LiveGameState
        """
        if state is None:
            state = self._game_states.get(game_pk) or LiveGameState(
                game_pk=game_pk,
                started_at=datetime.now(),
            )
            self._game_states[game_pk] = state

        state.poll_count += 1

        if state.last_timestamp is None:
            # Initial poll - get full payload
            snapshot = self.get_snapshot(game_pk)
            state.last_timestamp = snapshot.timestamp
            state.snapshots.append(snapshot)
            state.abstract_state = snapshot.data.get("gameData", {}).get("status", {}).get(
                "abstractGameState"
            )
            logger.info(
                f"Game {game_pk}: Initial poll, timestamp={snapshot.timestamp}, "
                f"state={state.abstract_state}"
            )
        else:
            # Subsequent poll - get diff
            diff = self.get_diff(game_pk, state.last_timestamp)

            if diff is not None:
                state.snapshots.append(diff)
                state.last_timestamp = diff.timestamp

                # Check if game state changed
                # Note: Diff format needs parsing to extract state
                logger.info(
                    f"Game {game_pk}: Poll #{state.poll_count}, "
                    f"new timestamp={diff.timestamp}"
                )
            else:
                logger.debug(f"Game {game_pk}: Poll #{state.poll_count}, no changes")

        # Check if game is complete
        if state.is_complete:
            state.finished_at = datetime.now()
            # Get final full payload
            final_snapshot = self.get_snapshot(game_pk)
            state.snapshots.append(final_snapshot)
            logger.info(f"Game {game_pk}: Game complete, final snapshot stored")

        return state

    def backfill_game(self, game_pk: int) -> list[TimestampedSnapshot]:
        """Backfill all snapshots for a completed game.

        Gets all timestamps and fetches full payload at each.

        Args:
            game_pk: Game primary key

        Returns:
            List of TimestampedSnapshots at each timestamp
        """
        timestamps = self.get_all_timestamps(game_pk)
        snapshots = []

        logger.info(f"Game {game_pk}: Backfilling {len(timestamps)} timestamps")

        for ts in timestamps:
            try:
                snapshot = self.get_snapshot(game_pk, timestamp=ts)
                snapshots.append(snapshot)
            except Exception as e:
                logger.warning(f"Game {game_pk}: Failed to get snapshot at {ts}: {e}")

        # Also get final (no timestamp) payload
        try:
            final = self.get_snapshot(game_pk)
            snapshots.append(final)
        except Exception as e:
            logger.warning(f"Game {game_pk}: Failed to get final snapshot: {e}")

        return snapshots

    def get_game_state(self, game_pk: int) -> LiveGameState | None:
        """Get the current tracking state for a game.

        Args:
            game_pk: Game primary key

        Returns:
            LiveGameState or None if not being tracked
        """
        return self._game_states.get(game_pk)

    def clear_game_state(self, game_pk: int) -> None:
        """Clear tracking state for a game.

        Args:
            game_pk: Game primary key
        """
        self._game_states.pop(game_pk, None)
