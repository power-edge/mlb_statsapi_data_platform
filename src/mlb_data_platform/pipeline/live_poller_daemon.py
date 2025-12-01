"""Live Game Poller Daemon.

Long-running process that continuously polls active MLB games.
Designed to run as a Kubernetes Deployment for sub-minute polling.

Architecture:
1. Check schedule every N minutes for active games (Live/Preview)
2. Poll each active game every 30 seconds
3. Store results to PostgreSQL with timestamps
4. Track last timestamp per game for efficient diff fetching

Usage:
    python -m mlb_data_platform.pipeline.live_poller_daemon

Environment Variables:
    POLL_INTERVAL_SECONDS: How often to poll each game (default: 30)
    SCHEDULE_CHECK_INTERVAL_SECONDS: How often to check for new games (default: 300)
    MAX_CONCURRENT_GAMES: Max games to poll at once (default: 20)
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import date, datetime
from typing import Any

from pymlb_statsapi import StatsAPI

from mlb_data_platform.pipeline.extractors import GameExtractor, ScheduleExtractor
from mlb_data_platform.pipeline.storage_adapter import StorageAdapter
from mlb_data_platform.storage.postgres import PostgresConfig, PostgresStorageBackend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class LivePollerDaemon:
    """Daemon that continuously polls active MLB games."""

    def __init__(
        self,
        poll_interval: int = 30,
        schedule_check_interval: int = 300,
        max_concurrent_games: int = 20,
    ):
        """Initialize the daemon.

        Args:
            poll_interval: Seconds between game polls (default 30)
            schedule_check_interval: Seconds between schedule checks (default 300)
            max_concurrent_games: Max games to poll concurrently (default 20)
        """
        self.poll_interval = poll_interval
        self.schedule_check_interval = schedule_check_interval
        self.max_concurrent_games = max_concurrent_games

        self.api = StatsAPI()
        self.backend: PostgresStorageBackend | None = None
        self.adapter: StorageAdapter | None = None

        # Track active games and their last timestamps
        self.active_games: dict[int, str | None] = {}  # game_pk -> last_timestamp
        self.game_states: dict[int, str] = {}  # game_pk -> abstract_game_state

        # Shutdown flag
        self.running = True

        # Statistics
        self.stats = {
            "games_polled": 0,
            "polls_completed": 0,
            "errors": 0,
            "started_at": datetime.now(),
        }

    def _init_storage(self) -> None:
        """Initialize PostgreSQL storage backend."""
        config = PostgresConfig(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            database=os.environ.get("POSTGRES_DB", "mlb_games"),
            user=os.environ.get("POSTGRES_USER", "mlb_admin"),
            password=os.environ.get("POSTGRES_PASSWORD", "mlb_admin_password"),
        )

        self.backend = PostgresStorageBackend(config)
        self.adapter = StorageAdapter(self.backend, upsert=False)
        logger.info(f"Connected to PostgreSQL at {config.host}:{config.port}/{config.database}")

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers."""
        def shutdown_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.running = False

        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)

    async def check_schedule(self) -> list[int]:
        """Check schedule for active games.

        Returns:
            List of game_pks that are currently active (Live or Preview)
        """
        try:
            today = date.today()
            response = self.api.Schedule.schedule(
                date=today.strftime("%Y-%m-%d"),
                sportId=1,
            )
            data = response.json()

            # Store schedule data
            if self.adapter:
                self.adapter.store(
                    endpoint="schedule",
                    method="schedule",
                    data=data,
                    metadata={"schedule_date": today.isoformat(), "sport_id": 1},
                )

            # Extract active games
            games = ScheduleExtractor.extract_games(data)
            active_pks = []

            for game in games:
                self.game_states[game.game_pk] = game.abstract_game_state or "Unknown"

                if game.is_active:  # Live or Preview
                    active_pks.append(game.game_pk)
                    if game.game_pk not in self.active_games:
                        self.active_games[game.game_pk] = None
                        logger.info(
                            f"New active game detected: {game.game_pk} "
                            f"({game.away_team} @ {game.home_team}) - {game.status}"
                        )

            # Remove games that are no longer active
            finished_pks = [
                pk for pk in self.active_games
                if pk not in active_pks
            ]
            for pk in finished_pks:
                state = self.game_states.get(pk, "Unknown")
                logger.info(f"Game {pk} is no longer active (state: {state})")
                del self.active_games[pk]

            logger.info(f"Active games: {len(active_pks)}")
            return active_pks

        except Exception as e:
            logger.error(f"Error checking schedule: {e}")
            self.stats["errors"] += 1
            return list(self.active_games.keys())

    async def poll_game(self, game_pk: int) -> dict[str, Any] | None:
        """Poll a single game for updates.

        Args:
            game_pk: Game primary key

        Returns:
            Game data dict or None if error
        """
        try:
            last_timestamp = self.active_games.get(game_pk)

            # Fetch game data (with timecode if we have a last timestamp)
            if last_timestamp:
                response = self.api.Game.liveGameV1(
                    game_pk=game_pk,
                    timecode=last_timestamp,
                )
            else:
                response = self.api.Game.liveGameV1(game_pk=game_pk)

            data = response.json()

            # Store game data
            if self.adapter:
                self.adapter.store(
                    endpoint="game",
                    method="liveGameV1",
                    data=data,
                    metadata={"game_pk": game_pk, "timecode": last_timestamp},
                )

            # Update last timestamp
            new_timestamp = GameExtractor.extract_latest_timestamp(data)
            if new_timestamp:
                self.active_games[game_pk] = new_timestamp

            # Check if game is finished
            game_state = GameExtractor.extract_game_state(data)
            if game_state == "Final":
                logger.info(f"Game {game_pk} is Final, removing from active polling")
                if game_pk in self.active_games:
                    del self.active_games[game_pk]

            self.stats["games_polled"] += 1
            return data

        except Exception as e:
            logger.error(f"Error polling game {game_pk}: {e}")
            self.stats["errors"] += 1
            return None

    async def poll_all_active_games(self) -> None:
        """Poll all active games concurrently."""
        game_pks = list(self.active_games.keys())[:self.max_concurrent_games]

        if not game_pks:
            return

        logger.debug(f"Polling {len(game_pks)} active games...")

        # Poll games concurrently
        tasks = [self.poll_game(pk) for pk in game_pks]
        await asyncio.gather(*tasks, return_exceptions=True)

        self.stats["polls_completed"] += 1

    async def run(self) -> None:
        """Main daemon loop."""
        logger.info("Starting Live Game Poller Daemon")
        logger.info(f"  Poll interval: {self.poll_interval}s")
        logger.info(f"  Schedule check interval: {self.schedule_check_interval}s")
        logger.info(f"  Max concurrent games: {self.max_concurrent_games}")

        self._setup_signal_handlers()
        self._init_storage()

        last_schedule_check = 0

        try:
            while self.running:
                now = datetime.now().timestamp()

                # Check schedule periodically
                if now - last_schedule_check >= self.schedule_check_interval:
                    await self.check_schedule()
                    last_schedule_check = now

                # Poll active games
                if self.active_games:
                    await self.poll_all_active_games()
                else:
                    logger.debug("No active games to poll")

                # Wait before next poll
                await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            logger.info("Daemon cancelled")
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """Clean shutdown."""
        logger.info("Shutting down Live Game Poller Daemon")
        logger.info(f"Statistics:")
        logger.info(f"  Games polled: {self.stats['games_polled']}")
        logger.info(f"  Poll cycles: {self.stats['polls_completed']}")
        logger.info(f"  Errors: {self.stats['errors']}")

        uptime = datetime.now() - self.stats["started_at"]
        logger.info(f"  Uptime: {uptime}")

        if self.backend:
            self.backend.close()
            logger.info("Database connection closed")


def main():
    """Entry point for the daemon."""
    daemon = LivePollerDaemon(
        poll_interval=int(os.environ.get("POLL_INTERVAL_SECONDS", "30")),
        schedule_check_interval=int(os.environ.get("SCHEDULE_CHECK_INTERVAL_SECONDS", "300")),
        max_concurrent_games=int(os.environ.get("MAX_CONCURRENT_GAMES", "20")),
    )

    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Daemon crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
