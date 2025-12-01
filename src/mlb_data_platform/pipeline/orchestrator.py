"""Pipeline orchestrator for hierarchical MLB data ingestion.

Orchestrates the flow:
    Season → Schedule → Game → Person/Team enrichment

Supports:
- Full season backfill (historical data)
- Daily schedule updates
- Live game polling with timestamp tracking
- Entity enrichment from game rosters
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from pymlb_statsapi import StatsAPI

from mlb_data_platform.pipeline.extractors import (
    GameExtractor,
    GameInfo,
    ScheduleExtractor,
    SeasonDates,
    SeasonExtractor,
)
from mlb_data_platform.pipeline.live_poller import LiveGamePoller

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    # Season settings
    sport_id: int = 1  # MLB
    include_spring_training: bool = False

    # Schedule settings
    days_ahead: int = 1  # How many days ahead to fetch
    days_behind: int = 0  # How many days behind to fetch (for updates)

    # Game settings
    fetch_final_games: bool = True
    fetch_live_games: bool = True
    fetch_preview_games: bool = False

    # Live polling settings
    poll_interval_seconds: int = 30
    max_poll_duration_hours: int = 6

    # Enrichment settings
    enrich_players: bool = True
    enrich_teams: bool = True

    # Backfill settings
    backfill_timestamps: bool = True  # Fetch all timestamps for completed games


@dataclass
class PipelineResult:
    """Results from a pipeline execution."""

    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None

    # Counts
    seasons_fetched: int = 0
    schedules_fetched: int = 0
    games_fetched: int = 0
    timestamps_fetched: int = 0
    players_fetched: int = 0
    teams_fetched: int = 0

    # Details
    game_pks: list[int] = field(default_factory=list)
    player_ids: list[int] = field(default_factory=list)
    team_ids: list[int] = field(default_factory=list)

    # Errors
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return (datetime.now() - self.started_at).total_seconds()


class PipelineOrchestrator:
    """Orchestrates hierarchical data ingestion pipelines.

    Usage:
        >>> api = StatsAPI()
        >>> orchestrator = PipelineOrchestrator(api)
        >>>
        >>> # Fetch today's data
        >>> result = orchestrator.run_daily()
        >>>
        >>> # Backfill a season
        >>> result = orchestrator.backfill_season(season_id="2024")
        >>>
        >>> # Fetch specific games
        >>> result = orchestrator.fetch_games([745123, 745124])
    """

    def __init__(
        self,
        api: StatsAPI | None = None,
        config: PipelineConfig | None = None,
        storage_callback: Callable | None = None,
    ):
        """Initialize the pipeline orchestrator.

        Args:
            api: StatsAPI instance (creates new one if None)
            config: Pipeline configuration
            storage_callback: Optional callback for storing fetched data
                Signature: callback(endpoint: str, method: str, data: dict, metadata: dict)
        """
        self.api = api or StatsAPI()
        self.config = config or PipelineConfig()
        self.storage_callback = storage_callback
        self.live_poller = LiveGamePoller(self.api, self.config.poll_interval_seconds)

    def _store(
        self,
        endpoint: str,
        method: str,
        data: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Store fetched data using callback if provided."""
        if self.storage_callback:
            self.storage_callback(endpoint, method, data, metadata or {})

    # =========================================================================
    # SEASON OPERATIONS
    # =========================================================================

    def fetch_seasons(self, sport_id: int | None = None) -> list[SeasonDates]:
        """Fetch season data and extract date ranges.

        Args:
            sport_id: Sport ID (default from config)

        Returns:
            List of SeasonDates
        """
        sport_id = sport_id or self.config.sport_id

        response = self.api.Season.seasons(sportId=sport_id)
        data = response.json()

        self._store("season", "seasons", data, {"sport_id": sport_id})

        return SeasonExtractor.extract_dates(data, sport_id)

    def get_current_season(self, sport_id: int | None = None) -> SeasonDates | None:
        """Get the current season's date ranges.

        Args:
            sport_id: Sport ID (default from config)

        Returns:
            Current SeasonDates or None
        """
        sport_id = sport_id or self.config.sport_id

        response = self.api.Season.seasons(sportId=sport_id)
        data = response.json()

        self._store("season", "seasons", data, {"sport_id": sport_id})

        return SeasonExtractor.get_current_season(data, sport_id)

    # =========================================================================
    # SCHEDULE OPERATIONS
    # =========================================================================

    def fetch_schedule(
        self,
        schedule_date: date | None = None,
        sport_id: int | None = None,
    ) -> list[GameInfo]:
        """Fetch schedule for a date and extract game info.

        Args:
            schedule_date: Date to fetch (default today)
            sport_id: Sport ID (default from config)

        Returns:
            List of GameInfo
        """
        schedule_date = schedule_date or date.today()
        sport_id = sport_id or self.config.sport_id

        response = self.api.Schedule.schedule(
            date=schedule_date.strftime("%Y-%m-%d"),
            sportId=sport_id,
        )
        data = response.json()

        self._store(
            "schedule",
            "schedule",
            data,
            {"schedule_date": schedule_date.isoformat(), "sport_id": sport_id},
        )

        return ScheduleExtractor.extract_games(data)

    def fetch_schedule_range(
        self,
        start_date: date,
        end_date: date,
        sport_id: int | None = None,
    ) -> dict[date, list[GameInfo]]:
        """Fetch schedules for a date range.

        Args:
            start_date: Start of range
            end_date: End of range
            sport_id: Sport ID (default from config)

        Returns:
            Dict mapping date to list of GameInfo
        """
        results = {}
        current = start_date

        while current <= end_date:
            games = self.fetch_schedule(current, sport_id)
            if games:
                results[current] = games
            current += timedelta(days=1)

        return results

    def get_active_games(self, sport_id: int | None = None) -> list[GameInfo]:
        """Get currently active games (live or preview).

        Args:
            sport_id: Sport ID (default from config)

        Returns:
            List of active GameInfo
        """
        games = self.fetch_schedule(sport_id=sport_id)
        return [g for g in games if g.is_active]

    # =========================================================================
    # GAME OPERATIONS
    # =========================================================================

    def fetch_game(
        self,
        game_pk: int,
        timecode: str | None = None,
    ) -> dict[str, Any]:
        """Fetch game data.

        Args:
            game_pk: Game primary key
            timecode: Optional timestamp (YYYYMMDD_HHMMSS)

        Returns:
            Raw game data dict
        """
        if timecode:
            response = self.api.Game.liveGameV1(game_pk=game_pk, timecode=timecode)
        else:
            response = self.api.Game.liveGameV1(game_pk=game_pk)

        data = response.json()

        self._store(
            "game",
            "liveGameV1",
            data,
            {"game_pk": game_pk, "timecode": timecode},
        )

        return data

    def fetch_game_timestamps(self, game_pk: int) -> list[str]:
        """Fetch all available timestamps for a game.

        Args:
            game_pk: Game primary key

        Returns:
            List of timestamp strings
        """
        response = self.api.Game.liveTimestampv11(game_pk=game_pk)
        data = response.json()

        # Response is array of strings
        timestamps = data if isinstance(data, list) else []

        self._store(
            "game",
            "liveTimestampv11",
            data,
            {"game_pk": game_pk},
        )

        return timestamps

    def fetch_games(self, game_pks: list[int]) -> list[dict[str, Any]]:
        """Fetch multiple games.

        Args:
            game_pks: List of game primary keys

        Returns:
            List of game data dicts
        """
        results = []
        for game_pk in game_pks:
            try:
                data = self.fetch_game(game_pk)
                results.append(data)
            except Exception as e:
                logger.error(f"Failed to fetch game {game_pk}: {e}")
        return results

    # =========================================================================
    # ENRICHMENT OPERATIONS
    # =========================================================================

    def fetch_person(self, person_id: int) -> dict[str, Any]:
        """Fetch person data.

        Args:
            person_id: Person ID

        Returns:
            Raw person data dict
        """
        response = self.api.Person.person(personIds=[person_id])
        data = response.json()

        self._store("person", "person", data, {"person_id": person_id})

        return data

    def fetch_team(self, team_id: int, season: str | None = None) -> dict[str, Any]:
        """Fetch team data.

        Args:
            team_id: Team ID
            season: Season year (default current)

        Returns:
            Raw team data dict
        """
        kwargs = {"teamId": team_id}
        if season:
            kwargs["season"] = season

        response = self.api.Team.teams(**kwargs)
        data = response.json()

        self._store("team", "teams", data, {"team_id": team_id, "season": season})

        return data

    def enrich_from_game(self, game_data: dict[str, Any]) -> PipelineResult:
        """Fetch person and team data referenced in a game.

        Args:
            game_data: Raw game data from liveGameV1

        Returns:
            PipelineResult with enrichment counts
        """
        result = PipelineResult()
        roster = GameExtractor.extract_roster(game_data)

        # Fetch players
        if self.config.enrich_players:
            for player_id in roster.player_ids:
                try:
                    self.fetch_person(player_id)
                    result.players_fetched += 1
                    result.player_ids.append(player_id)
                except Exception as e:
                    result.errors.append(f"Person {player_id}: {e}")

        # Fetch teams
        if self.config.enrich_teams:
            for team_id in [roster.home_team_id, roster.away_team_id]:
                if team_id:
                    try:
                        self.fetch_team(team_id)
                        result.teams_fetched += 1
                        result.team_ids.append(team_id)
                    except Exception as e:
                        result.errors.append(f"Team {team_id}: {e}")

        result.finished_at = datetime.now()
        return result

    # =========================================================================
    # PIPELINE OPERATIONS
    # =========================================================================

    def run_daily(self, target_date: date | None = None) -> PipelineResult:
        """Run daily pipeline: schedule → games → enrichment.

        Args:
            target_date: Date to process (default today)

        Returns:
            PipelineResult
        """
        target_date = target_date or date.today()
        result = PipelineResult()

        logger.info(f"Running daily pipeline for {target_date}")

        # 1. Fetch schedule
        games = self.fetch_schedule(target_date)
        result.schedules_fetched = 1

        # 2. Filter games based on config
        games_to_fetch = []
        for game in games:
            if game.is_final and self.config.fetch_final_games or game.is_live and self.config.fetch_live_games or game.is_preview and self.config.fetch_preview_games:
                games_to_fetch.append(game)

        logger.info(f"Found {len(games_to_fetch)} games to fetch")

        # 3. Fetch games
        for game_info in games_to_fetch:
            try:
                game_data = self.fetch_game(game_info.game_pk)
                result.games_fetched += 1
                result.game_pks.append(game_info.game_pk)

                # 4. Enrich with player/team data
                enrichment = self.enrich_from_game(game_data)
                result.players_fetched += enrichment.players_fetched
                result.teams_fetched += enrichment.teams_fetched
                result.player_ids.extend(enrichment.player_ids)
                result.team_ids.extend(enrichment.team_ids)
                result.errors.extend(enrichment.errors)

                # 5. Fetch timestamps for completed games
                if game_info.is_final and self.config.backfill_timestamps:
                    timestamps = self.fetch_game_timestamps(game_info.game_pk)
                    result.timestamps_fetched += len(timestamps)

            except Exception as e:
                result.errors.append(f"Game {game_info.game_pk}: {e}")
                logger.error(f"Failed to process game {game_info.game_pk}: {e}")

        result.finished_at = datetime.now()
        logger.info(
            f"Daily pipeline complete: {result.games_fetched} games, "
            f"{result.players_fetched} players, {result.teams_fetched} teams"
        )

        return result

    def backfill_season(
        self,
        season_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> PipelineResult:
        """Backfill an entire season or date range.

        Args:
            season_id: Season to backfill (e.g., "2024")
            start_date: Override start date
            end_date: Override end date

        Returns:
            PipelineResult
        """
        result = PipelineResult()

        # Get season dates if not provided
        if not start_date or not end_date:
            seasons = self.fetch_seasons()
            result.seasons_fetched = 1

            # Find matching season
            target_season = None
            for s in seasons:
                if season_id and s.season_id == season_id:
                    target_season = s
                    break
                elif not season_id:
                    target_season = s  # Use first/current
                    break

            if not target_season:
                result.errors.append(f"Season {season_id} not found")
                result.finished_at = datetime.now()
                return result

            start_date, end_date = target_season.get_date_range(
                self.config.include_spring_training
            )

        logger.info(f"Backfilling from {start_date} to {end_date}")

        # Fetch each day
        current = start_date
        while current <= end_date:
            daily_result = self.run_daily(current)

            result.schedules_fetched += daily_result.schedules_fetched
            result.games_fetched += daily_result.games_fetched
            result.timestamps_fetched += daily_result.timestamps_fetched
            result.players_fetched += daily_result.players_fetched
            result.teams_fetched += daily_result.teams_fetched
            result.game_pks.extend(daily_result.game_pks)
            result.player_ids.extend(daily_result.player_ids)
            result.team_ids.extend(daily_result.team_ids)
            result.errors.extend(daily_result.errors)

            current += timedelta(days=1)

        result.finished_at = datetime.now()
        logger.info(
            f"Season backfill complete: {result.games_fetched} games over "
            f"{result.schedules_fetched} days in {result.duration_seconds:.1f}s"
        )

        return result
