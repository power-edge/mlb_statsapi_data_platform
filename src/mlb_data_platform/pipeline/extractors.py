"""Data extractors for pipeline orchestration.

Extracts downstream identifiers from upstream data:
- SeasonExtractor: Extract date ranges from season data
- ScheduleExtractor: Extract game_pks and metadata from schedule data
- GameExtractor: Extract person_ids, team_ids from game data
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class SeasonDates:
    """Date ranges extracted from season data."""

    season_id: str
    sport_id: int
    regular_season_start: date | None = None
    regular_season_end: date | None = None
    spring_start: date | None = None
    spring_end: date | None = None
    postseason_start: date | None = None
    postseason_end: date | None = None
    all_star_date: date | None = None

    def get_date_range(self, include_spring: bool = False) -> tuple[date, date]:
        """Get the full date range for this season.

        Args:
            include_spring: Include spring training dates

        Returns:
            Tuple of (start_date, end_date)
        """
        start = self.spring_start if include_spring and self.spring_start else self.regular_season_start
        end = self.postseason_end or self.regular_season_end

        if not start or not end:
            raise ValueError(f"Season {self.season_id} missing date boundaries")

        return start, end


@dataclass
class GameInfo:
    """Game information extracted from schedule."""

    game_pk: int
    game_date: date
    game_datetime: datetime | None = None
    status_code: str | None = None
    detailed_state: str | None = None
    abstract_game_state: str | None = None
    home_team_id: int | None = None
    away_team_id: int | None = None
    venue_id: int | None = None

    @property
    def is_live(self) -> bool:
        """Check if game is currently live."""
        return self.abstract_game_state == "Live"

    @property
    def is_preview(self) -> bool:
        """Check if game is in preview/pre-game state."""
        return self.abstract_game_state == "Preview"

    @property
    def is_final(self) -> bool:
        """Check if game is final."""
        return self.abstract_game_state == "Final"

    @property
    def is_active(self) -> bool:
        """Check if game should be polled (live or preview)."""
        return self.is_live or self.is_preview


@dataclass
class GameRoster:
    """Roster information extracted from game data."""

    game_pk: int
    home_team_id: int
    away_team_id: int
    player_ids: list[int] = field(default_factory=list)
    umpire_ids: list[int] = field(default_factory=list)


class SeasonExtractor:
    """Extract date ranges and metadata from season API responses."""

    @staticmethod
    def extract_dates(data: dict[str, Any], sport_id: int = 1) -> list[SeasonDates]:
        """Extract season date ranges from Season.seasons response.

        Args:
            data: Raw API response from Season.seasons
            sport_id: Sport ID for context

        Returns:
            List of SeasonDates objects
        """
        seasons = data.get("seasons", [])
        results = []

        for season in seasons:
            season_dates = SeasonDates(
                season_id=season.get("seasonId", ""),
                sport_id=sport_id,
                regular_season_start=_parse_date(season.get("regularSeasonStartDate")),
                regular_season_end=_parse_date(season.get("regularSeasonEndDate")),
                spring_start=_parse_date(season.get("springStartDate")),
                spring_end=_parse_date(season.get("springEndDate")),
                postseason_start=_parse_date(season.get("postSeasonStartDate")),
                postseason_end=_parse_date(season.get("postSeasonEndDate")),
                all_star_date=_parse_date(season.get("allStarDate")),
            )
            results.append(season_dates)

        return results

    @staticmethod
    def get_current_season(data: dict[str, Any], sport_id: int = 1) -> SeasonDates | None:
        """Get the current season from season data.

        Args:
            data: Raw API response from Season.seasons
            sport_id: Sport ID for context

        Returns:
            Current SeasonDates or None
        """
        seasons = SeasonExtractor.extract_dates(data, sport_id)
        today = date.today()

        for season in seasons:
            start, end = season.regular_season_start, season.regular_season_end
            if start and end and start <= today <= end:
                return season

        # If no current season, return most recent
        return seasons[0] if seasons else None


class ScheduleExtractor:
    """Extract game information from schedule API responses."""

    @staticmethod
    def extract_games(data: dict[str, Any]) -> list[GameInfo]:
        """Extract game info from Schedule.schedule response.

        Args:
            data: Raw API response from Schedule.schedule

        Returns:
            List of GameInfo objects
        """
        games = []

        for date_entry in data.get("dates", []):
            game_date = _parse_date(date_entry.get("date"))

            for game in date_entry.get("games", []):
                game_info = GameInfo(
                    game_pk=game.get("gamePk"),
                    game_date=game_date,
                    game_datetime=_parse_datetime(game.get("gameDate")),
                    status_code=game.get("status", {}).get("statusCode"),
                    detailed_state=game.get("status", {}).get("detailedState"),
                    abstract_game_state=game.get("status", {}).get("abstractGameState"),
                    home_team_id=game.get("teams", {}).get("home", {}).get("team", {}).get("id"),
                    away_team_id=game.get("teams", {}).get("away", {}).get("team", {}).get("id"),
                    venue_id=game.get("venue", {}).get("id"),
                )
                games.append(game_info)

        return games

    @staticmethod
    def get_game_pks(data: dict[str, Any]) -> list[int]:
        """Extract just game_pks from schedule.

        Args:
            data: Raw API response from Schedule.schedule

        Returns:
            List of game_pk integers
        """
        return [g.game_pk for g in ScheduleExtractor.extract_games(data)]

    @staticmethod
    def get_live_games(data: dict[str, Any]) -> list[GameInfo]:
        """Get games that are currently live.

        Args:
            data: Raw API response from Schedule.schedule

        Returns:
            List of live GameInfo objects
        """
        return [g for g in ScheduleExtractor.extract_games(data) if g.is_live]

    @staticmethod
    def get_active_games(data: dict[str, Any]) -> list[GameInfo]:
        """Get games that should be polled (live or preview).

        Args:
            data: Raw API response from Schedule.schedule

        Returns:
            List of active GameInfo objects
        """
        return [g for g in ScheduleExtractor.extract_games(data) if g.is_active]


class GameExtractor:
    """Extract roster and metadata from game API responses."""

    @staticmethod
    def extract_roster(data: dict[str, Any]) -> GameRoster:
        """Extract roster information from Game.liveGameV1 response.

        Args:
            data: Raw API response from Game.liveGameV1

        Returns:
            GameRoster object
        """
        game_data = data.get("gameData", {})
        live_data = data.get("liveData", {})

        game_pk = game_data.get("game", {}).get("pk")

        # Get team IDs
        teams = game_data.get("teams", {})
        home_team_id = teams.get("home", {}).get("id")
        away_team_id = teams.get("away", {}).get("id")

        # Extract player IDs from boxscore
        player_ids = set()
        boxscore = live_data.get("boxscore", {})

        for team_key in ["home", "away"]:
            team_data = boxscore.get("teams", {}).get(team_key, {})

            # Players in various roles
            for player_id in team_data.get("players", {}).keys():
                # Format is "ID{player_id}"
                if player_id.startswith("ID"):
                    player_ids.add(int(player_id[2:]))

            # Batting order
            for player_id in team_data.get("battingOrder", []):
                player_ids.add(player_id)

            # Bench, bullpen, pitchers
            for key in ["bench", "bullpen", "pitchers"]:
                for player_id in team_data.get(key, []):
                    player_ids.add(player_id)

        # Extract umpire IDs
        umpire_ids = []
        for official in boxscore.get("officials", []):
            official_data = official.get("official", {})
            if official_data.get("id"):
                umpire_ids.append(official_data["id"])

        return GameRoster(
            game_pk=game_pk,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            player_ids=list(player_ids),
            umpire_ids=umpire_ids,
        )

    @staticmethod
    def extract_latest_timestamp(data: dict[str, Any]) -> str | None:
        """Extract the latest timestamp from game data.

        Args:
            data: Raw API response from Game.liveGameV1

        Returns:
            Timestamp string (YYYYMMDD_HHMMSS) or None
        """
        meta = data.get("metaData", {})
        return meta.get("timeStamp")

    @staticmethod
    def extract_game_state(data: dict[str, Any]) -> str | None:
        """Extract abstract game state from game data.

        Args:
            data: Raw API response from Game.liveGameV1

        Returns:
            Abstract game state ('Preview', 'Live', 'Final') or None
        """
        status = data.get("gameData", {}).get("status", {})
        return status.get("abstractGameState")


def _parse_date(date_str: str | None) -> date | None:
    """Parse date string (YYYY-MM-DD) to date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_datetime(datetime_str: str | None) -> datetime | None:
    """Parse ISO datetime string to datetime object."""
    if not datetime_str:
        return None
    try:
        # Handle ISO format with Z suffix
        if datetime_str.endswith("Z"):
            datetime_str = datetime_str[:-1] + "+00:00"
        return datetime.fromisoformat(datetime_str)
    except (ValueError, TypeError):
        return None
