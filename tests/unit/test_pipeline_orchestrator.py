"""Unit tests for pipeline orchestrator.

Tests the PipelineOrchestrator class that coordinates hierarchical
MLB data ingestion pipelines.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from mlb_data_platform.pipeline.orchestrator import (
    PipelineConfig,
    PipelineOrchestrator,
    PipelineResult,
)


class TestPipelineConfig:
    """Test PipelineConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = PipelineConfig()

        assert config.sport_id == 1  # MLB
        assert config.include_spring_training is False
        assert config.days_ahead == 1
        assert config.days_behind == 0
        assert config.fetch_final_games is True
        assert config.fetch_live_games is True
        assert config.fetch_preview_games is False
        assert config.poll_interval_seconds == 30
        assert config.max_poll_duration_hours == 6
        assert config.enrich_players is True
        assert config.enrich_teams is True
        assert config.backfill_timestamps is True

    def test_custom_values(self):
        """Test custom configuration values."""
        config = PipelineConfig(
            sport_id=11,  # Different sport
            include_spring_training=True,
            days_ahead=7,
            fetch_preview_games=True,
            poll_interval_seconds=60,
        )

        assert config.sport_id == 11
        assert config.include_spring_training is True
        assert config.days_ahead == 7
        assert config.fetch_preview_games is True
        assert config.poll_interval_seconds == 60


class TestPipelineResult:
    """Test PipelineResult dataclass."""

    def test_initial_values(self):
        """Test initial result values."""
        result = PipelineResult()

        assert result.seasons_fetched == 0
        assert result.schedules_fetched == 0
        assert result.games_fetched == 0
        assert result.timestamps_fetched == 0
        assert result.players_fetched == 0
        assert result.teams_fetched == 0
        assert result.game_pks == []
        assert result.player_ids == []
        assert result.team_ids == []
        assert result.errors == []
        assert result.finished_at is None

    def test_duration_seconds_ongoing(self):
        """Test duration calculation for ongoing pipeline."""
        result = PipelineResult()

        # Duration should be positive since started_at is set
        assert result.duration_seconds > 0

    def test_duration_seconds_completed(self):
        """Test duration calculation for completed pipeline."""
        from datetime import datetime

        result = PipelineResult()
        result.started_at = datetime(2024, 10, 27, 12, 0, 0)
        result.finished_at = datetime(2024, 10, 27, 12, 5, 30)

        assert result.duration_seconds == 330.0  # 5 minutes 30 seconds


class TestPipelineOrchestratorInit:
    """Test PipelineOrchestrator initialization."""

    def test_init_defaults(self):
        """Test initialization with defaults."""
        with patch("mlb_data_platform.pipeline.orchestrator.StatsAPI"):
            orchestrator = PipelineOrchestrator()

            assert orchestrator.config is not None
            assert orchestrator.api is not None
            assert orchestrator.storage_callback is None

    def test_init_with_api(self):
        """Test initialization with custom API."""
        mock_api = MagicMock()
        orchestrator = PipelineOrchestrator(api=mock_api)

        assert orchestrator.api is mock_api

    def test_init_with_config(self):
        """Test initialization with custom config."""
        config = PipelineConfig(sport_id=11)
        mock_api = MagicMock()

        orchestrator = PipelineOrchestrator(api=mock_api, config=config)

        assert orchestrator.config.sport_id == 11

    def test_init_with_storage_callback(self):
        """Test initialization with storage callback."""
        mock_api = MagicMock()
        callback = MagicMock()

        orchestrator = PipelineOrchestrator(api=mock_api, storage_callback=callback)

        assert orchestrator.storage_callback is callback


class TestPipelineOrchestratorSeasons:
    """Test season operations."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        return MagicMock()

    @pytest.fixture
    def orchestrator(self, mock_api):
        """Create orchestrator with mock API."""
        return PipelineOrchestrator(api=mock_api)

    def test_fetch_seasons(self, orchestrator, mock_api):
        """Test fetching season data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "seasons": [
                {
                    "seasonId": "2024",
                    "regularSeasonStartDate": "2024-03-28",
                    "regularSeasonEndDate": "2024-09-29",
                }
            ]
        }
        mock_api.Season.seasons.return_value = mock_response

        seasons = orchestrator.fetch_seasons()

        assert len(seasons) == 1
        assert seasons[0].season_id == "2024"
        mock_api.Season.seasons.assert_called_once_with(sportId=1)

    def test_fetch_seasons_custom_sport(self, orchestrator, mock_api):
        """Test fetching seasons for custom sport ID."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"seasons": []}
        mock_api.Season.seasons.return_value = mock_response

        orchestrator.fetch_seasons(sport_id=11)

        mock_api.Season.seasons.assert_called_once_with(sportId=11)

    def test_fetch_seasons_with_callback(self, mock_api):
        """Test that storage callback is invoked."""
        callback = MagicMock()
        orchestrator = PipelineOrchestrator(api=mock_api, storage_callback=callback)

        mock_response = MagicMock()
        mock_response.json.return_value = {"seasons": []}
        mock_api.Season.seasons.return_value = mock_response

        orchestrator.fetch_seasons()

        callback.assert_called_once()
        call_args = callback.call_args
        assert call_args[0][0] == "season"
        assert call_args[0][1] == "seasons"

    def test_get_current_season(self, orchestrator, mock_api):
        """Test getting current season."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "seasons": [
                {
                    "seasonId": "2024",
                    "regularSeasonStartDate": "2024-03-28",
                    "regularSeasonEndDate": "2024-09-29",
                }
            ]
        }
        mock_api.Season.seasons.return_value = mock_response

        season = orchestrator.get_current_season()

        assert season is not None
        assert season.season_id == "2024"


class TestPipelineOrchestratorSchedule:
    """Test schedule operations."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        return MagicMock()

    @pytest.fixture
    def orchestrator(self, mock_api):
        """Create orchestrator with mock API."""
        return PipelineOrchestrator(api=mock_api)

    def test_fetch_schedule(self, orchestrator, mock_api):
        """Test fetching schedule for a date."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dates": [
                {
                    "date": "2024-10-27",
                    "games": [
                        {
                            "gamePk": 745123,
                            "gameDate": "2024-10-27T20:00:00Z",
                            "status": {"abstractGameState": "Final"},
                            "teams": {"home": {"team": {}}, "away": {"team": {}}},
                            "venue": {},
                        }
                    ],
                }
            ]
        }
        mock_api.Schedule.schedule.return_value = mock_response

        games = orchestrator.fetch_schedule(date(2024, 10, 27))

        assert len(games) == 1
        assert games[0].game_pk == 745123
        mock_api.Schedule.schedule.assert_called_once_with(
            date="2024-10-27", sportId=1
        )

    def test_fetch_schedule_default_date(self, orchestrator, mock_api):
        """Test fetching schedule defaults to today."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"dates": []}
        mock_api.Schedule.schedule.return_value = mock_response

        with patch("mlb_data_platform.pipeline.orchestrator.date") as mock_date:
            mock_date.today.return_value = date(2024, 10, 27)

            orchestrator.fetch_schedule()

            mock_api.Schedule.schedule.assert_called_once_with(
                date="2024-10-27", sportId=1
            )

    def test_fetch_schedule_range(self, orchestrator, mock_api):
        """Test fetching schedule for date range."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dates": [
                {
                    "date": "2024-10-27",
                    "games": [{"gamePk": 745123, "status": {}, "teams": {"home": {"team": {}}, "away": {"team": {}}}, "venue": {}}],
                }
            ]
        }
        mock_api.Schedule.schedule.return_value = mock_response

        results = orchestrator.fetch_schedule_range(
            date(2024, 10, 27), date(2024, 10, 29)
        )

        # Should call schedule 3 times (27, 28, 29)
        assert mock_api.Schedule.schedule.call_count == 3
        assert len(results) == 3  # Each day has games

    def test_get_active_games(self, orchestrator, mock_api):
        """Test getting active games."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "dates": [
                {
                    "date": "2024-10-27",
                    "games": [
                        {
                            "gamePk": 745123,
                            "status": {"abstractGameState": "Live"},
                            "teams": {"home": {"team": {}}, "away": {"team": {}}},
                            "venue": {},
                        },
                        {
                            "gamePk": 745124,
                            "status": {"abstractGameState": "Final"},
                            "teams": {"home": {"team": {}}, "away": {"team": {}}},
                            "venue": {},
                        },
                    ],
                }
            ]
        }
        mock_api.Schedule.schedule.return_value = mock_response

        active = orchestrator.get_active_games()

        assert len(active) == 1
        assert active[0].game_pk == 745123


class TestPipelineOrchestratorGame:
    """Test game operations."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        return MagicMock()

    @pytest.fixture
    def orchestrator(self, mock_api):
        """Create orchestrator with mock API."""
        return PipelineOrchestrator(api=mock_api)

    def test_fetch_game(self, orchestrator, mock_api):
        """Test fetching game data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "gameData": {"game": {"pk": 745123}},
            "liveData": {},
        }
        mock_api.Game.liveGameV1.return_value = mock_response

        data = orchestrator.fetch_game(745123)

        assert data["gameData"]["game"]["pk"] == 745123
        mock_api.Game.liveGameV1.assert_called_once_with(game_pk=745123)

    def test_fetch_game_with_timecode(self, orchestrator, mock_api):
        """Test fetching game at specific timestamp."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"gameData": {}}
        mock_api.Game.liveGameV1.return_value = mock_response

        orchestrator.fetch_game(745123, timecode="20241027_200800")

        mock_api.Game.liveGameV1.assert_called_once_with(
            game_pk=745123, timecode="20241027_200800"
        )

    def test_fetch_game_timestamps(self, orchestrator, mock_api):
        """Test fetching game timestamps."""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            "20241027_200800",
            "20241027_201000",
        ]
        mock_api.Game.liveTimestampv11.return_value = mock_response

        timestamps = orchestrator.fetch_game_timestamps(745123)

        assert timestamps == ["20241027_200800", "20241027_201000"]

    def test_fetch_game_timestamps_non_list(self, orchestrator, mock_api):
        """Test handling non-list response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_api.Game.liveTimestampv11.return_value = mock_response

        timestamps = orchestrator.fetch_game_timestamps(745123)

        assert timestamps == []

    def test_fetch_games_multiple(self, orchestrator, mock_api):
        """Test fetching multiple games."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"gameData": {}}
        mock_api.Game.liveGameV1.return_value = mock_response

        results = orchestrator.fetch_games([745123, 745124, 745125])

        assert len(results) == 3
        assert mock_api.Game.liveGameV1.call_count == 3

    def test_fetch_games_handles_errors(self, orchestrator, mock_api):
        """Test that fetch_games continues on errors."""
        mock_api.Game.liveGameV1.side_effect = [
            Exception("API Error"),
            MagicMock(json=MagicMock(return_value={"gameData": {}})),
        ]

        results = orchestrator.fetch_games([745123, 745124])

        assert len(results) == 1  # Only successful one


class TestPipelineOrchestratorEnrichment:
    """Test enrichment operations."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        return MagicMock()

    @pytest.fixture
    def orchestrator(self, mock_api):
        """Create orchestrator with mock API."""
        return PipelineOrchestrator(api=mock_api)

    def test_fetch_person(self, orchestrator, mock_api):
        """Test fetching person data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"people": [{"id": 660271}]}
        mock_api.Person.person.return_value = mock_response

        data = orchestrator.fetch_person(660271)

        assert data["people"][0]["id"] == 660271
        mock_api.Person.person.assert_called_once_with(personIds=[660271])

    def test_fetch_team(self, orchestrator, mock_api):
        """Test fetching team data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"teams": [{"id": 119}]}
        mock_api.Team.teams.return_value = mock_response

        data = orchestrator.fetch_team(119)

        assert data["teams"][0]["id"] == 119
        mock_api.Team.teams.assert_called_once_with(teamId=119)

    def test_fetch_team_with_season(self, orchestrator, mock_api):
        """Test fetching team data with season."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"teams": []}
        mock_api.Team.teams.return_value = mock_response

        orchestrator.fetch_team(119, season="2024")

        mock_api.Team.teams.assert_called_once_with(teamId=119, season="2024")

    def test_enrich_from_game(self, orchestrator, mock_api):
        """Test enriching from game data."""
        # Configure enrichment to be enabled
        orchestrator.config.enrich_players = True
        orchestrator.config.enrich_teams = True

        # Mock person and team responses
        mock_api.Person.person.return_value = MagicMock(
            json=MagicMock(return_value={"people": []})
        )
        mock_api.Team.teams.return_value = MagicMock(
            json=MagicMock(return_value={"teams": []})
        )

        game_data = {
            "gameData": {
                "game": {"pk": 745123},
                "teams": {
                    "home": {"id": 119},
                    "away": {"id": 137},
                },
            },
            "liveData": {
                "boxscore": {
                    "teams": {
                        "home": {
                            "players": {"ID660271": {}},
                            "battingOrder": [],
                            "bench": [],
                            "bullpen": [],
                            "pitchers": [],
                        },
                        "away": {
                            "players": {},
                            "battingOrder": [],
                            "bench": [],
                            "bullpen": [],
                            "pitchers": [],
                        },
                    },
                    "officials": [],
                },
            },
        }

        result = orchestrator.enrich_from_game(game_data)

        assert result.players_fetched == 1
        assert result.teams_fetched == 2
        assert 660271 in result.player_ids
        assert 119 in result.team_ids
        assert 137 in result.team_ids

    def test_enrich_from_game_disabled(self, mock_api):
        """Test enrichment when disabled."""
        config = PipelineConfig(enrich_players=False, enrich_teams=False)
        orchestrator = PipelineOrchestrator(api=mock_api, config=config)

        game_data = {
            "gameData": {
                "game": {"pk": 745123},
                "teams": {"home": {"id": 119}, "away": {"id": 137}},
            },
            "liveData": {"boxscore": {}},
        }

        result = orchestrator.enrich_from_game(game_data)

        assert result.players_fetched == 0
        assert result.teams_fetched == 0
        mock_api.Person.person.assert_not_called()
        mock_api.Team.teams.assert_not_called()


class TestPipelineOrchestratorRunDaily:
    """Test run_daily pipeline operation."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        api = MagicMock()

        # Mock schedule response
        api.Schedule.schedule.return_value = MagicMock(
            json=MagicMock(
                return_value={
                    "dates": [
                        {
                            "date": "2024-10-27",
                            "games": [
                                {
                                    "gamePk": 745123,
                                    "status": {"abstractGameState": "Final"},
                                    "teams": {
                                        "home": {"team": {"id": 119}},
                                        "away": {"team": {"id": 137}},
                                    },
                                    "venue": {},
                                }
                            ],
                        }
                    ]
                }
            )
        )

        # Mock game response
        api.Game.liveGameV1.return_value = MagicMock(
            json=MagicMock(
                return_value={
                    "gameData": {
                        "game": {"pk": 745123},
                        "teams": {"home": {"id": 119}, "away": {"id": 137}},
                    },
                    "liveData": {"boxscore": {"teams": {"home": {}, "away": {}}, "officials": []}},
                }
            )
        )

        # Mock timestamps
        api.Game.liveTimestampv11.return_value = MagicMock(
            json=MagicMock(return_value=["20241027_200800"])
        )

        # Mock person/team
        api.Person.person.return_value = MagicMock(
            json=MagicMock(return_value={"people": []})
        )
        api.Team.teams.return_value = MagicMock(
            json=MagicMock(return_value={"teams": []})
        )

        return api

    def test_run_daily(self, mock_api):
        """Test running daily pipeline."""
        orchestrator = PipelineOrchestrator(api=mock_api)

        result = orchestrator.run_daily(date(2024, 10, 27))

        assert result.schedules_fetched == 1
        assert result.games_fetched == 1
        assert 745123 in result.game_pks
        assert result.finished_at is not None

    def test_run_daily_filters_games(self, mock_api):
        """Test that run_daily filters games based on config."""
        # Only fetch live games
        config = PipelineConfig(
            fetch_final_games=False,
            fetch_live_games=True,
            fetch_preview_games=False,
        )
        orchestrator = PipelineOrchestrator(api=mock_api, config=config)

        result = orchestrator.run_daily(date(2024, 10, 27))

        # No games should be fetched since mock has Final game
        assert result.games_fetched == 0

    def test_run_daily_handles_errors(self, mock_api):
        """Test that run_daily records errors."""
        mock_api.Game.liveGameV1.side_effect = Exception("API Error")
        orchestrator = PipelineOrchestrator(api=mock_api)

        result = orchestrator.run_daily(date(2024, 10, 27))

        assert len(result.errors) > 0
        assert "745123" in result.errors[0]


class TestPipelineOrchestratorBackfill:
    """Test backfill_season operation."""

    @pytest.fixture
    def mock_api(self):
        """Create mock StatsAPI."""
        api = MagicMock()

        # Mock seasons response
        api.Season.seasons.return_value = MagicMock(
            json=MagicMock(
                return_value={
                    "seasons": [
                        {
                            "seasonId": "2024",
                            "regularSeasonStartDate": "2024-10-27",
                            "regularSeasonEndDate": "2024-10-27",  # Single day for testing
                        }
                    ]
                }
            )
        )

        # Mock schedule
        api.Schedule.schedule.return_value = MagicMock(
            json=MagicMock(return_value={"dates": []})
        )

        return api

    def test_backfill_season(self, mock_api):
        """Test backfilling a season."""
        orchestrator = PipelineOrchestrator(api=mock_api)

        result = orchestrator.backfill_season(season_id="2024")

        assert result.seasons_fetched == 1
        assert result.schedules_fetched >= 1
        assert result.finished_at is not None

    def test_backfill_season_date_override(self, mock_api):
        """Test backfilling with custom date range."""
        orchestrator = PipelineOrchestrator(api=mock_api)

        result = orchestrator.backfill_season(
            start_date=date(2024, 10, 27),
            end_date=date(2024, 10, 28),
        )

        # Should not fetch seasons when dates provided
        mock_api.Season.seasons.assert_not_called()
        assert result.schedules_fetched == 2

    def test_backfill_season_not_found(self, mock_api):
        """Test backfilling non-existent season."""
        orchestrator = PipelineOrchestrator(api=mock_api)

        result = orchestrator.backfill_season(season_id="1900")

        assert len(result.errors) > 0
        assert "1900" in result.errors[0]
