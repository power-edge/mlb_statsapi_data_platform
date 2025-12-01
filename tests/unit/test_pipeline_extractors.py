"""Unit tests for pipeline extractors.

Tests the data extraction classes that parse MLB API responses:
- SeasonExtractor: Extract date ranges from season data
- ScheduleExtractor: Extract game info from schedule data
- GameExtractor: Extract roster info from game data
"""

from datetime import date

import pytest

from mlb_data_platform.pipeline.extractors import (
    GameExtractor,
    GameInfo,
    GameRoster,
    ScheduleExtractor,
    SeasonDates,
    SeasonExtractor,
)


class TestSeasonDates:
    """Test SeasonDates dataclass."""

    def test_get_date_range_regular_season(self):
        """Test getting date range for regular season only."""
        season = SeasonDates(
            season_id="2024",
            sport_id=1,
            regular_season_start=date(2024, 3, 28),
            regular_season_end=date(2024, 9, 29),
            spring_start=date(2024, 2, 22),
            spring_end=date(2024, 3, 26),
            postseason_start=date(2024, 10, 1),
            postseason_end=date(2024, 10, 30),
        )

        start, end = season.get_date_range(include_spring=False)

        assert start == date(2024, 3, 28)
        assert end == date(2024, 10, 30)  # Uses postseason_end

    def test_get_date_range_with_spring_training(self):
        """Test getting date range including spring training."""
        season = SeasonDates(
            season_id="2024",
            sport_id=1,
            regular_season_start=date(2024, 3, 28),
            regular_season_end=date(2024, 9, 29),
            spring_start=date(2024, 2, 22),
            spring_end=date(2024, 3, 26),
            postseason_end=date(2024, 10, 30),
        )

        start, end = season.get_date_range(include_spring=True)

        assert start == date(2024, 2, 22)  # Spring start
        assert end == date(2024, 10, 30)

    def test_get_date_range_no_postseason(self):
        """Test date range falls back to regular season end."""
        season = SeasonDates(
            season_id="2024",
            sport_id=1,
            regular_season_start=date(2024, 3, 28),
            regular_season_end=date(2024, 9, 29),
        )

        start, end = season.get_date_range()

        assert start == date(2024, 3, 28)
        assert end == date(2024, 9, 29)

    def test_get_date_range_missing_dates_raises(self):
        """Test that missing dates raise ValueError."""
        season = SeasonDates(season_id="2024", sport_id=1)

        with pytest.raises(ValueError, match="missing date boundaries"):
            season.get_date_range()


class TestGameInfo:
    """Test GameInfo dataclass properties."""

    def test_is_live(self):
        """Test is_live property."""
        game = GameInfo(
            game_pk=745123,
            game_date=date(2024, 10, 27),
            abstract_game_state="Live",
        )
        assert game.is_live is True
        assert game.is_preview is False
        assert game.is_final is False
        assert game.is_active is True

    def test_is_preview(self):
        """Test is_preview property."""
        game = GameInfo(
            game_pk=745123,
            game_date=date(2024, 10, 27),
            abstract_game_state="Preview",
        )
        assert game.is_live is False
        assert game.is_preview is True
        assert game.is_final is False
        assert game.is_active is True

    def test_is_final(self):
        """Test is_final property."""
        game = GameInfo(
            game_pk=745123,
            game_date=date(2024, 10, 27),
            abstract_game_state="Final",
        )
        assert game.is_live is False
        assert game.is_preview is False
        assert game.is_final is True
        assert game.is_active is False


class TestSeasonExtractor:
    """Test SeasonExtractor methods."""

    @pytest.fixture
    def sample_season_response(self):
        """Sample Season.seasons API response."""
        return {
            "seasons": [
                {
                    "seasonId": "2024",
                    "regularSeasonStartDate": "2024-03-28",
                    "regularSeasonEndDate": "2024-09-29",
                    "springStartDate": "2024-02-22",
                    "springEndDate": "2024-03-26",
                    "postSeasonStartDate": "2024-10-01",
                    "postSeasonEndDate": "2024-10-30",
                    "allStarDate": "2024-07-16",
                },
                {
                    "seasonId": "2023",
                    "regularSeasonStartDate": "2023-03-30",
                    "regularSeasonEndDate": "2023-10-01",
                    "springStartDate": "2023-02-24",
                    "springEndDate": "2023-03-28",
                    "postSeasonStartDate": "2023-10-03",
                    "postSeasonEndDate": "2023-11-01",
                    "allStarDate": "2023-07-11",
                },
            ]
        }

    def test_extract_dates(self, sample_season_response):
        """Test extracting season dates from API response."""
        seasons = SeasonExtractor.extract_dates(sample_season_response, sport_id=1)

        assert len(seasons) == 2

        # Check 2024 season
        s2024 = seasons[0]
        assert s2024.season_id == "2024"
        assert s2024.sport_id == 1
        assert s2024.regular_season_start == date(2024, 3, 28)
        assert s2024.regular_season_end == date(2024, 9, 29)
        assert s2024.spring_start == date(2024, 2, 22)
        assert s2024.postseason_end == date(2024, 10, 30)
        assert s2024.all_star_date == date(2024, 7, 16)

        # Check 2023 season
        s2023 = seasons[1]
        assert s2023.season_id == "2023"

    def test_extract_dates_empty_response(self):
        """Test extracting from empty response."""
        seasons = SeasonExtractor.extract_dates({})
        assert seasons == []

        seasons = SeasonExtractor.extract_dates({"seasons": []})
        assert seasons == []

    def test_get_current_season(self, sample_season_response, monkeypatch):
        """Test getting current season based on today's date."""
        # Mock date.today() to return a date within 2024 season
        class MockDate:
            @classmethod
            def today(cls):
                return date(2024, 7, 15)

        monkeypatch.setattr("mlb_data_platform.pipeline.extractors.date", MockDate)

        current = SeasonExtractor.get_current_season(sample_season_response, sport_id=1)

        assert current is not None
        assert current.season_id == "2024"

    def test_get_current_season_returns_first_if_none_match(
        self, sample_season_response, monkeypatch
    ):
        """Test that first season is returned if none match current date."""

        class MockDate:
            @classmethod
            def today(cls):
                return date(2025, 1, 15)  # Outside any season

        monkeypatch.setattr("mlb_data_platform.pipeline.extractors.date", MockDate)

        current = SeasonExtractor.get_current_season(sample_season_response, sport_id=1)

        assert current is not None
        assert current.season_id == "2024"  # Returns first


class TestScheduleExtractor:
    """Test ScheduleExtractor methods."""

    @pytest.fixture
    def sample_schedule_response(self):
        """Sample Schedule.schedule API response."""
        return {
            "dates": [
                {
                    "date": "2024-10-27",
                    "games": [
                        {
                            "gamePk": 745123,
                            "gameDate": "2024-10-27T20:08:00Z",
                            "status": {
                                "statusCode": "F",
                                "detailedState": "Final",
                                "abstractGameState": "Final",
                            },
                            "teams": {
                                "home": {"team": {"id": 119}},
                                "away": {"team": {"id": 137}},
                            },
                            "venue": {"id": 2680},
                        },
                        {
                            "gamePk": 745124,
                            "gameDate": "2024-10-27T17:00:00Z",
                            "status": {
                                "statusCode": "I",
                                "detailedState": "In Progress",
                                "abstractGameState": "Live",
                            },
                            "teams": {
                                "home": {"team": {"id": 110}},
                                "away": {"team": {"id": 111}},
                            },
                            "venue": {"id": 3313},
                        },
                    ],
                }
            ]
        }

    def test_extract_games(self, sample_schedule_response):
        """Test extracting games from schedule response."""
        games = ScheduleExtractor.extract_games(sample_schedule_response)

        assert len(games) == 2

        # Check first game (Final)
        g1 = games[0]
        assert g1.game_pk == 745123
        assert g1.game_date == date(2024, 10, 27)
        assert g1.status_code == "F"
        assert g1.detailed_state == "Final"
        assert g1.abstract_game_state == "Final"
        assert g1.home_team_id == 119
        assert g1.away_team_id == 137
        assert g1.venue_id == 2680
        assert g1.is_final is True

        # Check second game (Live)
        g2 = games[1]
        assert g2.game_pk == 745124
        assert g2.abstract_game_state == "Live"
        assert g2.is_live is True

    def test_extract_games_empty_response(self):
        """Test extracting from empty response."""
        games = ScheduleExtractor.extract_games({})
        assert games == []

        games = ScheduleExtractor.extract_games({"dates": []})
        assert games == []

    def test_get_game_pks(self, sample_schedule_response):
        """Test extracting just game_pks."""
        pks = ScheduleExtractor.get_game_pks(sample_schedule_response)

        assert pks == [745123, 745124]

    def test_get_live_games(self, sample_schedule_response):
        """Test getting only live games."""
        live = ScheduleExtractor.get_live_games(sample_schedule_response)

        assert len(live) == 1
        assert live[0].game_pk == 745124
        assert live[0].is_live is True

    def test_get_active_games(self, sample_schedule_response):
        """Test getting active games (live + preview)."""
        # Modify response to add a preview game
        sample_schedule_response["dates"][0]["games"].append(
            {
                "gamePk": 745125,
                "gameDate": "2024-10-27T23:00:00Z",
                "status": {
                    "statusCode": "P",
                    "detailedState": "Pre-Game",
                    "abstractGameState": "Preview",
                },
                "teams": {
                    "home": {"team": {"id": 112}},
                    "away": {"team": {"id": 113}},
                },
                "venue": {"id": 3000},
            }
        )

        active = ScheduleExtractor.get_active_games(sample_schedule_response)

        assert len(active) == 2
        pks = [g.game_pk for g in active]
        assert 745124 in pks  # Live
        assert 745125 in pks  # Preview
        assert 745123 not in pks  # Final


class TestGameExtractor:
    """Test GameExtractor methods."""

    @pytest.fixture
    def sample_game_response(self):
        """Sample Game.liveGameV1 API response (simplified)."""
        return {
            "metaData": {"timeStamp": "20241027_230500"},
            "gameData": {
                "game": {"pk": 745123},
                "teams": {
                    "home": {"id": 119, "name": "Los Angeles Dodgers"},
                    "away": {"id": 137, "name": "San Francisco Giants"},
                },
                "status": {
                    "abstractGameState": "Final",
                    "detailedState": "Final",
                    "statusCode": "F",
                },
            },
            "liveData": {
                "boxscore": {
                    "teams": {
                        "home": {
                            "players": {
                                "ID660271": {"person": {"id": 660271}},
                                "ID605131": {"person": {"id": 605131}},
                            },
                            "battingOrder": [660271, 605131, 518626],
                            "bench": [477132],
                            "bullpen": [669373, 641154],
                            "pitchers": [477132],
                        },
                        "away": {
                            "players": {
                                "ID543257": {"person": {"id": 543257}},
                                "ID656305": {"person": {"id": 656305}},
                            },
                            "battingOrder": [543257, 656305],
                            "bench": [],
                            "bullpen": [573186],
                            "pitchers": [573186],
                        },
                    },
                    "officials": [
                        {"official": {"id": 427395, "fullName": "CB Bucknor"}},
                        {"official": {"id": 427557, "fullName": "Chris Guccione"}},
                    ],
                },
            },
        }

    def test_extract_roster(self, sample_game_response):
        """Test extracting roster from game response."""
        roster = GameExtractor.extract_roster(sample_game_response)

        assert isinstance(roster, GameRoster)
        assert roster.game_pk == 745123
        assert roster.home_team_id == 119
        assert roster.away_team_id == 137

        # Check player IDs (should be unique set)
        assert 660271 in roster.player_ids
        assert 605131 in roster.player_ids
        assert 518626 in roster.player_ids  # From batting order
        assert 477132 in roster.player_ids  # From bench
        assert 543257 in roster.player_ids  # Away team
        assert 573186 in roster.player_ids  # Away bullpen

        # Check umpires
        assert 427395 in roster.umpire_ids
        assert 427557 in roster.umpire_ids

    def test_extract_roster_empty_boxscore(self):
        """Test extracting roster with empty boxscore."""
        response = {
            "gameData": {
                "game": {"pk": 745123},
                "teams": {
                    "home": {"id": 119},
                    "away": {"id": 137},
                },
            },
            "liveData": {"boxscore": {}},
        }

        roster = GameExtractor.extract_roster(response)

        assert roster.game_pk == 745123
        assert roster.home_team_id == 119
        assert roster.away_team_id == 137
        assert roster.player_ids == []
        assert roster.umpire_ids == []

    def test_extract_latest_timestamp(self, sample_game_response):
        """Test extracting timestamp from game response."""
        ts = GameExtractor.extract_latest_timestamp(sample_game_response)

        assert ts == "20241027_230500"

    def test_extract_latest_timestamp_missing(self):
        """Test extracting timestamp when missing."""
        response = {"gameData": {}}

        ts = GameExtractor.extract_latest_timestamp(response)

        assert ts is None

    def test_extract_game_state(self, sample_game_response):
        """Test extracting game state."""
        state = GameExtractor.extract_game_state(sample_game_response)

        assert state == "Final"

    def test_extract_game_state_live(self):
        """Test extracting live game state."""
        response = {
            "gameData": {
                "status": {
                    "abstractGameState": "Live",
                }
            }
        }

        state = GameExtractor.extract_game_state(response)

        assert state == "Live"


class TestDateParsing:
    """Test internal date parsing functions."""

    def test_game_datetime_parsing(self):
        """Test that ISO datetime strings are parsed correctly."""
        response = {
            "dates": [
                {
                    "date": "2024-10-27",
                    "games": [
                        {
                            "gamePk": 745123,
                            "gameDate": "2024-10-27T20:08:00Z",
                            "status": {"abstractGameState": "Final"},
                            "teams": {"home": {"team": {}}, "away": {"team": {}}},
                            "venue": {},
                        }
                    ],
                }
            ]
        }

        games = ScheduleExtractor.extract_games(response)

        assert len(games) == 1
        assert games[0].game_datetime is not None
        assert games[0].game_datetime.year == 2024
        assert games[0].game_datetime.month == 10
        assert games[0].game_datetime.day == 27

    def test_invalid_date_returns_none(self):
        """Test that invalid dates return None without raising."""
        response = {
            "dates": [
                {
                    "date": "invalid-date",
                    "games": [
                        {
                            "gamePk": 745123,
                            "gameDate": "not-a-datetime",
                            "status": {"abstractGameState": "Final"},
                            "teams": {"home": {"team": {}}, "away": {"team": {}}},
                            "venue": {},
                        }
                    ],
                }
            ]
        }

        games = ScheduleExtractor.extract_games(response)

        assert len(games) == 1
        assert games[0].game_date is None
        assert games[0].game_datetime is None
