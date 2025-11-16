"""ORM models for MLB Stats API data.

Contains two types of models:
1. Normalized models (game_live.py) - Flattened relational tables generated from schemas
2. Raw models (raw.py) - Raw API response storage for full replay capability

Usage - Normalized Models:
    from mlb_data_platform.models import LiveGameMetadata, LiveGamePlays

    # Create instance
    game = LiveGameMetadata(
        game_pk=747175,
        game_date=date(2024, 10, 25),
        home_team_name="Los Angeles Dodgers",
        away_team_name="New York Yankees",
    )

    # Query with SQLModel
    from mlb_data_platform.database import get_session
    from sqlmodel import select

    with get_session() as session:
        statement = select(LiveGameMetadata).where(LiveGameMetadata.game_pk == 747175)
        game = session.exec(statement).first()

Usage - Raw Models:
    from mlb_data_platform.models import RawLiveGameV1

    # Store raw API response
    raw_game = RawLiveGameV1(
        game_pk=747175,
        captured_at=datetime.utcnow(),
        data={"gameData": {...}, "liveData": {...}},  # Full JSONB response
        endpoint="game",
        method="liveGameV1",
        params={"game_pk": 747175},
        url="https://statsapi.mlb.com/api/v1.1/game/747175/feed/live",
        status_code=200,
    )

    with get_session() as session:
        session.add(raw_game)
"""

# Import normalized models
from mlb_data_platform.models.game_live import (
    BaseModel,
    LiveGameAwayBench,
    LiveGameAwayBullpen,
    LiveGameAwayBattingOrder,
    LiveGameAwayPitchers,
    LiveGameFieldingCredits,
    LiveGameHomeBench,
    LiveGameHomeBullpen,
    LiveGameHomeBattingOrder,
    LiveGameHomePitchers,
    LiveGameLinescoreInnings,
    LiveGameMetadata,
    LiveGamePitchEvents,
    LiveGamePlayActions,
    LiveGamePlayers,
    LiveGamePlays,
    LiveGameRunners,
    LiveGameScoringPlays,
    LiveGameUmpires,
    LiveGameVenueDetails,
)

# Import raw models
from mlb_data_platform.models.raw import (
    RawLiveGameV1,
    RawPerson,
    RawSchedule,
    RawSeasons,
    RawTeam,
    TransformationCheckpoint,
)

__all__ = [
    # Normalized models
    "BaseModel",
    "LiveGameMetadata",
    "LiveGameLinescoreInnings",
    "LiveGamePlayers",
    "LiveGamePlays",
    "LiveGamePitchEvents",
    "LiveGamePlayActions",
    "LiveGameFieldingCredits",
    "LiveGameRunners",
    "LiveGameScoringPlays",
    "LiveGameHomeBattingOrder",
    "LiveGameAwayBattingOrder",
    "LiveGameHomePitchers",
    "LiveGameAwayPitchers",
    "LiveGameHomeBench",
    "LiveGameAwayBench",
    "LiveGameHomeBullpen",
    "LiveGameAwayBullpen",
    "LiveGameUmpires",
    "LiveGameVenueDetails",
    # Raw models
    "RawLiveGameV1",
    "RawSchedule",
    "RawSeasons",
    "RawPerson",
    "RawTeam",
    "TransformationCheckpoint",
]
