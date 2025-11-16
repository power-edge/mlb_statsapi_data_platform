"""ORM models for raw data tables.

These models represent the raw API response storage layer that enables full replay capability.
All tables follow the pattern:
- Composite primary key: (entity_id, captured_at)
- JSONB data column for complete API response
- Metadata columns from pymlb_statsapi
- Append-only for historical versioning
"""

from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import BigInteger, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


# ============================================================================
# BASE MODEL FOR RAW TABLES
# ============================================================================


class RawTableBase(SQLModel):
    """Base model for all raw data tables.

    Provides common fields that all raw tables share:
    - captured_at: When the API request was made (from pymlb_statsapi)
    - data: Complete API response as JSONB
    - endpoint, method, params, url, status_code: Request metadata
    - ingested_at: When data was inserted into this table
    """

    captured_at: datetime = Field(
        sa_type=DateTime(timezone=True), primary_key=True, nullable=False
    )
    data: dict[str, Any] = Field(sa_type=JSONB, nullable=False)

    # Request metadata (from pymlb_statsapi)
    endpoint: str = Field(sa_type=String(50), nullable=False)
    method: str = Field(sa_type=String(100), nullable=False)
    params: Optional[dict[str, Any]] = Field(sa_type=JSONB, default=None)
    url: Optional[str] = Field(sa_type=Text, default=None)
    status_code: Optional[int] = Field(sa_type=Integer, default=None)

    # Ingestion tracking
    ingested_at: datetime = Field(
        sa_type=DateTime(timezone=True), nullable=False, default_factory=datetime.utcnow
    )


# ============================================================================
# GAME ENDPOINT RAW MODELS
# ============================================================================


class RawLiveGameV1(RawTableBase, table=True):
    """Raw API responses from Game.liveGameV1.

    Stores complete game data including:
    - All plays, pitches, runners, scoring plays
    - Player lineups, bench, bullpen
    - Team and venue information
    - Game metadata and status

    Primary key: (game_pk, captured_at) for versioning
    """

    __tablename__ = "live_game_v1_raw"
    __table_args__ = {"schema": "game"}

    game_pk: int = Field(sa_type=BigInteger, primary_key=True, nullable=False)


# ============================================================================
# SCHEDULE ENDPOINT RAW MODELS
# ============================================================================


class RawSchedule(RawTableBase, table=True):
    """Raw API responses from Schedule.schedule.

    Stores complete schedule data for a given date including:
    - All games scheduled for the date
    - Game status, times, and venue information
    - Series information

    Primary key: (schedule_date, captured_at) for versioning
    """

    __tablename__ = "schedule_raw"
    __table_args__ = {"schema": "schedule"}

    schedule_date: date = Field(primary_key=True, nullable=False)


# ============================================================================
# SEASON ENDPOINT RAW MODELS
# ============================================================================


class RawSeasons(RawTableBase, table=True):
    """Raw API responses from Season.seasons.

    Stores season metadata including:
    - Season years and dates
    - All-Star game information
    - Regular season and postseason dates

    Primary key: (sport_id, captured_at) for versioning
    """

    __tablename__ = "seasons_raw"
    __table_args__ = {"schema": "season"}

    sport_id: int = Field(sa_type=Integer, primary_key=True, nullable=False)


# ============================================================================
# PERSON ENDPOINT RAW MODELS
# ============================================================================


class RawPerson(RawTableBase, table=True):
    """Raw API responses from Person.person.

    Stores complete player/person data including:
    - Biographical information
    - Career statistics
    - Current team and position

    Primary key: (person_id, captured_at) for versioning
    """

    __tablename__ = "person_raw"
    __table_args__ = {"schema": "person"}

    person_id: int = Field(sa_type=BigInteger, primary_key=True, nullable=False)


# ============================================================================
# TEAM ENDPOINT RAW MODELS
# ============================================================================


class RawTeam(RawTableBase, table=True):
    """Raw API responses from Team.team.

    Stores complete team data including:
    - Team information and branding
    - League and division
    - Venue information

    Primary key: (team_id, captured_at) for versioning
    """

    __tablename__ = "team_raw"
    __table_args__ = {"schema": "team"}

    team_id: int = Field(sa_type=Integer, primary_key=True, nullable=False)


# ============================================================================
# TRANSFORMATION CHECKPOINT MODEL
# ============================================================================


class TransformationCheckpoint(SQLModel, table=True):
    """Track last processed timestamp for incremental transformations.

    Used to enable incremental processing:
    - Process only new data since last checkpoint
    - Enables efficient batch processing
    - Supports full replay by resetting checkpoint
    """

    __tablename__ = "transformation_checkpoints"
    __table_args__ = {"schema": "meta"}

    transformation_name: str = Field(
        sa_type=String(100), primary_key=True, nullable=False
    )
    source_table: str = Field(sa_type=String(100), primary_key=True, nullable=False)
    last_processed_at: datetime = Field(
        sa_type=DateTime(timezone=True), nullable=False
    )
    last_processed_count: int = Field(sa_type=BigInteger, nullable=False, default=0)
    updated_at: datetime = Field(
        sa_type=DateTime(timezone=True),
        nullable=False,
        default_factory=datetime.utcnow,
    )
