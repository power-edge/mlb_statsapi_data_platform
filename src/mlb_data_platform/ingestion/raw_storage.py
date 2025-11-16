"""Raw storage layer for ingesting API responses to PostgreSQL.

This module provides the core ingestion functionality that:
1. Takes responses from pymlb_statsapi (which includes metadata)
2. Stores them in raw PostgreSQL tables as JSONB
3. Enables full replay capability via append-only versioned storage

Design principles:
- PostgreSQL as single source of truth (no MinIO/S3 dependency)
- Append-only: Never delete or update raw data
- Versioning: composite primary key (entity_id, captured_at)
- Metadata preservation: endpoint, method, params, url, status_code from pymlb_statsapi
"""

from datetime import datetime
from typing import Any, Optional

from sqlmodel import Session, select

from mlb_data_platform.models.raw import (
    RawLiveGameV1,
    RawPerson,
    RawSchedule,
    RawSeasons,
    RawTeam,
)


class RawStorageClient:
    """Client for storing raw API responses in PostgreSQL.

    Usage:
        from mlb_data_platform.database import get_session
        from mlb_data_platform.ingestion import RawStorageClient

        storage = RawStorageClient()

        # Store API response (pymlb_statsapi format)
        with get_session() as session:
            storage.save_live_game(
                session=session,
                response=api_response,  # From pymlb_statsapi
            )
    """

    def save_live_game(
        self,
        session: Session,
        response: dict[str, Any],
        *,
        game_pk: Optional[int] = None,
        captured_at: Optional[datetime] = None,
    ) -> RawLiveGameV1:
        """Save raw Game.liveGameV1 response.

        Args:
            session: SQLModel session
            response: Response dict from pymlb_statsapi containing:
                - data: The actual API response
                - metadata: {endpoint, method, params, url, status_code, captured_at}
            game_pk: Override game_pk (extracted from response if None)
            captured_at: Override captured_at (extracted from metadata if None)

        Returns:
            RawLiveGameV1 instance that was saved

        Raises:
            ValueError: If response format is invalid
            IntegrityError: If duplicate (game_pk, captured_at) exists
        """
        # Extract from pymlb_statsapi response structure
        data = response.get("data") or response.get("response")
        metadata = response.get("metadata", {})

        if not data:
            raise ValueError("Response must contain 'data' or 'response' key")

        # Extract game_pk from data if not provided
        if game_pk is None:
            game_pk = data.get("gamePk") or data.get("gameData", {}).get("game", {}).get("pk")
            if not game_pk:
                raise ValueError("Could not extract game_pk from response")

        # Use captured_at from metadata if not provided
        if captured_at is None:
            captured_at_str = metadata.get("captured_at")
            if captured_at_str:
                if isinstance(captured_at_str, str):
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                elif isinstance(captured_at_str, datetime):
                    captured_at = captured_at_str
            else:
                captured_at = datetime.utcnow()

        # Create raw record
        raw_game = RawLiveGameV1(
            game_pk=game_pk,
            captured_at=captured_at,
            data=data,
            endpoint=metadata.get("endpoint", "game"),
            method=metadata.get("method", "liveGameV1"),
            params=metadata.get("params"),
            url=metadata.get("url"),
            status_code=metadata.get("status_code"),
        )

        session.add(raw_game)
        return raw_game

    def save_schedule(
        self,
        session: Session,
        response: dict[str, Any],
        *,
        schedule_date: Optional[str] = None,
        captured_at: Optional[datetime] = None,
    ) -> RawSchedule:
        """Save raw Schedule.schedule response.

        Args:
            session: SQLModel session
            response: Response dict from pymlb_statsapi
            schedule_date: Override schedule date (extracted from params if None)
            captured_at: Override captured_at (extracted from metadata if None)

        Returns:
            RawSchedule instance that was saved
        """
        data = response.get("data") or response.get("response")
        metadata = response.get("metadata", {})

        if not data:
            raise ValueError("Response must contain 'data' or 'response' key")

        # Extract schedule_date from params if not provided
        if schedule_date is None:
            params = metadata.get("params", {})
            schedule_date = params.get("date")
            if not schedule_date:
                raise ValueError("Could not extract schedule_date from response params")

        # Parse date string to date object
        from datetime import date as date_type

        if isinstance(schedule_date, str):
            schedule_date = date_type.fromisoformat(schedule_date)

        # Use captured_at from metadata if not provided
        if captured_at is None:
            captured_at_str = metadata.get("captured_at")
            if captured_at_str:
                if isinstance(captured_at_str, str):
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                elif isinstance(captured_at_str, datetime):
                    captured_at = captured_at_str
            else:
                captured_at = datetime.utcnow()

        raw_schedule = RawSchedule(
            schedule_date=schedule_date,
            captured_at=captured_at,
            data=data,
            endpoint=metadata.get("endpoint", "schedule"),
            method=metadata.get("method", "schedule"),
            params=metadata.get("params"),
            url=metadata.get("url"),
            status_code=metadata.get("status_code"),
        )

        session.add(raw_schedule)
        return raw_schedule

    def save_seasons(
        self,
        session: Session,
        response: dict[str, Any],
        *,
        sport_id: Optional[int] = None,
        captured_at: Optional[datetime] = None,
    ) -> RawSeasons:
        """Save raw Season.seasons response.

        Args:
            session: SQLModel session
            response: Response dict from pymlb_statsapi
            sport_id: Override sport_id (extracted from params if None)
            captured_at: Override captured_at (extracted from metadata if None)

        Returns:
            RawSeasons instance that was saved
        """
        data = response.get("data") or response.get("response")
        metadata = response.get("metadata", {})

        if not data:
            raise ValueError("Response must contain 'data' or 'response' key")

        if sport_id is None:
            params = metadata.get("params", {})
            sport_id = params.get("sportId") or params.get("sport_id", 1)

        if captured_at is None:
            captured_at_str = metadata.get("captured_at")
            if captured_at_str:
                if isinstance(captured_at_str, str):
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                elif isinstance(captured_at_str, datetime):
                    captured_at = captured_at_str
            else:
                captured_at = datetime.utcnow()

        raw_seasons = RawSeasons(
            sport_id=sport_id,
            captured_at=captured_at,
            data=data,
            endpoint=metadata.get("endpoint", "season"),
            method=metadata.get("method", "seasons"),
            params=metadata.get("params"),
            url=metadata.get("url"),
            status_code=metadata.get("status_code"),
        )

        session.add(raw_seasons)
        return raw_seasons

    def save_person(
        self,
        session: Session,
        response: dict[str, Any],
        *,
        person_id: Optional[int] = None,
        captured_at: Optional[datetime] = None,
    ) -> RawPerson:
        """Save raw Person.person response.

        Args:
            session: SQLModel session
            response: Response dict from pymlb_statsapi
            person_id: Override person_id (extracted from response/params if None)
            captured_at: Override captured_at (extracted from metadata if None)

        Returns:
            RawPerson instance that was saved
        """
        data = response.get("data") or response.get("response")
        metadata = response.get("metadata", {})

        if not data:
            raise ValueError("Response must contain 'data' or 'response' key")

        if person_id is None:
            # Try to extract from people array
            people = data.get("people", [])
            if people and len(people) > 0:
                person_id = people[0].get("id")
            if not person_id:
                params = metadata.get("params", {})
                person_ids = params.get("personIds") or params.get("person_ids")
                if person_ids:
                    # Extract first ID if comma-separated string or list
                    if isinstance(person_ids, str):
                        person_id = int(person_ids.split(",")[0])
                    elif isinstance(person_ids, list):
                        person_id = person_ids[0]

        if not person_id:
            raise ValueError("Could not extract person_id from response")

        if captured_at is None:
            captured_at_str = metadata.get("captured_at")
            if captured_at_str:
                if isinstance(captured_at_str, str):
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                elif isinstance(captured_at_str, datetime):
                    captured_at = captured_at_str
            else:
                captured_at = datetime.utcnow()

        raw_person = RawPerson(
            person_id=person_id,
            captured_at=captured_at,
            data=data,
            endpoint=metadata.get("endpoint", "person"),
            method=metadata.get("method", "person"),
            params=metadata.get("params"),
            url=metadata.get("url"),
            status_code=metadata.get("status_code"),
        )

        session.add(raw_person)
        return raw_person

    def save_team(
        self,
        session: Session,
        response: dict[str, Any],
        *,
        team_id: Optional[int] = None,
        captured_at: Optional[datetime] = None,
    ) -> RawTeam:
        """Save raw Team.team response.

        Args:
            session: SQLModel session
            response: Response dict from pymlb_statsapi
            team_id: Override team_id (extracted from response/params if None)
            captured_at: Override captured_at (extracted from metadata if None)

        Returns:
            RawTeam instance that was saved
        """
        data = response.get("data") or response.get("response")
        metadata = response.get("metadata", {})

        if not data:
            raise ValueError("Response must contain 'data' or 'response' key")

        if team_id is None:
            # Try to extract from teams array
            teams = data.get("teams", [])
            if teams and len(teams) > 0:
                team_id = teams[0].get("id")
            if not team_id:
                params = metadata.get("params", {})
                team_id = params.get("teamId") or params.get("team_id")

        if not team_id:
            raise ValueError("Could not extract team_id from response")

        if captured_at is None:
            captured_at_str = metadata.get("captured_at")
            if captured_at_str:
                if isinstance(captured_at_str, str):
                    captured_at = datetime.fromisoformat(captured_at_str.replace("Z", "+00:00"))
                elif isinstance(captured_at_str, datetime):
                    captured_at = captured_at_str
            else:
                captured_at = datetime.utcnow()

        raw_team = RawTeam(
            team_id=team_id,
            captured_at=captured_at,
            data=data,
            endpoint=metadata.get("endpoint", "team"),
            method=metadata.get("method", "team"),
            params=metadata.get("params"),
            url=metadata.get("url"),
            status_code=metadata.get("status_code"),
        )

        session.add(raw_team)
        return raw_team

    def get_latest_live_game(
        self, session: Session, game_pk: int
    ) -> Optional[RawLiveGameV1]:
        """Get the most recent raw data for a game.

        Args:
            session: SQLModel session
            game_pk: Game primary key

        Returns:
            Latest RawLiveGameV1 or None if not found
        """
        statement = (
            select(RawLiveGameV1)
            .where(RawLiveGameV1.game_pk == game_pk)
            .order_by(RawLiveGameV1.captured_at.desc())
            .limit(1)
        )
        return session.exec(statement).first()

    def get_unprocessed_games(
        self, session: Session, since: datetime
    ) -> list[RawLiveGameV1]:
        """Get all raw games captured since a timestamp.

        Useful for incremental processing.

        Args:
            session: SQLModel session
            since: Only return games captured after this timestamp

        Returns:
            List of RawLiveGameV1 instances
        """
        statement = (
            select(RawLiveGameV1)
            .where(RawLiveGameV1.captured_at > since)
            .order_by(RawLiveGameV1.captured_at.asc())
        )
        return list(session.exec(statement).all())

    def count_raw_games(self, session: Session) -> int:
        """Count total raw game records (all versions).

        Returns:
            Total count of raw game records
        """
        statement = select(RawLiveGameV1)
        return len(list(session.exec(statement).all()))
