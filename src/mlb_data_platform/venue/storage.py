"""
Venue data storage backend.

Handles saving venue enrichment data to PostgreSQL:
- Park factors from Baseball Savant
- Historical dimensions from Seamheads
- Venue metadata from MLB API
"""

import logging
from typing import Any

from ..storage.postgres import PostgresStorageBackend
from .models import ParkFactor, VenueData

logger = logging.getLogger(__name__)


class VenueStorageBackend:
    """
    Storage backend for venue enrichment data.

    Handles CRUD operations for venue-related tables:
    - venue.venues (master venue table)
    - venue.statcast_park_factors (Baseball Savant park factors)
    - venue.venue_history (historical dimension changes)
    - venue.fence_coordinates (detailed fence shape)
    - venue.venue_weather (historical weather)
    - venue.venue_metadata (extended metadata)
    """

    def __init__(self, postgres: PostgresStorageBackend):
        """
        Initialize venue storage backend.

        Args:
            postgres: PostgreSQL storage backend instance
        """
        self.postgres = postgres
        self.pool = postgres.pool

    def upsert_park_factors(
        self,
        park_factors: list[ParkFactor],
    ) -> int:
        """
        Upsert park factors to venue.statcast_park_factors table.

        Uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) to handle duplicates
        based on the UNIQUE constraint: (venue_id, season, stat_type, batter_hand, pitcher_hand, day_night)

        Args:
            park_factors: List of ParkFactor objects to save

        Returns:
            Number of rows affected (inserted or updated)

        Examples:
            >>> storage = VenueStorageBackend(postgres)
            >>> factors = scraper.fetch_park_factors(season=2024)
            >>> count = storage.upsert_park_factors(factors)
            >>> print(f"Saved {count} park factors")
        """
        if not park_factors:
            logger.warning("No park factors to save")
            return 0

        # SQL for UPSERT
        upsert_sql = """
            INSERT INTO venue.statcast_park_factors (
                venue_id,
                season,
                as_of_date,
                stat_type,
                batter_hand,
                pitcher_hand,
                park_factor,
                sample_size,
                games_played,
                day_night,
                roof_status,
                data_source
            ) VALUES (
                %(venue_id)s,
                %(season)s,
                %(as_of_date)s,
                %(stat_type)s,
                %(batter_hand)s,
                %(pitcher_hand)s,
                %(park_factor)s,
                %(sample_size)s,
                %(games_played)s,
                %(day_night)s,
                %(roof_status)s,
                %(data_source)s
            )
            ON CONFLICT (venue_id, season, stat_type, batter_hand, pitcher_hand, day_night)
            DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                park_factor = EXCLUDED.park_factor,
                sample_size = EXCLUDED.sample_size,
                games_played = EXCLUDED.games_played,
                roof_status = EXCLUDED.roof_status,
                created_at = CURRENT_TIMESTAMP
            RETURNING id;
        """

        rows_affected = 0

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    for factor in park_factors:
                        values = {
                            "venue_id": factor.venue_id,
                            "season": factor.season,
                            "as_of_date": factor.as_of_date,
                            "stat_type": factor.stat_type,
                            "batter_hand": factor.batter_hand,
                            "pitcher_hand": factor.pitcher_hand,
                            "park_factor": factor.park_factor,
                            "sample_size": factor.sample_size,
                            "games_played": factor.games_played,
                            "day_night": factor.day_night,
                            "roof_status": factor.roof_status,
                            "data_source": factor.data_source,
                        }

                        cur.execute(upsert_sql, values)
                        rows_affected += 1

                    conn.commit()

                logger.info(
                    f"Saved {rows_affected} park factors for season {park_factors[0].season}"
                )
                return rows_affected

            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to save park factors: {e}")
                raise

    def upsert_venue(
        self,
        venue: VenueData,
    ) -> int:
        """
        Upsert venue data to venue.venues table.

        Uses UPSERT (INSERT ... ON CONFLICT DO UPDATE) based on venue_id primary key.

        Args:
            venue: VenueData object to save

        Returns:
            venue_id of the inserted/updated row

        Examples:
            >>> storage = VenueStorageBackend(postgres)
            >>> venue = VenueData(venue_id=2, name="Fenway Park", city="Boston")
            >>> venue_id = storage.upsert_venue(venue)
        """
        upsert_sql = """
            INSERT INTO venue.venues (
                venue_id,
                name,
                venue_name_official,
                active,
                address,
                city,
                state,
                postal_code,
                country,
                latitude,
                longitude,
                elevation,
                timezone,
                capacity,
                turf_type,
                roof_type,
                surface_type,
                left_line,
                left_field,
                left_center,
                center_field,
                right_center,
                right_field,
                right_line,
                wall_height_left,
                wall_height_center,
                wall_height_right,
                wall_height_min,
                wall_height_max,
                has_manual_scoreboard,
                has_warning_track,
                notable_features,
                data_source,
                data_quality,
                last_verified_date,
                notes
            ) VALUES (
                %(venue_id)s,
                %(name)s,
                %(venue_name_official)s,
                %(active)s,
                %(address)s,
                %(city)s,
                %(state)s,
                %(postal_code)s,
                %(country)s,
                %(latitude)s,
                %(longitude)s,
                %(elevation)s,
                %(timezone)s,
                %(capacity)s,
                %(turf_type)s,
                %(roof_type)s,
                %(surface_type)s,
                %(left_line)s,
                %(left_field)s,
                %(left_center)s,
                %(center_field)s,
                %(right_center)s,
                %(right_field)s,
                %(right_line)s,
                %(wall_height_left)s,
                %(wall_height_center)s,
                %(wall_height_right)s,
                %(wall_height_min)s,
                %(wall_height_max)s,
                %(has_manual_scoreboard)s,
                %(has_warning_track)s,
                %(notable_features)s,
                %(data_source)s,
                %(data_quality)s,
                %(last_verified_date)s,
                %(notes)s
            )
            ON CONFLICT (venue_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                venue_name_official = EXCLUDED.venue_name_official,
                active = EXCLUDED.active,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                postal_code = EXCLUDED.postal_code,
                country = EXCLUDED.country,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                elevation = EXCLUDED.elevation,
                timezone = EXCLUDED.timezone,
                capacity = EXCLUDED.capacity,
                turf_type = EXCLUDED.turf_type,
                roof_type = EXCLUDED.roof_type,
                surface_type = EXCLUDED.surface_type,
                left_line = EXCLUDED.left_line,
                left_field = EXCLUDED.left_field,
                left_center = EXCLUDED.left_center,
                center_field = EXCLUDED.center_field,
                right_center = EXCLUDED.right_center,
                right_field = EXCLUDED.right_field,
                right_line = EXCLUDED.right_line,
                wall_height_left = EXCLUDED.wall_height_left,
                wall_height_center = EXCLUDED.wall_height_center,
                wall_height_right = EXCLUDED.wall_height_right,
                wall_height_min = EXCLUDED.wall_height_min,
                wall_height_max = EXCLUDED.wall_height_max,
                has_manual_scoreboard = EXCLUDED.has_manual_scoreboard,
                has_warning_track = EXCLUDED.has_warning_track,
                notable_features = EXCLUDED.notable_features,
                data_source = EXCLUDED.data_source,
                data_quality = EXCLUDED.data_quality,
                last_verified_date = EXCLUDED.last_verified_date,
                notes = EXCLUDED.notes,
                updated_at = CURRENT_TIMESTAMP
            RETURNING venue_id;
        """

        # Extract dimensions if present
        dimensions = venue.dimensions
        values = {
            "venue_id": venue.venue_id,
            "name": venue.name,
            "venue_name_official": venue.venue_name_official,
            "active": venue.active,
            "address": venue.address,
            "city": venue.city,
            "state": venue.state,
            "postal_code": venue.postal_code,
            "country": venue.country,
            "latitude": venue.latitude,
            "longitude": venue.longitude,
            "elevation": venue.elevation,
            "timezone": venue.timezone,
            "capacity": venue.capacity,
            "turf_type": venue.turf_type,
            "roof_type": venue.roof_type,
            "surface_type": venue.surface_type,
            "left_line": dimensions.left_line if dimensions else None,
            "left_field": dimensions.left_field if dimensions else None,
            "left_center": dimensions.left_center if dimensions else None,
            "center_field": dimensions.center_field if dimensions else None,
            "right_center": dimensions.right_center if dimensions else None,
            "right_field": dimensions.right_field if dimensions else None,
            "right_line": dimensions.right_line if dimensions else None,
            "wall_height_left": dimensions.wall_height_left if dimensions else None,
            "wall_height_center": dimensions.wall_height_center if dimensions else None,
            "wall_height_right": dimensions.wall_height_right if dimensions else None,
            "wall_height_min": dimensions.wall_height_min if dimensions else None,
            "wall_height_max": dimensions.wall_height_max if dimensions else None,
            "has_manual_scoreboard": venue.has_manual_scoreboard,
            "has_warning_track": venue.has_warning_track,
            "notable_features": venue.notable_features,
            "data_source": venue.data_source,
            "data_quality": venue.data_quality,
            "last_verified_date": venue.last_verified_date,
            "notes": venue.notes,
        }

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(upsert_sql, values)
                    venue_id = cur.fetchone()["venue_id"]
                    conn.commit()

                    logger.info(f"Saved venue {venue_id} ({venue.name})")
                    return venue_id

            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to save venue {venue.venue_id}: {e}")
                raise

    def get_venues(
        self,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Retrieve venues from venue.venues table.

        Args:
            active_only: If True, only return active venues

        Returns:
            List of venue records as dictionaries

        Examples:
            >>> storage = VenueStorageBackend(postgres)
            >>> venues = storage.get_venues(active_only=True)
            >>> for venue in venues:
            ...     print(f"{venue['name']} ({venue['city']})")
        """
        sql = """
            SELECT
                venue_id,
                name,
                venue_name_official,
                active,
                city,
                state,
                latitude,
                longitude,
                capacity,
                turf_type,
                roof_type,
                center_field,
                data_quality,
                last_verified_date
            FROM venue.venues
            WHERE active = %(active)s OR %(active_only)s = FALSE
            ORDER BY name;
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"active": True, "active_only": active_only})
                rows = cur.fetchall()

        logger.info(f"Retrieved {len(rows)} venues (active_only={active_only})")
        return [dict(row) for row in rows]

    def get_park_factors(
        self,
        venue_id: int | None = None,
        season: int | None = None,
        stat_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Retrieve park factors from venue.statcast_park_factors table.

        Args:
            venue_id: Filter by venue ID (optional)
            season: Filter by season (optional)
            stat_type: Filter by stat type (e.g., 'HR', '2B') (optional)

        Returns:
            List of park factor records as dictionaries

        Examples:
            >>> storage = VenueStorageBackend(postgres)
            >>> # Get all HR park factors for 2024
            >>> factors = storage.get_park_factors(season=2024, stat_type='HR')
            >>> # Get all park factors for Fenway Park
            >>> factors = storage.get_park_factors(venue_id=2)
        """
        sql = """
            SELECT
                pf.venue_id,
                v.name as venue_name,
                pf.season,
                pf.stat_type,
                pf.park_factor,
                pf.batter_hand,
                pf.pitcher_hand,
                pf.sample_size,
                pf.games_played,
                pf.as_of_date
            FROM venue.statcast_park_factors pf
            JOIN venue.venues v USING (venue_id)
            WHERE
                (%(venue_id)s IS NULL OR pf.venue_id = %(venue_id)s)
                AND (%(season)s IS NULL OR pf.season = %(season)s)
                AND (%(stat_type)s IS NULL OR pf.stat_type = %(stat_type)s)
            ORDER BY pf.venue_id, pf.season DESC, pf.stat_type;
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "venue_id": venue_id,
                        "season": season,
                        "stat_type": stat_type,
                    },
                )
                rows = cur.fetchall()

        logger.info(
            f"Retrieved {len(rows)} park factors "
            f"(venue_id={venue_id}, season={season}, stat_type={stat_type})"
        )
        return [dict(row) for row in rows]

    def refresh_materialized_views(self) -> None:
        """
        Refresh venue-related materialized views.

        Refreshes:
        - venue.current_park_factors (most recent park factors by venue)
        - venue.venue_summary (venue summary with latest park factors)

        Examples:
            >>> storage = VenueStorageBackend(postgres)
            >>> storage.refresh_materialized_views()
        """
        views = [
            "venue.current_park_factors",
            "venue.venue_summary",
        ]

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    for view in views:
                        logger.info(f"Refreshing materialized view: {view}")
                        cur.execute(f"REFRESH MATERIALIZED VIEW {view};")

                    conn.commit()
                    logger.info(f"Refreshed {len(views)} materialized views")

            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to refresh materialized views: {e}")
                raise
