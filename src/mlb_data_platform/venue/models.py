"""
Venue data models.

Pydantic models for venue enrichment data from external sources.
"""

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, Field


class ParkFactor(BaseModel):
    """
    Park factor from Baseball Savant Statcast data.

    Park factors show how a venue affects offensive statistics.
    100 = league average, >100 favors hitters, <100 favors pitchers.
    """

    venue_id: int = Field(..., description="MLB venue ID")
    venue_name: str = Field(..., description="Venue name")
    season: int = Field(..., description="Season year")
    as_of_date: date = Field(..., description="Date when factors were calculated")

    # Stat type
    stat_type: str = Field(
        ...,
        description="Statistic type (HR, 2B, 3B, 1B, SO, BB, wOBA, etc.)",
    )

    # Handedness
    batter_hand: Literal["L", "R", "Both"] = Field(
        "Both", description="Batter handedness"
    )
    pitcher_hand: Literal["L", "R", "Both"] = Field(
        "Both", description="Pitcher handedness"
    )

    # Park factor (100 = league average)
    park_factor: float = Field(
        ...,
        description="Park factor where 100 = league average",
        ge=0,
    )

    # Sample size
    sample_size: Optional[int] = Field(
        None, description="Number of events in sample"
    )
    games_played: Optional[int] = Field(None, description="Games played at venue")

    # Additional context
    day_night: Literal["Day", "Night", "Both"] = Field(
        "Both", description="Day/night game"
    )
    roof_status: Optional[Literal["Open", "Closed", "N/A"]] = Field(
        "N/A", description="Roof status (for retractable roof stadiums)"
    )

    # Metadata
    data_source: str = Field(
        "baseball_savant", description="Data source identifier"
    )


class VenueDimensions(BaseModel):
    """Ballpark field dimensions."""

    # Outfield distances (feet)
    left_line: Optional[int] = Field(
        None, description="Left field foul line distance"
    )
    left_field: Optional[int] = Field(None, description="Left field distance")
    left_center: Optional[int] = Field(None, description="Left-center distance")
    center_field: Optional[int] = Field(None, description="Center field distance")
    right_center: Optional[int] = Field(None, description="Right-center distance")
    right_field: Optional[int] = Field(None, description="Right field distance")
    right_line: Optional[int] = Field(
        None, description="Right field foul line distance"
    )

    # Wall heights (feet)
    wall_height_left: Optional[int] = Field(None, description="Left field wall height")
    wall_height_center: Optional[int] = Field(
        None, description="Center field wall height"
    )
    wall_height_right: Optional[int] = Field(
        None, description="Right field wall height"
    )
    wall_height_min: Optional[int] = Field(
        None, description="Minimum wall height in outfield"
    )
    wall_height_max: Optional[int] = Field(
        None, description="Maximum wall height in outfield (e.g., Green Monster = 37')"
    )


class VenueData(BaseModel):
    """
    Comprehensive venue data from MLB API and external sources.
    """

    venue_id: int = Field(..., description="MLB venue ID")
    name: str = Field(..., description="Venue name")
    venue_name_official: Optional[str] = Field(None, description="Official venue name")
    active: bool = Field(True, description="Is venue currently active")

    # Location
    address: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State/province")
    postal_code: Optional[str] = Field(None, description="Postal/ZIP code")
    country: str = Field("USA", description="Country code")

    # Coordinates
    latitude: Optional[float] = Field(None, description="Latitude", ge=-90, le=90)
    longitude: Optional[float] = Field(None, description="Longitude", ge=-180, le=180)
    elevation: Optional[int] = Field(None, description="Elevation in feet above sea level")
    timezone: Optional[str] = Field(None, description="Timezone (e.g., America/New_York)")

    # Field characteristics
    capacity: Optional[int] = Field(None, description="Seating capacity")
    turf_type: Optional[str] = Field(
        None, description="Turf type (Natural Grass, Artificial Turf, Hybrid)"
    )
    roof_type: Optional[str] = Field(
        None, description="Roof type (Open, Retractable, Fixed Dome, None)"
    )
    surface_type: Optional[str] = Field(None, description="Surface type")

    # Dimensions
    dimensions: Optional[VenueDimensions] = Field(None, description="Field dimensions")

    # Special features
    has_manual_scoreboard: bool = Field(False, description="Has manual scoreboard")
    has_warning_track: bool = Field(True, description="Has warning track")
    notable_features: list[str] = Field(
        default_factory=list,
        description="Notable features (Green Monster, Ivy, etc.)",
    )

    # Data quality tracking
    data_source: str = Field("mlb_api", description="Primary data source")
    data_quality: Literal["verified", "estimated", "unverified"] = Field(
        "verified", description="Data quality assessment"
    )
    last_verified_date: Optional[date] = Field(
        None, description="Date when data was last verified"
    )
    notes: Optional[str] = Field(None, description="Additional notes")
