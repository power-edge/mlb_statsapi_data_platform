"""
Venue enrichment module.

Provides scrapers and data enrichment for ballpark metadata:
- Baseball Savant park factors (Statcast)
- Seamheads.com historical dimensions
- Venue coordinates and metadata extraction from MLB API
"""

from .baseball_savant import BaseballSavantScraper
from .models import ParkFactor, VenueData, VenueDimensions
from .seamheads import SeamheadsScraper
from .storage import VenueStorageBackend

__all__ = [
    "BaseballSavantScraper",
    "SeamheadsScraper",
    "ParkFactor",
    "VenueData",
    "VenueDimensions",
    "VenueStorageBackend",
]
