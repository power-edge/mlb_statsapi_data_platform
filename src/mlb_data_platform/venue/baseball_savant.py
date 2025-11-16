"""
Baseball Savant park factors scraper.

Scrapes Statcast park factors from baseballsavant.mlb.com.
Park factors show how a venue affects offensive statistics (100 = league average).

Data source: https://baseballsavant.mlb.com/leaderboard/statcast-park-factors

Note: Baseball Savant serves data via JavaScript-rendered tables, so we'll use
httpx to fetch the page and parse the embedded JSON data from the script tags.
"""

import json
import logging
import re
from datetime import date
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import ParkFactor

logger = logging.getLogger(__name__)


class BaseballSavantScraper:
    """
    Scraper for Baseball Savant Statcast park factors.

    Baseball Savant provides park factors that show how each MLB venue
    affects various offensive statistics. Park factors are normalized
    so that 100 = league average.

    Examples:
        >>> scraper = BaseballSavantScraper()
        >>> factors = scraper.fetch_park_factors(season=2024)
        >>> for factor in factors:
        ...     print(f"{factor.venue_name}: {factor.stat_type}={factor.park_factor}")
    """

    BASE_URL = "https://baseballsavant.mlb.com"
    PARK_FACTORS_URL = f"{BASE_URL}/leaderboard/statcast-park-factors"

    # Default headers to mimic browser request
    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    def __init__(
        self,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        """
        Initialize Baseball Savant scraper.

        Args:
            timeout: HTTP request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.client = httpx.Client(
            timeout=timeout,
            headers=self.DEFAULT_HEADERS,
            follow_redirects=True,
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close HTTP client."""
        self.client.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_page(self, url: str, params: dict[str, Any] | None = None) -> str:
        """
        Fetch page content with retries.

        Args:
            url: URL to fetch
            params: Query parameters

        Returns:
            Page HTML content

        Raises:
            httpx.HTTPError: On HTTP errors after retries
        """
        logger.info(f"Fetching: {url} (params={params})")

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching {url}: {e}")
            raise

    def _extract_park_factors_data(self, html: str) -> list[dict[str, Any]]:
        """
        Extract park factors data from HTML page.

        Baseball Savant embeds data in JavaScript variables within <script> tags.
        We'll look for patterns like: `var data = [...]` or embedded JSON.

        Args:
            html: HTML page content

        Returns:
            List of park factor records as dictionaries

        Raises:
            ValueError: If data cannot be extracted from HTML
        """
        # Strategy 1: Look for JavaScript variable assignments
        # Pattern: var data = [...]; or const data = [...];
        pattern = r'(?:var|const|let)\s+\w+\s*=\s*(\[.*?\]);'
        matches = re.findall(pattern, html, re.DOTALL)

        for match in matches:
            try:
                data = json.loads(match)
                if isinstance(data, list) and len(data) > 0:
                    # Validate it looks like park factors data
                    if self._validate_park_factors_data(data):
                        logger.info(f"Extracted {len(data)} park factor records")
                        return data
            except json.JSONDecodeError:
                continue

        # Strategy 2: Look for inline JSON in data attributes
        # Pattern: data-json='...' or data-table='...'
        data_attr_pattern = r'data-(?:json|table)=["\'](\{.*?\})["\']'
        matches = re.findall(data_attr_pattern, html, re.DOTALL)

        for match in matches:
            try:
                data = json.loads(match)
                if isinstance(data, dict) and "rows" in data:
                    rows = data["rows"]
                    if self._validate_park_factors_data(rows):
                        logger.info(f"Extracted {len(rows)} park factor records from data attribute")
                        return rows
            except json.JSONDecodeError:
                continue

        # Strategy 3: If page has a CSV download link, fetch that instead
        csv_pattern = r'href="(/leaderboard/statcast-park-factors\?[^"]*&csv=true[^"]*)"'
        csv_match = re.search(csv_pattern, html)
        if csv_match:
            csv_path = csv_match.group(1)
            logger.info(f"Found CSV download link: {csv_path}")
            # Return marker that we found CSV (caller can fetch it)
            raise ValueError(
                f"Park factors available as CSV: {self.BASE_URL}{csv_path}. "
                "Use fetch_park_factors_csv() method instead."
            )

        raise ValueError(
            "Could not extract park factors data from HTML. "
            "Baseball Savant may have changed their page structure."
        )

    def _validate_park_factors_data(self, data: list[dict[str, Any]]) -> bool:
        """
        Validate that extracted data looks like park factors.

        Args:
            data: List of dictionaries to validate

        Returns:
            True if data looks valid
        """
        if not data or len(data) == 0:
            return False

        first_record = data[0]

        # Check for expected keys (may vary by Baseball Savant's format)
        expected_keys = {"venue_id", "venue_name", "park_factor"}

        # Also accept alternative naming conventions
        alt_keys_venue = {"venue", "ballpark", "stadium", "team"}
        alt_keys_factor = {"factor", "pf", "park_factor", "value"}

        has_venue = any(k in first_record for k in expected_keys.union(alt_keys_venue))
        has_factor = any(k in first_record for k in alt_keys_factor)

        return has_venue and has_factor

    def _parse_park_factor_record(
        self,
        record: dict[str, Any],
        season: int,
        as_of_date: date,
    ) -> ParkFactor:
        """
        Parse a single park factor record from Baseball Savant.

        Args:
            record: Raw record dictionary
            season: Season year
            as_of_date: Date when data was retrieved

        Returns:
            Parsed ParkFactor model

        Raises:
            ValueError: If record cannot be parsed
        """
        # Map field names (Baseball Savant may use different conventions)
        venue_id = record.get("venue_id") or record.get("venueId")
        venue_name = (
            record.get("venue_name")
            or record.get("venue")
            or record.get("ballpark")
            or record.get("stadium")
        )

        # Extract stat type
        stat_type = record.get("stat_type") or record.get("stat") or "Unknown"

        # Extract park factor
        park_factor = (
            record.get("park_factor")
            or record.get("factor")
            or record.get("pf")
            or record.get("value")
        )

        if not all([venue_id, venue_name, park_factor]):
            raise ValueError(f"Missing required fields in record: {record}")

        # Handedness (may not be present in all records)
        batter_hand = record.get("batter_hand", "Both")
        pitcher_hand = record.get("pitcher_hand", "Both")

        # Day/night
        day_night = record.get("day_night", "Both")

        # Sample size
        sample_size = record.get("sample_size") or record.get("n")
        games_played = record.get("games") or record.get("g")

        # Roof status
        roof_status = record.get("roof", "N/A")

        return ParkFactor(
            venue_id=int(venue_id),
            venue_name=str(venue_name),
            season=season,
            as_of_date=as_of_date,
            stat_type=str(stat_type),
            batter_hand=batter_hand,
            pitcher_hand=pitcher_hand,
            park_factor=float(park_factor),
            sample_size=int(sample_size) if sample_size else None,
            games_played=int(games_played) if games_played else None,
            day_night=day_night,
            roof_status=roof_status,
            data_source="baseball_savant",
        )

    def fetch_park_factors(
        self,
        season: int = 2024,
        rolling: int = 1,
    ) -> list[ParkFactor]:
        """
        Fetch park factors for a given season.

        Args:
            season: Season year (e.g., 2024)
            rolling: Number of seasons for rolling average (1 = single season, 3 = three-year)

        Returns:
            List of ParkFactor objects

        Raises:
            ValueError: If data cannot be scraped
            httpx.HTTPError: On HTTP errors

        Examples:
            >>> scraper = BaseballSavantScraper()
            >>> factors_2024 = scraper.fetch_park_factors(season=2024)
            >>> factors_3yr = scraper.fetch_park_factors(season=2024, rolling=3)
        """
        params = {
            "year": season,
            "rolling": rolling,
        }

        html = self._fetch_page(self.PARK_FACTORS_URL, params=params)

        try:
            raw_data = self._extract_park_factors_data(html)
        except ValueError as e:
            # Check if CSV is suggested
            if "CSV" in str(e):
                logger.warning(str(e))
                # Could implement CSV fetching here
                raise NotImplementedError(
                    "CSV download support not yet implemented. "
                    "Baseball Savant may require fetching CSV directly."
                )
            raise

        # Parse records
        as_of_date = date.today()
        park_factors = []

        for record in raw_data:
            try:
                factor = self._parse_park_factor_record(
                    record=record,
                    season=season,
                    as_of_date=as_of_date,
                )
                park_factors.append(factor)
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to parse record {record}: {e}")
                continue

        if not park_factors:
            raise ValueError(
                f"No park factors extracted for season {season}. "
                "Baseball Savant page structure may have changed."
            )

        logger.info(
            f"Successfully scraped {len(park_factors)} park factors "
            f"for season {season} (rolling={rolling})"
        )

        return park_factors

    def close(self):
        """Close HTTP client."""
        self.client.close()
