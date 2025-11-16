"""
Seamheads ballparks scraper.

Scrapes historical ballpark dimensions from seamheads.com.
Provides field dimensions, wall heights, and historical changes over time.

Data source: https://www.seamheads.com/ballparks/

The Seamheads database includes:
- Field dimensions (left, center, right, power alleys)
- Wall heights
- Seating capacity history
- Opening/closing dates
- Latitude/longitude
- Altitude
- Playing surface type
- Historical configuration changes
"""

import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import VenueData, VenueDimensions

logger = logging.getLogger(__name__)


class SeamheadsScraper:
    """
    Scraper for Seamheads.com ballparks database.

    Seamheads provides comprehensive historical data for MLB ballparks,
    including dimension changes over time, wall heights, and venue metadata.

    Examples:
        >>> scraper = SeamheadsScraper()
        >>> venue = scraper.fetch_ballpark_by_id("BOS07")  # Fenway Park
        >>> print(f"{venue.name}: CF = {venue.dimensions.center_field}'")
    """

    BASE_URL = "https://www.seamheads.com"
    BALLPARK_URL = f"{BASE_URL}/ballparks/ballpark.php"
    INDEX_URL = f"{BASE_URL}/ballparks/index.php"

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
        Initialize Seamheads scraper.

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

    def _parse_dimension_value(self, value_str: str) -> Optional[int]:
        """
        Parse dimension value from string.

        Handles formats like: "310", "310'", "310 ft", etc.

        Args:
            value_str: String containing dimension value

        Returns:
            Integer value in feet, or None if cannot parse
        """
        if not value_str:
            return None

        # Remove common suffixes and whitespace
        value_str = str(value_str).strip()
        value_str = re.sub(r"['\"]|ft|feet", "", value_str, flags=re.IGNORECASE)
        value_str = value_str.strip()

        # Try to extract first number
        match = re.search(r"(\d+)", value_str)
        if match:
            return int(match.group(1))

        return None

    def _parse_ballpark_page(self, html: str, park_id: str) -> VenueData:
        """
        Parse ballpark page HTML to extract venue data.

        Args:
            html: HTML content of ballpark page
            park_id: Seamheads park ID (e.g., "BOS07")

        Returns:
            VenueData object with parsed information

        Raises:
            ValueError: If required data cannot be extracted
        """
        soup = BeautifulSoup(html, "html.parser")

        # Extract venue name (usually in <h1> or <title>)
        name_elem = soup.find("h1")
        if not name_elem:
            # Try title tag
            title_elem = soup.find("title")
            if title_elem:
                # Title format: "Fenway Park | Seamheads.com"
                name = title_elem.text.split("|")[0].strip()
            else:
                raise ValueError(f"Could not find venue name for park_id={park_id}")
        else:
            name = name_elem.text.strip()

        logger.info(f"Parsing ballpark: {name}")

        # Initialize data structure
        venue_data = {
            "venue_id": 0,  # Will need to map Seamheads park_id to MLB venue_id
            "name": name,
            "active": True,  # Determine from dates
            "data_source": "seamheads",
            "data_quality": "verified",
            "last_verified_date": date.today(),
            "notes": f"Seamheads park_id: {park_id}",
        }

        dimensions_data = {}

        # Parse tables for structured data
        # Seamheads typically uses tables with labels and values
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    # Parse location data
                    if "city" in label:
                        venue_data["city"] = value
                    elif "state" in label:
                        venue_data["state"] = value
                    elif "address" in label:
                        venue_data["address"] = value

                    # Parse coordinates
                    elif "latitude" in label:
                        try:
                            venue_data["latitude"] = float(value)
                        except (ValueError, TypeError):
                            pass
                    elif "longitude" in label:
                        try:
                            venue_data["longitude"] = float(value)
                        except (ValueError, TypeError):
                            pass
                    elif "altitude" in label or "elevation" in label:
                        venue_data["elevation"] = self._parse_dimension_value(value)

                    # Parse capacity
                    elif "capacity" in label or "seating" in label:
                        try:
                            # Remove commas
                            capacity_str = re.sub(r"[,\s]", "", value)
                            venue_data["capacity"] = int(capacity_str)
                        except (ValueError, TypeError):
                            pass

                    # Parse surface
                    elif "surface" in label or "turf" in label:
                        venue_data["turf_type"] = value
                        venue_data["surface_type"] = value

                    # Parse roof type
                    elif "roof" in label:
                        venue_data["roof_type"] = value

                    # Parse dimensions
                    elif "left field" in label and "line" in label:
                        dimensions_data["left_line"] = self._parse_dimension_value(value)
                    elif "left field" in label or "left" in label:
                        dimensions_data["left_field"] = self._parse_dimension_value(value)
                    elif "left center" in label or "left-center" in label:
                        dimensions_data["left_center"] = self._parse_dimension_value(value)
                    elif "center field" in label or "center" in label:
                        dimensions_data["center_field"] = self._parse_dimension_value(value)
                    elif "right center" in label or "right-center" in label:
                        dimensions_data["right_center"] = self._parse_dimension_value(value)
                    elif "right field" in label and "line" in label:
                        dimensions_data["right_line"] = self._parse_dimension_value(value)
                    elif "right field" in label or "right" in label:
                        dimensions_data["right_field"] = self._parse_dimension_value(value)

                    # Parse wall heights
                    elif "wall" in label and "left" in label:
                        dimensions_data["wall_height_left"] = self._parse_dimension_value(value)
                    elif "wall" in label and "center" in label:
                        dimensions_data["wall_height_center"] = self._parse_dimension_value(value)
                    elif "wall" in label and "right" in label:
                        dimensions_data["wall_height_right"] = self._parse_dimension_value(value)

        # Create VenueDimensions if we have any dimension data
        if dimensions_data:
            venue_data["dimensions"] = VenueDimensions(**dimensions_data)

        # Create VenueData
        venue = VenueData(**venue_data)

        logger.info(f"Parsed venue: {venue.name} (dimensions={bool(dimensions_data)})")

        return venue

    def fetch_ballpark_by_id(self, park_id: str) -> VenueData:
        """
        Fetch ballpark data by Seamheads park ID.

        Args:
            park_id: Seamheads park ID (e.g., "BOS07" for Fenway Park)

        Returns:
            VenueData object with ballpark information

        Raises:
            ValueError: If ballpark cannot be found or parsed
            httpx.HTTPError: On HTTP errors

        Examples:
            >>> scraper = SeamheadsScraper()
            >>> fenway = scraper.fetch_ballpark_by_id("BOS07")
            >>> dodger = scraper.fetch_ballpark_by_id("LOS03")
        """
        params = {"parkID": park_id}
        html = self._fetch_page(self.BALLPARK_URL, params=params)

        venue = self._parse_ballpark_page(html, park_id)

        logger.info(f"Successfully fetched ballpark {park_id}: {venue.name}")

        return venue

    def fetch_all_ballparks(
        self,
        active_only: bool = True,
    ) -> list[VenueData]:
        """
        Fetch all ballparks from Seamheads database.

        Args:
            active_only: If True, only fetch currently active MLB parks

        Returns:
            List of VenueData objects

        Raises:
            ValueError: If index page cannot be parsed
            httpx.HTTPError: On HTTP errors

        Examples:
            >>> scraper = SeamheadsScraper()
            >>> venues = scraper.fetch_all_ballparks(active_only=True)
            >>> for venue in venues:
            ...     print(f"{venue.name}: {venue.city}")
        """
        params = {
            "active": "Yes" if active_only else "All",
            "sort": "park_a",
        }

        html = self._fetch_page(self.INDEX_URL, params=params)

        # Parse index page to extract all park IDs
        soup = BeautifulSoup(html, "html.parser")

        # Find all links to ballpark pages
        # Pattern: ballpark.php?parkID=XXX
        park_ids = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = re.search(r"parkID=([A-Z0-9]+)", href)
            if match:
                park_id = match.group(1)
                if park_id not in park_ids:
                    park_ids.append(park_id)

        logger.info(f"Found {len(park_ids)} ballparks to fetch")

        # Fetch each ballpark
        venues = []
        for i, park_id in enumerate(park_ids, 1):
            try:
                logger.info(f"Fetching {i}/{len(park_ids)}: {park_id}")
                venue = self.fetch_ballpark_by_id(park_id)
                venues.append(venue)
            except Exception as e:
                logger.warning(f"Failed to fetch park_id={park_id}: {e}")
                continue

        logger.info(f"Successfully fetched {len(venues)} ballparks")

        return venues

    def close(self):
        """Close HTTP client."""
        self.client.close()
