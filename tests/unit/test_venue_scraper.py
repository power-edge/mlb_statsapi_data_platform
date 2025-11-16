"""
Unit tests for venue data scrapers.

Tests the Baseball Savant and Seamheads scrapers with mock HTTP responses.
"""

import json
from datetime import date
from unittest.mock import Mock, patch

import pytest

from mlb_data_platform.venue.baseball_savant import BaseballSavantScraper
from mlb_data_platform.venue.models import ParkFactor


class TestBaseballSavantScraper:
    """Tests for Baseball Savant park factors scraper."""

    @pytest.fixture
    def scraper(self):
        """Create scraper instance."""
        return BaseballSavantScraper()

    @pytest.fixture
    def mock_html_with_js_data(self):
        """Mock HTML with embedded JavaScript data."""
        return """
        <html>
        <head><title>Park Factors</title></head>
        <body>
            <script>
                var parkFactorsData = [
                    {
                        "venue_id": 2,
                        "venue_name": "Fenway Park",
                        "stat_type": "HR",
                        "park_factor": 115.2,
                        "batter_hand": "Both",
                        "pitcher_hand": "Both",
                        "day_night": "Both",
                        "sample_size": 500,
                        "games": 81
                    },
                    {
                        "venue_id": 19,
                        "venue_name": "Coors Field",
                        "stat_type": "HR",
                        "park_factor": 134.8,
                        "batter_hand": "Both",
                        "pitcher_hand": "Both",
                        "day_night": "Both",
                        "sample_size": 520,
                        "games": 81
                    }
                ];
            </script>
        </body>
        </html>
        """

    @pytest.fixture
    def mock_html_with_data_attr(self):
        """Mock HTML with data attributes."""
        data = {
            "rows": [
                {
                    "venueId": 2,
                    "venue": "Fenway Park",
                    "stat": "HR",
                    "factor": 115.2,
                    "batter_hand": "Both",
                    "pitcher_hand": "Both",
                    "day_night": "Both",
                    "n": 500,
                    "g": 81,
                }
            ]
        }
        json_str = json.dumps(data).replace('"', "&quot;")
        return f"""
        <html>
        <body>
            <div data-table="{json_str}"></div>
        </body>
        </html>
        """

    def test_validate_park_factors_data_valid(self, scraper):
        """Test validation accepts valid park factors data."""
        data = [
            {
                "venue_id": 2,
                "venue_name": "Fenway Park",
                "park_factor": 115.2,
            }
        ]
        assert scraper._validate_park_factors_data(data) is True

    def test_validate_park_factors_data_alt_keys(self, scraper):
        """Test validation accepts alternative key names."""
        data = [
            {
                "venueId": 2,
                "ballpark": "Fenway Park",
                "factor": 115.2,
            }
        ]
        assert scraper._validate_park_factors_data(data) is True

    def test_validate_park_factors_data_invalid(self, scraper):
        """Test validation rejects invalid data."""
        data = [{"foo": "bar"}]
        assert scraper._validate_park_factors_data(data) is False

    def test_validate_park_factors_data_empty(self, scraper):
        """Test validation rejects empty data."""
        assert scraper._validate_park_factors_data([]) is False

    def test_extract_park_factors_from_js(self, scraper, mock_html_with_js_data):
        """Test extracting park factors from JavaScript variable."""
        data = scraper._extract_park_factors_data(mock_html_with_js_data)
        assert len(data) == 2
        assert data[0]["venue_name"] == "Fenway Park"
        assert data[1]["venue_name"] == "Coors Field"

    def test_parse_park_factor_record(self, scraper):
        """Test parsing a single park factor record."""
        record = {
            "venue_id": 2,
            "venue_name": "Fenway Park",
            "stat_type": "HR",
            "park_factor": 115.2,
            "batter_hand": "Both",
            "pitcher_hand": "Both",
            "day_night": "Both",
            "sample_size": 500,
            "games": 81,
        }

        factor = scraper._parse_park_factor_record(
            record=record,
            season=2024,
            as_of_date=date(2024, 11, 10),
        )

        assert isinstance(factor, ParkFactor)
        assert factor.venue_id == 2
        assert factor.venue_name == "Fenway Park"
        assert factor.season == 2024
        assert factor.stat_type == "HR"
        assert factor.park_factor == 115.2
        assert factor.batter_hand == "Both"
        assert factor.pitcher_hand == "Both"
        assert factor.sample_size == 500
        assert factor.games_played == 81

    def test_parse_park_factor_record_alt_keys(self, scraper):
        """Test parsing with alternative key names."""
        record = {
            "venueId": 2,
            "venue": "Fenway Park",
            "stat": "HR",
            "factor": 115.2,
        }

        factor = scraper._parse_park_factor_record(
            record=record,
            season=2024,
            as_of_date=date(2024, 11, 10),
        )

        assert factor.venue_id == 2
        assert factor.venue_name == "Fenway Park"
        assert factor.park_factor == 115.2

    def test_parse_park_factor_record_missing_required(self, scraper):
        """Test parsing fails with missing required fields."""
        record = {"venue_id": 2}

        with pytest.raises(ValueError, match="Missing required fields"):
            scraper._parse_park_factor_record(
                record=record,
                season=2024,
                as_of_date=date(2024, 11, 10),
            )

    @patch("mlb_data_platform.venue.baseball_savant.BaseballSavantScraper._fetch_page")
    def test_fetch_park_factors_success(self, mock_fetch, scraper, mock_html_with_js_data):
        """Test successful fetch of park factors."""
        mock_fetch.return_value = mock_html_with_js_data

        factors = scraper.fetch_park_factors(season=2024)

        assert len(factors) == 2
        assert all(isinstance(f, ParkFactor) for f in factors)
        assert factors[0].season == 2024
        assert factors[0].venue_name == "Fenway Park"
        assert factors[1].venue_name == "Coors Field"

        # Verify fetch was called with correct params
        mock_fetch.assert_called_once()
        call_args = mock_fetch.call_args
        assert call_args[1]["params"]["year"] == 2024
        assert call_args[1]["params"]["rolling"] == 1

    @patch("mlb_data_platform.venue.baseball_savant.BaseballSavantScraper._fetch_page")
    def test_fetch_park_factors_rolling_average(self, mock_fetch, scraper, mock_html_with_js_data):
        """Test fetch with rolling average."""
        mock_fetch.return_value = mock_html_with_js_data

        factors = scraper.fetch_park_factors(season=2024, rolling=3)

        assert len(factors) == 2
        mock_fetch.assert_called_once()
        call_args = mock_fetch.call_args
        assert call_args[1]["params"]["rolling"] == 3

    @patch("mlb_data_platform.venue.baseball_savant.BaseballSavantScraper._fetch_page")
    def test_fetch_park_factors_no_data(self, mock_fetch, scraper):
        """Test fetch fails when no valid data found."""
        mock_fetch.return_value = "<html><body>No data</body></html>"

        with pytest.raises(ValueError, match="Could not extract park factors"):
            scraper.fetch_park_factors(season=2024)

    def test_context_manager(self):
        """Test scraper works as context manager."""
        with BaseballSavantScraper() as scraper:
            assert scraper.client is not None

        # Client should be closed after exiting context
        assert scraper.client.is_closed

    def test_close(self, scraper):
        """Test explicit close."""
        assert not scraper.client.is_closed
        scraper.close()
        assert scraper.client.is_closed
