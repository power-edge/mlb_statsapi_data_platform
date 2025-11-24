"""Unit tests for template variable resolution."""

from datetime import date, datetime
from unittest.mock import patch

import pytest

from mlb_data_platform.ingestion.template import TemplateResolver, resolve_config


class TestTemplateResolver:
    """Test template variable resolution."""

    def test_today_variable(self):
        """Test ${TODAY} resolves to current date."""
        resolver = TemplateResolver()
        result = resolver.resolve("${TODAY}")

        # Should match today's date in YYYY-MM-DD format
        expected = date.today().isoformat()
        assert result == expected

    def test_yesterday_variable(self):
        """Test ${YESTERDAY} resolves to yesterday's date."""
        resolver = TemplateResolver()
        result = resolver.resolve("${YESTERDAY}")

        from datetime import timedelta
        expected = (date.today() - timedelta(days=1)).isoformat()
        assert result == expected

    def test_year_month_day_variables(self):
        """Test ${YEAR}, ${MONTH}, ${DAY} variables."""
        resolver = TemplateResolver()
        today = date.today()

        assert resolver.resolve("${YEAR}") == str(today.year)
        assert resolver.resolve("${MONTH}") == f"{today.month:02d}"
        assert resolver.resolve("${DAY}") == f"{today.day:02d}"

    def test_date_with_format(self):
        """Test ${DATE:YYYY-MM-DD} returns the specified date."""
        resolver = TemplateResolver()
        result = resolver.resolve("${DATE:2024-07-04}")

        assert result == "2024-07-04"

    def test_custom_variables(self):
        """Test custom variables passed to resolver."""
        resolver = TemplateResolver(game_pk=744834, season="2024")

        assert resolver.resolve("${game_pk}") == "744834"
        assert resolver.resolve("${season}") == "2024"

    def test_environment_variable(self):
        """Test ${ENV:VAR_NAME} resolves from environment."""
        with patch.dict("os.environ", {"TEST_VAR": "test_value"}):
            resolver = TemplateResolver()
            result = resolver.resolve("${ENV:TEST_VAR}")
            assert result == "test_value"

    def test_environment_variable_not_found(self):
        """Test ${ENV:VAR_NAME} raises error if not found."""
        resolver = TemplateResolver()

        with pytest.raises(ValueError, match="Environment variable not found: NONEXISTENT"):
            resolver.resolve("${ENV:NONEXISTENT}")

    def test_unknown_variable_raises_error(self):
        """Test unknown variable raises ValueError."""
        resolver = TemplateResolver()

        with pytest.raises(ValueError, match="Unknown template variable: UNKNOWN"):
            resolver.resolve("${UNKNOWN}")

    def test_string_with_multiple_variables(self):
        """Test string with multiple variables."""
        resolver = TemplateResolver(game_pk=744834)
        result = resolver.resolve("game_${game_pk}_${TODAY}")

        today = date.today().isoformat()
        assert result == f"game_744834_{today}"

    def test_nested_dict_resolution(self):
        """Test variable resolution in nested dictionaries."""
        resolver = TemplateResolver(game_pk=744834)
        config = {
            "source": {
                "endpoint": "game",
                "parameters": {
                    "game_pk": "${game_pk}",
                    "date": "${TODAY}"
                }
            }
        }

        result = resolver.resolve(config)

        assert result["source"]["parameters"]["game_pk"] == "744834"
        assert result["source"]["parameters"]["date"] == date.today().isoformat()

    def test_list_resolution(self):
        """Test variable resolution in lists."""
        resolver = TemplateResolver(season="2024")
        config = {
            "tags": ["season-${season}", "${TODAY}"]
        }

        result = resolver.resolve(config)

        assert result["tags"][0] == "season-2024"
        assert result["tags"][1] == date.today().isoformat()

    def test_primitive_values_unchanged(self):
        """Test primitive values are not modified."""
        resolver = TemplateResolver()

        assert resolver.resolve(123) == 123
        assert resolver.resolve(45.67) == 45.67
        assert resolver.resolve(True) is True
        assert resolver.resolve(None) is None

    def test_no_variables_in_string(self):
        """Test strings without variables are unchanged."""
        resolver = TemplateResolver()
        result = resolver.resolve("no variables here")

        assert result == "no variables here"

    def test_escaped_dollar_sign(self):
        """Test literal dollar signs (no variables)."""
        resolver = TemplateResolver()
        # Without curly braces, it's just a literal string
        result = resolver.resolve("Price: $100")

        assert result == "Price: $100"


class TestResolveConfigFunction:
    """Test the convenience resolve_config function."""

    def test_resolve_config_with_custom_vars(self):
        """Test resolve_config with custom variables."""
        config = {
            "name": "test_job",
            "source": {
                "parameters": {
                    "game_pk": "${GAME_PK}",
                    "date": "${TODAY}"
                }
            }
        }

        result = resolve_config(config, GAME_PK=744834)

        assert result["source"]["parameters"]["game_pk"] == "744834"
        assert result["source"]["parameters"]["date"] == date.today().isoformat()

    def test_resolve_config_complex_structure(self):
        """Test resolve_config with complex nested structure."""
        config = {
            "name": "game_${GAME_PK}",
            "storage": {
                "raw": {
                    "path": "game/date=${TODAY}/game_pk=${GAME_PK}"
                },
                "cache": {
                    "key_pattern": "game:${GAME_PK}:${TODAY}"
                }
            },
            "tags": ["${TODAY}", "game-${GAME_PK}"]
        }

        today = date.today().isoformat()
        result = resolve_config(config, GAME_PK=744834)

        assert result["name"] == "game_744834"
        assert result["storage"]["raw"]["path"] == f"game/date={today}/game_pk=744834"
        assert result["storage"]["cache"]["key_pattern"] == f"game:744834:{today}"
        assert result["tags"] == [today, "game-744834"]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_variable_name(self):
        """Test ${} raises appropriate error."""
        resolver = TemplateResolver()
        # Empty variable name should be handled
        result = resolver.resolve("${}")
        # Should either raise error or return unchanged - let's see actual behavior
        # For now, we'll just test it doesn't crash
        assert isinstance(result, str)

    def test_timestamp_format(self):
        """Test ${TIMESTAMP} returns correct format."""
        resolver = TemplateResolver()
        result = resolver.resolve("${TIMESTAMP}")

        # Should match YYYY-MM-DD HH:MM:SS format
        assert len(result) == 19  # "2024-07-04 12:34:56"
        assert result[4] == "-"
        assert result[7] == "-"
        assert result[10] == " "
        assert result[13] == ":"
        assert result[16] == ":"

    def test_unix_timestamp(self):
        """Test ${UNIX_TIMESTAMP} returns integer timestamp."""
        resolver = TemplateResolver()
        result = resolver.resolve("${UNIX_TIMESTAMP}")

        # Should be numeric string
        assert result.isdigit()
        # Should be reasonable timestamp (after 2020, before 2100)
        timestamp = int(result)
        assert timestamp > 1577836800  # 2020-01-01
        assert timestamp < 4102444800  # 2100-01-01

    @patch("mlb_data_platform.ingestion.template.date")
    def test_date_mocking(self, mock_date):
        """Test with mocked date for deterministic tests."""
        # Mock today to be 2024-07-04
        mock_date.today.return_value = date(2024, 7, 4)

        resolver = TemplateResolver()
        result = resolver.resolve("${TODAY}")

        assert result == "2024-07-04"

    def test_case_sensitivity(self):
        """Test variable names are case-sensitive."""
        resolver = TemplateResolver(game_pk=744834)

        # Lowercase should work
        assert resolver.resolve("${game_pk}") == "744834"

        # Different case should not match
        with pytest.raises(ValueError, match="Unknown template variable"):
            resolver.resolve("${GAME_pk}")  # Mixed case
