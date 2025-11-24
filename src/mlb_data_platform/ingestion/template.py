"""Template variable substitution for job configurations."""

import os
import re
from datetime import date, datetime, timedelta
from typing import Any


class TemplateResolver:
    """Resolve template variables in job configurations."""

    def __init__(self, **custom_vars):
        """Initialize template resolver with optional custom variables.

        Args:
            **custom_vars: Custom variables to make available (e.g., game_pk=12345)
        """
        self.custom_vars = custom_vars

    def resolve(self, value: Any) -> Any:
        """Resolve template variables in any value (str, dict, list, etc.).

        Args:
            value: Value to resolve (can be string, dict, list, or primitive)

        Returns:
            Resolved value with variables substituted
        """
        if isinstance(value, str):
            return self._resolve_string(value)
        elif isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.resolve(item) for item in value]
        else:
            return value

    def _resolve_string(self, text: str) -> str:
        """Resolve variables in a string.

        Supports:
        - ${TODAY} - Today's date (YYYY-MM-DD)
        - ${YESTERDAY} - Yesterday's date
        - ${DATE:YYYY-MM-DD} - Specific date format
        - ${YEAR} - Current year
        - ${MONTH} - Current month
        - ${DAY} - Current day
        - ${TIMESTAMP} - Current timestamp (YYYY-MM-DD HH:MM:SS)
        - ${ENV:VAR_NAME} - Environment variable
        - ${GAME_PK} - Custom variable (if provided)
        - etc.

        Args:
            text: String with ${VAR} placeholders

        Returns:
            String with variables substituted
        """
        # Find all ${...} patterns
        pattern = r'\$\{([^}]+)\}'

        def replace_var(match):
            var_name = match.group(1).strip()
            return self._get_variable_value(var_name)

        return re.sub(pattern, replace_var, text)

    def _get_variable_value(self, var_name: str) -> str:
        """Get value for a variable name.

        Args:
            var_name: Variable name (e.g., 'TODAY', 'ENV:DATABASE_URL')

        Returns:
            Variable value as string
        """
        # Date/time variables
        now = datetime.now()
        today = date.today()

        if var_name == "TODAY":
            return today.isoformat()
        elif var_name == "YESTERDAY":
            return (today - timedelta(days=1)).isoformat()
        elif var_name == "YEAR":
            return str(today.year)
        elif var_name == "MONTH":
            return f"{today.month:02d}"
        elif var_name == "DAY":
            return f"{today.day:02d}"
        elif var_name == "TIMESTAMP":
            return now.strftime("%Y-%m-%d %H:%M:%S")
        elif var_name == "UNIX_TIMESTAMP":
            return str(int(now.timestamp()))

        # Date with format: ${DATE:2024-07-15}
        elif var_name.startswith("DATE:"):
            return var_name.split(":", 1)[1]

        # Environment variables: ${ENV:VAR_NAME}
        elif var_name.startswith("ENV:"):
            env_var = var_name.split(":", 1)[1]
            value = os.getenv(env_var)
            if value is None:
                raise ValueError(f"Environment variable not found: {env_var}")
            return value

        # Custom variables (e.g., GAME_PK, SEASON, etc.)
        elif var_name in self.custom_vars:
            return str(self.custom_vars[var_name])

        # Unknown variable
        else:
            raise ValueError(f"Unknown template variable: {var_name}")


def resolve_config(config: dict[str, Any], **custom_vars) -> dict[str, Any]:
    """Convenience function to resolve all variables in a config dict.

    Args:
        config: Configuration dictionary (from YAML)
        **custom_vars: Custom variables to substitute

    Returns:
        Resolved configuration with all variables substituted

    Example:
        >>> config = {"parameters": {"date": "${TODAY}", "game_pk": "${GAME_PK}"}}
        >>> resolve_config(config, game_pk=123456)
        {"parameters": {"date": "2024-07-15", "game_pk": "123456"}}
    """
    resolver = TemplateResolver(**custom_vars)
    return resolver.resolve(config)
