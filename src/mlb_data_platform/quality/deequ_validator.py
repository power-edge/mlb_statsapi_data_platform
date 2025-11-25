"""Standalone PyDeequ data quality validator with predefined MLB data rules.

This module provides a simpler, standalone validator for common MLB data quality
checks without requiring schema mapping configuration. It's useful for:
- Quick validation during development
- Ad-hoc data quality checks
- Testing and debugging transformations
- Validating data before loading to database

For production use with full schema integration, use validator.py instead.

Example:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.quality.deequ_validator import DeequValidator
    >>>
    >>> spark = SparkSession.builder.appName("validation").getOrCreate()
    >>> validator = DeequValidator(spark)
    >>>
    >>> # Validate game DataFrame
    >>> game_df = spark.read.table("game.live_game_v1")
    >>> result = validator.validate_game_data(game_df)
    >>>
    >>> if result.is_success():
    ...     print("Validation passed!")
    ... else:
    ...     print(f"Validation failed: {result.get_errors()}")
    ...     print(f"Metrics: {result.get_metrics()}")
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydeequ.checks import Check, CheckLevel, CheckStatus
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class ValidationStatus(str, Enum):
    """Validation result status."""

    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DeequValidationResult:
    """Result of PyDeequ validation with convenience methods.

    Attributes:
        status: Overall validation status (SUCCESS, WARNING, or ERROR)
        passed_count: Number of checks that passed
        failed_count: Number of checks that failed
        warning_count: Number of warnings generated
        error_messages: List of error messages from failed checks
        warning_messages: List of warning messages
        metrics: Data quality metrics collected during validation
        raw_result: Original PyDeequ VerificationResult object
    """

    status: ValidationStatus
    passed_count: int
    failed_count: int
    warning_count: int
    error_messages: list[str] = field(default_factory=list)
    warning_messages: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    raw_result: VerificationResult | None = None

    def is_success(self) -> bool:
        """Check if validation succeeded without errors.

        Returns:
            True if status is SUCCESS or WARNING, False if ERROR
        """
        return self.status != ValidationStatus.ERROR

    def has_warnings(self) -> bool:
        """Check if validation has warnings.

        Returns:
            True if there are warnings
        """
        return self.warning_count > 0

    def get_errors(self) -> list[str]:
        """Get list of error messages.

        Returns:
            List of error messages
        """
        return self.error_messages

    def get_warnings(self) -> list[str]:
        """Get list of warning messages.

        Returns:
            List of warning messages
        """
        return self.warning_messages

    def get_metrics(self) -> dict[str, Any]:
        """Get data quality metrics.

        Returns:
            Dictionary of metric name to value
        """
        return self.metrics

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for logging/reporting.

        Returns:
            Dictionary representation
        """
        return {
            "status": self.status.value,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "warning_count": self.warning_count,
            "errors": self.error_messages,
            "warnings": self.warning_messages,
            "metrics": self.metrics,
        }

    def __str__(self) -> str:
        """String representation of validation result.

        Returns:
            Human-readable summary
        """
        return (
            f"ValidationResult(status={self.status.value}, "
            f"passed={self.passed_count}, failed={self.failed_count}, "
            f"warnings={self.warning_count})"
        )


class DeequValidator:
    """Standalone PyDeequ data quality validator with predefined MLB data rules.

    This validator provides predefined quality checks for common MLB data tables:
    - Game data (live_game_v1)
    - Schedule data
    - Season data
    - Player data

    You can also define custom validation rules using the validate() method.

    Example:
        >>> validator = DeequValidator(spark)
        >>>
        >>> # Use predefined validation for game data
        >>> game_result = validator.validate_game_data(game_df)
        >>>
        >>> # Use custom validation rules
        >>> custom_result = validator.validate(
        ...     df=my_df,
        ...     check_builder=lambda check: check
        ...         .hasSize(lambda sz: sz > 0)
        ...         .isComplete("required_column")
        ...         .isUnique("id_column")
        ... )
    """

    def __init__(self, spark: SparkSession):
        """Initialize validator with SparkSession.

        Args:
            spark: Active SparkSession instance
        """
        self.spark = spark

    def validate(
        self,
        df: DataFrame,
        check_builder: Callable[[Check], Check],
        check_name: str = "custom_check",
        check_level: CheckLevel = CheckLevel.Error,
        log_results: bool = True,
    ) -> DeequValidationResult:
        """Validate DataFrame using custom PyDeequ checks.

        Args:
            df: DataFrame to validate
            check_builder: Function that takes a Check and adds constraints to it
            check_name: Name for the check (for logging)
            check_level: CheckLevel.Error or CheckLevel.Warning
            log_results: Whether to log validation results

        Returns:
            DeequValidationResult with validation details

        Example:
            >>> result = validator.validate(
            ...     df=my_df,
            ...     check_builder=lambda check: check
            ...         .hasSize(lambda sz: sz > 0)
            ...         .isComplete("id")
            ...         .isUnique("id")
            ...         .isNonNegative("amount"),
            ...     check_name="my_validation"
            ... )
        """
        # Create check using builder function
        check = Check(self.spark, check_level, check_name)
        check = check_builder(check)

        # Run verification
        verification_result = VerificationSuite(self.spark).onData(df).addCheck(check).run()

        # Parse results
        result = self._parse_verification_result(verification_result)

        # Log results if requested
        if log_results:
            self._log_validation_result(check_name, result)

        return result

    def validate_game_data(
        self,
        df: DataFrame,
        check_level: CheckLevel = CheckLevel.Error,
        log_results: bool = True,
    ) -> DeequValidationResult:
        """Validate game data with predefined quality rules.

        Quality rules checked:
        - game_pk must be positive (> 0)
        - game_date must not be null
        - home_team_id must be positive
        - away_team_id must be positive
        - game_state must be in ['Final', 'Live', 'Preview', 'Scheduled']
        - captured_at must not be null
        - home_team_id must be different from away_team_id

        Args:
            df: Game DataFrame to validate
            check_level: CheckLevel.Error or CheckLevel.Warning
            log_results: Whether to log validation results

        Returns:
            DeequValidationResult
        """

        def build_game_checks(check: Check) -> Check:
            return (
                check.hasSize(lambda sz: sz > 0, "DataFrame should not be empty")
                .isComplete("game_pk", "game_pk should not be null")
                .isNonNegative("game_pk", "game_pk should be non-negative")
                .satisfies("game_pk > 0", "game_pk should be positive", lambda x: x == 1.0)
                .isComplete("game_date", "game_date should not be null")
                .isComplete("home_team_id", "home_team_id should not be null")
                .isNonNegative("home_team_id", "home_team_id should be non-negative")
                .isComplete("away_team_id", "away_team_id should not be null")
                .isNonNegative("away_team_id", "away_team_id should be non-negative")
                .satisfies(
                    "home_team_id != away_team_id",
                    "home_team_id must be different from away_team_id",
                )
                .isContainedIn(
                    "game_state",
                    ["Final", "Live", "Preview", "Scheduled"],
                    "game_state should be a valid state",
                )
                .isComplete("captured_at", "captured_at should not be null")
            )

        return self.validate(
            df=df,
            check_builder=build_game_checks,
            check_name="game_data_validation",
            check_level=check_level,
            log_results=log_results,
        )

    def validate_schedule_data(
        self,
        df: DataFrame,
        check_level: CheckLevel = CheckLevel.Error,
        log_results: bool = True,
    ) -> DeequValidationResult:
        """Validate schedule data with predefined quality rules.

        Quality rules checked:
        - schedule_date must not be null
        - sport_id must be positive
        - total_games must be >= 0
        - captured_at must not be null

        Args:
            df: Schedule DataFrame to validate
            check_level: CheckLevel.Error or CheckLevel.Warning
            log_results: Whether to log validation results

        Returns:
            DeequValidationResult
        """

        def build_schedule_checks(check: Check) -> Check:
            return (
                check.hasSize(lambda sz: sz > 0, "DataFrame should not be empty")
                .isComplete("schedule_date", "schedule_date should not be null")
                .isComplete("sport_id", "sport_id should not be null")
                .isNonNegative("sport_id", "sport_id should be non-negative")
                .satisfies("sport_id > 0", "sport_id should be positive", lambda x: x == 1.0)
                .isComplete("total_games", "total_games should not be null")
                .isNonNegative("total_games", "total_games should be non-negative")
                .isComplete("captured_at", "captured_at should not be null")
            )

        return self.validate(
            df=df,
            check_builder=build_schedule_checks,
            check_name="schedule_data_validation",
            check_level=check_level,
            log_results=log_results,
        )

    def validate_season_data(
        self,
        df: DataFrame,
        check_level: CheckLevel = CheckLevel.Error,
        log_results: bool = True,
    ) -> DeequValidationResult:
        """Validate season data with predefined quality rules.

        Quality rules checked:
        - sport_id must be positive
        - season_id must not be null
        - captured_at must not be null

        Args:
            df: Season DataFrame to validate
            check_level: CheckLevel.Error or CheckLevel.Warning
            log_results: Whether to log validation results

        Returns:
            DeequValidationResult
        """

        def build_season_checks(check: Check) -> Check:
            return (
                check.hasSize(lambda sz: sz > 0, "DataFrame should not be empty")
                .isComplete("sport_id", "sport_id should not be null")
                .isNonNegative("sport_id", "sport_id should be non-negative")
                .satisfies("sport_id > 0", "sport_id should be positive", lambda x: x == 1.0)
                .isComplete("season_id", "season_id should not be null")
                .isComplete("captured_at", "captured_at should not be null")
            )

        return self.validate(
            df=df,
            check_builder=build_season_checks,
            check_name="season_data_validation",
            check_level=check_level,
            log_results=log_results,
        )

    def validate_player_data(
        self,
        df: DataFrame,
        check_level: CheckLevel = CheckLevel.Error,
        log_results: bool = True,
    ) -> DeequValidationResult:
        """Validate player data with predefined quality rules.

        Quality rules checked:
        - player_id must be positive
        - full_name must not be null
        - captured_at must not be null

        Args:
            df: Player DataFrame to validate
            check_level: CheckLevel.Error or CheckLevel.Warning
            log_results: Whether to log validation results

        Returns:
            DeequValidationResult
        """

        def build_player_checks(check: Check) -> Check:
            return (
                check.hasSize(lambda sz: sz > 0, "DataFrame should not be empty")
                .isComplete("player_id", "player_id should not be null")
                .isNonNegative("player_id", "player_id should be non-negative")
                .satisfies("player_id > 0", "player_id should be positive", lambda x: x == 1.0)
                .isComplete("full_name", "full_name should not be null")
                .isComplete("captured_at", "captured_at should not be null")
            )

        return self.validate(
            df=df,
            check_builder=build_player_checks,
            check_name="player_data_validation",
            check_level=check_level,
            log_results=log_results,
        )

    def _parse_verification_result(
        self, verification_result: VerificationResult
    ) -> DeequValidationResult:
        """Parse PyDeequ VerificationResult into our result object.

        Args:
            verification_result: PyDeequ VerificationResult

        Returns:
            DeequValidationResult
        """
        check_results = verification_result.checkResults

        passed = 0
        failed = 0
        warnings = 0
        errors = []
        warning_msgs = []

        for check_name, check_result in check_results.items():
            if check_result.status == CheckStatus.Success:
                passed += 1
            elif check_result.status == CheckStatus.Warning:
                warnings += 1
                warning_msgs.append(f"{check_name}: {check_result.message}")
            else:
                failed += 1
                errors.append(f"{check_name}: {check_result.message}")

        # Determine overall status
        if failed > 0:
            status = ValidationStatus.ERROR
        elif warnings > 0:
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.SUCCESS

        # Extract metrics if available
        metrics = {}
        if hasattr(verification_result, "successMetricsAsDataFrame"):
            try:
                metrics_df = verification_result.successMetricsAsDataFrame(self.spark)
                # Convert to dict for easier handling
                metrics_rows = metrics_df.collect()
                for row in metrics_rows:
                    metric_dict = row.asDict()
                    metric_name = metric_dict.get("name", "unknown")
                    metric_value = metric_dict.get("value", None)
                    if metric_name and metric_value is not None:
                        metrics[metric_name] = metric_value
            except Exception as e:
                logger.warning(f"Failed to extract metrics: {e}")

        return DeequValidationResult(
            status=status,
            passed_count=passed,
            failed_count=failed,
            warning_count=warnings,
            error_messages=errors,
            warning_messages=warning_msgs,
            metrics=metrics,
            raw_result=verification_result,
        )

    def _log_validation_result(self, check_name: str, result: DeequValidationResult) -> None:
        """Log validation results.

        Args:
            check_name: Name of the check that was run
            result: Validation result to log
        """
        if result.is_success():
            if result.has_warnings():
                logger.warning(
                    f"{check_name} completed with warnings: "
                    f"{result.passed_count} passed, {result.warning_count} warnings"
                )
                for warning in result.get_warnings():
                    logger.warning(f"  - {warning}")
            else:
                logger.info(f"{check_name} passed: {result.passed_count} checks succeeded")
        else:
            logger.error(
                f"{check_name} failed: {result.passed_count} passed, {result.failed_count} failed"
            )
            for error in result.get_errors():
                logger.error(f"  - {error}")


# Example usage
if __name__ == "__main__":
    """Example demonstrating DeequValidator usage."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # Create SparkSession
    spark = (
        SparkSession.builder.appName("deequ_validator_example")
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.5")
        .getOrCreate()
    )

    # Create sample game data
    game_schema = StructType(
        [
            StructField("game_pk", IntegerType(), False),
            StructField("game_date", StringType(), False),
            StructField("home_team_id", IntegerType(), False),
            StructField("away_team_id", IntegerType(), False),
            StructField("game_state", StringType(), False),
            StructField("captured_at", TimestampType(), False),
        ]
    )

    game_data = [
        (12345, "2024-07-15", 111, 112, "Final", "2024-07-15 22:00:00"),
        (12346, "2024-07-15", 113, 114, "Live", "2024-07-15 20:30:00"),
        (12347, "2024-07-16", 115, 116, "Scheduled", "2024-07-16 10:00:00"),
    ]

    game_df = spark.createDataFrame(game_data, game_schema)

    # Initialize validator
    validator = DeequValidator(spark)

    # Validate game data
    print("Validating game data...")
    game_result = validator.validate_game_data(game_df)

    print(f"\nValidation Result: {game_result}")
    print(f"Status: {game_result.status.value}")
    print(f"Passed: {game_result.passed_count}")
    print(f"Failed: {game_result.failed_count}")
    print(f"Warnings: {game_result.warning_count}")

    if not game_result.is_success():
        print("\nErrors:")
        for error in game_result.get_errors():
            print(f"  - {error}")

    if game_result.has_warnings():
        print("\nWarnings:")
        for warning in game_result.get_warnings():
            print(f"  - {warning}")

    print("\nMetrics:")
    for metric_name, metric_value in game_result.get_metrics().items():
        print(f"  {metric_name}: {metric_value}")

    # Example of custom validation
    print("\n" + "=" * 80)
    print("Custom validation example:")
    print("=" * 80)

    custom_result = validator.validate(
        df=game_df,
        check_builder=lambda check: check.hasSize(lambda sz: sz >= 3)
        .isComplete("game_pk")
        .isUnique("game_pk"),
        check_name="custom_game_validation",
    )

    print(f"\nCustom Validation Result: {custom_result}")

    spark.stop()
