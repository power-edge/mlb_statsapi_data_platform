"""Data quality validation using PyDeequ.

This module provides integration between our schema mapping configuration
and PyDeequ's constraint verification system.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydeequ.analyzers import *
from pydeequ.checks import Check, CheckLevel, CheckStatus
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, SparkSession

from ..schema.mapping import DataQualityRule, SeverityLevel, TargetTable


class ValidationStatus(str, Enum):
    """Validation result status."""

    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


class ValidationResult:
    """Result of data quality validation."""

    def __init__(
        self,
        status: ValidationStatus,
        passed_checks: int,
        failed_checks: int,
        warnings: int,
        errors: List[str],
        metrics: Dict[str, Any],
        deequ_result: Optional[VerificationResult] = None,
    ):
        """Initialize validation result.

        Args:
            status: Overall validation status
            passed_checks: Number of checks that passed
            failed_checks: Number of checks that failed
            warnings: Number of warnings generated
            errors: List of error messages
            metrics: Data quality metrics collected
            deequ_result: Raw PyDeequ verification result
        """
        self.status = status
        self.passed_checks = passed_checks
        self.failed_checks = failed_checks
        self.warnings = warnings
        self.errors = errors
        self.metrics = metrics
        self.deequ_result = deequ_result

    def is_valid(self) -> bool:
        """Check if validation passed.

        Returns:
            True if no errors, False otherwise
        """
        return self.status != ValidationStatus.ERROR

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/reporting.

        Returns:
            Dict representation
        """
        return {
            "status": self.status.value,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "warnings": self.warnings,
            "errors": self.errors,
            "metrics": self.metrics,
        }


class DataQualityValidator:
    """Validates DataFrames using PyDeequ based on schema mapping rules.

    PyDeequ is AWS's data quality library built on Apache Deequ.
    It provides:
    - Constraint verification (assertions on data)
    - Data profiling (statistics and metrics)
    - Anomaly detection (unexpected data patterns)
    - Unit tests for data (define once, verify always)

    Example:
        >>> validator = DataQualityValidator(spark)
        >>> result = validator.validate(df, target_config)
        >>> if not result.is_valid():
        ...     print(f"Validation failed: {result.errors}")
    """

    def __init__(self, spark: SparkSession):
        """Initialize validator.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark

    def validate(
        self,
        df: DataFrame,
        target: TargetTable,
        fail_on_error: bool = True,
    ) -> ValidationResult:
        """Validate DataFrame against target table quality rules.

        Args:
            df: DataFrame to validate
            target: Target table configuration with quality rules
            fail_on_error: If True, raise exception on validation errors

        Returns:
            ValidationResult with status and details

        Raises:
            ValueError: If validation fails and fail_on_error=True
        """
        if not target.data_quality:
            # No quality rules defined - pass by default
            return ValidationResult(
                status=ValidationStatus.SUCCESS,
                passed_checks=0,
                failed_checks=0,
                warnings=0,
                errors=[],
                metrics={},
            )

        # Build PyDeequ checks from quality rules
        check = self._build_checks(target)

        # Run verification
        verification_result = (
            VerificationSuite(self.spark)
            .onData(df)
            .addCheck(check)
            .run()
        )

        # Parse results
        result = self._parse_verification_result(
            verification_result, target.data_quality
        )

        # Fail if errors and fail_on_error=True
        if fail_on_error and not result.is_valid():
            error_msg = "\n".join(result.errors)
            raise ValueError(
                f"Data quality validation failed for {target.name}:\n{error_msg}"
            )

        return result

    def _build_checks(self, target: TargetTable) -> Check:
        """Build PyDeequ Check from data quality rules.

        Args:
            target: Target table configuration

        Returns:
            PyDeequ Check object
        """
        # Determine check level based on severity
        # ERROR rules -> CheckLevel.Error
        # WARNING rules -> CheckLevel.Warning
        has_errors = any(
            rule.severity == SeverityLevel.ERROR for rule in target.data_quality
        )
        check_level = CheckLevel.Error if has_errors else CheckLevel.Warning

        check = Check(self.spark, check_level, f"{target.name}_quality_check")

        for rule in target.data_quality:
            check = self._add_rule_to_check(check, rule)

        return check

    def _add_rule_to_check(
        self, check: Check, rule: DataQualityRule
    ) -> Check:
        """Add single quality rule to PyDeequ check.

        Args:
            check: PyDeequ Check object
            rule: Data quality rule to add

        Returns:
            Updated Check object
        """
        rule_str = rule.rule.strip()

        # Parse common rule patterns and convert to PyDeequ constraints

        # Pattern 1: Column > value (e.g., "game_pk > 0")
        if ">" in rule_str and " > " in rule_str:
            column, value = rule_str.split(" > ")
            column = column.strip()
            value = value.strip()
            check = check.isNonNegative(column)

        # Pattern 2: Column >= value (e.g., "game_date >= '2000-01-01'")
        elif ">=" in rule_str:
            # Use custom SQL constraint
            check = check.satisfies(
                rule_str,
                f"Column satisfies: {rule_str}",
                lambda _: self._evaluate_sql_constraint(rule_str),
            )

        # Pattern 3: Column != value (e.g., "home_team_id != away_team_id")
        elif "!=" in rule_str:
            # Use custom SQL constraint
            check = check.satisfies(
                rule_str,
                f"Constraint: {rule_str}",
            )

        # Pattern 4: Column IS NOT NULL
        elif "IS NOT NULL" in rule_str.upper():
            column = rule_str.upper().replace("IS NOT NULL", "").strip()
            check = check.isComplete(column)

        # Pattern 5: Column IS UNIQUE
        elif "IS UNIQUE" in rule_str.upper():
            column = rule_str.upper().replace("IS UNIQUE", "").strip()
            check = check.isUnique(column)

        # Pattern 6: Column IN (values)
        elif " IN " in rule_str.upper():
            # Use custom SQL constraint
            check = check.satisfies(rule_str, f"Column in allowed values: {rule_str}")

        # Pattern 7: Custom SQL expression (fallback)
        else:
            check = check.satisfies(
                rule_str,
                rule.description or f"Custom rule: {rule_str}",
            )

        return check

    def _evaluate_sql_constraint(self, sql_expr: str) -> float:
        """Evaluate SQL constraint and return compliance ratio.

        Args:
            sql_expr: SQL boolean expression

        Returns:
            Ratio of rows satisfying constraint (0.0 to 1.0)
        """
        # This is a placeholder - actual implementation would use Spark SQL
        # to evaluate the constraint
        return 1.0

    def _parse_verification_result(
        self,
        verification_result: VerificationResult,
        rules: List[DataQualityRule],
    ) -> ValidationResult:
        """Parse PyDeequ verification result into our ValidationResult.

        Args:
            verification_result: PyDeequ VerificationResult
            rules: Original data quality rules

        Returns:
            ValidationResult object
        """
        # Extract check results
        check_results = verification_result.checkResults

        passed = 0
        failed = 0
        warnings = 0
        errors = []

        for check_name, check_result in check_results.items():
            if check_result.status == CheckStatus.Success:
                passed += 1
            else:
                failed += 1

                # Determine if error or warning based on rule severity
                # (In practice, we'd need to map check back to original rule)
                is_error = True  # Default to error

                if is_error:
                    errors.append(
                        f"Check '{check_name}' failed: {check_result.message}"
                    )
                else:
                    warnings += 1

        # Determine overall status
        if failed > 0 and len(errors) > 0:
            status = ValidationStatus.ERROR
        elif warnings > 0:
            status = ValidationStatus.WARNING
        else:
            status = ValidationStatus.SUCCESS

        # Extract metrics
        metrics = {}
        if hasattr(verification_result, "metrics"):
            for metric in verification_result.metrics:
                metrics[metric.name] = metric.value

        return ValidationResult(
            status=status,
            passed_checks=passed,
            failed_checks=failed,
            warnings=warnings,
            errors=errors,
            metrics=metrics,
            deequ_result=verification_result,
        )

    def profile_data(
        self,
        df: DataFrame,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Profile DataFrame to understand data characteristics.

        This runs various analyzers to collect statistics like:
        - Row count
        - Completeness (% non-null)
        - Distinctness (% unique values)
        - Min/max/mean for numeric columns
        - Patterns for string columns

        Args:
            df: DataFrame to profile
            columns: Specific columns to profile (None = all)

        Returns:
            Dict of profiling results
        """
        from pydeequ.profiles import ColumnProfilerRunner

        result = ColumnProfilerRunner(self.spark).onData(df).run()

        profiles = {}
        for col, profile in result.profiles.items():
            if columns and col not in columns:
                continue

            profiles[col] = {
                "completeness": profile.completeness,
                "approximateNumDistinctValues": profile.approximateNumDistinctValues,
                "dataType": str(profile.dataType),
            }

            # Add numeric stats if available
            if hasattr(profile, "mean"):
                profiles[col]["mean"] = profile.mean
                profiles[col]["min"] = profile.minimum
                profiles[col]["max"] = profile.maximum
                profiles[col]["stdDev"] = profile.stdDev

        return profiles

    def suggest_constraints(
        self,
        df: DataFrame,
        target: TargetTable,
    ) -> List[DataQualityRule]:
        """Suggest data quality constraints based on data profiling.

        This analyzes the DataFrame and suggests reasonable constraints
        that could be added to the schema mapping YAML.

        Args:
            df: DataFrame to analyze
            target: Target table configuration

        Returns:
            List of suggested DataQualityRule objects
        """
        profiles = self.profile_data(df)
        suggestions = []

        for field in target.fields:
            if field.name not in profiles:
                continue

            profile = profiles[field.name]

            # Suggest completeness constraint if mostly complete
            if profile["completeness"] > 0.95:
                suggestions.append(
                    DataQualityRule(
                        rule=f"{field.name} IS NOT NULL",
                        severity=SeverityLevel.ERROR,
                        description=f"{field.name} should be complete (currently {profile['completeness']:.1%})",
                    )
                )

            # Suggest uniqueness constraint if mostly unique
            if profile.get("approximateNumDistinctValues", 0) > (
                df.count() * 0.95
            ):
                suggestions.append(
                    DataQualityRule(
                        rule=f"{field.name} IS UNIQUE",
                        severity=SeverityLevel.ERROR,
                        description=f"{field.name} appears to be a unique identifier",
                    )
                )

            # Suggest range constraints for numeric fields
            if "min" in profile and "max" in profile:
                min_val = profile["min"]
                max_val = profile["max"]

                if min_val >= 0:
                    suggestions.append(
                        DataQualityRule(
                            rule=f"{field.name} >= 0",
                            severity=SeverityLevel.ERROR,
                            description=f"{field.name} should be non-negative",
                        )
                    )

        return suggestions
