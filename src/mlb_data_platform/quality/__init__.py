"""Data quality validation using PyDeequ.

PyDeequ is AWS's data quality library for PySpark, built on top of Apache Deequ.
It provides unit tests for data, allowing you to define constraints and
automatically verify them on large datasets.

Why PyDeequ over Great Expectations:
- Native PySpark integration (better performance)
- Designed for big data (scales to billions of rows)
- Apache 2.0 license (permissive)
- AWS-maintained (reliable)
- Works with our Kappa architecture

This module provides two validators:
- DeequValidator: Standalone validator with predefined MLB data rules (requires SPARK_VERSION env)
- DataQualityValidator: Schema-driven validator integrated with mapping config

Note: Both validators require SPARK_VERSION environment variable to be set.
Set SPARK_VERSION=3.5 before importing these validators.
"""

# PyDeequ requires SPARK_VERSION environment variable - lazy import all validators
try:
    from .validator import DataQualityValidator, ValidationResult
    from .deequ_validator import DeequValidationResult, DeequValidator, ValidationStatus
except RuntimeError:
    # PyDeequ not configured - will be available when SPARK_VERSION is set
    DataQualityValidator = None  # type: ignore
    ValidationResult = None  # type: ignore
    DeequValidator = None  # type: ignore
    DeequValidationResult = None  # type: ignore
    ValidationStatus = None  # type: ignore

__all__ = [
    "DeequValidator",
    "DeequValidationResult",
    "ValidationStatus",
    "DataQualityValidator",
    "ValidationResult",
]
