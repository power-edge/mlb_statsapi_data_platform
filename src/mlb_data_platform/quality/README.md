# Data Quality Validation Module

This module provides PyDeequ-based data quality validation for MLB data platform.

## Overview

PyDeequ is AWS's data quality library built on Apache Deequ. It provides:
- Constraint verification (assertions on data)
- Data profiling (statistics and metrics)
- Anomaly detection (unexpected data patterns)
- Unit tests for data (define once, verify always)

## Components

### 1. DeequValidator (Standalone)

**File**: `deequ_validator.py`

A standalone validator with predefined quality rules for common MLB data tables. Use this for:
- Quick validation during development
- Ad-hoc data quality checks
- Testing and debugging transformations
- Validating data before loading to database

**Supported Data Types**:
- Game data (`validate_game_data`)
- Schedule data (`validate_schedule_data`)
- Season data (`validate_season_data`)
- Player data (`validate_player_data`)
- Custom validation (`validate`)

**Example Usage**:
```python
from pyspark.sql import SparkSession
from mlb_data_platform.quality import DeequValidator

# Initialize
spark = SparkSession.builder.appName("validation").getOrCreate()
validator = DeequValidator(spark)

# Validate game data
game_df = spark.read.table("game.live_game_v1")
result = validator.validate_game_data(game_df)

if result.is_success():
    print("✅ Validation passed!")
else:
    print(f"❌ Validation failed: {result.get_errors()}")
```

### 2. DataQualityValidator (Schema-Driven)

**File**: `validator.py`

Schema-driven validator integrated with YAML mapping configuration. Use this for:
- Production validation pipelines
- Configuration-driven quality rules
- Integration with schema mapping system
- Automated constraint suggestion

**Example Usage**:
```python
from mlb_data_platform.quality import DataQualityValidator
from mlb_data_platform.schema.mapping import TargetTable

# Initialize
validator = DataQualityValidator(spark)

# Validate against schema mapping config
result = validator.validate(df, target_config, fail_on_error=True)

# Profile data to understand characteristics
profiles = validator.profile_data(df)

# Get constraint suggestions
suggestions = validator.suggest_constraints(df, target_config)
```

## Predefined Quality Rules

### Game Data
- `game_pk` must be positive (> 0)
- `game_date` must not be null
- `home_team_id` must be positive
- `away_team_id` must be positive
- `game_state` must be in ['Final', 'Live', 'Preview', 'Scheduled']
- `captured_at` must not be null
- `home_team_id` must be different from `away_team_id`

### Schedule Data
- `schedule_date` must not be null
- `sport_id` must be positive
- `total_games` must be >= 0
- `captured_at` must not be null

### Season Data
- `sport_id` must be positive
- `season_id` must not be null
- `captured_at` must not be null

### Player Data
- `player_id` must be positive
- `full_name` must not be null
- `captured_at` must not be null

## Custom Validation

You can define custom validation rules using the `validate()` method:

```python
from pydeequ.checks import CheckLevel

# Custom validation with builder pattern
result = validator.validate(
    df=my_df,
    check_builder=lambda check: check
        .hasSize(lambda sz: sz > 0)
        .isComplete("required_column")
        .isUnique("id_column")
        .isNonNegative("amount")
        .isContainedIn("status", ["active", "inactive"])
        .satisfies("amount > 0", "Amount must be positive"),
    check_name="my_custom_validation",
    check_level=CheckLevel.Error
)
```

## Validation Result

Both validators return a result object with:

**Methods**:
- `is_success()` - Returns True if validation passed
- `has_warnings()` - Returns True if there are warnings
- `get_errors()` - Returns list of error messages
- `get_warnings()` - Returns list of warning messages
- `get_metrics()` - Returns data quality metrics
- `to_dict()` - Returns dict representation for logging

**Attributes**:
- `status` - ValidationStatus (SUCCESS, WARNING, or ERROR)
- `passed_count` - Number of checks that passed
- `failed_count` - Number of checks that failed
- `warning_count` - Number of warnings

**Example**:
```python
result = validator.validate_game_data(df)

print(f"Status: {result.status.value}")
print(f"Passed: {result.passed_count}")
print(f"Failed: {result.failed_count}")

if not result.is_success():
    for error in result.get_errors():
        print(f"  ❌ {error}")
```

## PyDeequ Check Types

PyDeequ provides many built-in checks:

### Completeness
```python
.isComplete("column")  # No nulls
```

### Uniqueness
```python
.isUnique("column")  # All values unique
.hasUniqueness("column", lambda x: x > 0.95)  # 95%+ unique
```

### Numeric Constraints
```python
.isNonNegative("column")  # >= 0
.isPositive("column")  # > 0
.hasMin("column", lambda x: x >= 0)
.hasMax("column", lambda x: x <= 100)
```

### Categorical Constraints
```python
.isContainedIn("column", ["A", "B", "C"])  # In allowed set
```

### Custom Constraints
```python
.satisfies("column_a > column_b", "A must be greater than B")
```

### Size Constraints
```python
.hasSize(lambda sz: sz > 0)  # Non-empty
.hasSize(lambda sz: sz >= 100)  # At least 100 rows
```

## Integration with Transform Jobs

Use validators in PySpark transform jobs:

```python
from mlb_data_platform.quality import DeequValidator

class GameTransform:
    def __init__(self, spark):
        self.spark = spark
        self.validator = DeequValidator(spark)

    def transform(self, raw_df):
        # Validate input
        input_result = self.validator.validate_game_data(raw_df)
        if not input_result.is_success():
            raise ValueError(f"Input validation failed: {input_result.get_errors()}")

        # Transform
        transformed_df = self._apply_transformations(raw_df)

        # Validate output
        output_result = self.validator.validate_game_data(transformed_df)
        if not output_result.is_success():
            raise ValueError(f"Output validation failed: {output_result.get_errors()}")

        return transformed_df
```

## Examples

See `/examples/deequ_validation_example.py` for a complete working example.

Run it with:
```bash
uv run python examples/deequ_validation_example.py
```

## Requirements

- PySpark >= 3.5.0
- pydeequ >= 1.2.0 (already in dependencies)

PyDeequ requires the Deequ JAR file. When creating SparkSession, include:
```python
spark = SparkSession.builder \
    .appName("validation") \
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.5") \
    .getOrCreate()
```

## References

- [PyDeequ GitHub](https://github.com/awslabs/python-deequ)
- [PyDeequ Documentation](https://pydeequ.readthedocs.io/)
- [Apache Deequ](https://github.com/awslabs/deequ)
- [AWS Big Data Blog on Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
