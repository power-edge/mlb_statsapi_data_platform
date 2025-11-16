"""Data profiling tool to analyze JSON responses and infer optimal schema types."""

import re
from collections import Counter, defaultdict
from typing import Any, Optional

from pydantic import BaseModel


class FieldProfile(BaseModel):
    """Statistical profile of a field from sample data."""

    field_name: str
    json_path: str

    # Sample statistics
    sample_count: int = 0
    null_count: int = 0
    unique_count: int = 0

    # Type detection
    detected_types: dict[str, int] = {}  # type_name -> count
    recommended_type: str = "text"

    # For numeric types
    is_always_integer: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    # For string types
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    common_patterns: list[str] = []  # Regex patterns that match

    # For potential categorical fields
    distinct_values: list[Any] = []  # Store if < 50 unique values
    is_likely_categorical: bool = False
    is_likely_boolean: bool = False

    # For potential ID fields
    is_likely_primary_key: bool = False
    is_likely_foreign_key: bool = False

    # Special types
    is_likely_date: bool = False
    is_likely_timestamp: bool = False
    is_likely_zipcode: bool = False
    is_likely_phone: bool = False
    is_likely_coordinate: bool = False  # lat/lon


class DataProfiler:
    """Analyzes JSON data to recommend optimal PostgreSQL and Avro types.

    This addresses the problem of strings that should be integers, zip codes,
    phone numbers, coordinates, etc.
    """

    def __init__(self):
        self.field_profiles: dict[str, FieldProfile] = {}

    def profile_json_data(self, data: dict[str, Any], json_path_prefix: str = "$") -> None:
        """Recursively profile JSON data structure.

        Args:
            data: JSON data to profile
            json_path_prefix: Current JSONPath prefix
        """
        if not isinstance(data, dict):
            return

        for key, value in data.items():
            current_path = f"{json_path_prefix}.{key}"

            # Initialize or get existing profile
            if current_path not in self.field_profiles:
                self.field_profiles[current_path] = FieldProfile(
                    field_name=key, json_path=current_path
                )

            profile = self.field_profiles[current_path]
            profile.sample_count += 1

            # Analyze value
            self._analyze_value(profile, value)

            # Recursively profile nested objects
            if isinstance(value, dict):
                self.profile_json_data(value, current_path)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Profile first item in array
                self.profile_json_data(value[0], f"{current_path}[0]")

    def _analyze_value(self, profile: FieldProfile, value: Any) -> None:
        """Analyze a single value and update profile statistics."""
        if value is None:
            profile.null_count += 1
            return

        # Detect Python type
        python_type = type(value).__name__
        profile.detected_types[python_type] = profile.detected_types.get(python_type, 0) + 1

        # String analysis
        if isinstance(value, str):
            self._analyze_string(profile, value)

        # Numeric analysis
        elif isinstance(value, (int, float)):
            self._analyze_numeric(profile, value)

        # Boolean
        elif isinstance(value, bool):
            profile.is_likely_boolean = True

        # List/Array
        elif isinstance(value, list):
            profile.recommended_type = "jsonb"  # Store as JSONB array

        # Dict/Object
        elif isinstance(value, dict):
            profile.recommended_type = "jsonb"  # Store as JSONB object

    def _analyze_string(self, profile: FieldProfile, value: str) -> None:
        """Analyze string value for patterns and type hints."""
        # Track length
        length = len(value)
        if profile.min_length is None or length < profile.min_length:
            profile.min_length = length
        if profile.max_length is None or length > profile.max_length:
            profile.max_length = length

        # Check if string is actually a number
        if self._is_integer_string(value):
            profile.is_always_integer = True
            num_value = int(value)
            if profile.min_value is None or num_value < profile.min_value:
                profile.min_value = num_value
            if profile.max_value is None or num_value > profile.max_value:
                profile.max_value = num_value

        # Check for special patterns
        patterns = []

        # Date patterns
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            profile.is_likely_date = True
            patterns.append("DATE_ISO")

        # Timestamp patterns
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value):
            profile.is_likely_timestamp = True
            patterns.append("TIMESTAMP_ISO")

        # Zip code patterns (US)
        if re.match(r"^\d{5}(-\d{4})?$", value):
            profile.is_likely_zipcode = True
            patterns.append("ZIPCODE_US")

        # Phone patterns
        if re.match(r"^\d{3}-\d{3}-\d{4}$", value) or re.match(r"^\(\d{3}\)\s?\d{3}-\d{4}$", value):
            profile.is_likely_phone = True
            patterns.append("PHONE_US")

        # Coordinate patterns (latitude/longitude)
        try:
            float_val = float(value)
            if -90 <= float_val <= 90:
                profile.is_likely_coordinate = True
                patterns.append("COORDINATE")
        except ValueError:
            pass

        # ID patterns (ends with Id, id, PK, pk, _id)
        if re.search(r"(Id|id|PK|pk|_id)$", profile.field_name):
            if value.isdigit():
                profile.is_likely_foreign_key = True
                patterns.append("ID_NUMERIC")

        profile.common_patterns.extend(patterns)

    def _analyze_numeric(self, profile: FieldProfile, value: float | int) -> None:
        """Analyze numeric value."""
        if profile.min_value is None or value < profile.min_value:
            profile.min_value = value
        if profile.max_value is None or value > profile.max_value:
            profile.max_value = value

        # Check if always integer
        if isinstance(value, int) or value == int(value):
            profile.is_always_integer = True

        # Check if likely an ID
        if "_id" in profile.field_name.lower() or profile.field_name.lower().endswith("id"):
            profile.is_likely_foreign_key = True

    @staticmethod
    def _is_integer_string(value: str) -> bool:
        """Check if string represents an integer."""
        try:
            int(value)
            return True
        except ValueError:
            return False

    def finalize_profiles(self) -> None:
        """Finalize all profiles and make type recommendations."""
        for profile in self.field_profiles.values():
            if profile.sample_count == 0:
                continue

            # Calculate null percentage
            null_pct = profile.null_count / profile.sample_count

            # Determine recommended type
            profile.recommended_type = self._recommend_type(profile)

            # Determine if likely categorical (< 50 unique values and < 50% of samples)
            if profile.unique_count < 50 and profile.unique_count < profile.sample_count * 0.5:
                profile.is_likely_categorical = True

            # Determine if likely primary key (always unique, never null)
            if profile.null_count == 0 and profile.unique_count == profile.sample_count:
                if "id" in profile.field_name.lower() or "pk" in profile.field_name.lower():
                    profile.is_likely_primary_key = True

    def _recommend_type(self, profile: FieldProfile) -> str:
        """Recommend optimal PostgreSQL type based on profile.

        Returns:
            PostgreSQL type string (e.g., 'bigint', 'varchar(100)', 'numeric(10,2)')
        """
        # Special pattern types
        if profile.is_likely_timestamp:
            return "timestamptz"
        if profile.is_likely_date:
            return "date"
        if profile.is_likely_boolean:
            return "boolean"
        if profile.is_likely_coordinate:
            return "numeric(10, 7)"  # 7 decimal places for coordinates

        # Numeric types
        if profile.is_always_integer:
            if profile.max_value is not None:
                if profile.max_value <= 32767:
                    return "smallint"
                elif profile.max_value <= 2147483647:
                    return "int"
                else:
                    return "bigint"
            return "bigint"  # Default for unknown range

        # Check most common detected type
        if profile.detected_types:
            most_common = max(profile.detected_types.items(), key=lambda x: x[1])[0]

            if most_common == "int":
                return "bigint"
            elif most_common == "float":
                return "numeric"
            elif most_common == "bool":
                return "boolean"
            elif most_common == "dict" or most_common == "list":
                return "jsonb"

        # String types - use varchar with appropriate length
        if profile.max_length is not None:
            if profile.is_likely_zipcode:
                return "varchar(10)"  # zip+4
            elif profile.is_likely_phone:
                return "varchar(20)"
            elif profile.max_length <= 10:
                return "varchar(10)"
            elif profile.max_length <= 50:
                return "varchar(50)"
            elif profile.max_length <= 100:
                return "varchar(100)"
            elif profile.max_length <= 255:
                return "varchar(255)"
            else:
                return "text"

        return "text"  # Default fallback

    def get_profile(self, json_path: str) -> Optional[FieldProfile]:
        """Get profile for a specific JSON path."""
        return self.field_profiles.get(json_path)

    def get_all_profiles(self) -> list[FieldProfile]:
        """Get all field profiles."""
        return list(self.field_profiles.values())

    def generate_schema_recommendations(self) -> dict[str, Any]:
        """Generate schema recommendations based on profiling.

        Returns:
            Dict with recommended schema structure
        """
        self.finalize_profiles()

        recommendations = {
            "fields": [],
            "primary_key_candidates": [],
            "foreign_key_candidates": [],
            "index_candidates": [],
            "categorical_fields": [],
            "coordinate_fields": [],
        }

        for profile in self.field_profiles.values():
            field_rec = {
                "name": profile.field_name,
                "json_path": profile.json_path,
                "recommended_type": profile.recommended_type,
                "nullable": profile.null_count > 0,
                "sample_count": profile.sample_count,
            }

            recommendations["fields"].append(field_rec)

            # Categorize special fields
            if profile.is_likely_primary_key:
                recommendations["primary_key_candidates"].append(profile.field_name)

            if profile.is_likely_foreign_key:
                recommendations["foreign_key_candidates"].append({
                    "field": profile.field_name,
                    "json_path": profile.json_path,
                })

            if profile.is_likely_categorical:
                recommendations["categorical_fields"].append({
                    "field": profile.field_name,
                    "unique_values": profile.unique_count,
                })

            if profile.is_likely_coordinate:
                recommendations["coordinate_fields"].append(profile.field_name)

            # Recommend indexes for FK, dates, and frequently queried fields
            if (
                profile.is_likely_foreign_key
                or profile.is_likely_date
                or profile.is_likely_timestamp
            ):
                recommendations["index_candidates"].append(profile.field_name)

        return recommendations

    def print_summary(self) -> None:
        """Print human-readable summary of profiling results."""
        self.finalize_profiles()

        print("=" * 80)
        print("DATA PROFILING SUMMARY")
        print("=" * 80)
        print(f"Total fields profiled: {len(self.field_profiles)}")
        print()

        # Group by recommended type
        type_groups = defaultdict(list)
        for profile in self.field_profiles.values():
            type_groups[profile.recommended_type].append(profile)

        print("Fields by recommended type:")
        for pg_type, profiles in sorted(type_groups.items()):
            print(f"  {pg_type}: {len(profiles)} fields")

        print()
        print("Special field categories:")
        print(f"  Primary key candidates: {sum(1 for p in self.field_profiles.values() if p.is_likely_primary_key)}")
        print(f"  Foreign key candidates: {sum(1 for p in self.field_profiles.values() if p.is_likely_foreign_key)}")
        print(f"  Categorical fields: {sum(1 for p in self.field_profiles.values() if p.is_likely_categorical)}")
        print(f"  Coordinate fields: {sum(1 for p in self.field_profiles.values() if p.is_likely_coordinate)}")
        print(f"  Date/timestamp fields: {sum(1 for p in self.field_profiles.values() if p.is_likely_date or p.is_likely_timestamp)}")


def profile_stub_data(stub_files: list[str]) -> DataProfiler:
    """Profile multiple stub files and return combined profiler.

    Args:
        stub_files: List of paths to gzipped JSON stub files

    Returns:
        DataProfiler with combined statistics
    """
    import gzip
    import json

    profiler = DataProfiler()

    for stub_file in stub_files:
        try:
            with gzip.open(stub_file, "rt") as f:
                stub_data = json.load(f)

            # Extract response data
            if "response" in stub_data:
                profiler.profile_json_data(stub_data["response"])
            else:
                profiler.profile_json_data(stub_data)

        except Exception as e:
            print(f"Warning: Could not profile {stub_file}: {e}")

    return profiler
