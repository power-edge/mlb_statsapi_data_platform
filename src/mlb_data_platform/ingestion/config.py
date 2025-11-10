"""Job configuration models using Pydantic."""

from enum import Enum
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class JobType(str, Enum):
    """Job execution types."""

    BATCH = "batch"
    SCHEDULED = "scheduled"
    STREAMING = "streaming"


class StubMode(str, Enum):
    """Stub mode for testing with pymlb_statsapi."""

    CAPTURE = "capture"  # Make real API calls and save responses
    REPLAY = "replay"  # Use saved responses (no API calls)
    PASSTHROUGH = "passthrough"  # Make real API calls (default)


class BackoffStrategy(str, Enum):
    """Retry backoff strategies."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


class RetryConfig(BaseModel):
    """Retry configuration for API calls."""

    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay: int = Field(default=1, ge=0)  # seconds
    max_delay: int = Field(default=60, ge=1)  # seconds


class SourceConfig(BaseModel):
    """API source configuration."""

    endpoint: str = Field(..., description="pymlb_statsapi endpoint name (e.g., 'game', 'schedule')")
    method: str = Field(..., description="Endpoint method name (e.g., 'liveGameV1', 'schedule')")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="API method parameters"
    )


class IngestionConfig(BaseModel):
    """Ingestion behavior configuration."""

    rate_limit: int = Field(default=30, ge=1, description="Max requests per minute")
    retry: RetryConfig = Field(default_factory=RetryConfig)
    timeout: int = Field(default=30, ge=1, le=300, description="Request timeout in seconds")


class RawStorageConfig(BaseModel):
    """Raw storage configuration."""

    backend: Literal["postgres", "s3", "both"] = "postgres"
    table: Optional[str] = None  # PostgreSQL table name (schema.table)
    partition_by: Optional[str] = None  # Partition column
    upsert_keys: list[str] = Field(default_factory=list)  # Keys for upsert
    format: Literal["jsonb", "avro"] = "jsonb"


class CacheStorageConfig(BaseModel):
    """Cache storage configuration."""

    backend: Literal["redis"]
    key_pattern: str = Field(..., description="Redis key pattern (supports ${var} substitution)")
    ttl: int = Field(..., ge=0, description="Time to live in seconds")
    also_cache: list[dict[str, str]] = Field(
        default_factory=list, description="Additional cache entries with JSONPath"
    )


class ArchiveStorageConfig(BaseModel):
    """Archive storage configuration."""

    backend: Literal["s3", "minio"]
    bucket: str
    path: str = Field(..., description="S3 path (supports ${var} substitution)")
    format: Literal["json", "avro"] = "avro"
    compression: Literal["none", "gzip", "snappy", "lz4"] = "snappy"
    save_frequency: Literal["every_poll", "on_change", "on_completion"] = "on_completion"


class StorageConfig(BaseModel):
    """Complete storage configuration."""

    raw: Optional[RawStorageConfig] = None
    cache: Optional[CacheStorageConfig] = None
    archive: Optional[ArchiveStorageConfig] = None


class TransformConfig(BaseModel):
    """PySpark transform configuration."""

    enabled: bool = True
    mode: Literal["batch", "streaming"] = "batch"
    spark_job: str = Field(..., description="PySpark job name (without .py)")
    output_tables: list[str] = Field(default_factory=list)
    merge_mode: Literal["full", "incremental"] = "full"
    watermark: Optional[str] = None  # For streaming
    spark_config: dict[str, Any] = Field(default_factory=dict)


class StreamingConfig(BaseModel):
    """Streaming job configuration."""

    poll_interval: int = Field(..., ge=1, description="Polling interval in seconds")
    max_duration: int = Field(default=14400, description="Max duration in seconds")
    stop_conditions: list[dict[str, Any]] = Field(default_factory=list)


class NotificationConfig(BaseModel):
    """Notification configuration."""

    on_failure: list[dict[str, Any]] = Field(default_factory=list)
    on_success: list[dict[str, Any]] = Field(default_factory=list)


class JobMetadata(BaseModel):
    """Job metadata."""

    owner: str = "data-engineering"
    team: str = "mlb-data-platform"
    priority: Literal["low", "medium", "high"] = "medium"
    tags: list[str] = Field(default_factory=list)
    sla_hours: Optional[int] = None
    sla_minutes: Optional[int] = None
    sla_seconds: Optional[int] = None


class JobConfig(BaseModel):
    """Complete job configuration."""

    name: str = Field(..., description="Unique job name")
    description: Optional[str] = None
    type: JobType
    schedule: Optional[str] = None  # Cron expression
    trigger: Optional[Literal["event", "manual"]] = None

    # Source
    source: SourceConfig

    # Ingestion
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)

    # Storage
    storage: StorageConfig

    # Transform (optional)
    transform: Optional[TransformConfig] = None

    # Streaming (for streaming jobs)
    streaming: Optional[StreamingConfig] = None

    # Dependencies
    dependencies: list[dict[str, Any]] = Field(default_factory=list)

    # Notifications
    notifications: Optional[NotificationConfig] = None

    # Metadata
    metadata: JobMetadata = Field(default_factory=JobMetadata)

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: Optional[str], info) -> Optional[str]:
        """Validate cron schedule for scheduled jobs."""
        if info.data.get("type") == JobType.SCHEDULED and not v:
            raise ValueError("schedule is required for scheduled jobs")
        return v

    @field_validator("streaming")
    @classmethod
    def validate_streaming(cls, v: Optional[StreamingConfig], info) -> Optional[StreamingConfig]:
        """Validate streaming config for streaming jobs."""
        if info.data.get("type") == JobType.STREAMING and not v:
            raise ValueError("streaming config is required for streaming jobs")
        return v

    def get_table_name(self) -> str:
        """Get PostgreSQL table name from config or generate from endpoint/method."""
        if self.storage.raw and self.storage.raw.table:
            return self.storage.raw.table

        # Generate table name: endpoint.method_name
        schema = self.source.endpoint.lower()
        table = self.source.method.lower()

        # Convert camelCase to snake_case
        import re

        table = re.sub(r"(?<!^)(?=[A-Z])", "_", table).lower()

        return f"{schema}.{table}"


def load_job_config(path: str | Path) -> JobConfig:
    """Load job configuration from YAML file.

    Args:
        path: Path to YAML configuration file

    Returns:
        Validated JobConfig instance

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Job config not found: {config_path}")

    with open(config_path) as f:
        config_data = yaml.safe_load(f)

    return JobConfig(**config_data)
