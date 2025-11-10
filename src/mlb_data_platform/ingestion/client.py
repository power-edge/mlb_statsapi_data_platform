"""MLB Stats API client with stub mode support and schema integration."""

import time
from datetime import datetime, timezone
from typing import Any, Optional

from pymlb_statsapi import api
from pymlb_statsapi.model.factory import APIResponse
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
)

from ..schema.registry import get_registry
from .config import BackoffStrategy, JobConfig, StubMode


class MLBStatsAPIClient:
    """Wrapper around pymlb_statsapi with enhanced features.

    Features:
    - Stub mode support (capture/replay/passthrough)
    - Automatic schema detection and validation
    - Rate limiting
    - Retry logic with configurable backoff
    - Metadata enrichment
    """

    def __init__(
        self,
        job_config: JobConfig,
        stub_mode: StubMode = StubMode.PASSTHROUGH,
    ):
        """Initialize MLB Stats API client.

        Args:
            job_config: Job configuration
            stub_mode: Stub mode for testing
        """
        self.job_config = job_config
        self.stub_mode = stub_mode
        self.schema_registry = get_registry()

        # Get schema metadata for this endpoint/method
        self.schema_metadata = self.schema_registry.get_schema_by_endpoint(
            job_config.source.endpoint, job_config.source.method
        )

        # Rate limiting state
        self._last_request_time: Optional[float] = None
        self._request_count = 0
        self._minute_start_time = time.time()

    def fetch(self, **override_params) -> dict[str, Any]:
        """Fetch data from MLB Stats API.

        Args:
            **override_params: Override parameters from job config

        Returns:
            Dict containing:
                - metadata: Request/response metadata
                - data: API response data
                - schema_metadata: Schema information

        Raises:
            ValueError: If endpoint or method not found
            RuntimeError: If API call fails after retries
        """
        # Merge parameters
        params = {**self.job_config.source.parameters, **override_params}

        # Apply rate limiting
        self._apply_rate_limit()

        # Get endpoint and method from pymlb_statsapi
        endpoint_name = self.job_config.source.endpoint
        method_name = self.job_config.source.method

        try:
            endpoint = api.get_endpoint(endpoint_name)
        except AttributeError:
            raise ValueError(f"Endpoint not found: {endpoint_name}")

        if not hasattr(endpoint, method_name):
            raise ValueError(
                f"Method '{method_name}' not found on endpoint '{endpoint_name}'"
            )

        # Set stub mode for pymlb_statsapi (if supported)
        # Note: This assumes pymlb_statsapi supports STUB_MODE environment variable
        import os

        if self.stub_mode != StubMode.PASSTHROUGH:
            os.environ["STUB_MODE"] = self.stub_mode.value

        # Build retry decorator based on config
        retry_decorator = self._build_retry_decorator()

        # Make API call with retry logic
        @retry_decorator
        def _make_request() -> APIResponse:
            method = getattr(endpoint, method_name)
            return method(**params)

        start_time = datetime.now(timezone.utc)
        response = _make_request()
        end_time = datetime.now(timezone.utc)

        # Extract response data
        response_data = response.json()

        # Build metadata
        metadata = {
            "request": {
                "endpoint_name": endpoint_name,
                "method_name": method_name,
                "path_params": {},  # TODO: Extract from response metadata
                "query_params": params,
                "url": response.get_metadata().get("url", ""),
                "timestamp": start_time.isoformat(),
            },
            "response": {
                "status_code": 200,  # pymlb_statsapi doesn't expose this yet
                "elapsed_ms": (end_time - start_time).total_seconds() * 1000,
                "captured_at": end_time.isoformat(),
            },
            "schema": {
                "endpoint": endpoint_name,
                "method": method_name,
                "version": self.schema_metadata.version if self.schema_metadata else "v1",
                "table_name": (
                    self.schema_metadata.schema_name if self.schema_metadata else None
                ),
            },
            "ingestion": {
                "job_name": self.job_config.name,
                "stub_mode": self.stub_mode.value,
            },
        }

        # Extract key fields based on schema metadata
        extracted_fields = {}
        if self.schema_metadata:
            extracted_fields = self._extract_fields(response_data, self.schema_metadata)

        return {
            "metadata": metadata,
            "data": response_data,
            "extracted_fields": extracted_fields,
            "schema_metadata": self.schema_metadata.model_dump() if self.schema_metadata else None,
        }

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting based on job configuration."""
        if not self.job_config.ingestion.rate_limit:
            return

        current_time = time.time()

        # Reset counter every minute
        if current_time - self._minute_start_time >= 60:
            self._request_count = 0
            self._minute_start_time = current_time

        # Check if we've exceeded rate limit
        if self._request_count >= self.job_config.ingestion.rate_limit:
            # Wait until next minute
            sleep_time = 60 - (current_time - self._minute_start_time)
            if sleep_time > 0:
                time.sleep(sleep_time)
            self._request_count = 0
            self._minute_start_time = time.time()

        # Increment counter
        self._request_count += 1

        # Minimum delay between requests (to smooth out traffic)
        if self._last_request_time:
            min_delay = 60.0 / self.job_config.ingestion.rate_limit
            elapsed = time.time() - self._last_request_time
            if elapsed < min_delay:
                time.sleep(min_delay - elapsed)

        self._last_request_time = time.time()

    def _build_retry_decorator(self):
        """Build retry decorator based on job configuration."""
        retry_config = self.job_config.ingestion.retry

        # Determine wait strategy
        if retry_config.backoff == BackoffStrategy.EXPONENTIAL:
            wait_strategy = wait_exponential(
                multiplier=retry_config.initial_delay,
                max=retry_config.max_delay,
            )
        elif retry_config.backoff == BackoffStrategy.LINEAR:
            # Simple linear backoff (not ideal but available)
            wait_strategy = wait_fixed(retry_config.initial_delay)
        else:  # CONSTANT
            wait_strategy = wait_fixed(retry_config.initial_delay)

        return retry(
            retry=retry_if_exception_type((ConnectionError, TimeoutError)),
            stop=stop_after_attempt(retry_config.max_attempts),
            wait=wait_strategy,
            reraise=True,
        )

    def _extract_fields(
        self, data: dict[str, Any], schema_metadata
    ) -> dict[str, Any]:
        """Extract fields from response data based on schema metadata.

        Args:
            data: Response data
            schema_metadata: Schema metadata with field definitions

        Returns:
            Dict of extracted field values
        """
        extracted = {}

        for field in schema_metadata.fields:
            if not field.json_path:
                continue

            try:
                value = self._extract_json_path(data, field.json_path)
                if value is not None:
                    extracted[field.name] = value
            except Exception as e:
                # Log warning but continue
                print(f"Warning: Could not extract field {field.name}: {e}")

        return extracted

    @staticmethod
    def _extract_json_path(data: dict[str, Any], json_path: str) -> Any:
        """Extract value from nested dict using JSONPath-like syntax.

        Args:
            data: Source data dict
            json_path: Path like '$.gameData.game.season'

        Returns:
            Extracted value or None
        """
        # Remove leading $. if present
        path = json_path.lstrip("$").lstrip(".")

        # Split path and traverse
        current = data
        for key in path.split("."):
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None

            if current is None:
                return None

        return current

    def get_schema_info(self) -> dict[str, Any]:
        """Get schema information for this client's endpoint/method.

        Returns:
            Dict with schema metadata
        """
        if not self.schema_metadata:
            return {
                "endpoint": self.job_config.source.endpoint,
                "method": self.job_config.source.method,
                "schema_found": False,
            }

        return {
            "endpoint": self.schema_metadata.endpoint,
            "method": self.schema_metadata.method,
            "schema_name": self.schema_metadata.schema_name,
            "version": self.schema_metadata.version,
            "schema_found": True,
            "primary_keys": self.schema_metadata.primary_keys,
            "partition_keys": self.schema_metadata.partition_keys,
            "num_fields": len(self.schema_metadata.fields),
            "num_relationships": len(self.schema_metadata.relationships),
            "relationships": [
                {
                    "from": r.from_schema,
                    "to": r.to_schema,
                    "type": r.relationship_type.value,
                    "on": f"{r.from_field} -> {r.to_field}",
                }
                for r in self.schema_metadata.relationships
            ],
        }
