"""Argo Workflows client for submitting and managing workflows.

Provides a Python interface to the Argo Workflows API for:
- Submitting workflows from YAML templates
- Monitoring workflow status
- Fetching workflow logs
- Managing workflow lifecycle

Usage:
    >>> from mlb_data_platform.orchestration import create_argo_client
    >>>
    >>> client = create_argo_client()
    >>> workflow = client.submit_daily_pipeline(date="2024-06-15")
    >>> print(f"Submitted: {workflow.name}")
    >>>
    >>> status = client.wait_for_completion(workflow.name)
    >>> print(f"Status: {status}")
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
import yaml

logger = logging.getLogger(__name__)

# Default workflow templates directory
WORKFLOWS_DIR = Path(__file__).parents[3] / "config" / "workflows"


class WorkflowStatus(Enum):
    """Argo workflow status values."""

    PENDING = "Pending"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    ERROR = "Error"
    SKIPPED = "Skipped"
    UNKNOWN = "Unknown"


@dataclass
class WorkflowInfo:
    """Information about a submitted workflow."""

    name: str
    namespace: str
    uid: str
    status: WorkflowStatus
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    message: str = ""

    @property
    def is_complete(self) -> bool:
        """Check if workflow has completed (success or failure)."""
        return self.status in (
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED,
            WorkflowStatus.ERROR,
        )

    @property
    def is_successful(self) -> bool:
        """Check if workflow completed successfully."""
        return self.status == WorkflowStatus.SUCCEEDED

    @property
    def duration_seconds(self) -> float | None:
        """Get workflow duration in seconds."""
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


@dataclass
class ArgoConfig:
    """Configuration for Argo Workflows client."""

    # Argo server connection
    host: str = "localhost"
    port: int = 2746
    secure: bool = False
    token: str | None = None

    # Default namespace
    namespace: str = "mlb-data-platform"

    # Request settings
    timeout: float = 30.0
    verify_ssl: bool = True

    # Workflow defaults
    service_account: str = "mlb-data-platform-sa"

    @property
    def base_url(self) -> str:
        """Get base URL for Argo API."""
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}/api/v1"


class ArgoClient:
    """Client for interacting with Argo Workflows API.

    Provides methods for submitting, monitoring, and managing workflows.
    """

    def __init__(self, config: ArgoConfig | None = None):
        """Initialize Argo client.

        Args:
            config: Argo configuration (uses defaults if None)
        """
        self.config = config or ArgoConfig()
        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = {"Content-Type": "application/json"}
            if self.config.token:
                headers["Authorization"] = f"Bearer {self.config.token}"

            self._client = httpx.Client(
                base_url=self.config.base_url,
                headers=headers,
                timeout=self.config.timeout,
                verify=self.config.verify_ssl,
            )

        return self._client

    def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # =========================================================================
    # WORKFLOW SUBMISSION
    # =========================================================================

    def submit_workflow(
        self,
        workflow_yaml: str | Path | dict,
        parameters: dict[str, Any] | None = None,
        namespace: str | None = None,
    ) -> WorkflowInfo:
        """Submit a workflow from YAML file or dict.

        Args:
            workflow_yaml: Path to YAML file, YAML string, or workflow dict
            parameters: Override workflow parameters
            namespace: Namespace (uses config default if None)

        Returns:
            WorkflowInfo with submission details
        """
        namespace = namespace or self.config.namespace

        # Load workflow spec
        if isinstance(workflow_yaml, dict):
            spec = workflow_yaml
        elif isinstance(workflow_yaml, Path) or (
            isinstance(workflow_yaml, str) and Path(workflow_yaml).exists()
        ):
            with open(workflow_yaml) as f:
                spec = yaml.safe_load(f)
        else:
            spec = yaml.safe_load(workflow_yaml)

        # Override parameters if provided
        if parameters:
            if "spec" not in spec:
                spec["spec"] = {}
            if "arguments" not in spec["spec"]:
                spec["spec"]["arguments"] = {}
            if "parameters" not in spec["spec"]["arguments"]:
                spec["spec"]["arguments"]["parameters"] = []

            # Update or add parameters
            existing_params = {
                p["name"]: p for p in spec["spec"]["arguments"]["parameters"]
            }
            for name, value in parameters.items():
                if name in existing_params:
                    existing_params[name]["value"] = str(value)
                else:
                    spec["spec"]["arguments"]["parameters"].append(
                        {"name": name, "value": str(value)}
                    )

        # Submit workflow
        client = self._get_client()
        response = client.post(
            f"/workflows/{namespace}",
            json={"workflow": spec},
        )
        response.raise_for_status()

        data = response.json()
        return self._parse_workflow_info(data)

    def submit_daily_pipeline(
        self,
        target_date: date | str | None = None,
        sport_id: int = 1,
        enrich: bool = True,
        dry_run: bool = False,
    ) -> WorkflowInfo:
        """Submit the daily pipeline workflow.

        Args:
            target_date: Date to process (default: today)
            sport_id: Sport ID (1 = MLB)
            enrich: Fetch player/team enrichment
            dry_run: Show what would be fetched

        Returns:
            WorkflowInfo
        """
        workflow_path = WORKFLOWS_DIR / "workflow-pipeline-daily.yaml"

        params = {
            "sport-id": str(sport_id),
            "enrich": str(enrich).lower(),
            "dry-run": str(dry_run).lower(),
        }

        if target_date:
            if isinstance(target_date, date):
                params["date"] = target_date.isoformat()
            else:
                params["date"] = target_date

        return self.submit_workflow(workflow_path, parameters=params)

    def submit_backfill(
        self,
        season: str | None = None,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        sport_id: int = 1,
        enrich: bool = True,
    ) -> WorkflowInfo:
        """Submit a backfill workflow.

        Args:
            season: Season year (e.g., "2024")
            start_date: Start of date range
            end_date: End of date range
            sport_id: Sport ID (1 = MLB)
            enrich: Fetch player/team enrichment

        Returns:
            WorkflowInfo
        """
        workflow_path = WORKFLOWS_DIR / "workflow-pipeline-backfill.yaml"

        params = {
            "sport-id": str(sport_id),
            "enrich": str(enrich).lower(),
        }

        if season:
            params["season"] = season

        if start_date:
            if isinstance(start_date, date):
                params["start-date"] = start_date.isoformat()
            else:
                params["start-date"] = start_date

        if end_date:
            if isinstance(end_date, date):
                params["end-date"] = end_date.isoformat()
            else:
                params["end-date"] = end_date

        return self.submit_workflow(workflow_path, parameters=params)

    def submit_games(
        self,
        game_pks: list[int],
        enrich: bool = True,
    ) -> WorkflowInfo:
        """Submit workflow to fetch specific games.

        Args:
            game_pks: List of game primary keys
            enrich: Fetch player/team enrichment

        Returns:
            WorkflowInfo
        """
        # Use daily workflow with games parameter
        # (Would need a games-specific workflow for production)
        workflow_path = WORKFLOWS_DIR / "workflow-pipeline-daily.yaml"

        params = {
            "game-pks": ",".join(str(pk) for pk in game_pks),
            "enrich": str(enrich).lower(),
        }

        return self.submit_workflow(workflow_path, parameters=params)

    # =========================================================================
    # WORKFLOW MONITORING
    # =========================================================================

    def get_workflow(
        self,
        name: str,
        namespace: str | None = None,
    ) -> WorkflowInfo:
        """Get workflow status by name.

        Args:
            name: Workflow name
            namespace: Namespace (uses config default if None)

        Returns:
            WorkflowInfo
        """
        namespace = namespace or self.config.namespace

        client = self._get_client()
        response = client.get(f"/workflows/{namespace}/{name}")
        response.raise_for_status()

        data = response.json()
        return self._parse_workflow_info(data)

    def list_workflows(
        self,
        namespace: str | None = None,
        label_selector: str | None = None,
        limit: int = 50,
    ) -> list[WorkflowInfo]:
        """List workflows.

        Args:
            namespace: Namespace (uses config default if None)
            label_selector: Filter by labels (e.g., "app=mlb-data-platform")
            limit: Maximum number of workflows to return

        Returns:
            List of WorkflowInfo
        """
        namespace = namespace or self.config.namespace

        params = {"listOptions.limit": limit}
        if label_selector:
            params["listOptions.labelSelector"] = label_selector

        client = self._get_client()
        response = client.get(f"/workflows/{namespace}", params=params)
        response.raise_for_status()

        data = response.json()
        items = data.get("items", []) or []
        return [self._parse_workflow_info(item) for item in items]

    def wait_for_completion(
        self,
        name: str,
        namespace: str | None = None,
        poll_interval: float = 5.0,
        timeout: float = 3600.0,
    ) -> WorkflowStatus:
        """Wait for workflow to complete.

        Args:
            name: Workflow name
            namespace: Namespace
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait

        Returns:
            Final WorkflowStatus

        Raises:
            TimeoutError: If workflow doesn't complete within timeout
        """
        start_time = time.time()

        while True:
            workflow = self.get_workflow(name, namespace)

            if workflow.is_complete:
                return workflow.status

            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Workflow {name} did not complete within {timeout}s"
                )

            logger.info(f"Workflow {name} status: {workflow.status.value}")
            time.sleep(poll_interval)

    def get_workflow_logs(
        self,
        name: str,
        namespace: str | None = None,
        container: str = "main",
    ) -> str:
        """Get workflow logs.

        Args:
            name: Workflow name
            namespace: Namespace
            container: Container name

        Returns:
            Log content as string
        """
        namespace = namespace or self.config.namespace

        client = self._get_client()
        response = client.get(
            f"/workflows/{namespace}/{name}/log",
            params={"logOptions.container": container},
        )
        response.raise_for_status()

        return response.text

    # =========================================================================
    # WORKFLOW MANAGEMENT
    # =========================================================================

    def terminate_workflow(
        self,
        name: str,
        namespace: str | None = None,
    ) -> None:
        """Terminate a running workflow.

        Args:
            name: Workflow name
            namespace: Namespace
        """
        namespace = namespace or self.config.namespace

        client = self._get_client()
        response = client.put(f"/workflows/{namespace}/{name}/terminate")
        response.raise_for_status()

        logger.info(f"Terminated workflow: {name}")

    def delete_workflow(
        self,
        name: str,
        namespace: str | None = None,
    ) -> None:
        """Delete a workflow.

        Args:
            name: Workflow name
            namespace: Namespace
        """
        namespace = namespace or self.config.namespace

        client = self._get_client()
        response = client.delete(f"/workflows/{namespace}/{name}")
        response.raise_for_status()

        logger.info(f"Deleted workflow: {name}")

    def retry_workflow(
        self,
        name: str,
        namespace: str | None = None,
    ) -> WorkflowInfo:
        """Retry a failed workflow.

        Args:
            name: Workflow name
            namespace: Namespace

        Returns:
            New WorkflowInfo
        """
        namespace = namespace or self.config.namespace

        client = self._get_client()
        response = client.put(f"/workflows/{namespace}/{name}/retry")
        response.raise_for_status()

        data = response.json()
        return self._parse_workflow_info(data)

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _parse_workflow_info(self, data: dict) -> WorkflowInfo:
        """Parse workflow API response into WorkflowInfo.

        Args:
            data: API response dict

        Returns:
            WorkflowInfo
        """
        metadata = data.get("metadata", {})
        status = data.get("status", {})

        # Parse status
        phase = status.get("phase", "Unknown")
        try:
            workflow_status = WorkflowStatus(phase)
        except ValueError:
            workflow_status = WorkflowStatus.UNKNOWN

        # Parse timestamps
        created_at = None
        if metadata.get("creationTimestamp"):
            created_at = datetime.fromisoformat(
                metadata["creationTimestamp"].replace("Z", "+00:00")
            )

        started_at = None
        if status.get("startedAt"):
            started_at = datetime.fromisoformat(
                status["startedAt"].replace("Z", "+00:00")
            )

        finished_at = None
        if status.get("finishedAt"):
            finished_at = datetime.fromisoformat(
                status["finishedAt"].replace("Z", "+00:00")
            )

        return WorkflowInfo(
            name=metadata.get("name", ""),
            namespace=metadata.get("namespace", self.config.namespace),
            uid=metadata.get("uid", ""),
            status=workflow_status,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            message=status.get("message", ""),
        )


def create_argo_client(
    host: str = "localhost",
    port: int = 2746,
    namespace: str = "mlb-data-platform",
    token: str | None = None,
) -> ArgoClient:
    """Create Argo client with common configuration.

    Args:
        host: Argo server host
        port: Argo server port
        namespace: Default namespace
        token: Auth token (optional)

    Returns:
        Configured ArgoClient
    """
    config = ArgoConfig(
        host=host,
        port=port,
        namespace=namespace,
        token=token,
    )
    return ArgoClient(config)
