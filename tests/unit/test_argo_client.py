"""Tests for the Argo Workflows client.

Tests the ArgoClient for workflow submission and management.
"""

import pytest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, mock_open

from mlb_data_platform.orchestration.argo_client import (
    ArgoClient,
    ArgoConfig,
    WorkflowInfo,
    WorkflowStatus,
    create_argo_client,
)


class TestArgoConfig:
    """Tests for ArgoConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ArgoConfig()

        assert config.host == "localhost"
        assert config.port == 2746
        assert config.secure is False
        assert config.token is None
        assert config.namespace == "mlb-data-platform"

    def test_base_url_http(self):
        """Test base URL generation for HTTP."""
        config = ArgoConfig(host="argo.local", port=8080, secure=False)
        assert config.base_url == "http://argo.local:8080/api/v1"

    def test_base_url_https(self):
        """Test base URL generation for HTTPS."""
        config = ArgoConfig(host="argo.prod", port=443, secure=True)
        assert config.base_url == "https://argo.prod:443/api/v1"


class TestWorkflowInfo:
    """Tests for WorkflowInfo dataclass."""

    def test_is_complete_succeeded(self):
        """Test is_complete for succeeded workflow."""
        info = WorkflowInfo(
            name="test",
            namespace="default",
            uid="123",
            status=WorkflowStatus.SUCCEEDED,
        )
        assert info.is_complete is True
        assert info.is_successful is True

    def test_is_complete_failed(self):
        """Test is_complete for failed workflow."""
        info = WorkflowInfo(
            name="test",
            namespace="default",
            uid="123",
            status=WorkflowStatus.FAILED,
        )
        assert info.is_complete is True
        assert info.is_successful is False

    def test_is_complete_running(self):
        """Test is_complete for running workflow."""
        info = WorkflowInfo(
            name="test",
            namespace="default",
            uid="123",
            status=WorkflowStatus.RUNNING,
        )
        assert info.is_complete is False
        assert info.is_successful is False

    def test_duration_seconds(self):
        """Test duration calculation."""
        start = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 6, 15, 10, 5, 30, tzinfo=timezone.utc)

        info = WorkflowInfo(
            name="test",
            namespace="default",
            uid="123",
            status=WorkflowStatus.SUCCEEDED,
            started_at=start,
            finished_at=end,
        )

        assert info.duration_seconds == 330.0  # 5 minutes 30 seconds

    def test_duration_seconds_not_finished(self):
        """Test duration when workflow not finished."""
        info = WorkflowInfo(
            name="test",
            namespace="default",
            uid="123",
            status=WorkflowStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        assert info.duration_seconds is None


class TestWorkflowStatus:
    """Tests for WorkflowStatus enum."""

    def test_all_statuses(self):
        """Test all status values exist."""
        assert WorkflowStatus.PENDING.value == "Pending"
        assert WorkflowStatus.RUNNING.value == "Running"
        assert WorkflowStatus.SUCCEEDED.value == "Succeeded"
        assert WorkflowStatus.FAILED.value == "Failed"
        assert WorkflowStatus.ERROR.value == "Error"
        assert WorkflowStatus.SKIPPED.value == "Skipped"
        assert WorkflowStatus.UNKNOWN.value == "Unknown"


class TestArgoClient:
    """Tests for ArgoClient class."""

    @pytest.fixture
    def client(self):
        """Create client with default config."""
        return ArgoClient()

    @pytest.fixture
    def mock_httpx_client(self):
        """Create mock httpx client."""
        return MagicMock()

    # =========================================================================
    # INITIALIZATION TESTS
    # =========================================================================

    def test_init_default_config(self, client):
        """Test client initializes with default config."""
        assert client.config.host == "localhost"
        assert client.config.port == 2746

    def test_init_custom_config(self):
        """Test client with custom config."""
        config = ArgoConfig(host="custom.host", port=9999)
        client = ArgoClient(config)
        assert client.config.host == "custom.host"
        assert client.config.port == 9999

    def test_context_manager(self):
        """Test client as context manager."""
        with ArgoClient() as client:
            assert client is not None

    # =========================================================================
    # WORKFLOW PARSING TESTS
    # =========================================================================

    def test_parse_workflow_info_basic(self, client):
        """Test parsing basic workflow response."""
        data = {
            "metadata": {
                "name": "test-workflow",
                "namespace": "mlb-data-platform",
                "uid": "abc-123",
            },
            "status": {
                "phase": "Running",
            },
        }

        info = client._parse_workflow_info(data)

        assert info.name == "test-workflow"
        assert info.namespace == "mlb-data-platform"
        assert info.uid == "abc-123"
        assert info.status == WorkflowStatus.RUNNING

    def test_parse_workflow_info_with_timestamps(self, client):
        """Test parsing workflow with timestamps."""
        data = {
            "metadata": {
                "name": "test",
                "namespace": "default",
                "uid": "123",
                "creationTimestamp": "2024-06-15T10:00:00Z",
            },
            "status": {
                "phase": "Succeeded",
                "startedAt": "2024-06-15T10:00:05Z",
                "finishedAt": "2024-06-15T10:05:00Z",
                "message": "Workflow completed",
            },
        }

        info = client._parse_workflow_info(data)

        assert info.status == WorkflowStatus.SUCCEEDED
        assert info.created_at is not None
        assert info.started_at is not None
        assert info.finished_at is not None
        assert info.message == "Workflow completed"

    def test_parse_workflow_info_unknown_status(self, client):
        """Test parsing workflow with unknown status."""
        data = {
            "metadata": {"name": "test", "namespace": "default", "uid": "123"},
            "status": {"phase": "InvalidStatus"},
        }

        info = client._parse_workflow_info(data)
        assert info.status == WorkflowStatus.UNKNOWN

    # =========================================================================
    # WORKFLOW SUBMISSION TESTS
    # =========================================================================

    def test_submit_workflow_from_dict(self, client):
        """Test submitting workflow from dict."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {
                "name": "submitted-workflow",
                "namespace": "mlb-data-platform",
                "uid": "new-123",
            },
            "status": {"phase": "Pending"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.post.return_value = mock_response
            mock_get.return_value = mock_http

            workflow_spec = {
                "metadata": {"name": "test"},
                "spec": {"entrypoint": "main"},
            }

            result = client.submit_workflow(workflow_spec)

            assert result.name == "submitted-workflow"
            assert result.status == WorkflowStatus.PENDING
            mock_http.post.assert_called_once()

    def test_submit_workflow_with_parameters(self, client):
        """Test submitting workflow with parameter overrides."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"name": "test", "namespace": "default", "uid": "123"},
            "status": {"phase": "Pending"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.post.return_value = mock_response
            mock_get.return_value = mock_http

            workflow_spec = {
                "metadata": {"name": "test"},
                "spec": {
                    "arguments": {
                        "parameters": [{"name": "date", "value": "2024-01-01"}]
                    }
                },
            }

            client.submit_workflow(workflow_spec, parameters={"date": "2024-06-15"})

            # Check parameters were updated in the call
            call_args = mock_http.post.call_args
            submitted = call_args.kwargs["json"]["workflow"]
            params = submitted["spec"]["arguments"]["parameters"]
            date_param = next(p for p in params if p["name"] == "date")
            assert date_param["value"] == "2024-06-15"

    def test_submit_daily_pipeline(self, client):
        """Test submitting daily pipeline."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"name": "pipeline-daily", "namespace": "default", "uid": "123"},
            "status": {"phase": "Pending"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.post.return_value = mock_response
            mock_get.return_value = mock_http

            # Mock file read
            workflow_yaml = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: pipeline-daily
spec:
  entrypoint: daily-pipeline
  arguments:
    parameters:
      - name: date
        value: ""
"""
            with patch("builtins.open", mock_open(read_data=workflow_yaml)):
                result = client.submit_daily_pipeline(
                    target_date=date(2024, 6, 15), sport_id=1
                )

            assert result.name == "pipeline-daily"

    def test_submit_backfill(self, client):
        """Test submitting backfill workflow."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"name": "backfill", "namespace": "default", "uid": "123"},
            "status": {"phase": "Pending"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.post.return_value = mock_response
            mock_get.return_value = mock_http

            workflow_yaml = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: backfill
spec:
  entrypoint: backfill
  arguments:
    parameters: []
"""
            with patch("builtins.open", mock_open(read_data=workflow_yaml)):
                result = client.submit_backfill(
                    season="2024",
                    start_date=date(2024, 6, 1),
                    end_date=date(2024, 6, 30),
                )

            assert result.name == "backfill"

    # =========================================================================
    # WORKFLOW MONITORING TESTS
    # =========================================================================

    def test_get_workflow(self, client):
        """Test getting workflow by name."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"name": "test", "namespace": "default", "uid": "123"},
            "status": {"phase": "Running"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.get.return_value = mock_response
            mock_get.return_value = mock_http

            result = client.get_workflow("test")

            assert result.name == "test"
            assert result.status == WorkflowStatus.RUNNING

    def test_list_workflows(self, client):
        """Test listing workflows."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "items": [
                {
                    "metadata": {"name": "wf1", "namespace": "default", "uid": "1"},
                    "status": {"phase": "Succeeded"},
                },
                {
                    "metadata": {"name": "wf2", "namespace": "default", "uid": "2"},
                    "status": {"phase": "Running"},
                },
            ]
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.get.return_value = mock_response
            mock_get.return_value = mock_http

            result = client.list_workflows()

            assert len(result) == 2
            assert result[0].name == "wf1"
            assert result[1].name == "wf2"

    def test_list_workflows_empty(self, client):
        """Test listing workflows when none exist."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"items": None}

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.get.return_value = mock_response
            mock_get.return_value = mock_http

            result = client.list_workflows()

            assert len(result) == 0

    # =========================================================================
    # WORKFLOW MANAGEMENT TESTS
    # =========================================================================

    def test_terminate_workflow(self, client):
        """Test terminating a workflow."""
        mock_response = MagicMock()

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.put.return_value = mock_response
            mock_get.return_value = mock_http

            client.terminate_workflow("running-wf")

            mock_http.put.assert_called_once()
            call_url = mock_http.put.call_args[0][0]
            assert "terminate" in call_url

    def test_delete_workflow(self, client):
        """Test deleting a workflow."""
        mock_response = MagicMock()

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.delete.return_value = mock_response
            mock_get.return_value = mock_http

            client.delete_workflow("old-wf")

            mock_http.delete.assert_called_once()

    def test_retry_workflow(self, client):
        """Test retrying a failed workflow."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "metadata": {"name": "retried-wf", "namespace": "default", "uid": "new"},
            "status": {"phase": "Pending"},
        }

        with patch.object(client, "_get_client") as mock_get:
            mock_http = MagicMock()
            mock_http.put.return_value = mock_response
            mock_get.return_value = mock_http

            result = client.retry_workflow("failed-wf")

            assert result.name == "retried-wf"
            assert result.status == WorkflowStatus.PENDING


class TestCreateArgoClient:
    """Tests for create_argo_client factory function."""

    def test_creates_client_with_defaults(self):
        """Test factory creates client with defaults."""
        client = create_argo_client()

        assert isinstance(client, ArgoClient)
        assert client.config.host == "localhost"
        assert client.config.port == 2746

    def test_creates_client_with_custom_values(self):
        """Test factory with custom configuration."""
        client = create_argo_client(
            host="argo.prod",
            port=8080,
            namespace="production",
            token="secret-token",
        )

        assert client.config.host == "argo.prod"
        assert client.config.port == 8080
        assert client.config.namespace == "production"
        assert client.config.token == "secret-token"
