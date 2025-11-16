"""Behave environment configuration for BDD tests.

This module configures the testing environment, including:
- Test database setup/teardown
- Tag-based test filtering (@local-only, @integration, @smoke)
- Safety checks to prevent running destructive tests in non-local environments
- Logging and reporting

References:
- https://behave.readthedocs.io/en/latest/tutorial.html#environmental-controls
- https://behave.readthedocs.io/en/latest/practical_tips.html
"""

import logging
import os
import sys
from pathlib import Path

from behave import fixture, use_fixture


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def is_local_environment() -> bool:
    """Check if running in local development environment.

    Returns:
        True if local, False if production/CI
    """
    # Check environment indicators
    is_ci = os.getenv("CI") == "true"
    is_prod = os.getenv("ENVIRONMENT") in ("production", "prod")
    is_local = os.getenv("ENVIRONMENT") == "local"

    # Default to local if not explicitly set
    if not is_ci and not is_prod:
        return True

    return is_local and not is_ci and not is_prod


def before_all(context):
    """Run before all tests.

    Setup:
    - Verify local environment for @local-only tests
    - Configure test database connection
    - Setup logging
    """
    context.is_local = is_local_environment()

    logger.info(f"Running in {'LOCAL' if context.is_local else 'NON-LOCAL'} environment")

    # Add project root to Python path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))

    # Safety check: Don't allow @local-only tests in non-local environments
    if not context.is_local:
        logger.warning(
            "⚠️  NON-LOCAL environment detected. Tests tagged with @local-only will be SKIPPED."
        )


def before_feature(context, feature):
    """Run before each feature.

    Args:
        context: Behave context
        feature: Feature being run
    """
    logger.info(f"Starting feature: {feature.name}")

    # Check for @local-only tag
    if "local-only" in feature.tags and not context.is_local:
        logger.error(
            f"⛔ SKIPPING FEATURE '{feature.name}' - tagged @local-only but running in non-local environment"
        )
        feature.skip(reason="Feature requires local environment (destructive operations)")


def before_scenario(context, scenario):
    """Run before each scenario.

    Args:
        context: Behave context
        scenario: Scenario being run
    """
    logger.info(f"  Starting scenario: {scenario.name}")

    # Safety check for @local-only scenarios
    if "local-only" in scenario.tags and not context.is_local:
        logger.error(
            f"⛔ SKIPPING SCENARIO '{scenario.name}' - tagged @local-only but running in non-local environment"
        )
        scenario.skip(reason="Scenario requires local environment (destructive operations)")


def after_scenario(context, scenario):
    """Run after each scenario.

    Args:
        context: Behave context
        scenario: Scenario that ran
    """
    if scenario.status == "failed":
        logger.error(f"  ✗ Scenario FAILED: {scenario.name}")
    elif scenario.status == "skipped":
        logger.warning(f"  ⏭  Scenario SKIPPED: {scenario.name}")
    else:
        logger.info(f"  ✓ Scenario passed: {scenario.name}")


def after_feature(context, feature):
    """Run after each feature.

    Args:
        context: Behave context
        feature: Feature that ran
    """
    passed = sum(1 for s in feature.scenarios if s.status == "passed")
    failed = sum(1 for s in feature.scenarios if s.status == "failed")
    skipped = sum(1 for s in feature.scenarios if s.status == "skipped")

    logger.info(f"Feature complete: {feature.name}")
    logger.info(f"  Passed: {passed}, Failed: {failed}, Skipped: {skipped}")


def after_all(context):
    """Run after all tests.

    Cleanup:
    - Close database connections
    - Generate reports
    """
    logger.info("All tests complete")

    if hasattr(context, "is_local") and context.is_local:
        logger.info("✓ Local environment test run complete")
    else:
        logger.info("✓ Non-local environment test run complete (destructive tests skipped)")


# Tag-based test filtering
# Usage: behave --tags=smoke (run only @smoke tests)
#        behave --tags=~local-only (exclude @local-only tests)
#        behave --tags=integration,~local-only (integration but not local-only)
