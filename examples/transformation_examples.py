"""Examples of using transformations with the __call__ pattern.

The __call__ pattern makes transformations behave like functions,
which is intuitive and flexible. You instantiate once with configuration,
then call multiple times with different parameters.
"""

from datetime import date

from pyspark.sql import SparkSession

from mlb_data_platform.transform.base import TransformMode
from mlb_data_platform.transform.game import GameLiveV1Transformation

# Initialize Spark
spark = SparkSession.builder.appName("transformation-examples").getOrCreate()


# =============================================================================
# Example 1: Batch Processing - Specific Games
# =============================================================================
def example_batch_specific_games():
    """Process specific completed games (e.g., World Series)."""
    # Create transformation instance (batch mode)
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Call with game PKs (like calling a function!)
    metrics = transform(
        game_pks=[747175, 747176, 747177],  # 2024 World Series games
        validate=True,  # Run data quality checks
        export_to_delta=True,  # Export to Delta Lake
    )

    print(f"Processed {len(metrics['target_tables'])} tables")
    print(f"Write metrics: {metrics['write_metrics']}")


# =============================================================================
# Example 2: Batch Processing - Date Range
# =============================================================================
def example_batch_date_range():
    """Backfill all games for October 2024 (playoffs)."""
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Call with date range
    metrics = transform(
        start_date=date(2024, 10, 1),
        end_date=date(2024, 10, 31),
        season="2024",
        validate=True,
    )

    print(f"Backfilled {metrics['source_row_count']} games")


# =============================================================================
# Example 3: Batch Processing - Full Season
# =============================================================================
def example_batch_full_season():
    """Process entire 2024 regular season."""
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Call with season parameter
    metrics = transform(
        season="2024",
        game_states=["Final"],  # Only completed games
        export_to_s3=True,
        s3_bucket="mlb-data-platform",
    )

    print(f"Processed {metrics['source_row_count']} completed games")


# =============================================================================
# Example 4: Batch Processing - Team-Specific
# =============================================================================
def example_batch_team_games():
    """Process all Yankees home games."""
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Call with team and venue filters
    metrics = transform(
        team_ids=[147],  # Yankees team ID
        venue_ids=[3313],  # Yankee Stadium
        season="2024",
    )

    print(f"Processed {metrics['source_row_count']} Yankees home games")


# =============================================================================
# Example 5: Streaming Mode - Live Games
# =============================================================================
def example_streaming_live_games():
    """Stream live games during the season."""
    # Create transformation instance (streaming mode)
    transform = GameLiveV1Transformation(spark, mode=TransformMode.STREAMING)

    # Call with streaming parameters
    query = transform(
        game_states=["Live"],  # Only live games
        checkpoint_location="s3://checkpoints/game-live",
        trigger_interval="30 seconds",  # Poll every 30 seconds
        validate=True,
    )

    # Streaming query runs continuously
    query.awaitTermination()


# =============================================================================
# Example 6: Partial Transformation - Specific Tables Only
# =============================================================================
def example_partial_transformation():
    """Only generate metadata and plays tables (skip pitch events)."""
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Call with target_tables filter
    metrics = transform(
        game_pks=[747175],
        target_tables=[
            "game.live_game_metadata",
            "game.live_game_plays",
        ],  # Only these 2 tables
        validate=False,  # Skip validation for faster processing
    )

    print(f"Generated {len(metrics['target_tables'])} tables (instead of 17)")


# =============================================================================
# Example 7: Re-usable Transformation Instance
# =============================================================================
def example_reusable_instance():
    """One transformation instance, multiple calls."""
    # Create once
    transform = GameLiveV1Transformation(
        spark,
        mode=TransformMode.BATCH,
        enable_quality_checks=True,
        fail_on_quality_error=True,
    )

    # Call multiple times with different parameters
    # (Useful in Argo Workflows for parallel game processing)

    # Process game 1
    metrics1 = transform(game_pks=[747175])

    # Process game 2
    metrics2 = transform(game_pks=[747176])

    # Process game 3
    metrics3 = transform(game_pks=[747177])

    print(f"Processed 3 games in parallel")


# =============================================================================
# Example 8: Data Correction - Replay with Filters
# =============================================================================
def example_data_correction():
    """Fix data after bug in transformation logic."""
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    # Found a bug that affected playoff games
    # Fix the bug in code, then replay:
    metrics = transform(
        start_date=date(2024, 10, 1),
        end_date=date(2024, 11, 1),
        write_mode="append",  # Defensive upserts handle duplicates
    )

    # Defensive upserts ensure:
    # - Corrected data overwrites buggy data
    # - Newer data is never touched
    print("Data corrected via replay")


# =============================================================================
# Example 9: Argo Workflow Integration
# =============================================================================
def example_argo_workflow_task():
    """
    This is how transformation would be called from Argo Workflow.

    Argo passes parameters as environment variables or CLI args,
    which we parse and pass to __call__.
    """
    import os

    # Parse environment variables
    game_pks = os.getenv("GAME_PKS", "").split(",")
    game_pks = [int(pk) for pk in game_pks if pk]

    validate = os.getenv("VALIDATE", "true").lower() == "true"
    export_to_s3 = os.getenv("EXPORT_TO_S3", "false").lower() == "true"

    # Create and call transformation
    transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)

    metrics = transform(
        game_pks=game_pks,
        validate=validate,
        export_to_s3=export_to_s3,
    )

    # Argo captures this output
    print(f"METRICS: {metrics}")


# =============================================================================
# Example 10: Advanced - Streaming with State Management
# =============================================================================
def example_streaming_with_state():
    """
    Stream live games with checkpoint for fault tolerance.

    If the job crashes, it resumes from the last checkpoint.
    """
    transform = GameLiveV1Transformation(spark, mode=TransformMode.STREAMING)

    # Call with checkpoint location
    query = transform(
        checkpoint_location="s3://checkpoints/game-live/run-001",
        trigger_interval="10 seconds",  # More frequent updates
        export_to_delta=True,  # Stream to Delta Lake
    )

    # If this crashes and restarts, it picks up where it left off
    # thanks to the checkpoint
    query.awaitTermination()


if __name__ == "__main__":
    # Run examples
    print("=" * 80)
    print("Example 1: Batch - Specific Games")
    print("=" * 80)
    example_batch_specific_games()

    print("\n" + "=" * 80)
    print("Example 2: Batch - Date Range")
    print("=" * 80)
    example_batch_date_range()

    print("\n" + "=" * 80)
    print("Example 6: Partial Transformation")
    print("=" * 80)
    example_partial_transformation()

    print("\n" + "=" * 80)
    print("Example 7: Re-usable Instance")
    print("=" * 80)
    example_reusable_instance()
