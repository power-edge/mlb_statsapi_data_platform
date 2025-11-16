"""Game.liveGameV1() transformation.

Transforms raw game.live_game_v1 JSONB table into 17 normalized relational tables.

Example usage:
    >>> from pyspark.sql import SparkSession
    >>> from mlb_data_platform.transform.game import GameLiveV1Transformation
    >>> from mlb_data_platform.transform.base import TransformMode
    >>>
    >>> spark = SparkSession.builder.appName("game-transform").getOrCreate()
    >>>
    >>> # Batch mode: Process specific games
    >>> transform = GameLiveV1Transformation(spark, mode=TransformMode.BATCH)
    >>> metrics = transform(game_pks=[747175, 747176])
    >>>
    >>> # Streaming mode: Process live games
    >>> transform = GameLiveV1Transformation(spark, mode=TransformMode.STREAMING)
    >>> query = transform(checkpoint_location="s3://checkpoints/game-live")
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from ...quality.validator import DataQualityValidator
from ..base import BaseTransformation, TransformMode

console = Console()


class GameLiveV1Transformation(BaseTransformation):
    """Transform Game.liveGameV1() raw data into 17 normalized tables.

    This transformation handles the most comprehensive game data source,
    extracting:
    - Game metadata (teams, venue, status, weather)
    - Linescore (inning-by-inning scoring)
    - Players (all 51+ players in game)
    - Plays (all at-bats, ~75 per game)
    - Pitch events (every pitch, ~250 per game)
    - Runners, scoring plays, lineups, pitchers, bench, bullpen, umpires, venue

    The transformation is idempotent and uses defensive upserts to ensure
    data consistency even when re-run.
    """

    def __init__(
        self,
        spark: SparkSession,
        mode: TransformMode = TransformMode.BATCH,
        enable_quality_checks: bool = True,
        fail_on_quality_error: bool = True,
    ):
        """Initialize Game.liveGameV1 transformation.

        Args:
            spark: SparkSession instance
            mode: Batch or streaming mode
            enable_quality_checks: Run PyDeequ data quality validation
            fail_on_quality_error: Fail transformation if quality checks fail
        """
        super().__init__(spark, endpoint="game", method="live_game_v1", mode=mode)

        self.enable_quality_checks = enable_quality_checks
        self.fail_on_quality_error = fail_on_quality_error

        if enable_quality_checks:
            self.validator = DataQualityValidator(spark)

    def __call__(
        self,
        # Filtering options
        game_pks: Optional[List[int]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        season: Optional[str] = None,
        team_ids: Optional[List[int]] = None,
        venue_ids: Optional[List[int]] = None,
        game_states: Optional[List[str]] = None,  # ["Preview", "Live", "Final"]
        # Processing options
        target_tables: Optional[List[str]] = None,  # Specific tables to generate
        validate: Optional[bool] = None,  # Override enable_quality_checks
        # Export options
        export_to_s3: bool = False,
        export_to_delta: bool = False,
        s3_bucket: Optional[str] = None,
        # Streaming options
        checkpoint_location: Optional[str] = None,
        trigger_interval: str = "30 seconds",
        # Write options
        write_mode: str = "append",
        partition_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Execute the transformation pipeline.

        Args:
            game_pks: Specific game PKs to process
            start_date: Start date for date range filter
            end_date: End date for date range filter
            season: Filter by season (e.g., "2024")
            team_ids: Filter by team IDs (home or away)
            venue_ids: Filter by venue IDs
            game_states: Filter by game state (Preview, Live, Final)
            target_tables: Specific target tables to generate (None = all)
            validate: Run data quality checks (overrides enable_quality_checks)
            export_to_s3: Export normalized tables to S3 as Parquet
            export_to_delta: Export to Delta Lake
            s3_bucket: S3 bucket for exports
            checkpoint_location: Checkpoint dir for streaming (required for streaming)
            trigger_interval: Streaming trigger interval
            write_mode: Write mode (append, overwrite)
            partition_date: Date for partitioning (defaults to today)

        Returns:
            Dict of execution metrics

        Examples:
            >>> # Process specific completed games
            >>> metrics = transform(
            ...     game_pks=[747175, 747176],
            ...     validate=True
            ... )

            >>> # Process date range (batch backfill)
            >>> metrics = transform(
            ...     start_date=date(2024, 10, 1),
            ...     end_date=date(2024, 10, 31),
            ...     season="2024"
            ... )

            >>> # Process only live games
            >>> metrics = transform(
            ...     game_states=["Live"],
            ...     validate=True,
            ...     export_to_delta=True
            ... )

            >>> # Stream live games (streaming mode)
            >>> transform = GameLiveV1Transformation(spark, mode=STREAMING)
            >>> query = transform(
            ...     checkpoint_location="s3://checkpoints/game-live",
            ...     trigger_interval="10 seconds"
            ... )
        """
        # Validate streaming requirements
        if self.mode == TransformMode.STREAMING and not checkpoint_location:
            raise ValueError(
                "checkpoint_location is required for streaming mode"
            )

        # Build filter condition
        filter_condition = self._build_filter_condition(
            game_pks=game_pks,
            start_date=start_date,
            end_date=end_date,
            season=season,
            team_ids=team_ids,
            venue_ids=venue_ids,
            game_states=game_states,
        )

        console.print(f"\n[bold green]Starting Game.liveGameV1 Transformation[/bold green]")
        console.print(f"Mode: [cyan]{self.mode.value}[/cyan]")
        console.print(f"Filter: [yellow]{filter_condition or 'None (all data)'}[/yellow]")

        # Step 1: Read source data
        console.print("\n[bold]Step 1: Reading source data...[/bold]")
        source_df = self.read_source(filter_condition=filter_condition)

        if self.mode == TransformMode.BATCH:
            row_count = source_df.count()
            console.print(f"✓ Loaded [green]{row_count}[/green] raw records")

        # Step 2: Transform to target tables
        console.print("\n[bold]Step 2: Transforming to normalized tables...[/bold]")
        target_dfs = self.transform(source_df, target_tables=target_tables)

        console.print(f"✓ Generated [green]{len(target_dfs)}[/green] target tables")

        # Step 3: Data quality validation
        should_validate = validate if validate is not None else self.enable_quality_checks

        if should_validate:
            console.print("\n[bold]Step 3: Running data quality checks...[/bold]")
            self._validate_targets(target_dfs)

        # Step 4: Write to PostgreSQL
        console.print("\n[bold]Step 4: Writing to PostgreSQL...[/bold]")
        write_metrics = self.write_targets(
            target_dfs,
            checkpoint_location=checkpoint_location,
            mode=write_mode,
        )

        # Step 5: Export to S3/Delta (if requested)
        export_metrics = {}
        if export_to_s3 or export_to_delta:
            console.print("\n[bold]Step 5: Exporting to S3/Delta Lake...[/bold]")
            export_metrics = self._export_targets(
                target_dfs,
                export_to_s3=export_to_s3,
                export_to_delta=export_to_delta,
                s3_bucket=s3_bucket,
            )

        # Compile metrics
        metrics = {
            "mode": self.mode.value,
            "filter_condition": filter_condition,
            "target_tables": list(target_dfs.keys()),
            "write_metrics": write_metrics,
            "export_metrics": export_metrics,
        }

        if self.mode == TransformMode.BATCH:
            metrics["source_row_count"] = row_count

        console.print("\n[bold green]✓ Transformation complete![/bold green]")

        return metrics

    def _build_filter_condition(
        self,
        game_pks: Optional[List[int]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        season: Optional[str] = None,
        team_ids: Optional[List[int]] = None,
        venue_ids: Optional[List[int]] = None,
        game_states: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Build SQL WHERE clause from filter parameters.

        Args:
            game_pks: Game PKs to filter
            start_date: Start date
            end_date: End date
            season: Season filter
            team_ids: Team IDs
            venue_ids: Venue IDs
            game_states: Game states

        Returns:
            SQL WHERE clause string or None
        """
        conditions = []

        if game_pks:
            pks_str = ",".join(str(pk) for pk in game_pks)
            conditions.append(f"game_pk IN ({pks_str})")

        if start_date:
            conditions.append(f"game_date >= '{start_date.isoformat()}'")

        if end_date:
            conditions.append(f"game_date <= '{end_date.isoformat()}'")

        if season:
            conditions.append(f"season = '{season}'")

        if team_ids:
            ids_str = ",".join(str(id) for id in team_ids)
            conditions.append(
                f"(home_team_id IN ({ids_str}) OR away_team_id IN ({ids_str}))"
            )

        if venue_ids:
            ids_str = ",".join(str(id) for id in venue_ids)
            conditions.append(f"venue_id IN ({ids_str})")

        if game_states:
            states_str = ",".join(f"'{s}'" for s in game_states)
            # Extract game state from JSONB
            conditions.append(
                f"data->>'gameData'->>'status'->>'abstractGameState' IN ({states_str})"
            )

        if not conditions:
            return None

        return " AND ".join(conditions)

    def _validate_targets(self, target_dfs: Dict[str, DataFrame]) -> None:
        """Run data quality validation on target DataFrames.

        Args:
            target_dfs: Dict of table name → DataFrame

        Raises:
            ValueError: If validation fails and fail_on_quality_error=True
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            for table_name, df in target_dfs.items():
                task = progress.add_task(f"Validating {table_name}...", total=None)

                target_config = self.config.get_target(table_name)
                result = self.validator.validate(
                    df, target_config, fail_on_error=self.fail_on_quality_error
                )

                if result.is_valid():
                    progress.update(
                        task,
                        description=f"✓ {table_name} ({result.passed_checks} checks passed)",
                    )
                else:
                    progress.update(
                        task,
                        description=f"✗ {table_name} ({result.failed_checks} checks failed)",
                    )

                progress.remove_task(task)

    def _export_targets(
        self,
        target_dfs: Dict[str, DataFrame],
        export_to_s3: bool = False,
        export_to_delta: bool = False,
        s3_bucket: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Export target DataFrames to S3/Delta Lake.

        Args:
            target_dfs: Dict of table name → DataFrame
            export_to_s3: Export to S3 as Parquet
            export_to_delta: Export to Delta Lake
            s3_bucket: S3 bucket name

        Returns:
            Dict of export metrics
        """
        metrics = {}

        for table_name, df in target_dfs.items():
            target_config = self.config.get_target(table_name)

            if export_to_s3:
                s3_config = self.config.get_export_config("s3")
                if s3_config and s3_config.enabled:
                    # Build S3 path
                    bucket = s3_bucket or s3_config.bucket
                    path = f"s3://{bucket}/{s3_config.path}/{table_name}"

                    # Write as Parquet
                    df.write.mode("append").partitionBy(
                        s3_config.partition_by or []
                    ).parquet(path)

                    metrics[f"{table_name}_s3"] = {
                        "path": path,
                        "format": "parquet",
                    }

            if export_to_delta:
                delta_config = self.config.get_export_config("delta")
                if delta_config and delta_config.enabled:
                    # Build Delta path
                    path = f"{delta_config.path}/{table_name}"

                    # Write as Delta
                    df.write.format("delta").mode("append").option(
                        "mergeSchema", "true"
                    ).save(path)

                    metrics[f"{table_name}_delta"] = {
                        "path": path,
                        "format": "delta",
                    }

        return metrics
