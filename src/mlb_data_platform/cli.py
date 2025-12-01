"""Command-line interface for MLB Data Platform."""

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from .ingestion.client import MLBStatsAPIClient
from .ingestion.config import StubMode, load_job_config
from .ingestion.template import resolve_config
from .schema.registry import get_registry
from .storage.postgres import PostgresConfig, PostgresStorageBackend

app = typer.Typer(
    name="mlb-etl",
    help="MLB Stats API Data Platform - ETL CLI",
    add_completion=False,
)
console = Console()


@app.command()
def ingest(
    job: str = typer.Option(..., "--job", "-j", help="Path to job configuration YAML"),
    stub_mode: StubMode = typer.Option(
        StubMode.PASSTHROUGH, "--stub-mode", help="Stub mode: capture, replay, passthrough"
    ),
    game_pks: str = typer.Option("", "--game-pks", help="Comma-separated game PKs (for parallel workflows)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be ingested without saving"),
    save: bool = typer.Option(False, "--save", help="Save ingested data to PostgreSQL"),
    db_host: str = typer.Option("localhost", "--db-host", help="PostgreSQL host"),
    db_port: int = typer.Option(5432, "--db-port", help="PostgreSQL port"),
    db_name: str = typer.Option("mlb_games", "--db-name", help="PostgreSQL database name"),
    db_user: str = typer.Option("mlb_admin", "--db-user", help="PostgreSQL user"),
    db_password: str = typer.Option("mlb_admin_password", "--db-password", help="PostgreSQL password"),
    upsert: bool = typer.Option(False, "--upsert", help="Use UPSERT instead of INSERT (requires primary keys)"),
):
    """Run data ingestion job."""
    console.print(f"[bold green]Starting ingestion job:[/bold green] {job}")
    console.print(f"Stub mode: [cyan]{stub_mode.value}[/cyan]")

    if save:
        console.print(f"Save to PostgreSQL: [green]enabled[/green] ({db_host}:{db_port}/{db_name})")

    try:
        # Prepare template variables
        template_vars = {}
        if game_pks:
            # For single game_pk (first in list)
            pk_list = [int(pk.strip()) for pk in game_pks.split(",")]
            if pk_list:
                template_vars["GAME_PK"] = pk_list[0]
                template_vars["GAME_PKS"] = game_pks

        # Load job configuration with variable resolution
        job_config = load_job_config(job, resolve_vars=True, **template_vars)
        console.print(f"✓ Loaded job config: [yellow]{job_config.name}[/yellow]")
        console.print(f"  Type: {job_config.type.value}")
        console.print(f"  Endpoint: {job_config.source.endpoint}.{job_config.source.method}")

        # Create client
        client = MLBStatsAPIClient(job_config, stub_mode=stub_mode)

        # Create storage backend if saving
        storage_backend = None
        if save and not dry_run:
            pg_config = PostgresConfig(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password,
            )
            storage_backend = PostgresStorageBackend(pg_config)
            console.print("[green]✓ Connected to PostgreSQL[/green]")

        # Display schema info
        schema_info = client.get_schema_info()
        if schema_info["schema_found"]:
            console.print("\n[bold]Schema Information:[/bold]")
            console.print(f"  Table: [cyan]{schema_info['schema_name']}[/cyan]")
            console.print(f"  Version: {schema_info['version']}")
            console.print(f"  Primary Keys: {', '.join(schema_info['primary_keys'])}")
            console.print(f"  Partition Keys: {', '.join(schema_info['partition_keys'])}")
            console.print(f"  Fields: {schema_info['num_fields']}")

            if schema_info['relationships']:
                console.print(f"\n  [bold]Relationships ({len(schema_info['relationships'])}):[/bold]")
                for rel in schema_info['relationships']:
                    console.print(f"    {rel['from']} -> {rel['to']} ({rel['type']}) on {rel['on']}")
        else:
            console.print("[yellow]  ⚠ No schema metadata found[/yellow]")

        # Handle multiple game_pks (for parallel execution)
        if game_pks:
            game_pk_list = [pk.strip() for pk in game_pks.split(",")]
            console.print(f"\n[bold]Fetching data for {len(game_pk_list)} games...[/bold]")

            for i, game_pk in enumerate(game_pk_list, 1):
                console.print(f"\n[{i}/{len(game_pk_list)}] Game PK: {game_pk}")

                if storage_backend:
                    result = client.fetch_and_save(
                        storage_backend=storage_backend,
                        upsert=upsert,
                        game_pk=game_pk
                    )
                    console.print(f"[green]✓ Saved to database (row_id: {result['row_id']})[/green]")
                else:
                    result = client.fetch(game_pk=game_pk)

                _display_ingestion_result(result, dry_run)
        else:
            # Single fetch
            console.print("\n[bold]Fetching data...[/bold]")

            if storage_backend:
                result = client.fetch_and_save(
                    storage_backend=storage_backend,
                    upsert=upsert,
                )
                console.print(f"[green]✓ Saved to database (row_id: {result['row_id']})[/green]")
            else:
                result = client.fetch()

            _display_ingestion_result(result, dry_run)

        # Close storage backend
        if storage_backend:
            storage_backend.close()

        if dry_run:
            console.print("\n[yellow]⚠ Dry run - data not saved[/yellow]")
        elif save:
            console.print("\n[green]✓ Ingestion and storage complete[/green]")
        else:
            console.print("\n[green]✓ Ingestion complete (not saved)[/green]")

    except FileNotFoundError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())
        raise typer.Exit(1)


def _display_ingestion_result(result: dict, dry_run: bool = False):
    """Display ingestion result in a nice format."""
    metadata = result["metadata"]
    data = result["data"]
    extracted = result.get("extracted_fields", {})

    # Request info
    req_info = Table(title="Request", show_header=False, box=None)
    req_info.add_column("Key", style="cyan")
    req_info.add_column("Value")
    req_info.add_row("URL", metadata["request"]["url"])
    req_info.add_row("Timestamp", metadata["request"]["timestamp"])
    req_info.add_row("Parameters", json.dumps(metadata["request"]["query_params"], indent=2))
    console.print(req_info)

    # Response info
    console.print(f"\n[bold]Response:[/bold]")
    console.print(f"  Status: [green]{metadata['response']['status_code']}[/green]")
    console.print(f"  Duration: {metadata['response']['elapsed_ms']:.2f}ms")
    console.print(f"  Captured: {metadata['response']['captured_at']}")

    # Extracted fields
    if extracted:
        console.print(f"\n[bold]Extracted Fields:[/bold]")
        for key, value in extracted.items():
            console.print(f"  [cyan]{key}[/cyan]: {value}")

    # Data preview (first few keys)
    if isinstance(data, dict):
        console.print(f"\n[bold]Data Preview:[/bold]")
        data_tree = Tree("[cyan]Response Data[/cyan]")

        for i, (key, value) in enumerate(data.items()):
            if i >= 5:  # Limit to first 5 keys
                data_tree.add(f"... ({len(data) - 5} more keys)")
                break

            if isinstance(value, dict):
                data_tree.add(f"{key}: {{object}} with {len(value)} keys")
            elif isinstance(value, list):
                data_tree.add(f"{key}: [array] with {len(value)} items")
            else:
                value_str = str(value)[:50]
                data_tree.add(f"{key}: {value_str}")

        console.print(data_tree)


@app.command()
def transform(
    job: str = typer.Option(..., "--job", "-j", help="Path to job configuration YAML"),
    spark_master: str = typer.Option("local[*]", "--spark-master", help="Spark master URL"),
):
    """Run PySpark transformation job."""
    console.print(f"[bold green]Starting transform job:[/bold green] {job}")
    console.print(f"Spark master: {spark_master}")

    # TODO: Implement transform logic
    console.print("[yellow]Transform not yet implemented[/yellow]")


@app.command()
def schema(
    action: str = typer.Argument(..., help="Action: list, show, generate, validate"),
    endpoint: str = typer.Option("", "--endpoint", "-e", help="Endpoint name"),
    method: str = typer.Option("", "--method", "-m", help="Method name"),
    output: str = typer.Option("", "--output", "-o", help="Output file path"),
):
    """Manage Avro schemas."""
    console.print(f"[bold green]Schema action:[/bold green] {action}")

    registry = get_registry()

    if action == "list":
        table = Table(title="Available Schemas")
        table.add_column("Schema Name", style="cyan")
        table.add_column("Endpoint", style="magenta")
        table.add_column("Method", style="green")
        table.add_column("Version", style="yellow")
        table.add_column("Fields", justify="right")
        table.add_column("Relationships", justify="right")

        for schema_name in sorted(registry.list_schemas()):
            schema_meta = registry.get_schema(schema_name)
            if schema_meta:
                table.add_row(
                    schema_name,
                    schema_meta.endpoint,
                    schema_meta.method,
                    schema_meta.version,
                    str(len(schema_meta.fields)),
                    str(len(schema_meta.relationships)),
                )

        console.print(table)

    elif action == "show":
        if not endpoint or not method:
            console.print("[red]Error: --endpoint and --method required for 'show'[/red]")
            raise typer.Exit(1)

        schema_meta = registry.get_schema_by_endpoint(endpoint, method)
        if not schema_meta:
            console.print(f"[red]Schema not found for {endpoint}.{method}[/red]")
            raise typer.Exit(1)

        # Display detailed schema info
        panel = Panel(
            f"""[bold]Schema:[/bold] {schema_meta.schema_name}
[bold]Endpoint:[/bold] {schema_meta.endpoint}
[bold]Method:[/bold] {schema_meta.method}
[bold]Version:[/bold] {schema_meta.version}
[bold]Description:[/bold] {schema_meta.description or 'N/A'}

[bold]Primary Keys:[/bold] {', '.join(schema_meta.primary_keys)}
[bold]Partition Keys:[/bold] {', '.join(schema_meta.partition_keys)}
""",
            title=f"Schema: {schema_meta.schema_name}",
            expand=False,
        )
        console.print(panel)

        # Fields table
        fields_table = Table(title="Fields", show_header=True)
        fields_table.add_column("Name", style="cyan")
        fields_table.add_column("Type", style="green")
        fields_table.add_column("Nullable", justify="center")
        fields_table.add_column("PK", justify="center")
        fields_table.add_column("FK", justify="center")
        fields_table.add_column("Indexed", justify="center")
        fields_table.add_column("JSONPath", style="yellow")

        for field in schema_meta.fields:
            fields_table.add_row(
                field.name,
                field.type,
                "✓" if field.nullable else "✗",
                "✓" if field.is_primary_key else "",
                "✓" if field.is_foreign_key else "",
                "✓" if field.is_indexed else "",
                field.json_path or "",
            )

        console.print(fields_table)

        # Relationships
        if schema_meta.relationships:
            rel_table = Table(title="Relationships")
            rel_table.add_column("From Schema", style="cyan")
            rel_table.add_column("To Schema", style="magenta")
            rel_table.add_column("Type", style="green")
            rel_table.add_column("On", style="yellow")

            for rel in schema_meta.relationships:
                rel_table.add_row(
                    rel.from_schema,
                    rel.to_schema,
                    rel.relationship_type.value,
                    f"{rel.from_field} -> {rel.to_field}",
                )

            console.print(rel_table)

    elif action == "generate":
        if not endpoint or not method:
            console.print("[red]Error: --endpoint and --method required for 'generate'[/red]")
            raise typer.Exit(1)

        schema_meta = registry.get_schema_by_endpoint(endpoint, method)
        if not schema_meta:
            console.print(f"[red]Schema not found for {endpoint}.{method}[/red]")
            raise typer.Exit(1)

        avro_schema = registry.generate_avro_schema(schema_meta.schema_name)
        if not avro_schema:
            console.print("[red]Failed to generate Avro schema[/red]")
            raise typer.Exit(1)

        if output:
            output_path = Path(output)
            registry.save_avro_schema(schema_meta.schema_name, output_path)
            console.print(f"[green]✓ Avro schema saved to: {output_path}[/green]")
        else:
            console.print(JSON(json.dumps(avro_schema, indent=2)))

    else:
        console.print(f"[yellow]Action '{action}' not yet implemented[/yellow]")


@app.command()
def db(
    action: str = typer.Argument(..., help="Action: init, migrate, status"),
):
    """Database management."""
    console.print(f"[bold green]Database action:[/bold green] {action}")

    # TODO: Implement database management
    console.print("[yellow]Database management not yet implemented[/yellow]")


@app.command()
def workflow(
    action: str = typer.Argument(..., help="Action: submit, list, logs"),
    name: str = typer.Option("", "--name", "-n", help="Workflow name"),
):
    """Argo Workflows management."""
    console.print(f"[bold green]Workflow action:[/bold green] {action}")

    # TODO: Implement Argo Workflows integration
    console.print("[yellow]Workflow management not yet implemented[/yellow]")


@app.command()
def venue(
    action: str = typer.Argument(..., help="Action: fetch-park-factors, fetch-seamheads, list-venues, list-park-factors, refresh-views"),
    season: int = typer.Option(2024, "--season", "-s", help="Season year"),
    venue_id: int = typer.Option(None, "--venue-id", help="Filter by venue ID"),
    park_id: str = typer.Option(None, "--park-id", help="Seamheads park ID (e.g., BOS07)"),
    stat_type: str = typer.Option(None, "--stat-type", help="Filter by stat type (HR, 2B, etc.)"),
    rolling: int = typer.Option(1, "--rolling", help="Rolling average (1=single season, 3=three-year)"),
    active_only: bool = typer.Option(True, "--active-only/--all", help="Fetch only active ballparks"),
    db_host: str = typer.Option("localhost", "--db-host", help="PostgreSQL host"),
    db_port: int = typer.Option(65254, "--db-port", help="PostgreSQL port"),
    db_name: str = typer.Option("mlb_games", "--db-name", help="PostgreSQL database name"),
    db_user: str = typer.Option("mlb_admin", "--db-user", help="PostgreSQL user"),
    db_password: str = typer.Option("mlb_admin_password", "--db-password", help="PostgreSQL password"),
):
    """Venue enrichment operations."""
    from .venue import BaseballSavantScraper, SeamheadsScraper, VenueStorageBackend

    console.print(f"[bold green]Venue action:[/bold green] {action}")

    # Setup PostgreSQL connection
    pg_config = PostgresConfig(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
    )
    postgres = PostgresStorageBackend(pg_config)
    venue_storage = VenueStorageBackend(postgres)

    if action == "fetch-park-factors":
        console.print(f"Fetching park factors for season [cyan]{season}[/cyan] (rolling={rolling})")

        try:
            with BaseballSavantScraper() as scraper:
                park_factors = scraper.fetch_park_factors(season=season, rolling=rolling)

                console.print(f"[green]✓[/green] Scraped {len(park_factors)} park factors")

                # Save to database
                count = venue_storage.upsert_park_factors(park_factors)
                console.print(f"[green]✓[/green] Saved {count} park factors to database")

                # Refresh materialized views
                console.print("Refreshing materialized views...")
                venue_storage.refresh_materialized_views()
                console.print("[green]✓[/green] Materialized views refreshed")

                # Show sample
                table = Table(title=f"Park Factors - Season {season}")
                table.add_column("Venue", style="cyan")
                table.add_column("Stat", style="magenta")
                table.add_column("Factor", justify="right", style="yellow")
                table.add_column("Sample", justify="right")

                for factor in park_factors[:10]:  # Show first 10
                    table.add_row(
                        factor.venue_name,
                        factor.stat_type,
                        f"{factor.park_factor:.1f}",
                        str(factor.sample_size or "N/A"),
                    )

                if len(park_factors) > 10:
                    console.print(f"Showing 10 of {len(park_factors)} park factors")

                console.print(table)

        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)

    elif action == "list-venues":
        console.print("Retrieving venues from database...")

        venues = venue_storage.get_venues(active_only=True)

        table = Table(title="MLB Venues")
        table.add_column("ID", justify="right", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("City", style="magenta")
        table.add_column("State", style="yellow")
        table.add_column("Capacity", justify="right")
        table.add_column("CF Distance", justify="right")

        for venue in venues:
            table.add_row(
                str(venue["venue_id"]),
                venue["name"],
                venue["city"] or "N/A",
                venue["state"] or "N/A",
                str(venue["capacity"]) if venue["capacity"] else "N/A",
                f"{venue['center_field']}'" if venue["center_field"] else "N/A",
            )

        console.print(table)
        console.print(f"Total venues: {len(venues)}")

    elif action == "list-park-factors":
        console.print(
            f"Retrieving park factors (venue_id={venue_id}, season={season}, stat_type={stat_type})..."
        )

        factors = venue_storage.get_park_factors(
            venue_id=venue_id,
            season=season,
            stat_type=stat_type,
        )

        table = Table(title="Park Factors")
        table.add_column("Venue", style="cyan")
        table.add_column("Season", justify="right", style="yellow")
        table.add_column("Stat", style="magenta")
        table.add_column("Factor", justify="right", style="green")
        table.add_column("Sample", justify="right")

        for factor in factors:
            table.add_row(
                factor["venue_name"],
                str(factor["season"]),
                factor["stat_type"],
                f"{factor['park_factor']:.1f}",
                str(factor["sample_size"]) if factor["sample_size"] else "N/A",
            )

        console.print(table)
        console.print(f"Total park factors: {len(factors)}")

    elif action == "fetch-seamheads":
        console.print("Fetching ballpark data from Seamheads.com...")

        try:
            with SeamheadsScraper() as scraper:
                if park_id:
                    # Fetch specific ballpark
                    console.print(f"Fetching park_id: [cyan]{park_id}[/cyan]")
                    venues = [scraper.fetch_ballpark_by_id(park_id)]
                else:
                    # Fetch all ballparks
                    console.print(f"Fetching {'active' if active_only else 'all'} ballparks...")
                    venues = scraper.fetch_all_ballparks(active_only=active_only)

                console.print(f"[green]✓[/green] Scraped {len(venues)} ballpark(s)")

                # Save to database
                saved_count = 0
                for venue in venues:
                    try:
                        venue_storage.upsert_venue(venue)
                        saved_count += 1
                    except Exception as e:
                        console.print(f"[yellow]Warning:[/yellow] Failed to save {venue.name}: {e}")

                console.print(f"[green]✓[/green] Saved {saved_count}/{len(venues)} venue(s) to database")

                # Show sample
                table = Table(title="Fetched Venues")
                table.add_column("Name", style="cyan")
                table.add_column("City", style="magenta")
                table.add_column("State", style="yellow")
                table.add_column("CF Distance", justify="right")
                table.add_column("Capacity", justify="right")

                for venue in venues[:10]:  # Show first 10
                    cf_dist = (
                        f"{venue.dimensions.center_field}'"
                        if venue.dimensions and venue.dimensions.center_field
                        else "N/A"
                    )
                    capacity = str(venue.capacity) if venue.capacity else "N/A"

                    table.add_row(
                        venue.name,
                        venue.city or "N/A",
                        venue.state or "N/A",
                        cf_dist,
                        capacity,
                    )

                if len(venues) > 10:
                    console.print(f"Showing 10 of {len(venues)} venues")

                console.print(table)

        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)

    elif action == "refresh-views":
        console.print("Refreshing materialized views...")
        venue_storage.refresh_materialized_views()
        console.print("[green]✓[/green] Materialized views refreshed")

    else:
        console.print(f"[red]Unknown action:[/red] {action}")
        console.print("Valid actions: fetch-park-factors, fetch-seamheads, list-venues, list-park-factors, refresh-views")
        raise typer.Exit(code=1)


@app.command()
def pipeline(
    action: str = typer.Argument(..., help="Action: daily, backfill, games, status"),
    target_date: str = typer.Option("", "--date", "-d", help="Target date (YYYY-MM-DD)"),
    season: str = typer.Option("", "--season", "-s", help="Season year for backfill (e.g., 2024)"),
    start_date: str = typer.Option("", "--start", help="Start date for range (YYYY-MM-DD)"),
    end_date: str = typer.Option("", "--end", help="End date for range (YYYY-MM-DD)"),
    game_pks: str = typer.Option("", "--game-pks", "-g", help="Comma-separated game PKs"),
    sport_id: int = typer.Option(1, "--sport-id", help="Sport ID (1=MLB)"),
    save: bool = typer.Option(False, "--save", help="Save data to PostgreSQL"),
    upsert: bool = typer.Option(True, "--upsert/--insert", help="Use upsert (default) or insert"),
    enrich: bool = typer.Option(True, "--enrich/--no-enrich", help="Fetch player/team enrichment"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be fetched"),
    db_host: str = typer.Option("localhost", "--db-host", help="PostgreSQL host"),
    db_port: int = typer.Option(5432, "--db-port", help="PostgreSQL port"),
    db_name: str = typer.Option("mlb_games", "--db-name", help="PostgreSQL database name"),
    db_user: str = typer.Option("mlb_admin", "--db-user", help="PostgreSQL user"),
    db_password: str = typer.Option("mlb_admin_password", "--db-password", help="PostgreSQL password"),
):
    """Run pipeline orchestration commands.

    Actions:
        daily    - Fetch today's schedule and games
        backfill - Backfill a season or date range
        games    - Fetch specific games by game_pk
        status   - Show pipeline status and statistics
    """
    from datetime import date as dt_date
    from datetime import datetime

    from pymlb_statsapi import StatsAPI

    from .pipeline import PipelineConfig, PipelineOrchestrator, create_storage_callback

    console.print(f"[bold green]Pipeline action:[/bold green] {action}")

    # Parse dates
    parsed_target_date = None
    if target_date:
        parsed_target_date = dt_date.fromisoformat(target_date)

    parsed_start_date = None
    if start_date:
        parsed_start_date = dt_date.fromisoformat(start_date)

    parsed_end_date = None
    if end_date:
        parsed_end_date = dt_date.fromisoformat(end_date)

    # Parse game_pks
    game_pk_list = []
    if game_pks:
        game_pk_list = [int(pk.strip()) for pk in game_pks.split(",")]

    # Setup storage if saving
    adapter = None
    backend = None
    storage_callback = None

    if save and not dry_run:
        adapter, backend = create_storage_callback(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            upsert=upsert,
        )
        storage_callback = adapter.store
        console.print(f"[green]✓ Connected to PostgreSQL[/green] ({db_host}:{db_port}/{db_name})")

    # Create pipeline config
    config = PipelineConfig(
        sport_id=sport_id,
        enrich_players=enrich,
        enrich_teams=enrich,
    )

    # Create orchestrator
    api = StatsAPI()
    orchestrator = PipelineOrchestrator(
        api=api,
        config=config,
        storage_callback=storage_callback,
    )

    try:
        if action == "daily":
            target = parsed_target_date or dt_date.today()
            console.print(f"Running daily pipeline for: [cyan]{target}[/cyan]")

            if dry_run:
                console.print("[yellow]Dry run - showing schedule only[/yellow]")
                games = orchestrator.fetch_schedule(target)
                _display_schedule(games)
            else:
                result = orchestrator.run_daily(target)
                _display_pipeline_result(result, adapter)

        elif action == "backfill":
            if season:
                console.print(f"Backfilling season: [cyan]{season}[/cyan]")
                result = orchestrator.backfill_season(
                    season_id=season,
                    start_date=parsed_start_date,
                    end_date=parsed_end_date,
                )
            elif parsed_start_date and parsed_end_date:
                console.print(
                    f"Backfilling range: [cyan]{parsed_start_date}[/cyan] to [cyan]{parsed_end_date}[/cyan]"
                )
                result = orchestrator.backfill_season(
                    start_date=parsed_start_date,
                    end_date=parsed_end_date,
                )
            else:
                console.print("[red]Error: Specify --season or --start/--end for backfill[/red]")
                raise typer.Exit(1)

            _display_pipeline_result(result, adapter)

        elif action == "games":
            if not game_pk_list:
                console.print("[red]Error: Specify --game-pks for games action[/red]")
                raise typer.Exit(1)

            console.print(f"Fetching {len(game_pk_list)} games: {game_pk_list}")

            from .pipeline import PipelineResult

            result = PipelineResult()

            for game_pk in game_pk_list:
                try:
                    game_data = orchestrator.fetch_game(game_pk)
                    result.games_fetched += 1
                    result.game_pks.append(game_pk)
                    console.print(f"[green]✓[/green] Fetched game {game_pk}")

                    if enrich:
                        enrichment = orchestrator.enrich_from_game(game_data)
                        result.players_fetched += enrichment.players_fetched
                        result.teams_fetched += enrichment.teams_fetched

                except Exception as e:
                    result.errors.append(f"Game {game_pk}: {e}")
                    console.print(f"[red]✗[/red] Failed game {game_pk}: {e}")

            result.finished_at = datetime.now()
            _display_pipeline_result(result, adapter)

        elif action == "status":
            console.print("[bold]Pipeline Status[/bold]")

            # Show current season info
            current_season = orchestrator.get_current_season()
            if current_season:
                console.print(f"\n[bold]Current Season:[/bold]")
                console.print(f"  Season ID: [cyan]{current_season.season_id}[/cyan]")
                console.print(f"  Regular Season: {current_season.regular_start} to {current_season.regular_end}")
                if current_season.spring_start:
                    console.print(f"  Spring Training: {current_season.spring_start} to {current_season.spring_end}")

            # Show today's schedule
            today_games = orchestrator.fetch_schedule()
            console.print(f"\n[bold]Today's Games:[/bold] {len(today_games)}")
            _display_schedule(today_games, limit=5)

        else:
            console.print(f"[red]Unknown action:[/red] {action}")
            console.print("Valid actions: daily, backfill, games, status")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Pipeline error:[/red] {e}")
        import traceback
        console.print(traceback.format_exc())
        raise typer.Exit(1)

    finally:
        if backend:
            backend.close()
            console.print("[dim]Closed database connection[/dim]")


def _display_schedule(games: list, limit: int = 0):
    """Display schedule games in a table."""
    table = Table(title="Schedule")
    table.add_column("Game PK", style="cyan", justify="right")
    table.add_column("Status", style="green")
    table.add_column("Away", style="magenta")
    table.add_column("Home", style="yellow")
    table.add_column("Time", style="dim")

    display_games = games[:limit] if limit else games

    for game in display_games:
        table.add_row(
            str(game.game_pk),
            game.status,
            game.away_team,
            game.home_team,
            game.game_datetime.strftime("%H:%M") if game.game_datetime else "TBD",
        )

    if limit and len(games) > limit:
        table.add_row("...", f"+{len(games) - limit} more", "", "", "")

    console.print(table)


def _display_pipeline_result(result, adapter=None):
    """Display pipeline execution result."""
    console.print("\n[bold]Pipeline Result[/bold]")

    table = Table(show_header=False, box=None)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    table.add_row("Duration", f"{result.duration_seconds:.1f}s")
    table.add_row("Schedules fetched", str(result.schedules_fetched))
    table.add_row("Games fetched", str(result.games_fetched))
    table.add_row("Timestamps fetched", str(result.timestamps_fetched))
    table.add_row("Players fetched", str(result.players_fetched))
    table.add_row("Teams fetched", str(result.teams_fetched))

    if result.errors:
        table.add_row("Errors", f"[red]{len(result.errors)}[/red]")

    console.print(table)

    # Show storage stats if adapter provided
    if adapter:
        stats = adapter.get_stats()
        console.print("\n[bold]Storage Stats[/bold]")
        storage_table = Table(show_header=False, box=None)
        storage_table.add_column("Metric", style="cyan")
        storage_table.add_column("Value", justify="right")

        storage_table.add_row("Inserts", str(stats["inserts"]))
        storage_table.add_row("Upserts", str(stats["upserts"]))
        storage_table.add_row("Errors", str(stats["errors"]))

        if stats["by_table"]:
            storage_table.add_row("", "")
            storage_table.add_row("[bold]By Table[/bold]", "")
            for table_name, count in stats["by_table"].items():
                storage_table.add_row(f"  {table_name}", str(count))

        console.print(storage_table)

    # Show errors if any
    if result.errors:
        console.print("\n[bold red]Errors:[/bold red]")
        for error in result.errors[:10]:  # Limit to first 10
            console.print(f"  [red]•[/red] {error}")
        if len(result.errors) > 10:
            console.print(f"  ... and {len(result.errors) - 10} more errors")

    console.print("\n[green]✓ Pipeline complete[/green]")


@app.command()
def version():
    """Show version information."""
    from . import __version__

    console.print(f"[bold]MLB Data Platform[/bold] version: [green]{__version__}[/green]")
    console.print("Python: 3.11+")
    console.print("PySpark: 3.5+")


if __name__ == "__main__":
    app()
