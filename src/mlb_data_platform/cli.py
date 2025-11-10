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
from .schema.registry import get_registry

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
):
    """Run data ingestion job."""
    console.print(f"[bold green]Starting ingestion job:[/bold green] {job}")
    console.print(f"Stub mode: [cyan]{stub_mode.value}[/cyan]")

    try:
        # Load job configuration
        job_config = load_job_config(job)
        console.print(f"✓ Loaded job config: [yellow]{job_config.name}[/yellow]")
        console.print(f"  Type: {job_config.type.value}")
        console.print(f"  Endpoint: {job_config.source.endpoint}.{job_config.source.method}")

        # Create client
        client = MLBStatsAPIClient(job_config, stub_mode=stub_mode)

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
                result = client.fetch(game_pk=game_pk)
                _display_ingestion_result(result, dry_run)
        else:
            # Single fetch
            console.print("\n[bold]Fetching data...[/bold]")
            result = client.fetch()
            _display_ingestion_result(result, dry_run)

        if dry_run:
            console.print("\n[yellow]⚠ Dry run - data not saved[/yellow]")
        else:
            console.print("\n[green]✓ Ingestion complete[/green]")

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
def version():
    """Show version information."""
    from . import __version__

    console.print(f"[bold]MLB Data Platform[/bold] version: [green]{__version__}[/green]")
    console.print("Python: 3.11+")
    console.print("PySpark: 3.5+")


if __name__ == "__main__":
    app()
