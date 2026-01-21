"""CLI entry point for SFMC Inventory Tool.

Provides both TUI and command-line interfaces for running extractions.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from . import __version__
from .core.config import get_config, get_config_with_account
from .extractors import list_extractors, EXTRACTORS
from .orchestration import (
    ExtractorRunner,
    RunnerConfig,
    get_preset,
    list_presets,
    PRESETS,
)
from .output import SnapshotWriter, CSVExporter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# CLI app
app = typer.Typer(
    name="sfmc-inv2",
    help="SFMC Inventory Tool - Extract and catalog Salesforce Marketing Cloud objects",
    add_completion=False,
)

console = Console()


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"sfmc-inv2 version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        help="Show version and exit",
    ),
) -> None:
    """SFMC Inventory Tool - Extract and catalog SFMC objects."""
    pass


@app.command()
def run(
    extract: Optional[list[str]] = typer.Option(
        None,
        "--extract",
        "-e",
        help="Object types to extract (e.g., automations,data_extensions)",
    ),
    preset: Optional[str] = typer.Option(
        None,
        "--preset",
        "-p",
        help="Use preset: quick, full, content, or journey",
    ),
    account_id: Optional[str] = typer.Option(
        None,
        "--account-id",
        "-a",
        help="Override SFMC Account/MID (Business Unit ID)",
    ),
    output_dir: Path = typer.Option(
        Path("./inventory"),
        "--output-dir",
        "-o",
        help="Output directory",
    ),
    output_format: str = typer.Option(
        "json",
        "--format",
        "-f",
        help="Output format: json, csv, or both",
    ),
    no_tui: bool = typer.Option(
        False,
        "--no-tui",
        help="Skip TUI, run in CLI mode",
    ),
    include_details: bool = typer.Option(
        True,
        "--details/--no-details",
        help="Include object details",
    ),
    include_content: bool = typer.Option(
        False,
        "--content/--no-content",
        help="Include content (SQL, scripts)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Enable verbose logging",
    ),
) -> None:
    """Run inventory extraction.

    Without --extract or --preset, launches the interactive TUI.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate configuration
    config = get_config()
    errors = config.validate()
    if errors:
        console.print("[red]Configuration errors:[/red]")
        for error in errors:
            console.print(f"  - {error}")
        console.print("\nPlease set environment variables or create a .env file.")
        console.print("See .env.example for required variables.")
        raise typer.Exit(1)

    # Handle account_id override
    effective_account_id = account_id or config.account_id
    if account_id:
        # Reset client singletons when switching BUs
        from .clients.auth import reset_token_manager, get_token_manager
        from .clients.rest_client import reset_rest_client, get_rest_client
        from .clients.soap_client import reset_soap_client, get_soap_client

        reset_token_manager()
        reset_rest_client()
        reset_soap_client()

        # Create config with overridden account_id
        config = get_config_with_account(account_id)

        # Re-initialize clients with new config (important!)
        get_token_manager(config)
        get_rest_client(config)
        get_soap_client(config)

        console.print(f"[cyan]Using Business Unit (MID): {account_id}[/cyan]")

    # Determine extractors to run
    extractors: list[str] = []

    if preset:
        try:
            preset_config = get_preset(preset)
            extractors = preset_config["extractors"]
            console.print(f"Using preset: {preset} ({preset_config['description']})")
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(1)

    if extract:
        # Parse comma-separated list or multiple --extract options
        for item in extract:
            extractors.extend(e.strip() for e in item.split(","))

    # Validate extractors
    available = list_extractors()
    for name in extractors:
        if name not in available:
            console.print(f"[red]Unknown extractor: {name}[/red]")
            console.print(f"Available: {', '.join(available)}")
            raise typer.Exit(1)

    # Launch TUI or CLI mode
    if not extractors and not no_tui:
        # Launch TUI
        from .tui import run_tui
        run_tui()
    elif extractors:
        # Run in CLI mode
        asyncio.run(
            run_cli_extraction(
                extractors,
                output_dir,
                output_format,
                include_details,
                include_content,
                effective_account_id,
            )
        )
    else:
        # No extractors specified in CLI mode
        console.print("[yellow]No extractors specified.[/yellow]")
        console.print("Use --extract or --preset to specify what to extract.")
        console.print("Or run without --no-tui for interactive mode.")
        raise typer.Exit(1)


async def run_cli_extraction(
    extractors: list[str],
    output_dir: Path,
    output_format: str,
    include_details: bool,
    include_content: bool,
    account_id: Optional[str] = None,
) -> None:
    """Run extraction in CLI mode with progress display."""
    if account_id:
        config = get_config_with_account(account_id)
    else:
        config = get_config()

    console.print(f"\n[bold]SFMC Inventory Extraction[/bold]")
    console.print(f"Extractors: {', '.join(extractors)}")
    console.print(f"Output: {output_dir}")
    console.print()

    # Track progress per extractor
    progress_state: dict[str, dict] = {
        name: {"status": "Pending", "items": 0, "stage": ""}
        for name in extractors
    }

    def progress_callback(extractor: str, current: int, total: int, stage: str) -> None:
        if extractor in progress_state:
            progress_state[extractor].update({
                "status": "Running" if "Completed" not in stage else "Completed",
                "items": current if current > 0 else progress_state[extractor]["items"],
                "stage": stage,
            })

    # Configure runner
    runner_config = RunnerConfig(
        include_details=include_details,
        include_content=include_content,
        progress_callback=progress_callback,
    )

    runner = ExtractorRunner(runner_config)

    # Run with progress display
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Running extraction...", total=None)

        result = await runner.run(extractors)

        progress.update(task, description="Writing output...")

        # Write output
        writer = SnapshotWriter(
            output_dir,
            subdomain=config.subdomain,
            account_id=config.account_id,
        )
        output_path = await writer.write(result)

        # Export CSV if requested
        if output_format in ("csv", "both"):
            csv_exporter = CSVExporter(output_path / "exports")
            for name, extractor_result in result.results.items():
                if extractor_result.items:
                    csv_exporter.export(
                        extractor_result.items,
                        name,
                        f"{name}.csv",
                    )

    # Print summary
    stats = result.get_statistics()

    console.print()
    table = Table(title="Extraction Summary")
    table.add_column("Extractor", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Items", justify="right")
    table.add_column("Duration", justify="right")

    for name, extractor_result in result.results.items():
        status = "OK" if extractor_result.success else "FAILED"
        style = "green" if extractor_result.success else "red"
        table.add_row(
            name,
            f"[{style}]{status}[/{style}]",
            str(extractor_result.item_count),
            f"{extractor_result.duration_seconds:.1f}s",
        )

    console.print(table)

    console.print()
    console.print(f"[bold]Total objects:[/bold] {stats.total_objects}")
    console.print(f"[bold]Relationships:[/bold] {stats.total_relationships}")
    console.print(f"[bold]Duration:[/bold] {stats.total_duration_seconds:.1f}s")
    console.print(f"[bold]Output:[/bold] {output_path}")


@app.command()
def list_types() -> None:
    """List available object types for extraction."""
    table = Table(title="Available Object Types")
    table.add_column("Name", style="cyan")
    table.add_column("Description")

    for name in list_extractors():
        extractor_class = EXTRACTORS[name]
        table.add_row(name, extractor_class.description)

    console.print(table)


@app.command()
def list_presets_cmd() -> None:
    """List available presets."""
    table = Table(title="Available Presets")
    table.add_column("Name", style="cyan")
    table.add_column("Description")
    table.add_column("Extractors")

    for name, desc in list_presets().items():
        extractors = ", ".join(PRESETS[name]["extractors"])
        table.add_row(name, desc, extractors)

    console.print(table)


@app.command()
def check_config() -> None:
    """Check configuration and connectivity."""
    config = get_config()

    console.print("[bold]Configuration Check[/bold]\n")

    # Check required fields
    errors = config.validate()
    if errors:
        console.print("[red]Missing configuration:[/red]")
        for error in errors:
            console.print(f"  - {error}")
        console.print("\nPlease set environment variables or create a .env file.")
        raise typer.Exit(1)

    console.print(f"[green]Subdomain:[/green] {config.subdomain}")
    console.print(f"[green]Client ID:[/green] {config.client_id[:8]}...")
    if config.account_id:
        console.print(f"[green]Account ID:[/green] {config.account_id}")

    # Test authentication
    console.print("\n[bold]Testing authentication...[/bold]")

    try:
        from .clients.auth import get_token

        with console.status("Authenticating..."):
            token = get_token(config)

        console.print("[green]Authentication successful![/green]")
        console.print(f"Token: {token[:20]}...")

    except Exception as e:
        console.print(f"[red]Authentication failed:[/red] {e}")
        raise typer.Exit(1)


# Alias commands
app.command("presets")(list_presets_cmd)
app.command("types")(list_types)
app.command("check")(check_config)


if __name__ == "__main__":
    app()
