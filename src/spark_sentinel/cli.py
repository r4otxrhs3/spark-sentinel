"""Command-line interface: `sentinel <command>`."""

from __future__ import annotations

import platform
import shutil
from importlib import metadata

import typer

from spark_sentinel import __version__

app = typer.Typer(
    name="sentinel",
    help="Find performance problems in Spark jobs from their event logs.",
    no_args_is_help=True,
    add_completion=False,
)

CORE_DEPENDENCIES = ["typer", "orjson", "zstandard", "pyarrow", "duckdb", "jinja2"]
OPTIONAL_DEPENDENCIES = ["pyspark", "pytest", "ruff"]


def _installed_version(package: str) -> str | None:
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError:
        return None


@app.command()
def version() -> None:
    """Print the Spark Sentinel version."""
    typer.echo(f"spark-sentinel {__version__}")


@app.command()
def doctor() -> None:
    """Check that dependencies are installed and show their versions."""
    typer.echo(f"spark-sentinel  {__version__}")
    typer.echo(f"python          {platform.python_version()}")

    missing = []
    typer.echo("\ncore dependencies:")
    for package in CORE_DEPENDENCIES:
        found = _installed_version(package)
        typer.echo(f"  {package:<12} {found or 'MISSING'}")
        if found is None:
            missing.append(package)

    typer.echo("\noptional (lab jobs and development):")
    for package in OPTIONAL_DEPENDENCIES:
        typer.echo(f"  {package:<12} {_installed_version(package) or '-'}")
    typer.echo(f"  {'java':<12} {shutil.which('java') or '- (only needed to run lab jobs)'}")

    if missing:
        typer.echo(f"\nMissing core dependencies: {', '.join(missing)}", err=True)
        raise typer.Exit(code=1)
    typer.echo("\nAll core dependencies found.")
