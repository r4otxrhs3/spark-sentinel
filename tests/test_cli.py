from typer.testing import CliRunner

from spark_sentinel import __version__
from spark_sentinel.cli import app

runner = CliRunner()


def test_version_prints_package_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_doctor_finds_core_dependencies():
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0, result.output
    assert "duckdb" in result.output
    assert "MISSING" not in result.output


def test_no_arguments_shows_help():
    result = runner.invoke(app, [])
    assert "Usage" in result.output
