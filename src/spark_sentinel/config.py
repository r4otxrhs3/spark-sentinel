"""Settings for `sentinel analyze`, from a sentinel.toml file and the command line.

Precedence (later wins): built-in defaults < sentinel.toml < command-line options.
sentinel.toml in the current folder is read automatically; --config picks another file.

    # sentinel.toml
    profile = "laptop"          # default | laptop
    fail_on = "high"            # never | info | low | medium | high
    format  = "text"            # text | md | html | json
    select  = ["SKEW", "SPILL001"]
    ignore  = ["PLAN002"]

    [thresholds.SKEW001]
    min_time_lost_ms = 5000

    [diff]                      # sentinel diff: ignore changes smaller than these
    min_change_pct = 25
    min_time_ms = 30000
    min_bytes = 268435456
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from spark_sentinel.findings import Severity
from spark_sentinel.profiles import PROFILES

CONFIG_FILE = "sentinel.toml"
FORMATS = ("text", "md", "html", "json")
_KNOWN_KEYS = {"profile", "fail_on", "format", "select", "ignore", "thresholds", "diff"}
_THRESHOLD_ARG = re.compile(r"^([A-Za-z]+\d{3})\.([a-z_]+)=(.+)$")


class ConfigError(ValueError):
    """A setting is invalid: shown to the user as-is."""


@dataclass
class Settings:
    profile: str = "default"
    fail_on: Severity | None = None  # None = never fail because of findings
    format: str = "text"
    select: list[str] | None = None
    ignore: list[str] = field(default_factory=list)
    thresholds: dict[str, dict[str, Any]] = field(default_factory=dict)
    diff: dict[str, Any] = field(default_factory=dict)  # sentinel diff thresholds
    source: Path | None = None  # the config file used, if any


def parse_fail_on(value: str | None) -> Severity | None:
    if value is None or value.strip().lower() in ("never", "none", ""):
        return None
    try:
        return Severity.parse(value)
    except ValueError as exc:
        raise ConfigError(f"fail_on: {exc} (or 'never')") from None


def parse_threshold(arg: str) -> tuple[str, str, str]:
    """'SKEW001.min_time_lost_ms=5000' -> ('SKEW001', 'min_time_lost_ms', '5000')."""
    match = _THRESHOLD_ARG.match(arg.strip())
    if not match:
        raise ConfigError(
            f"--threshold '{arg}' must look like RULE.name=value, "
            "e.g. SKEW001.min_time_lost_ms=5000"
        )
    rule, key, value = match.groups()
    return rule.upper(), key, value


def _split(values: list[str] | None) -> list[str]:
    """Accept repeated options and comma-separated lists alike."""
    return [v.strip() for item in values or [] for v in item.split(",") if v.strip()]


def load_settings(
    config_path: Path | None = None,
    *,
    profile: str | None = None,
    fail_on: str | None = None,
    fmt: str | None = None,
    select: list[str] | None = None,
    ignore: list[str] | None = None,
    thresholds: list[str] | None = None,
) -> Settings:
    """Combine the config file (explicit, or ./sentinel.toml if present) with CLI options."""
    settings = Settings()
    path = config_path or (Path(CONFIG_FILE) if Path(CONFIG_FILE).is_file() else None)
    if path is not None:
        _apply_file(settings, path)

    if profile is not None:
        settings.profile = profile
    if fail_on is not None:
        settings.fail_on = parse_fail_on(fail_on)
    if fmt is not None:
        settings.format = fmt
    if select:
        settings.select = _split(select)
    if ignore:
        settings.ignore = [*settings.ignore, *_split(ignore)]
    for arg in thresholds or []:
        rule, key, value = parse_threshold(arg)
        settings.thresholds.setdefault(rule, {})[key] = value

    if settings.profile not in PROFILES:
        raise ConfigError(
            f"Unknown profile '{settings.profile}'. Choose from: {', '.join(PROFILES)}"
        )
    if settings.format not in FORMATS:
        raise ConfigError(f"Unknown format '{settings.format}'. Choose from: {', '.join(FORMATS)}")
    return settings


def _apply_file(settings: Settings, path: Path) -> None:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path} is not valid TOML: {exc}") from None

    unknown = set(data) - _KNOWN_KEYS
    if unknown:
        raise ConfigError(
            f"{path}: unknown setting(s) {sorted(unknown)}. Known: {sorted(_KNOWN_KEYS)}"
        )
    settings.source = path
    settings.profile = data.get("profile", settings.profile)
    if "fail_on" in data:
        settings.fail_on = parse_fail_on(str(data["fail_on"]))
    settings.format = data.get("format", settings.format)
    if "select" in data:
        settings.select = _split(list(data["select"]))
    settings.ignore = _split(list(data.get("ignore", [])))
    for rule, values in (data.get("thresholds") or {}).items():
        if not isinstance(values, dict):
            raise ConfigError(f"{path}: [thresholds.{rule}] must be a table of name = value")
        settings.thresholds.setdefault(rule.upper(), {}).update(values)
    diff = data.get("diff") or {}
    if not isinstance(diff, dict):
        raise ConfigError(f"{path}: [diff] must be a table of name = value")
    settings.diff.update(diff)
