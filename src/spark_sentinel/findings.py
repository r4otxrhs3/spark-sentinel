"""Step 7: findings.

A Finding is what every rule returns: what is wrong, where, the numbers that prove it, how
serious it is, and how to fix it. Every rule uses this one shape, so reports, CI exit codes,
JSON output and diff mode all work for any rule.

    Finding(
        rule_id="SKEW001",
        severity=Severity.HIGH,
        title="Stage 4: slowest task is 38x the median",
        location=Location(app_id="app-1", stage_id=4, sql_execution_id=2),
        evidence=(
            Evidence("slowest task", 372_000, Unit.MS, compare_to=9_800, compare_label="median"),
            Evidence("its shuffle read", 2.1e9, Unit.BYTES, compare_to=48e6,
                     compare_label="median"),
        ),
        cause="Data skew on a join key: the slow task read 44x more data.",
        fix=("Enable spark.sql.adaptive.skewJoin.enabled.", "Salt the hot key."),
        impact_ms=362_000,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any


class Severity(IntEnum):
    """How much a finding matters. Ordered, so `severity >= Severity.MEDIUM` works."""

    INFO = 0  # worth knowing, no action needed
    LOW = 1  # small cost, fix when convenient
    MEDIUM = 2  # noticeable waste of time or money
    HIGH = 3  # dominates the run, or will break at scale

    @classmethod
    def parse(cls, text: str) -> Severity:
        """Accept 'high', 'HIGH', 'med', 'medium', ... (used by --fail-on)."""
        key = text.strip().upper()
        aliases = {"MED": "MEDIUM", "WARN": "MEDIUM", "CRIT": "HIGH", "CRITICAL": "HIGH"}
        try:
            return cls[aliases.get(key, key)]
        except KeyError:
            names = ", ".join(s.name.lower() for s in cls)
            raise ValueError(f"Unknown severity '{text}'. Choose from: {names}") from None

    @property
    def label(self) -> str:
        return {"MEDIUM": "MED"}.get(self.name, self.name)


class Unit(StrEnum):
    """How an evidence value is measured, which decides how it is printed."""

    BYTES = "bytes"
    MS = "ms"
    COUNT = "count"
    RATIO = "ratio"  # printed as 38x
    PERCENT = "percent"  # value 0-100
    TEXT = "text"


@dataclass(frozen=True)
class Evidence:
    """One measured number behind a finding, optionally against what it is compared with."""

    label: str
    value: float | int | str | None
    unit: Unit = Unit.COUNT
    compare_to: float | int | None = None
    compare_label: str | None = None  # e.g. "median", "target", "baseline"

    def __str__(self) -> str:
        text = f"{self.label} {format_value(self.value, self.unit)}"
        if self.compare_to is not None:
            vs = format_value(self.compare_to, self.unit)
            text += f" vs {self.compare_label} {vs}" if self.compare_label else f" vs {vs}"
        return text

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "value": self.value,
            "unit": self.unit.value,
            "compare_to": self.compare_to,
            "compare_label": self.compare_label,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Evidence:
        return cls(
            label=data["label"],
            value=data["value"],
            unit=Unit(data.get("unit", "count")),
            compare_to=data.get("compare_to"),
            compare_label=data.get("compare_label"),
        )


@dataclass(frozen=True)
class Location:
    """Where a problem is: the app, and as precisely as known, the stage, job and SQL query.

    Rules usually get this from AnalysisContext.locate_stage(), which follows
    stage -> job_stages -> jobs -> sql so every finding can name the query that caused it.
    """

    app_id: str
    stage_id: int | None = None
    stage_attempt: int | None = None
    job_ids: tuple[int, ...] = ()
    sql_execution_id: int | None = None
    sql_description: str | None = None
    plan_node: str | None = None  # e.g. "Scan parquet (1)"
    task_ids: tuple[int, ...] = ()  # the worst offending tasks, if relevant

    def label(self) -> str:
        """Short human label: 'Query 0 · stage 4', 'Query 2 · Scan parquet (1)', 'App'."""
        parts = []
        if self.sql_execution_id is not None:
            parts.append(f"Query {self.sql_execution_id}")
        if self.stage_id is not None:
            attempt = f" (attempt {self.stage_attempt})" if self.stage_attempt else ""
            parts.append(f"stage {self.stage_id}{attempt}")
        elif self.job_ids:
            parts.append("job " + ", ".join(map(str, self.job_ids)))
        if self.plan_node:
            parts.append(self.plan_node)
        if not parts:
            return "App"
        text = " · ".join(parts)
        return text[0].upper() + text[1:]

    def key(self) -> tuple:
        """Identity used to de-duplicate findings and to match them across runs."""
        return (self.app_id, self.sql_execution_id, self.stage_id, self.plan_node)

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "stage_id": self.stage_id,
            "stage_attempt": self.stage_attempt,
            "job_ids": list(self.job_ids),
            "sql_execution_id": self.sql_execution_id,
            "sql_description": self.sql_description,
            "plan_node": self.plan_node,
            "task_ids": list(self.task_ids),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Location:
        return cls(
            app_id=data["app_id"],
            stage_id=data.get("stage_id"),
            stage_attempt=data.get("stage_attempt"),
            job_ids=tuple(data.get("job_ids") or ()),
            sql_execution_id=data.get("sql_execution_id"),
            sql_description=data.get("sql_description"),
            plan_node=data.get("plan_node"),
            task_ids=tuple(data.get("task_ids") or ()),
        )


@dataclass(frozen=True)
class Finding:
    """One problem found by one rule in one place."""

    rule_id: str  # e.g. "SKEW001"
    severity: Severity
    title: str  # one line, starts with the place: "Stage 4: slowest task is 38x the median"
    location: Location
    evidence: tuple[Evidence, ...] = ()
    cause: str | None = None  # the diagnosis, e.g. "Data skew: the slow task read 44x more"
    fix: tuple[str, ...] = ()  # concrete actions, most effective first
    impact_ms: int | None = None  # estimated wall-clock time lost, used to rank findings
    category: str = "performance"  # skew, spill, io, plan, partitions, cost, ...
    details: dict[str, Any] = field(default_factory=dict)  # extra machine-readable data

    def __post_init__(self) -> None:
        if not self.rule_id or not self.title:
            raise ValueError("A finding needs a rule_id and a title")
        if not isinstance(self.severity, Severity):
            object.__setattr__(self, "severity", Severity(self.severity))

    def sort_key(self) -> tuple:
        """Most severe first, then biggest time impact, then stable by rule and place."""
        loc = self.location
        return (
            -self.severity,
            -(self.impact_ms or 0),
            self.rule_id,
            loc.app_id,
            -1 if loc.sql_execution_id is None else loc.sql_execution_id,
            -1 if loc.stage_id is None else loc.stage_id,
        )

    def fingerprint(self) -> str:
        """Stable ID for this problem in this place, for de-duplication and diff mode."""
        parts = [self.rule_id, *("" if p is None else str(p) for p in self.location.key())]
        return ":".join(parts)

    def headline(self) -> str:
        """'HIGH  SKEW001  Stage 4: slowest task is 38x the median'"""
        return f"{self.severity.label:<4}  {self.rule_id}  {self.title}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.name.lower(),
            "title": self.title,
            "location": self.location.to_dict(),
            "evidence": [e.to_dict() for e in self.evidence],
            "cause": self.cause,
            "fix": list(self.fix),
            "impact_ms": self.impact_ms,
            "category": self.category,
            "details": self.details,
            "fingerprint": self.fingerprint(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Finding:
        return cls(
            rule_id=data["rule_id"],
            severity=Severity.parse(data["severity"]),
            title=data["title"],
            location=Location.from_dict(data["location"]),
            evidence=tuple(Evidence.from_dict(e) for e in data.get("evidence", [])),
            cause=data.get("cause"),
            fix=tuple(data.get("fix", [])),
            impact_ms=data.get("impact_ms"),
            category=data.get("category", "performance"),
            details=data.get("details", {}),
        )


# --- formatting


def format_value(value: float | int | str | None, unit: Unit) -> str:
    if value is None:
        return "n/a"
    if unit is Unit.TEXT or isinstance(value, str):
        return str(value)
    if unit is Unit.BYTES:
        return format_bytes(value)
    if unit is Unit.MS:
        return format_duration(value)
    if unit is Unit.RATIO:
        return f"{value:.1f}x" if value < 10 else f"{value:.0f}x"
    if unit is Unit.PERCENT:
        return f"{value:.0f}%"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}"


def format_bytes(n: float) -> str:
    """1536 -> '1.5 KB', 2.1e9 -> '2.0 GB'. Binary units, as Spark's UI uses."""
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def format_duration(ms: float) -> str:
    """85 -> '85 ms', 9800 -> '9.8s', 372000 -> '6m12s', 7_500_000 -> '2h05m'."""
    ms = float(ms)
    if ms < 1000:
        return f"{ms:.0f} ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(round(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"
