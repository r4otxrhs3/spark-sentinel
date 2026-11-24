"""Step 7: running rules against stored apps.

    result = analyze(Warehouse(".sentinel"))                       # every rule, every app
    result = analyze(wh, app_ids=["local-123"], select=["SKEW001"],
                     overrides={"SKEW001": {"min_time_lost_ms": 1000}})
    for finding in result.findings: print(finding.headline())
    sys.exit(result.exit_code(fail_on=Severity.HIGH))

Each rule gets an AnalysisContext for one app: SQL access to that app's tables, its Spark
config (with Spark's defaults filled in), and locate_stage(), which traces a stage to the
jobs and SQL query that ran it.
"""

from __future__ import annotations

import re
import traceback
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any

import duckdb

from spark_sentinel.findings import Finding, Location, Severity
from spark_sentinel.plan import PlanNode, final_nodes
from spark_sentinel.rules import Rule, load_builtin_rules
from spark_sentinel.store import Warehouse, run_query

# Spark's defaults for settings the rules look at. An event log only lists settings that were
# set explicitly, so a missing key means "the default", not "off". (Spark 3.2+ / 4.x values.)
SPARK_DEFAULTS: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.autoBroadcastJoinThreshold": "10MB",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.files.maxPartitionBytes": "128MB",
    "spark.sql.files.openCostInBytes": "4MB",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.memory.fraction": "0.6",
    "spark.memory.storageFraction": "0.5",
    "spark.dynamicAllocation.enabled": "false",
}

RESERVED_MEMORY_BYTES = 300 * 1024 * 1024  # Spark keeps 300 MB of each JVM heap for itself

_SIZE_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*([kmgtp]?)(i?b)?\s*$", re.IGNORECASE)
_SIZE_POWER = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4, "p": 5}


def parse_size(text: str | None, default_unit: str = "b") -> int | None:
    """Spark size strings to bytes: '10MB', '10m', '512k', '1g', '10485760', '-1'.

    Spark sizes are binary (1m = 1024 * 1024). A bare number uses default_unit
    (memory settings like spark.executor.memory=4096 mean MB, so pass 'm' for those).
    """
    if text is None:
        return None
    match = _SIZE_RE.match(str(text))
    if not match:
        return None
    number, prefix, suffix = match.groups()
    if not prefix and not suffix:
        prefix = default_unit.lower().rstrip("b")
    return int(float(number) * 1024 ** _SIZE_POWER[prefix.lower()])


class AnalysisContext:
    """What a rule sees: one app's tables, config and helpers."""

    def __init__(self, con: duckdb.DuckDBPyConnection, app_id: str) -> None:
        self.con = con
        self.app_id = app_id

    # --- data

    def rows(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Run SQL against this app's tables (tasks, stages, ...); rows come back as dicts."""
        columns, rows = run_query(self.con, sql, params)
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def scalar(self, sql: str, params: list[Any] | None = None) -> Any:
        rows = run_query(self.con, sql, params)[1]
        return rows[0][0] if rows else None

    @cached_property
    def app(self) -> dict[str, Any]:
        found = self.rows("SELECT * FROM app")
        return found[0] if found else {"app_id": self.app_id}

    @cached_property
    def config(self) -> dict[str, str]:
        """Settings that were set explicitly for this app."""
        return {r["key"]: r["value"] for r in self.rows("SELECT key, value FROM config")}

    def conf(self, key: str, default: str | None = None) -> str | None:
        """A setting's effective value: explicit value, else Spark's default, else `default`."""
        return self.config.get(key, SPARK_DEFAULTS.get(key, default))

    def conf_bool(self, key: str, default: bool = False) -> bool:
        value = self.conf(key)
        return default if value is None else value.strip().lower() == "true"

    def conf_bytes(self, key: str, default_unit: str = "b") -> int | None:
        return parse_size(self.conf(key), default_unit)

    def conf_is_default(self, key: str) -> bool:
        return key not in self.config

    # --- plans

    @cached_property
    def plans(self) -> dict[int, list[PlanNode]]:
        """SQL execution id -> the nodes of its final physical plan (what actually ran)."""
        rows = self.rows("SELECT execution_id, final_plan FROM sql ORDER BY execution_id")
        return {r["execution_id"]: final_nodes(r["final_plan"]) for r in rows}

    def plan_for_stage(self, stage_id: int) -> list[PlanNode]:
        """Final plan nodes of the SQL query that ran this stage ([] for non-SQL stages)."""
        exec_id = self._stage_links.get(stage_id, {}).get("sql_execution_id")
        return self.plans.get(exec_id, []) if exec_id is not None else []

    # --- resources

    @cached_property
    def is_local(self) -> bool:
        return str(self.conf("spark.master", "")).startswith("local")

    @cached_property
    def executor_cores(self) -> int:
        """Cores (task slots) per executor. In local mode the driver is the only executor."""
        where = "" if self.is_local else "WHERE executor_id <> 'driver'"
        cores = self.scalar(f"SELECT max(cores) FROM executors {where}")
        return int(cores or self.conf("spark.executor.cores") or 1)

    @cached_property
    def total_cores(self) -> int:
        where = "" if self.is_local else "WHERE executor_id <> 'driver'"
        cores = self.scalar(f"SELECT sum(cores) FROM executors {where}")
        return int(cores or self.executor_cores)

    @cached_property
    def executor_memory_bytes(self) -> int:
        key = "spark.driver.memory" if self.is_local else "spark.executor.memory"
        return self.conf_bytes(key, default_unit="m") or 1024**3

    @cached_property
    def memory_per_task_bytes(self) -> int:
        """Rough execution memory one task can use: (heap - 300 MB) x memory.fraction / cores."""
        fraction = float(self.conf("spark.memory.fraction") or 0.6)
        usable = max(self.executor_memory_bytes - RESERVED_MEMORY_BYTES, 0) * fraction
        return int(usable / max(self.executor_cores, 1))

    # --- locations

    @cached_property
    def _stage_links(self) -> dict[int, dict[str, Any]]:
        """stage_id -> {job_ids, sql_execution_id, sql_description}, via job_stages and jobs."""
        rows = self.rows(
            """
            SELECT js.stage_id,
                   list(DISTINCT js.job_id ORDER BY js.job_id)  AS job_ids,
                   min(j.sql_execution_id)                     AS sql_execution_id,
                   min(q.description)                          AS sql_description
            FROM job_stages js
            JOIN jobs j     ON j.job_id = js.job_id
            LEFT JOIN sql q ON q.execution_id = j.sql_execution_id
            GROUP BY js.stage_id
            """
        )
        return {r["stage_id"]: r for r in rows}

    def locate_stage(
        self,
        stage_id: int,
        stage_attempt: int | None = None,
        *,
        task_ids: Iterable[int] = (),
        plan_node: str | None = None,
    ) -> Location:
        """A Location for a stage, filled in with its jobs and the SQL query that ran it."""
        link = self._stage_links.get(stage_id, {})
        return Location(
            app_id=self.app_id,
            stage_id=stage_id,
            stage_attempt=stage_attempt,
            job_ids=tuple(link.get("job_ids") or ()),
            sql_execution_id=link.get("sql_execution_id"),
            sql_description=link.get("sql_description"),
            plan_node=plan_node,
            task_ids=tuple(task_ids),
        )

    def locate_sql(self, execution_id: int, *, plan_node: str | None = None) -> Location:
        description = self.scalar(
            "SELECT description FROM sql WHERE execution_id = ?", [execution_id]
        )
        return Location(
            app_id=self.app_id,
            sql_execution_id=execution_id,
            sql_description=description,
            plan_node=plan_node,
        )

    def locate_app(self) -> Location:
        return Location(app_id=self.app_id)


@dataclass
class RuleError:
    rule_id: str
    app_id: str
    message: str
    traceback: str


@dataclass
class AnalysisResult:
    findings: list[Finding] = field(default_factory=list)
    errors: list[RuleError] = field(default_factory=list)
    apps: list[str] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)

    @property
    def max_severity(self) -> Severity | None:
        return max((f.severity for f in self.findings), default=None)

    def count(self, severity: Severity) -> int:
        return sum(1 for f in self.findings if f.severity == severity)

    def exit_code(self, fail_on: Severity | None = None) -> int:
        """0 = OK; 1 = a finding at or above `fail_on`; 2 = a rule crashed."""
        if self.errors:
            return 2
        worst = self.max_severity
        if fail_on is not None and worst is not None and worst >= fail_on:
            return 1
        return 0


def resolve_rules(
    select: Iterable[str] | None = None,
    ignore: Iterable[str] | None = None,
    overrides: dict[str, dict[str, Any]] | None = None,
    available: dict[str, type[Rule]] | None = None,
) -> list[Rule]:
    """Pick and configure rules. select/ignore accept IDs ('SKEW001') or prefixes ('SKEW')."""
    available = available if available is not None else load_builtin_rules()
    overrides = overrides or {}

    def matches(rule_id: str, patterns: Iterable[str]) -> bool:
        return any(rule_id.startswith(p.strip().upper()) for p in patterns)

    for pattern in [*(select or ()), *(ignore or ()), *overrides]:
        if not any(rid.startswith(pattern.strip().upper()) for rid in available):
            raise ValueError(f"No rule matches '{pattern}'. Rules: {', '.join(available)}")

    chosen = [
        cls
        for rid, cls in sorted(available.items())
        if (select is None or matches(rid, select)) and not matches(rid, ignore or ())
    ]
    return [cls(overrides.get(cls.id)) for cls in chosen]


def analyze(
    warehouse: Warehouse,
    *,
    app_ids: Iterable[str] | None = None,
    rules: list[Rule] | None = None,
    select: Iterable[str] | None = None,
    ignore: Iterable[str] | None = None,
    overrides: dict[str, dict[str, Any]] | None = None,
) -> AnalysisResult:
    """Run rules over apps. One rule crashing is recorded in result.errors, not raised."""
    if rules is None:
        rules = resolve_rules(select, ignore, overrides)
    app_ids = list(app_ids) if app_ids is not None else warehouse.app_ids()
    result = AnalysisResult(apps=app_ids, rules=[r.id for r in rules])

    for app_id in app_ids:
        con = warehouse.connect(app_id)
        try:
            ctx = AnalysisContext(con, app_id)
            for rule in rules:
                try:
                    found = list(rule.evaluate(ctx) or ())
                except Exception as exc:
                    result.errors.append(
                        RuleError(
                            rule.id, app_id, f"{type(exc).__name__}: {exc}", traceback.format_exc()
                        )
                    )
                    continue
                result.findings.extend(found)
        finally:
            con.close()

    result.findings = _dedupe(sorted(result.findings, key=Finding.sort_key))
    return result


def _dedupe(findings: list[Finding]) -> list[Finding]:
    """Keep the first (most severe) finding per rule and place."""
    seen: set[str] = set()
    unique = []
    for finding in findings:
        key = finding.fingerprint()
        if key not in seen:
            seen.add(key)
            unique.append(finding)
    return unique
