"""Comparing two runs of the same Spark app: which queries got slower or faster, and why.

    result = compare(Warehouse(".sentinel"), "local-base", "local-new", profile="laptop")
    for q in result.regressions: print(q.headline)   # Query 0: 1.5x slower (4.0s -> 6.0s)

Noise control: a change counts only when it crosses BOTH a relative threshold (min_change_pct)
and an absolute one (min_time_ms for times, min_bytes for data). Run-to-run noise in wall
time is large (a cold first run can be 75% slower with nothing changed), so a slowdown is
"confirmed" only when something deterministic changed with it: the plan, the data volume,
spill, or a related setting. Unconfirmed slowdowns are reported, but don't fail a build.
"""

from __future__ import annotations

import difflib
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from spark_sentinel.analysis import analyze
from spark_sentinel.diff.plans import PlanInfo, plan_info
from spark_sentinel.findings import Finding, Severity, format_bytes, format_duration
from spark_sentinel.profiles import PROFILES
from spark_sentinel.store import Warehouse, run_query

MB = 1024 * 1024

# Thresholds per profile. Laptop values come from measuring identical runs of
# lab/diff/sales_report.py: warm reruns differ by up to 25% but under 0.5 s per query.
DIFF_THRESHOLDS: dict[str, dict[str, float]] = {
    "default": {"min_change_pct": 25, "min_time_ms": 30_000, "min_bytes": 256 * MB},
    "laptop": {"min_change_pct": 30, "min_time_ms": 1_000, "min_bytes": 8 * MB},
}


def diff_thresholds(profile: str, overrides: dict[str, Any] | None = None) -> dict[str, float]:
    if profile not in PROFILES or profile not in DIFF_THRESHOLDS:
        raise ValueError(f"Unknown profile '{profile}'. Choose from: {', '.join(PROFILES)}")
    thresholds = dict(DIFF_THRESHOLDS[profile])
    for key, value in (overrides or {}).items():
        if key not in thresholds:
            raise ValueError(
                f"Unknown diff threshold '{key}'. Choose from: {', '.join(thresholds)}"
            )
        try:
            thresholds[key] = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"Diff threshold {key} must be a number, not '{value}'") from None
    return thresholds


# --- one run


@dataclass
class QueryRun:
    execution_id: int
    description: str
    duration_ms: int
    error: str | None
    plan: PlanInfo
    metrics: dict[str, float]  # see _QUERY_METRICS

    @property
    def label(self) -> str:
        return f"Query {self.execution_id}"


@dataclass
class AppRun:
    app_id: str
    app_name: str
    spark_version: str
    master: str
    duration_ms: int
    task_time_ms: float
    cores: int
    executors: int
    config: dict[str, str]
    queries: list[QueryRun]


_QUERY_METRICS = """
WITH query_stages AS (
    SELECT DISTINCT j.sql_execution_id AS execution_id, js.stage_id
    FROM jobs j JOIN job_stages js USING (job_id)
    WHERE j.sql_execution_id IS NOT NULL
)
SELECT q.execution_id,
       count(t.task_id)                          AS tasks,
       coalesce(sum(t.run_time_ms), 0)           AS task_time_ms,
       coalesce(max(t.run_time_ms), 0)           AS max_task_ms,
       coalesce(sum(t.gc_time_ms), 0)            AS gc_time_ms,
       coalesce(sum(t.input_bytes), 0)           AS input_bytes,
       coalesce(sum(t.output_bytes), 0)          AS output_bytes,
       coalesce(sum(t.shuffle_write_bytes), 0)   AS shuffle_bytes,
       coalesce(sum(t.disk_spill_bytes), 0)      AS spill_bytes
FROM query_stages q
LEFT JOIN tasks t ON t.stage_id = q.stage_id
GROUP BY q.execution_id
"""


def load_run(warehouse: Warehouse, app_id: str) -> AppRun:
    con = warehouse.connect(app_id)
    try:

        def rows(sql: str) -> list[dict[str, Any]]:
            columns, values = run_query(con, sql)
            return [dict(zip(columns, row, strict=True)) for row in values]

        app = (rows("SELECT * FROM app") or [{}])[0]
        metrics = {r.pop("execution_id"): r for r in rows(_QUERY_METRICS)}
        queries = [
            QueryRun(
                execution_id=r["execution_id"],
                description=r["description"] or "",
                duration_ms=r["duration_ms"] or 0,
                error=r["error"] or None,
                plan=plan_info(r["initial_plan"], r["final_plan"]),
                metrics={k: float(v or 0) for k, v in metrics.get(r["execution_id"], {}).items()},
            )
            for r in rows(
                "SELECT execution_id, description, duration_ms, error, initial_plan, final_plan "
                "FROM sql ORDER BY execution_id"
            )
        ]
        local = str(app.get("master") or "").startswith("local")
        where = "" if local else "WHERE executor_id <> 'driver'"
        cores, executors = run_query(
            con, f"SELECT coalesce(sum(cores), 0), count(*) FROM executors {where}"
        )[1][0]
        task_time = run_query(con, "SELECT coalesce(sum(run_time_ms), 0) FROM tasks")[1][0][0]
        config = dict(run_query(con, "SELECT key, value FROM config")[1])
    finally:
        con.close()
    return AppRun(
        app_id=app_id,
        app_name=app.get("app_name") or "",
        spark_version=app.get("spark_version") or "",
        master=app.get("master") or "",
        duration_ms=app.get("duration_ms") or 0,
        task_time_ms=float(task_time),
        cores=int(cores),
        executors=int(executors),
        config=config,
        queries=queries,
    )


# --- matching


def match_queries(
    base: list[QueryRun], new: list[QueryRun]
) -> list[tuple[QueryRun | None, QueryRun | None]]:
    """Pair each new query with the same query in the baseline.

    Strictest fingerprint first (same plan), then same inputs and outputs, then same inputs.
    Within a fingerprint, the n-th occurrence pairs with the n-th, so a query that runs twice
    pairs in order. Execution ids are never compared: adding one query shifts all of them.
    """
    pairs: dict[int, int] = {}  # new index -> base index
    used: set[int] = set()
    for key in ("plan_key", "shape_key", "sources_key"):
        waiting: dict[str, list[int]] = {}
        for i, q in enumerate(base):
            k = getattr(q.plan, key)
            if i not in used and k is not None:
                waiting.setdefault(k, []).append(i)
        for j, q in enumerate(new):
            k = getattr(q.plan, key)
            if j in pairs or k is None or not waiting.get(k):
                continue
            i = waiting[k].pop(0)
            pairs[j] = i
            used.add(i)

    result: list[tuple[QueryRun | None, QueryRun | None]] = [
        (base[pairs[j]] if j in pairs else None, q) for j, q in enumerate(new)
    ]
    result += [(q, None) for i, q in enumerate(base) if i not in used]
    return result


# --- comparing


@dataclass(frozen=True)
class Change:
    """One metric in both runs, and whether the difference counts."""

    name: str
    label: str
    unit: str  # "ms" or "bytes"
    before: float
    after: float
    significant: bool

    @property
    def ratio(self) -> float | None:
        return self.after / self.before if self.before else None

    def text(self) -> str:
        fmt = format_duration if self.unit == "ms" else format_bytes
        return f"{self.label} {fmt(self.before)} → {fmt(self.after)}"


@dataclass
class QueryDiff:
    base: QueryRun | None
    new: QueryRun | None
    status: str  # slower | faster | changed | unchanged | new | removed | failed
    confirmed: bool = False
    plan_changed: bool = False
    changes: list[Change] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    plan_diff: list[str] = field(default_factory=list)

    @property
    def run(self) -> QueryRun:
        return self.new or self.base  # type: ignore[return-value]

    @property
    def label(self) -> str:
        label = self.run.label
        if self.base and self.new and self.base.execution_id != self.new.execution_id:
            label += f" (was {self.base.execution_id})"
        return label

    @property
    def what(self) -> str:
        """What the query does, for people: where it writes, else Spark's description."""
        target = self.run.plan.target
        return f"writes {target}" if target else self.run.description

    @property
    def time(self) -> Change | None:
        return next((c for c in self.changes if c.name == "duration_ms"), None)

    @property
    def headline(self) -> str:
        time = self.time
        if self.status in ("slower", "faster") and time:
            if self.status == "slower":
                speed = f"{time.ratio:.1f}x slower" if time.ratio else "slower"
            else:
                speed = f"{time.before / time.after:.1f}x faster" if time.after else "faster"
            why = f": {self.reasons[0]}" if self.reasons and self.confirmed else ""
            return (
                f"{self.label}: {speed} ({format_duration(time.before)} → "
                f"{format_duration(time.after)}){why}"
            )
        if self.status == "new":
            return f"{self.label}: new query ({format_duration(self.run.duration_ms)})"
        if self.status == "removed":
            return f"{self.label}: no longer runs (took {format_duration(self.run.duration_ms)})"
        if self.status == "failed":
            return f"{self.label}: failed in the new run"
        if self.status == "changed":
            why = f": {self.reasons[0]}" if self.reasons else ""
            return f"{self.label}: same speed, but it changed{why}"
        return f"{self.label}: unchanged"


@dataclass(frozen=True)
class SettingChange:
    key: str
    before: str | None  # None: not set (Spark's default)
    after: str | None

    def text(self) -> str:
        return f"{self.key}: {self.before or '(default)'} → {self.after or '(default)'}"


@dataclass
class DiffResult:
    base: AppRun
    new: AppRun
    queries: list[QueryDiff]
    settings: list[SettingChange]
    app_changes: list[Change]
    environment: list[str]  # Spark version, cores, executors that changed
    new_findings: list[Finding]
    fixed_findings: list[Finding]
    thresholds: dict[str, float]
    profile: str

    @property
    def regressions(self) -> list[QueryDiff]:
        return [q for q in self.queries if q.status == "slower"]

    @property
    def confirmed_regressions(self) -> list[QueryDiff]:
        return [q for q in self.regressions if q.confirmed]

    def count(self, status: str) -> int:
        return sum(1 for q in self.queries if q.status == status)

    def summary_line(self) -> str:
        parts = []
        for status, word in (
            ("slower", "slower"),
            ("faster", "faster"),
            ("changed", "changed"),
            ("new", "new"),
            ("removed", "removed"),
            ("failed", "failed"),
            ("unchanged", "unchanged"),
        ):
            n = self.count(status)
            if n:
                parts.append(f"{n} {word}")
        queries = len(self.queries)
        text = f"{queries} {'query' if queries == 1 else 'queries'}: " + ", ".join(parts or ["-"])
        if self.new_findings:
            text += f"; {len(self.new_findings)} new problem(s)"
        if self.fixed_findings:
            text += f"; {len(self.fixed_findings)} fixed"
        return text

    def exit_code(self, fail_on: list[str]) -> int:
        """1 if the result matches any of fail_on: 'slower' (a confirmed slowdown), or a
        severity ('high', 'medium', ...) for a new problem at least that severe."""
        for condition in fail_on:
            if condition == "never":
                continue
            if condition == "slower":
                if self.confirmed_regressions or self.count("failed"):
                    return 1
                continue
            worst = Severity.parse(condition)
            if any(f.severity >= worst for f in self.new_findings):
                return 1
        return 0


def compare(
    warehouse: Warehouse,
    base_app: str,
    new_app: str,
    *,
    profile: str = "default",
    thresholds: dict[str, Any] | None = None,
    rule_overrides: dict[str, dict[str, Any]] | None = None,
    findings: bool = True,
) -> DiffResult:
    limits = diff_thresholds(profile, thresholds)
    base, new = load_run(warehouse, base_app), load_run(warehouse, new_app)
    settings = setting_changes(base.config, new.config)
    queries = [
        _compare_query(b, n, limits, settings) for b, n in match_queries(base.queries, new.queries)
    ]
    order = {"failed": 0, "slower": 1, "faster": 2, "changed": 3, "new": 4, "removed": 5}
    queries.sort(key=lambda q: (order.get(q.status, 9), q.run.execution_id))

    app_changes = [
        _change("duration_ms", "run time", "ms", base.duration_ms, new.duration_ms, limits),
        _change("task_time_ms", "task time", "ms", base.task_time_ms, new.task_time_ms, limits),
    ]
    environment = []
    if base.spark_version != new.spark_version:
        environment.append(f"Spark {base.spark_version} → {new.spark_version}")
    if base.cores != new.cores:
        environment.append(f"{base.cores} → {new.cores} cores")
    if base.executors != new.executors and not new.master.startswith("local"):
        environment.append(f"{base.executors} → {new.executors} executors")

    new_findings: list[Finding] = []
    fixed: list[Finding] = []
    if findings:
        new_findings, fixed = _finding_changes(
            warehouse, base_app, new_app, queries, profile, rule_overrides
        )
    return DiffResult(
        base=base,
        new=new,
        queries=queries,
        settings=settings,
        app_changes=app_changes,
        environment=environment,
        new_findings=new_findings,
        fixed_findings=fixed,
        thresholds=limits,
        profile=profile,
    )


def _change(
    name: str, label: str, unit: str, before: float, after: float, limits: dict[str, float]
) -> Change:
    minimum = limits["min_time_ms"] if unit == "ms" else limits["min_bytes"]
    delta = abs(after - before)
    big_enough = delta >= minimum
    relative = before == 0 or delta / before * 100 >= limits["min_change_pct"]
    return Change(name, label, unit, before, after, big_enough and relative)


_DATA_METRICS = (
    ("spill_bytes", "spill to disk"),
    ("input_bytes", "input"),
    ("shuffle_bytes", "shuffle"),
    ("output_bytes", "output"),
)


def _compare_query(
    base: QueryRun | None,
    new: QueryRun | None,
    limits: dict[str, float],
    settings: list[SettingChange],
) -> QueryDiff:
    if new is None:
        return QueryDiff(base, None, "removed")
    if base is None:
        return QueryDiff(None, new, "failed" if new.error else "new")
    if new.error and not base.error:
        return QueryDiff(base, new, "failed", confirmed=True, reasons=[f"error: {new.error}"])

    m0, m1 = base.metrics, new.metrics
    changes = [_change("duration_ms", "time", "ms", base.duration_ms, new.duration_ms, limits)]
    changes.append(
        _change(
            "task_time_ms",
            "task time",
            "ms",
            m0.get("task_time_ms", 0),
            m1.get("task_time_ms", 0),
            limits,
        )
    )
    changes += [
        _change(name, label, "bytes", m0.get(name, 0), m1.get(name, 0), limits)
        for name, label in _DATA_METRICS
    ]
    for name, label in (("max_task_ms", "slowest task"), ("gc_time_ms", "GC time")):
        changes.append(_change(name, label, "ms", m0.get(name, 0), m1.get(name, 0), limits))

    plan_changed = base.plan.plan_key != new.plan.plan_key
    reasons = _reasons(base, new, changes, plan_changed, settings)
    deterministic = bool(reasons)  # plan, data, spill or settings: not noise

    time = changes[0]
    if time.significant:
        status = "slower" if time.after > time.before else "faster"
    elif plan_changed or any(c.significant for c in changes if c.unit == "bytes"):
        status = "changed"
    else:
        status = "unchanged"
    if status in ("slower", "faster") and not deterministic:
        reasons = [
            "the plan, data volume and settings are the same, so this may be noise "
            "(a busy or cold machine): re-run to confirm"
        ]

    plan_diff = []
    if plan_changed and base.plan.tree != new.plan.tree:
        plan_diff = [
            line
            for line in difflib.unified_diff(base.plan.tree, new.plan.tree, lineterm="", n=1)
            if not line.startswith(("---", "+++"))
        ]
    return QueryDiff(
        base,
        new,
        status,
        confirmed=deterministic,
        plan_changed=plan_changed,
        changes=changes,
        reasons=reasons,
        plan_diff=plan_diff,
    )


# Settings that explain a kind of change: (what changed, setting prefixes)
_RELATED_SETTINGS = {
    "join": (
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.adaptive.autoBroadcastJoinThreshold",
        "spark.sql.join.preferSortMergeJoin",
        "spark.sql.adaptive.",
    ),
    "python": ("spark.sql.execution.pythonUDF.arrow", "spark.sql.execution.arrow"),
    "shuffle": ("spark.sql.shuffle.partitions", "spark.sql.adaptive.", "spark.sql.files."),
    "memory": (
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.memory.",
        "spark.executor.cores",
        "spark.sql.shuffle.partitions",
    ),
}


def _related(kind: str, settings: list[SettingChange]) -> str:
    keys = [s for s in settings if s.key.startswith(_RELATED_SETTINGS[kind])]
    if not keys:
        return ""
    first = keys[0]
    return (
        f" (after {first.key} changed from {first.before or 'the default'} "
        f"to {first.after or 'the default'})"
    )


_JOIN_WORDS = {
    "BroadcastHashJoin": "broadcast join",
    "SortMergeJoin": "sort-merge join",
    "ShuffledHashJoin": "shuffled hash join",
    "BroadcastNestedLoopJoin": "nested-loop join",
    "CartesianProduct": "cartesian product",
}
_PYTHON_WORDS = {
    "BatchEvalPython": "a row-by-row Python UDF",
    "ArrowEvalPython": "an Arrow Python UDF",
}


def _swap_text(
    before: Counter[str], after: Counter[str], words: dict[str, str], noun: str
) -> str | None:
    gone, came = before - after, after - before
    if not gone and not came:
        return None
    gone_names = [words.get(k, k) for k in sorted(gone.elements())]
    came_names = [words.get(k, k) for k in sorted(came.elements())]
    if len(gone_names) == 1 and len(came_names) == 1:
        return f"the {gone_names[0]} became a {came_names[0]}"
    parts = []
    if came_names:
        parts.append("new " + ", ".join(came_names))
    if gone_names:
        parts.append("no more " + ", ".join(gone_names))
    return f"{noun}: " + "; ".join(parts)


def _reasons(
    base: QueryRun,
    new: QueryRun,
    changes: list[Change],
    plan_changed: bool,
    settings: list[SettingChange],
) -> list[str]:
    """Deterministic differences, most likely cause first. Empty = nothing but timing."""
    reasons: list[str] = []
    p0, p1 = base.plan, new.plan
    joins = _swap_text(p0.joins, p1.joins, _JOIN_WORDS, "joins")
    shuffles = p1.shuffles - p0.shuffles
    if joins:
        if shuffles:
            joins += f", which {'adds' if shuffles > 0 else 'removes'} {abs(shuffles)} shuffle"
            joins += "s" if abs(shuffles) > 1 else ""
        reasons.append(joins + _related("join", settings))
    python = _swap_text(p0.python_evals, p1.python_evals, _PYTHON_WORDS, "Python")
    if python:
        if not p0.python_evals and len(p1.python_evals) == 1:
            python = f"it now runs {_PYTHON_WORDS.get(next(iter(p1.python_evals)), 'Python')}"
        elif not p1.python_evals and len(p0.python_evals) == 1:
            python = f"it no longer runs {_PYTHON_WORDS.get(next(iter(p0.python_evals)), 'Python')}"
        reasons.append(python + _related("python", settings))
    if shuffles and not joins:
        reasons.append(
            f"it shuffles data {p1.shuffles} time{'s' if p1.shuffles != 1 else ''} instead of "
            f"{p0.shuffles}" + _related("shuffle", settings)
        )

    by_name = {c.name: c for c in changes}
    spill = by_name["spill_bytes"]
    if spill.significant:
        if spill.before == 0:
            reasons.append(
                f"it now spills {format_bytes(spill.after)} to disk" + _related("memory", settings)
            )
        elif spill.after == 0:
            reasons.append(f"it no longer spills to disk (was {format_bytes(spill.before)})")
        else:
            reasons.append(f"{spill.text()}" + _related("memory", settings))
    for name, verb in (
        ("input_bytes", "reads"),
        ("shuffle_bytes", "shuffles"),
        ("output_bytes", "writes"),
    ):
        c = by_name[name]
        if not c.significant or (name == "shuffle_bytes" and (joins or shuffles)):
            continue
        if c.before and c.after:
            factor = c.after / c.before
            amount = f"{factor:.1f}x more" if factor >= 1 else f"{1 / factor:.1f}x less"
            reasons.append(
                f"it {verb} {amount} data ({format_bytes(c.before)} → {format_bytes(c.after)})"
            )
        else:
            reasons.append(f"it {verb} {format_bytes(c.after)} (was {format_bytes(c.before)})")

    if plan_changed and not reasons:
        w0, w1 = p0.written, p1.written
        if w0 and w1 and w0 != w1:
            gone = [c for c in w0 if c not in w1]
            came = [c for c in w1 if c not in w0]
            change = f"{', '.join(gone) or '-'} → {', '.join(came) or '-'}" if gone or came else ""
            reasons.append(
                "the columns it writes changed" + (f" ({change})" if change else " order")
            )
        else:
            reasons.append("its plan changed" + _related("shuffle", settings))

    # Timing details explain a slowdown but don't confirm one by themselves.
    if reasons:
        for name in ("gc_time_ms", "max_task_ms"):
            c = by_name[name]
            if c.significant and c.after > c.before:
                reasons.append(c.text())
    return reasons


# --- settings


_IGNORED_PREFIXES = (
    "spark.app.",
    "spark.eventLog.",
    "spark.ui.",
    "spark.history.",
    "spark.driver.host",
    "spark.driver.port",
    "spark.driver.appUIAddress",
    "spark.blockManager.port",
    "spark.executor.id",
    "spark.submit.pyFiles",
    "spark.repl.",
    "spark.yarn.app",
    "spark.kubernetes.driver.pod.name",
    "spark.databricks.clusterUsageTags.",
)
_IGNORED_SUFFIXES = (".port", ".host", ".dir", ".dirs", "Time", ".id", ".url")
_SECRET_WORDS = ("password", "secret", "token", "credential", ".key", "auth")


def _ignored(key: str) -> bool:
    lowered = key.lower()
    return (
        key.startswith(_IGNORED_PREFIXES)
        or key.endswith(_IGNORED_SUFFIXES)
        or any(word in lowered for word in _SECRET_WORDS)
    )


def setting_changes(before: dict[str, str], after: dict[str, str]) -> list[SettingChange]:
    """Settings that differ, minus ones that change every run (ids, ports, times, log
    folders) and anything that looks like a secret."""
    return [
        SettingChange(key, before.get(key), after.get(key))
        for key in sorted(before.keys() | after.keys())
        if before.get(key) != after.get(key) and not _ignored(key)
    ]


# --- findings


def _finding_changes(
    warehouse: Warehouse,
    base_app: str,
    new_app: str,
    queries: list[QueryDiff],
    profile: str,
    overrides: dict[str, dict[str, Any]] | None,
) -> tuple[list[Finding], list[Finding]]:
    """Problems found in the new run but not the baseline, and the other way round.

    A finding is the same problem in both runs if it's the same rule on the same (matched)
    query; app-level findings match on the rule alone.
    """
    before = analyze(warehouse, app_ids=[base_app], profile=profile, overrides=overrides).findings
    after = analyze(warehouse, app_ids=[new_app], profile=profile, overrides=overrides).findings
    base_to_new = {q.base.execution_id: q.new.execution_id for q in queries if q.base and q.new}

    def key_new(f: Finding) -> tuple:
        return (f.rule_id, f.location.sql_execution_id)

    def key_base(f: Finding) -> tuple:
        sql_id = f.location.sql_execution_id
        if sql_id is None:
            return (f.rule_id, None)
        return (f.rule_id, base_to_new.get(sql_id, f"gone-{sql_id}"))

    keys_before = Counter(key_base(f) for f in before)
    keys_after = Counter(key_new(f) for f in after)
    new_findings, fixed = [], []
    seen: Counter[tuple] = Counter()
    for f in after:
        k = key_new(f)
        seen[k] += 1
        if seen[k] > keys_before[k]:
            new_findings.append(f)
    seen = Counter()
    for f in before:
        k = key_base(f)
        seen[k] += 1
        if seen[k] > keys_after[k]:
            fixed.append(f)
    return new_findings, fixed
