"""Making two runs' plans comparable, and recognizing the same query in both.

A physical plan's text changes on every run even when the code doesn't: expression ids
(`customer_id#2`), operator numbers (`SortMergeJoin (12)`), `plan_id=83`, codegen ids, AQE's
runtime sizes, and output paths that may point to a temporary folder. normalize() removes
all of that, so equal text means "same plan".

Queries are recognized by three fingerprints, from strict to loose:

    plan      the whole normalized plan before AQE (the code, settings and input as planned)
    shape     what goes in and out: the scans (with columns), the write (with columns) and the
              result's columns. Survives a change of join strategy or a new filter.
    sources   only the scans. Survives a change of what is computed from the same input.

The plan before AQE is used because it depends only on the code, the settings and the
input's statistics, not on how the run went.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass

from spark_sentinel.plan import PlanNode, final_nodes, parse_nodes

_NORMALIZE: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"#\d+L?\b"), ""),  # expression ids: amount#3 -> amount
    (re.compile(r"\s*\[codegen id : \d+\]"), ""),
    (re.compile(r"\bplan_id=\d+"), "plan_id"),
    (re.compile(r",? ?Statistics\([^)]*\)"), ""),  # AQE runtime sizes
    (re.compile(r"(?m)^\(\d+\) "), ""),  # detail block headers: "(12) SortMergeJoin"
    (re.compile(r" \(\d+\)"), ""),  # tree: "SortMergeJoin Inner (12)"
    (re.compile(r"operator id[:= ]+\d+"), "operator id"),  # ReusedExchange, subqueries
    (re.compile(r"\bSubquery:\d+"), "Subquery"),
    (re.compile(r"isFinalPlan=(?:true|false)"), ""),
    # Paths: output folders are often temporary; input folders differ between machines.
    (
        re.compile(
            r"\b(?:file|hdfs|s3a?|s3n|gs|abfss?|wasbs?|dbfs|viewfs|oss|cos|jdbc):"
            r"[^\s,\]\)]*"
        ),
        "<path>",
    ),
    (re.compile(r"\[path=[^\]]*\]"), "[path=<path>]"),
    (re.compile(r"-?\b\d{13,}\b"), "<n>"),  # timestamps, random seeds Spark picked
    (re.compile(r"[ \t]+$", re.MULTILINE), ""),
]

JOINS = (
    "BroadcastHashJoin",
    "SortMergeJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
)
PYTHON_EVALS = (
    "BatchEvalPython",  # row by row (pickled)
    "ArrowEvalPython",  # Arrow batches
    "MapInPandas",
    "MapInArrow",
    "FlatMapGroupsInPandas",
    "FlatMapCoGroupsInPandas",
    "AggregateInPandas",
    "WindowInPandas",
)
_SOURCE_PREFIXES = ("Scan ", "BatchScan", "Range", "LocalTableScan", "InMemoryTableScan")


def normalize(plan: str | None) -> str:
    return _apply(plan or "").strip()


def _apply(text: str) -> str:
    for pattern, replacement in _NORMALIZE:
        text = pattern.sub(replacement, text)
    return text


def _hash(*parts: object) -> str:
    return hashlib.sha1(repr(parts).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class PlanInfo:
    """What diff needs from one query's plans."""

    plan_key: str
    shape_key: str
    sources_key: str | None  # None when the query reads nothing we recognize
    tree: tuple[str, ...]  # normalized tree of the plan that ran, for showing differences
    joins: Counter[str]
    python_evals: Counter[str]
    shuffles: int
    target: str | None  # where it writes, e.g. "report/revenue" (last two path parts)


def plan_info(initial_plan: str | None, final_plan: str | None) -> PlanInfo:
    planned = initial_plan or final_plan or ""
    nodes = list(parse_nodes(planned).values())
    ran = final_nodes(final_plan or initial_plan)

    sources = sorted(_describe(n) for n in nodes if n.name.startswith(_SOURCE_PREFIXES))
    sinks = sorted(_describe(n) for n in nodes if n.name.startswith("Execute "))
    root_output = normalize(_root(planned).get("Output", "")) if nodes else ""

    names = [n.name.split(" ")[0] for n in ran]
    return PlanInfo(
        plan_key=_hash(normalize(planned)),
        shape_key=_hash(sources, sinks, root_output),
        sources_key=_hash(sources) if sources else None,
        tree=_final_tree_lines(final_plan or initial_plan),
        joins=Counter(n for n in names if n in JOINS),
        python_evals=Counter(n for n in names if n in PYTHON_EVALS),
        shuffles=sum(1 for n in ran if n.name == "Exchange"),
        target=_target(ran),
    )


def _describe(node: PlanNode) -> str:
    """A scan or write, without ids or paths: name plus the columns it reads or writes."""
    columns = node.get("ReadSchema") or node.get("Output") or ""
    arguments = node.get("Arguments") or ""
    return normalize(f"{node.name} {columns} {arguments}")


def _root(plan: str) -> PlanNode | dict:
    """The top node of the plan (its first tree line), for the result's columns."""
    for line in plan.splitlines()[1:3]:
        match = re.search(r"\((\d+)\)", line)
        if match:
            return parse_nodes(plan).get(int(match.group(1)), {})
    return {}


def _final_tree_lines(plan: str | None) -> tuple[str, ...]:
    """The tree of the plan that ran (or the only plan), normalized, one line per operator."""
    tree = (plan or "").split("\n\n", 1)[0].splitlines()
    if any("== Final Plan ==" in line for line in tree):
        start = next(i for i, line in enumerate(tree) if "== Final Plan ==" in line) + 1
        end = next((i for i, line in enumerate(tree) if "== Initial Plan ==" in line), len(tree))
        tree = tree[start:end]
    elif tree and tree[0].startswith("=="):
        tree = tree[1:]
    lines = [_apply(line) for line in tree if line.strip()]
    indent = min((len(line) - len(line.lstrip()) for line in lines), default=0)
    return tuple(line[indent:] for line in lines)


def _target(ran: list[PlanNode]) -> str | None:
    for node in ran:
        if node.name.startswith("Execute "):
            arguments = node.get("Arguments") or ""
            path = arguments.split(",", 1)[0].strip()
            parts = [p for p in re.split(r"[/\\]", path) if p and not p.endswith(":")]
            if parts:
                return "/".join(parts[-2:])
    return None
