"""Reading Spark's formatted physical plans (the `physicalPlanDescription` text).

A formatted plan has a tree, then one detail block per node:

    == Physical Plan ==
    AdaptiveSparkPlan (24)
    +- == Final Plan ==
       ...
          +- CartesianProduct Inner (7)          <- tree: names and ids
       ...
    +- == Initial Plan ==
       ...
          +- CartesianProduct Inner (17)         <- the SAME node again, before AQE

    (7) CartesianProduct                         <- detail blocks, for BOTH plans
    Join type: Inner
    Join condition: (a_val#2L < (b_val#5L - 80))

With AQE the text holds the final AND the initial plan, and every node's details appear
twice under different ids. Rules must read final_nodes() only, or they report every
problem twice and may flag things AQE already fixed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_DETAIL_HEADER = re.compile(r"^\((\d+)\) (.+?)\s*(?:\[codegen id : \d+\])?$")
_TREE_ID = re.compile(r"\((\d+)\)")
_DETAIL_LINE = re.compile(r"^([A-Za-z][\w ]*?)\s*(?:\[\d+\])?\s*:\s?(.*)$")


@dataclass(frozen=True)
class PlanNode:
    id: int
    name: str  # e.g. "Filter", "Scan parquet", "BatchEvalPython", "CartesianProduct"
    details: dict[str, str] = field(default_factory=dict)  # "Condition" -> "pythonUDF0#16"

    @property
    def label(self) -> str:
        return f"{self.name} ({self.id})"

    def get(self, key: str, default: str | None = None) -> str | None:
        return self.details.get(key, default)


def parse_nodes(plan: str) -> dict[int, PlanNode]:
    """Every node detail block in the text, by id (final and initial plans alike)."""
    nodes: dict[int, PlanNode] = {}
    current: tuple[int, str] | None = None
    details: dict[str, str] = {}

    def close() -> None:
        if current is not None:
            nodes[current[0]] = PlanNode(current[0], current[1], dict(details))

    for line in plan.splitlines():
        header = _DETAIL_HEADER.match(line)
        if header:
            close()
            current, details = (int(header.group(1)), header.group(2).strip()), {}
        elif not line.strip():
            close()
            current, details = None, {}
        elif current is not None:
            detail = _DETAIL_LINE.match(line)
            if detail:
                details[detail.group(1).strip()] = detail.group(2).strip()
    close()
    return nodes


def final_node_ids(plan: str) -> set[int]:
    """Ids of the nodes in the plan that actually ran.

    With AQE: the nodes under '== Final Plan ==' (when the query finished), else the ones under
    '== Current Plan ==' (a plan update mid-query). Without AQE: every node in the tree.
    """
    tree = plan.split("\n\n", 1)[0]
    lines = tree.splitlines()
    section = None
    ids: set[int] = set()
    has_sections = any(m in tree for m in ("== Final Plan ==", "== Current Plan =="))
    for line in lines:
        if "== Final Plan ==" in line or "== Current Plan ==" in line:
            section = "final"
            continue
        if "== Initial Plan ==" in line:
            section = "initial"
            continue
        if has_sections and section != "final":
            continue
        match = _TREE_ID.search(line)
        if match:
            ids.add(int(match.group(1)))
    return ids


def final_nodes(plan: str | None) -> list[PlanNode]:
    """The nodes of the plan that ran, with their details, in id order."""
    if not plan:
        return []
    nodes = parse_nodes(plan)
    return [nodes[i] for i in sorted(final_node_ids(plan)) if i in nodes]
