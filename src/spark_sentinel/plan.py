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


# --- the tree (structure and AQE runtime statistics)

_TREE_LINE = re.compile(
    r"^(?P<lead>[\s:|+\-]*?)(?:\* )?(?P<name>[A-Za-z][^()]*?) \((?P<id>\d+)\)(?P<rest>.*)$"
)
_STATS = re.compile(
    r"Statistics\(sizeInBytes=(?P<size>[\d.]+) (?P<unit>[KMGTPE]?i?B)"
    r"(?:, rowCount=(?P<rows>[\d.E+]+))?"
)
_UNITS = {"B": 0, "KiB": 1, "MiB": 2, "GiB": 3, "TiB": 4, "PiB": 5, "EiB": 6}


@dataclass(frozen=True)
class TreeNode:
    id: int
    name: str  # as written in the tree, e.g. "SortMergeJoin Inner", "ShuffleQueryStage"
    column: int  # indentation: children are deeper
    parent: int | None
    size_bytes: int | None = None  # AQE runtime statistics, on query stages
    row_count: int | None = None


def final_tree(plan: str | None) -> dict[int, TreeNode]:
    """The final plan's tree: each node's parent, and AQE runtime sizes where Spark shows them.

    Query stages that finished carry runtime statistics, e.g.
    `ShuffleQueryStage (10), Statistics(sizeInBytes=39.1 KiB, rowCount=1.00E+3)`.
    """
    if not plan:
        return {}
    tree_text = plan.split("\n\n", 1)[0]
    has_sections = "== Final Plan ==" in tree_text or "== Current Plan ==" in tree_text
    section = None
    nodes: dict[int, TreeNode] = {}
    stack: list[TreeNode] = []  # path from the root to the previous line
    for line in tree_text.splitlines():
        if "== Final Plan ==" in line or "== Current Plan ==" in line:
            section, stack = "final", []
            continue
        if "== Initial Plan ==" in line:
            section = "initial"
            continue
        if has_sections and section != "final":
            continue
        match = _TREE_LINE.match(line)
        if not match:
            continue
        column = len(match.group("lead"))
        while stack and stack[-1].column >= column:
            stack.pop()
        size = rows = None
        stats = _STATS.search(match.group("rest"))
        if stats:
            size = int(float(stats.group("size")) * 1024 ** _UNITS.get(stats.group("unit"), 0))
            rows = int(float(stats.group("rows"))) if stats.group("rows") else None
        node = TreeNode(
            id=int(match.group("id")),
            name=match.group("name").strip(),
            column=column,
            parent=stack[-1].id if stack else None,
            size_bytes=size,
            row_count=rows,
        )
        nodes[node.id] = node
        stack.append(node)
    return nodes


def children(tree: dict[int, TreeNode], node_id: int) -> list[TreeNode]:
    return [n for n in tree.values() if n.parent == node_id]


def first_with_stats(tree: dict[int, TreeNode], node_id: int) -> TreeNode | None:
    """The nearest node at or below node_id that carries runtime statistics."""
    queue = [tree[node_id]]
    while queue:
        node = queue.pop(0)
        if node.size_bytes is not None:
            return node
        queue.extend(children(tree, node.id))
    return None
