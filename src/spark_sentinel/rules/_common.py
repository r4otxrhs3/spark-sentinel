"""Small helpers shared by rules. (Leading underscore: not scanned for rules.)"""

from __future__ import annotations

import math
import re

from spark_sentinel.findings import format_bytes
from spark_sentinel.plan import PlanNode

MB = 1024 * 1024
GB = 1024 * MB

_EXPR_ID = re.compile(r"#\d+L?")


def clean_expr(text: str) -> str:
    """Drop Spark's expression ids: 'customer_id#2' -> 'customer_id'."""
    return _EXPR_ID.sub("", text)


def ceil_div(total: float, per_item: float) -> int:
    return max(1, math.ceil(total / per_item)) if per_item > 0 else 1


def round_partitions(n: int) -> int:
    """A tidy partition count to suggest: 1-10 as is, then 2 significant digits."""
    if n <= 10:
        return max(n, 1)
    digits = len(str(n)) - 2
    return int(math.ceil(n / 10**digits) * 10**digits)


def explicit_repartition(nodes: list[PlanNode]) -> int | None:
    """N if the plan has a shuffle from .repartition(N) in user code, else None."""
    for node in nodes:
        args = node.get("Arguments", "")
        if node.name == "Exchange" and "REPARTITION_BY_NUM" in args:
            match = re.search(r"\(.*?(\d+)\)", args)
            if match:
                return int(match.group(1))
    return None


def size(n: float) -> str:
    return format_bytes(n)
