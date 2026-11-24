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


def spark_version(ctx) -> tuple[int, int]:
    """(major, minor) of the app's Spark version, (0, 0) if unknown."""
    match = re.match(r"(\d+)\.(\d+)", str(ctx.app.get("spark_version") or ""))
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)


def single_partition_exchange(nodes: list[PlanNode]) -> PlanNode | None:
    """The Exchange that moves all rows into one partition, if the plan has one."""
    for node in nodes:
        if node.name == "Exchange" and node.get("Arguments", "").startswith("SinglePartition"):
            return node
    return None


def query_input_records(ctx, execution_id: int) -> int:
    """Rows the query's tasks read from files/sources (exact, unlike input bytes)."""
    value = ctx.scalar(
        """
        SELECT sum(t.input_records)
        FROM tasks t
        JOIN job_stages js ON js.stage_id = t.stage_id
        JOIN jobs j        ON j.job_id = js.job_id
        WHERE j.sql_execution_id = ?
        """,
        [execution_id],
    )
    return int(value or 0)
