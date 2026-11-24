"""PLAN004: a sort-merge join where one side was small enough to broadcast."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.plan import children, final_tree, first_with_stats
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import MB, clean_expr, size

# Which side a broadcast hash join can broadcast, by join type.
_BROADCASTABLE = {
    "Inner": ("left", "right"),
    "Cross": ("left", "right"),
    "LeftOuter": ("right",),
    "LeftSemi": ("right",),
    "LeftAnti": ("right",),
    "ExistenceJoin": ("right",),
    "RightOuter": ("left",),
}


class MissedBroadcast(Rule):
    id = "PLAN004"
    name = "Missed broadcast join"
    category = "plan"
    description = (
        "A sort-merge join shuffled and sorted both sides although one side turned out small "
        "enough to broadcast. A broadcast hash join sends the small side to every executor and "
        "skips shuffling the big one."
    )
    thresholds = {
        "max_broadcast_bytes": 10 * MB,  # Spark's default autoBroadcastJoinThreshold
    }
    docs = (
        docs_links.BROADCAST_JOINS,
        docs_links.AQE_SMJ_TO_BROADCAST,
        docs_links.JOIN_HINTS,
    )

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        plans = ctx.rows("SELECT execution_id, final_plan FROM sql ORDER BY execution_id")
        details = {exec_id: {n.id: n for n in nodes} for exec_id, nodes in ctx.plans.items()}
        for plan in plans:
            tree = final_tree(plan["final_plan"])
            for node in tree.values():
                if not node.name.startswith("SortMergeJoin"):
                    continue
                sides = children(tree, node.id)
                if len(sides) != 2:
                    continue
                stats = [first_with_stats(tree, side.id) for side in sides]
                if any(s is None or s.size_bytes is None for s in stats):
                    continue  # no runtime sizes (e.g. AQE off): can't judge
                info = details.get(plan["execution_id"], {}).get(node.id)
                join_type = info.get("Join type", "Inner") if info else "Inner"
                eligible = _BROADCASTABLE.get(join_type, ())
                options = [
                    (side, s)
                    for side, s in zip(("left", "right"), stats, strict=True)
                    if side in eligible and s.size_bytes <= self.t("max_broadcast_bytes")
                ]
                if not options:
                    continue
                side, small = min(options, key=lambda item: item[1].size_bytes)
                big = stats[1] if side == "left" else stats[0]
                keys = clean_expr(info.get("Left keys", "")).strip("[]") if info else ""
                yield self._finding(ctx, plan["execution_id"], node, info, side, small, big, keys)

    def _finding(self, ctx, exec_id, node, info, side, small, big, keys) -> Finding:
        threshold = ctx.conf_bytes("spark.sql.autoBroadcastJoinThreshold")
        aqe_on = ctx.conf_bool("spark.sql.adaptive.enabled")
        if threshold is not None and threshold < 0:
            cause = (
                "Broadcast joins are disabled for this app (spark.sql.autoBroadcastJoinThreshold"
                "=-1), so Spark shuffled and sorted both sides."
            )
            fix_setting = (
                "Remove spark.sql.autoBroadcastJoinThreshold=-1 (default 10 MB) so Spark can "
                "broadcast small sides automatically."
            )
        elif not aqe_on:
            cause = (
                "Spark planned this join before knowing the real sizes, and AQE is off, so it "
                "couldn't switch to a broadcast join at runtime."
            )
            fix_setting = "Turn on AQE (spark.sql.adaptive.enabled=true) so Spark can switch to "
            fix_setting += "a broadcast join once it sees the real sizes."
        else:
            cause = (
                f"The {side} side was only {size(small.size_bytes)} at runtime, but Spark kept the "
                "sort-merge join (its size estimate at planning time was probably too high)."
            )
            fix_setting = (
                "Keep table statistics up to date (ANALYZE TABLE ... COMPUTE STATISTICS) so the "
                "planner sees the small side for what it is."
            )
        fixes = (
            f"Broadcast the small {side} side explicitly: wrap it in F.broadcast(small_df) in "
            "the join, or use the /*+ BROADCAST(t) */ SQL hint.",
            fix_setting,
        )
        rows = f" ({small.row_count:,} rows)" if small.row_count is not None else ""
        return Finding(
            rule_id=self.id,
            severity=Severity.MEDIUM,
            title=(
                f"Query {exec_id}: sort-merge join where one side is only {size(small.size_bytes)}"
            ),
            location=ctx.locate_sql(exec_id, plan_node=f"SortMergeJoin ({node.id})"),
            evidence=(
                Evidence(
                    f"{side} side at runtime{rows}",
                    small.size_bytes,
                    Unit.BYTES,
                    self.t("max_broadcast_bytes"),
                    "broadcast limit",
                ),
                Evidence("other side", big.size_bytes, Unit.BYTES),
                Evidence("join type", info.get("Join type", "?") if info else "?", Unit.TEXT),
                *([Evidence("join keys", keys, Unit.TEXT)] if keys else []),
            ),
            cause=cause,
            fix=fixes,
            category=self.category,
            details={"small_side": side, "small_bytes": small.size_bytes},
        )
