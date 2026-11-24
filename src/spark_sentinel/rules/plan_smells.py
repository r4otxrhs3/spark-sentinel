"""PLAN001: a Python UDF in a filter blocks pushdown. PLAN002: cartesian / nested-loop joins.

Both read the final physical plan (ctx.plans), i.e. what actually ran after AQE.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.plan import PlanNode
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import clean_expr, query_input_records, spark_version

_PYTHON_EVAL = ("BatchEvalPython", "ArrowEvalPython")
_UDF_CALL = re.compile(r"(\w+)\([^()]*\)#\d+")


def _udf_names(nodes: list[PlanNode]) -> list[str]:
    names: list[str] = []
    for node in nodes:
        if node.name in _PYTHON_EVAL:
            first_list = node.get("Arguments", "").split("], [")[0]
            names += [n for n in _UDF_CALL.findall(first_list) if n not in names]
    return names


class PythonUdfFilter(Rule):
    id = "PLAN001"
    name = "Python UDF in a filter"
    category = "plan"
    description = (
        "A filter calls a Python UDF. Spark can't see inside it, so the filter can't be "
        "pushed into the file scan: every row is read, sent to a Python worker, and only then "
        "filtered."
    )
    thresholds: dict = {}
    docs = (docs_links.ARROW_PYTHON_UDFS, docs_links.PANDAS_UDFS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        for exec_id, nodes in ctx.plans.items():
            udf_filters = [
                n for n in nodes if n.name == "Filter" and "pythonUDF" in n.get("Condition", "")
            ]
            if not udf_filters:
                continue
            scans = [n for n in nodes if n.name.startswith("Scan ")]
            # The formatted plan OMITS the PushedFilters line when nothing is pushed down.
            unpushed = [s for s in scans if s.get("PushedFilters", "[]") == "[]"]
            udfs = _udf_names(nodes) or ["a Python UDF"]
            read = ctx.rows(
                """
                SELECT sum(t.input_records) AS records
                FROM tasks t
                JOIN job_stages js ON js.stage_id = t.stage_id
                JOIN jobs j        ON j.job_id = js.job_id
                WHERE j.sql_execution_id = ?
                """,
                [exec_id],
            )[0]
            filt = udf_filters[0]
            udf_list = ", ".join(udfs)
            evidence = [Evidence("Python UDF(s) used in the filter", udf_list, Unit.TEXT)]
            if read["records"]:
                evidence.append(
                    Evidence("rows read from files, all sent to Python", read["records"])
                )
            for scan in unpushed:
                evidence.append(Evidence(f"filters pushed into {scan.label}", "none", Unit.TEXT))

            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM if unpushed else Severity.LOW,
                title=f"Query {exec_id}: Python UDF {udf_list} in a filter blocks pushdown",
                location=ctx.locate_sql(exec_id, plan_node=filt.label),
                evidence=tuple(evidence),
                cause=(
                    "Spark can't translate a Python function into a file-scan filter, so it "
                    "reads every row and evaluates the UDF in a separate Python process."
                    if unpushed
                    else "The UDF runs row by row in Python; other filters were pushed down."
                ),
                fix=(
                    f"Rewrite {udf_list} with built-in column functions (e.g. "
                    "F.col('country') == 'IN', F.isin, F.regexp_like) so Spark can push the "
                    "filter into the scan.",
                    "If the UDF must stay, apply any cheap built-in filters before it, so fewer "
                    "rows reach Python.",
                    "If it can't be avoided, use a vectorized pandas_udf: still no pushdown, but "
                    "much faster than a row-at-a-time UDF.",
                ),
                category=self.category,
                details={"udfs": udfs, "scans_without_pushdown": [s.label for s in unpushed]},
            )


class CartesianJoin(Rule):
    id = "PLAN002"
    name = "Cartesian or nested-loop join"
    category = "plan"
    description = (
        "A join without an equality condition compares every row of one side with every row "
        "of the other. Its cost grows with the product of the input sizes."
    )
    thresholds: dict = {}
    docs = (docs_links.JOIN_HINTS, docs_links.BROADCAST_JOINS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        for exec_id, nodes in ctx.plans.items():
            for node in nodes:
                if node.name not in ("CartesianProduct", "BroadcastNestedLoopJoin"):
                    continue
                cartesian = node.name == "CartesianProduct"
                condition = clean_expr(node.get("Join condition", "None"))
                broadcast_off = (ctx.conf_bytes("spark.sql.autoBroadcastJoinThreshold") or 0) < 0
                fixes = [
                    "Join on an equality between keys if there is one, so Spark can use a hash "
                    "or sort-merge join.",
                    "For range conditions (a < b), bucket the values first (e.g. join on "
                    "floor(value / width) and neighbouring buckets), then apply the exact "
                    "condition.",
                ]
                if cartesian:
                    fixes.append(
                        "If one side is small, broadcast it (F.broadcast(df)): a broadcast "
                        "nested-loop join avoids shuffling the product."
                        + (
                            " Broadcasting is disabled for this app "
                            "(spark.sql.autoBroadcastJoinThreshold=-1)."
                            if broadcast_off
                            else ""
                        )
                    )
                yield Finding(
                    rule_id=self.id,
                    severity=Severity.HIGH if cartesian else Severity.LOW,
                    title=f"Query {exec_id}: {node.name} join compares every pair of rows",
                    location=ctx.locate_sql(exec_id, plan_node=node.label),
                    evidence=(
                        Evidence("join type", node.get("Join type", "?"), Unit.TEXT),
                        Evidence("join condition", condition, Unit.TEXT),
                    ),
                    cause=(
                        "The join condition has no equality between columns of the two sides, "
                        f"so Spark falls back to {node.name}."
                    ),
                    fix=tuple(fixes),
                    category=self.category,
                    details={"node": node.label, "condition": condition},
                )


def _udf_outputs(node: PlanNode) -> list[str]:
    """Output attributes of a *EvalPython node.

    '[f(x#1)#5], [pythonUDF0#16]' -> ['pythonUDF0#16']
    """
    parts = node.get("Arguments", "").split("], [")
    return re.findall(r"pythonUDF\d+#\d+", parts[1]) if len(parts) > 1 else []


class RowByRowPythonUdf(Rule):
    id = "PLAN005"
    name = "Row-at-a-time Python UDF"
    category = "plan"
    description = (
        "A Python UDF runs row by row: every row is pickled, sent to a Python worker, processed "
        "alone and sent back. Arrow Python UDFs and pandas UDFs move data in columnar batches; "
        "built-in functions avoid Python altogether. UDFs used only in filters are PLAN001."
    )
    thresholds: dict = {}
    docs = (docs_links.ARROW_PYTHON_UDFS, docs_links.PANDAS_UDFS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        version = spark_version(ctx)
        for exec_id, nodes in ctx.plans.items():
            filter_conditions = " ".join(
                n.get("Condition", "") for n in nodes if n.name == "Filter"
            )
            row_by_row = [
                n
                for n in nodes
                if n.name == "BatchEvalPython"
                and not all(out in filter_conditions for out in _udf_outputs(n) or ["?"])
            ]
            if not row_by_row:
                continue
            udfs = ", ".join(_udf_names(row_by_row)) or "a Python UDF"
            rows = query_input_records(ctx, exec_id)
            evidence = [Evidence("Python UDF(s) run row by row", udfs, Unit.TEXT)]
            if rows:
                evidence.append(Evidence("rows read by the query, all sent to Python", rows))
            evidence.append(Evidence("Spark version", ctx.app.get("spark_version"), Unit.TEXT))
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM,
                title=f"Query {exec_id}: Python UDF {udfs} runs row by row",
                location=ctx.locate_sql(exec_id, plan_node=row_by_row[0].label),
                evidence=tuple(evidence),
                cause=self._cause(ctx, version),
                fix=(
                    f"Replace {udfs} with built-in functions if possible (F.when, "
                    "F.regexp_extract, F.transform, ...): no Python worker, and Spark can "
                    "optimize the expression.",
                    "Otherwise use an Arrow Python UDF: @udf(..., useArrow=True), or set "
                    "spark.sql.execution.pythonUDF.arrow.enabled=true for the whole session "
                    "(Spark 3.5+).",
                    "Or rewrite it as a pandas_udf that works on a whole pandas Series at once.",
                ),
                category=self.category,
                details={"udfs": _udf_names(row_by_row), "spark_version": list(version)},
            )

    @staticmethod
    def _cause(ctx: AnalysisContext, version: tuple[int, int]) -> str:
        setting = ctx.config.get("spark.sql.execution.pythonUDF.arrow.enabled", "").lower()
        if setting == "false":
            return (
                "Arrow optimization for Python UDFs is turned off for this app "
                "(spark.sql.execution.pythonUDF.arrow.enabled=false)."
            )
        if version >= (4, 2):
            return (
                "Spark 4.2+ runs Python UDFs with Arrow by default, so this one either set "
                "useArrow=False or fell back silently because pandas or PyArrow isn't installed "
                "where it ran."
            )
        if version >= (3, 5):
            return (
                "Before Spark 4.2, Python UDFs run pickled row by row unless useArrow=True or "
                "spark.sql.execution.pythonUDF.arrow.enabled=true."
            )
        return (
            "Before Spark 3.5, plain Python UDFs always run row by row; pandas_udf is the "
            "vectorized alternative."
        )
