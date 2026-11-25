"""Step 12: diff mode. Compare two runs of the same app and explain what got slower.

from spark_sentinel.diff import compare, render_diff
result = compare(Warehouse(".sentinel"), base_app_id, new_app_id, profile="laptop")
print(render_diff(result, "text"))
"""

from spark_sentinel.diff.compare import (
    DIFF_THRESHOLDS,
    DiffResult,
    QueryDiff,
    compare,
    diff_thresholds,
    match_queries,
    setting_changes,
)
from spark_sentinel.diff.render import render_diff

__all__ = [
    "DIFF_THRESHOLDS",
    "DiffResult",
    "QueryDiff",
    "compare",
    "diff_thresholds",
    "match_queries",
    "render_diff",
    "setting_changes",
]
