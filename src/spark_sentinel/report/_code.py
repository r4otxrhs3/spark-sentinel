"""Find code-like tokens in prose (config keys, df./F. calls, repartition(N)) to format them."""

from __future__ import annotations

import re
from collections.abc import Callable

_CODE = re.compile(
    r"spark(?:\.\w+)+(?:=[\w\-.]*\w)?"  # spark.sql.adaptive.enabled=true
    r"|\b(?:df|F)\.\S+"  # df.groupBy('k').count()..., F.broadcast(df)
    r"|\b(?:repartition|coalesce)\(\d*\)"  # repartition(500)
    r"|/\*\+ [A-Z_]+\(\w*\) \*/"  # /*+ BROADCAST(t) */
)


def _trim(token: str) -> tuple[str, str]:
    """Split trailing punctuation and unbalanced ')' off a token: ('f(x)', ').')."""
    tail = ""
    while token and (
        token[-1] in ".,;:" or (token[-1] == ")" and token.count(")") > token.count("("))
    ):
        token, tail = token[:-1], token[-1] + tail
    return token, tail


def mark_code(text: str, wrap: Callable[[str], str]) -> str:
    """Replace every code-like token in text with wrap(token)."""

    def replace(match: re.Match) -> str:
        token, tail = _trim(match.group(0))
        return wrap(token) + tail

    return _CODE.sub(replace, text)
