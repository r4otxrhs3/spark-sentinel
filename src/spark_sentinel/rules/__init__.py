"""Steps 7-8: the rule interface, and the rules themselves (one module each, added in Step 8).

Writing a rule
--------------
Create a module in this package (e.g. rules/skew.py) with one class per rule:

    class DataSkew(Rule):
        id = "SKEW001"
        name = "Task skew"
        category = "skew"
        description = "One task in a stage takes far longer than its siblings."
        thresholds = {"min_ratio": 5.0, "min_time_lost_ms": 30_000}

        def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
            for row in ctx.rows("SELECT ... FROM tasks ... HAVING ...", [self.t("min_ratio")]):
                yield Finding(rule_id=self.id, severity=..., title=...,
                              location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
                              evidence=(...), cause="...", fix=(...))

That's all: defining the class registers it (no list to edit), and every module in this
package is imported automatically. Rules only read the tables through `ctx`, never files.

Conventions
- id: CATEGORY + 3 digits, unique (SKEW001, SPILL001, PLAN001, ...).
- Thresholds: every number a rule compares against goes in `thresholds`, so users and tests
  can change it (laptop logs need far lower thresholds than production).
- Ratios in SQL: guard with NULLIF(x, 0). In DuckDB x / 0 is inf, not NULL.
- A rule must not raise for odd data; return no findings instead. (If it does raise, the
  engine records the error and carries on with the other rules.)
"""

from __future__ import annotations

import importlib
import pkgutil
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from spark_sentinel.analysis import AnalysisContext
    from spark_sentinel.findings import Finding

RULE_ID_RE = re.compile(r"^[A-Z]{2,12}\d{3}$")

REGISTRY: dict[str, type[Rule]] = {}


class Rule(ABC):
    """Base class for every rule. Subclasses are registered when they are defined.

    Pass `register=False` in the class statement to keep a rule out of the registry
    (useful in tests): `class MyRule(Rule, register=False): ...`
    """

    id: ClassVar[str]
    name: ClassVar[str]
    category: ClassVar[str] = "performance"
    description: ClassVar[str] = ""
    thresholds: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, register: bool = True, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not register:
            return
        for attr in ("id", "name"):
            if not isinstance(getattr(cls, attr, None), str):
                raise TypeError(f"Rule {cls.__name__} must define a class attribute '{attr}'")
        if not RULE_ID_RE.match(cls.id):
            raise ValueError(f"Rule id '{cls.id}' must look like SKEW001 (letters + 3 digits)")
        existing = REGISTRY.get(cls.id)
        if existing is not None and existing.__qualname__ != cls.__qualname__:
            raise ValueError(
                f"Rule id {cls.id} is used by both {existing.__name__} and {cls.__name__}"
            )
        REGISTRY[cls.id] = cls

    def __init__(self, overrides: dict[str, Any] | None = None) -> None:
        self.settings: dict[str, Any] = dict(self.thresholds)
        for key, value in (overrides or {}).items():
            if key not in self.thresholds:
                valid = ", ".join(self.thresholds) or "none"
                raise ValueError(f"{self.id} has no threshold '{key}'. Valid: {valid}")
            default = self.thresholds[key]
            try:
                self.settings[key] = type(default)(value) if default is not None else value
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"{self.id}.{key} must be a {type(default).__name__}, got {value!r}"
                ) from exc

    def t(self, key: str) -> Any:
        """Current value of a threshold (default, or the user's override)."""
        return self.settings[key]

    @abstractmethod
    def evaluate(self, ctx: AnalysisContext) -> Iterable[Finding]:
        """Inspect one app through ctx and yield (or return) its findings."""


def load_builtin_rules() -> dict[str, type[Rule]]:
    """Import every module in this package so their rules register; return the registry."""
    for module in pkgutil.iter_modules(__path__):
        if not module.name.startswith("_"):
            importlib.import_module(f"{__name__}.{module.name}")
    return dict(sorted(REGISTRY.items()))
