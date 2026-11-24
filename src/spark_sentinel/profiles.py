"""Named sets of threshold overrides.

Rules' built-in thresholds are tuned for production jobs (minutes-long stages, GBs of data).
Logs from a laptop, a CI test job or the gallery are 100-1000x smaller, so some problems need
lower thresholds to show up at all.

    default   the rules' own thresholds: use for real clusters
    laptop    for small local runs, CI test jobs and the gallery

User overrides (--threshold, config file) are applied on top of the chosen profile.
"""

from __future__ import annotations

from typing import Any

PROFILES: dict[str, dict[str, dict[str, Any]]] = {
    "default": {},
    "laptop": {
        # A laptop straggler costs seconds, not the 30 s production default. Gallery skew loses
        # ~2 s; nothing else in the gallery is flagged even at 0 ms (see tests/test_matrix.py).
        "SKEW001": {"min_time_lost_ms": 1_000},
    },
}


def profile_overrides(
    name: str, overrides: dict[str, dict[str, Any]] | None = None
) -> dict[str, dict[str, Any]]:
    """The profile's overrides with the user's `overrides` merged on top (per threshold)."""
    if name not in PROFILES:
        raise ValueError(f"Unknown profile '{name}'. Choose from: {', '.join(PROFILES)}")
    merged = {rule: dict(values) for rule, values in PROFILES[name].items()}
    for rule, values in (overrides or {}).items():
        merged.setdefault(rule, {}).update(values)
    return merged
