"""Step 9: correctness AND false alarms.

The promise: every bad gallery job triggers its rule, and the clean job triggers nothing.
This suite checks that promise against

- the expected-findings table in lab/gallery/README.md (parsed here, so docs and tests
  can't drift apart),
- logs from two Spark versions: 4.3 (zstd, rolled) and 3.5 (single-file zstd),
- both threshold profiles: "laptop" and "default" (production),
- a "stressed" configuration several times more sensitive than either (false-alarm margin),
- exact threshold boundaries (synthetic apps), and
- a quality contract every finding must meet, whichever rule produced it.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from functools import cache
from pathlib import Path

import pytest

from conftest import GB, MB, AppBuilder, task
from spark_sentinel.analysis import AnalysisResult, analyze
from spark_sentinel.findings import Finding, Severity
from spark_sentinel.profiles import PROFILES, profile_overrides
from spark_sentinel.rules import RULE_ID_RE, load_builtin_rules
from spark_sentinel.store import Warehouse, ingest

ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / "fixtures" / "eventlogs"
SPARK_VERSIONS = {"4.3": FIXTURES / "gallery", "3.5": FIXTURES / "gallery-spark35"}
README = ROOT / "lab" / "gallery" / "README.md"

# Rules with no gallery job (their README column would be empty); covered by synthetic tests.
NO_GALLERY_JOB = {"PART002"}

# Several times more sensitive than any profile: the clean job must STILL report nothing.
STRESSED = {
    "SKEW001": {"min_ratio": 2.5, "min_time_lost_ms": 0, "min_tasks": 3},
    "SPILL001": {"min_disk_spill_bytes": 1},
    "FILES001": {"min_files": 2, "max_median_file_bytes": 64 * MB},
    "PART001": {"min_tasks": 20, "min_bytes_per_task": 32 * MB},
    "PART002": {"max_bytes_per_task": 256 * MB},
    "GC001": {"min_gc_fraction": 0.12, "min_gc_ms": 0},
    "PAR001": {"min_stage_ms": 1_500},
    "PLAN003": {"min_bytes": MB},
    "PLAN004": {"max_broadcast_bytes": GB},
}


# --- the expected table, read from the README


_CELL = re.compile(r"^✅(?P<laptop>\*)?(?:x(?P<count>\d+))?$")


@cache
def expected_table() -> dict[str, dict[str, tuple[str, int]]]:
    """job -> {rule_id: ("always" | "laptop", count)} from the README's ✅, ✅*, ✅x2 cells."""
    lines = README.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("| Job |"))
    header = [c.strip() for c in lines[start].strip("|").split("|")]
    table: dict[str, dict[str, str]] = {}
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip("|").split("|")]
        job = cells[0].strip("`")
        table[job] = {}
        for rule, cell in zip(header, cells, strict=False):
            if RULE_ID_RE.match(rule) and cell:
                match = _CELL.match(cell)
                assert match, (
                    f"README cell for {job}/{rule} must be ✅, ✅*, ✅xN or ✅*xN: {cell!r}"
                )
                when = "laptop" if match.group("laptop") else "always"
                table[job][rule] = (when, int(match.group("count") or 1))
    return table


def expected_counts(job: str, profile: str) -> dict[str, int]:
    """rule -> how many findings the job should produce with this profile."""
    return {
        rule: count
        for rule, (when, count) in expected_table()[job].items()
        if when == "always" or profile == "laptop"
    }


def expected(job: str, profile: str) -> set[str]:
    return set(expected_counts(job, profile))


# --- running the gallery


@cache
def warehouse(version: str) -> Warehouse:
    import tempfile

    root = Path(tempfile.mkdtemp(prefix=f"sentinel-{version}-"))
    ingest(SPARK_VERSIONS[version], root)
    return Warehouse(root)


@cache
def run(version: str, profile: str = "laptop", stressed: bool = False) -> AnalysisResult:
    overrides = STRESSED if stressed else None
    return analyze(warehouse(version), profile=profile, overrides=overrides)


def found_by_job(version: str, result: AnalysisResult) -> dict[str, list[str]]:
    names = dict(warehouse(version).query("SELECT app_id, app_name FROM app")[1])
    by_job: dict[str, list[str]] = {n.removeprefix("gallery-"): [] for n in names.values()}
    for f in result.findings:
        by_job[names[f.location.app_id].removeprefix("gallery-")].append(f.rule_id)
    return by_job


VERSIONS = list(SPARK_VERSIONS)


# --- the README table itself


def test_readme_table_covers_every_job_and_rule():
    table = expected_table()
    rules = set(load_builtin_rules())
    columns = {rule for cells in table.values() for rule in cells}
    gallery_jobs = {p.name for p in SPARK_VERSIONS["4.3"].iterdir() if p.is_dir()}

    assert set(table) == gallery_jobs, "README table and fixture jobs differ"
    assert table["clean"] == {}, "the clean job must expect no findings"
    assert columns <= rules, f"README names unknown rules: {columns - rules}"
    assert rules - columns == NO_GALLERY_JOB, "every rule needs a gallery job or a synthetic test"


@pytest.mark.parametrize("version", VERSIONS)
def test_gallery_logs_are_from_the_expected_spark_version(version):
    versions = warehouse(version).query("SELECT DISTINCT spark_version FROM app")[1]
    assert [v for (v,) in versions] == [{"4.3": "4.3.0", "3.5": "3.5.10"}[version]]


# --- the matrix


@pytest.mark.parametrize("profile", ["laptop", "default"])
@pytest.mark.parametrize("version", VERSIONS)
def test_every_job_triggers_exactly_its_rules(version, profile):
    result = run(version, profile)
    assert result.errors == []
    found = found_by_job(version, result)
    problems = []
    for job in expected_table():
        want, got = expected_counts(job, profile), Counter(found[job])
        if missing := set(want) - set(got):
            problems.append(f"{job}: MISSED {sorted(missing)}")
        if extra := set(got) - set(want):
            problems.append(f"{job}: FALSE ALARM {sorted(extra)}")
        for rule in set(want) & set(got):
            if want[rule] != got[rule]:
                problems.append(f"{job}: {rule} reported {got[rule]}x, expected {want[rule]}x")
    assert problems == [], f"Spark {version}, profile {profile}"


@pytest.mark.parametrize("profile", list(PROFILES))
@pytest.mark.parametrize("version", VERSIONS)
def test_clean_job_reports_nothing(version, profile):
    assert found_by_job(version, run(version, profile))["clean"] == []


@pytest.mark.parametrize("version", VERSIONS)
def test_clean_job_reports_nothing_even_when_rules_are_far_more_sensitive(version):
    result = run(version, stressed=True)
    found = found_by_job(version, result)
    assert found["clean"] == []
    for job in expected_table():  # and nothing that should be found is lost
        assert expected(job, "laptop") <= set(found[job]), job


def test_both_spark_versions_give_the_same_findings():
    def summary(version: str) -> list[tuple]:
        names = dict(warehouse(version).query("SELECT app_id, app_name FROM app")[1])
        return sorted(
            (names[f.location.app_id], f.rule_id, f.severity.name, f.location.stage_id)
            for f in run(version).findings
        )

    assert summary("4.3") == summary("3.5")


def test_analysis_is_deterministic():
    first = analyze(warehouse("4.3"), profile="laptop")
    second = analyze(warehouse("4.3"), profile="laptop")
    assert [f.to_dict() for f in first.findings] == [f.to_dict() for f in second.findings]


# --- the quality contract for every finding


def all_findings() -> list[tuple[str, Finding]]:
    return [(v, f) for v in VERSIONS for f in run(v, stressed=True).findings]


@pytest.mark.parametrize(
    ("version", "finding"),
    all_findings(),
    ids=lambda x: x if isinstance(x, str) else x.fingerprint(),
)
def test_every_finding_meets_the_quality_contract(version, finding):
    f = finding
    app_level = f.location.stage_id is None and f.location.sql_execution_id is None
    assert RULE_ID_RE.match(f.rule_id)
    assert f.severity >= Severity.LOW or (app_level and f.severity is Severity.INFO)
    assert re.match(r"^(Stage \d+|Query \d+|App): ", f.title), f.title
    assert len(f.title) <= 100, f.title
    assert f.cause and f.cause[-1] in ".)", f.cause
    assert f.fix, "every finding needs at least one fix"
    assert all(fix[-1] in ".)" and len(fix) > 20 for fix in f.fix), f.fix
    assert f.evidence, "every finding needs evidence"
    for e in f.evidence:
        text = str(e)
        assert not re.search(r"\b(None|nan|inf|n/a)\b", text), text
    if not app_level:
        assert f.location.sql_execution_id is not None, "gallery problems all belong to a query"
    assert f.references, "every finding links to the docs its advice follows"
    assert f.impact_ms is None or f.impact_ms >= 0
    assert Finding.from_dict(json.loads(json.dumps(f.to_dict()))) == f


@pytest.mark.parametrize("version", VERSIONS)
def test_fingerprints_are_unique(version):
    prints = [f.fingerprint() for f in run(version, stressed=True).findings]
    assert len(prints) == len(set(prints))


@pytest.mark.parametrize("rule_id", sorted(load_builtin_rules()))
def test_every_rule_is_documented_and_configurable(rule_id):
    rule = load_builtin_rules()[rule_id]
    assert rule.name and rule.category and len(rule.description) > 40
    for key, value in rule.thresholds.items():
        assert isinstance(value, int | float) and not isinstance(value, bool), key
        assert re.match(r"^[a-z_]+$", key), key
        rule({key: value})  # every threshold can be overridden with its own default


def test_profiles_only_name_real_rules_and_thresholds():
    rules = load_builtin_rules()
    for name, overrides in PROFILES.items():
        for rule_id, values in overrides.items():
            rules[rule_id](values)  # raises on an unknown threshold
        assert profile_overrides(name, {"SKEW001": {"min_ratio": 9.0}})["SKEW001"]["min_ratio"] == 9
    with pytest.raises(ValueError, match="Unknown profile"):
        profile_overrides("cluster")


# --- exact threshold boundaries (synthetic apps)


def boundary(run_rules, builder, rule, overrides=None) -> bool:
    result = run_rules(builder, select=[rule], overrides=overrides)
    assert result.errors == []
    return any(f.rule_id == rule for f in result.findings)


@pytest.mark.parametrize(("slow_ms", "fires"), [(10_000, True), (9_999, False)])
def test_skew_ratio_boundary(run_rules, slow_ms, fires):
    # median 2,000 ms; min_ratio 5 -> the slowest task must take >= 10,000 ms
    app = AppBuilder().add(*[task() for _ in range(12)], task(launch_ms=5_000, duration_ms=slow_ms))
    assert boundary(run_rules, app, "SKEW001", {"SKEW001": {"min_time_lost_ms": 0}}) is fires


@pytest.mark.parametrize(("lost_ms", "fires"), [(30_000, True), (29_999, False)])
def test_skew_time_lost_boundary(run_rules, lost_ms, fires):
    app = AppBuilder().add(
        *[task() for _ in range(12)], task(launch_ms=5_000, duration_ms=2_000 + lost_ms)
    )
    assert boundary(run_rules, app, "SKEW001") is fires


@pytest.mark.parametrize(("spill", "fires"), [(100 * MB, True), (100 * MB - 1, False)])
def test_spill_boundary(run_rules, spill, fires):
    app = AppBuilder().add(task(disk_spill=spill), *[task() for _ in range(3)])
    assert boundary(run_rules, app, "SPILL001") is fires


@pytest.mark.parametrize(("files", "fires"), [(100, True), (99, False)])
def test_small_files_count_boundary(run_rules, files, fires):
    app = AppBuilder().add(*[task(output_bytes=MB, output_records=5) for _ in range(files)])
    assert boundary(run_rules, app, "FILES001") is fires


@pytest.mark.parametrize(("size", "fires"), [(32 * MB - 1, True), (32 * MB, False)])
def test_small_files_size_boundary(run_rules, size, fires):
    app = AppBuilder().add(*[task(output_bytes=size, output_records=5) for _ in range(100)])
    assert boundary(run_rules, app, "FILES001") is fires


@pytest.mark.parametrize(("tasks", "fires"), [(200, True), (199, False)])
def test_too_many_partitions_task_boundary(run_rules, tasks, fires):
    app = AppBuilder().add(*[task(shuffle_read=MB) for _ in range(tasks)])
    assert boundary(run_rules, app, "PART001") is fires


@pytest.mark.parametrize(("per_task", "fires"), [(8 * MB - 1, True), (8 * MB, False)])
def test_too_many_partitions_size_boundary(run_rules, per_task, fires):
    app = AppBuilder().add(*[task(shuffle_read=per_task) for _ in range(300)])
    assert boundary(run_rules, app, "PART001") is fires


@pytest.mark.parametrize(("per_task", "fires"), [(GB + 1, True), (GB, False)])
def test_too_few_partitions_boundary(run_rules, per_task, fires):
    app = AppBuilder().add(*[task(shuffle_read=per_task) for _ in range(4)])
    assert boundary(run_rules, app, "PART002") is fires


def test_suggested_partition_counts_hit_the_target(run_rules):
    app = AppBuilder().add(*[task(shuffle_read=3 * GB) for _ in range(4)])
    (f,) = run_rules(app, select=["PART002"]).findings
    assert f.details["suggested_partitions"] == math.ceil(12 * GB / (128 * MB))
