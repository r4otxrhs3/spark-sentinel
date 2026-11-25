"""Step 13: the GitHub Action (action.yml + action/run_action.py).

The action's "Compare runs" step is run here the way GitHub runs it: its `run:` script through
bash, with its `env:` filled in from the inputs. Comments go to a fake GitHub API on localhost
that records every request.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")  # dev dependency; the oldest-dependencies CI job lacks it

ROOT = Path(__file__).resolve().parents[1]
DIFF = ROOT / "tests" / "fixtures" / "eventlogs" / "diff"
ACTION = yaml.safe_load((ROOT / "action.yml").read_text())

spec = importlib.util.spec_from_file_location("run_action", ROOT / "action" / "run_action.py")
run_action = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_action)


# --- a fake GitHub API


class FakeGitHub:
    def __init__(self) -> None:
        self.comments: list[dict] = []
        self.requests: list[tuple[str, str, dict | None]] = []
        self.status = 200  # set to 403 to act like a fork's read-only token
        self.next_id = 100

    def handle(self, method: str, path: str, body: dict | None) -> tuple[int, object]:
        self.requests.append((method, path, body))
        if self.status != 200:
            return self.status, {"message": "Resource not accessible by integration"}
        if method == "GET" and re.match(r"/repos/o/r/issues/\d+/comments", path):
            page = int(re.search(r"[?&]page=(\d+)", path).group(1))
            return 200, self.comments[(page - 1) * 100 : page * 100]
        if method == "POST":
            self.next_id += 1
            self.comments.append({"id": self.next_id, "body": body["body"]})
            return 201, self.comments[-1]
        if method == "PATCH":
            cid = int(path.rsplit("/", 1)[1])
            (c,) = [c for c in self.comments if c["id"] == cid]
            c["body"] = body["body"]
            return 200, c
        return 404, {"message": "Not Found"}


@pytest.fixture
def github():
    fake = FakeGitHub()

    class Handler(BaseHTTPRequestHandler):
        def _serve(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            assert self.headers["Authorization"] == "Bearer test-token"
            status, data = fake.handle(self.command, self.path, body)
            payload = json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_PATCH = _serve

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    fake.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield fake
    server.shutdown()


# --- running the action's step


def compare_step() -> dict:
    (step,) = [s for s in ACTION["runs"]["steps"] if s.get("id") == "diff"]
    return step


def run_step(tmp_path: Path, github: FakeGitHub | None, event: dict | None, **inputs) -> dict:
    """Run action.yml's "Compare runs" step as GitHub would; return its outputs and files."""
    values = {name: spec.get("default", "") for name, spec in ACTION["inputs"].items()}
    values["github-token"] = "test-token"
    values.update({k.replace("_", "-"): v for k, v in inputs.items()})

    def fill(text: str) -> str:
        text = re.sub(r"\$\{\{\s*inputs\.([\w-]+)\s*\}\}", lambda m: str(values[m[1]]), text)
        text = text.replace("${{ github.action_path }}", str(ROOT))
        return text.replace("${{ steps.install.outputs.python }}", sys.executable)

    step = compare_step()
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event or {}))
    (tmp_path / "runner").mkdir(exist_ok=True)
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "RUNNER_TEMP": str(tmp_path / "runner"),
        "GITHUB_OUTPUT": str(tmp_path / "output"),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary.md"),
        "GITHUB_EVENT_PATH": str(event_path),
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_API_URL": github.url if github else "http://127.0.0.1:9",
    }
    environment.update({k: fill(str(v)) for k, v in step["env"].items()})
    proc = subprocess.run(
        ["bash", "-c", step["run"]], env=environment, capture_output=True, text=True, timeout=300
    )
    outputs = {}
    if (tmp_path / "output").exists():
        for line in (tmp_path / "output").read_text().splitlines():
            key, _, value = line.partition("=")
            outputs[key] = value
    summary = tmp_path / "summary.md"
    return {
        "code": proc.returncode,
        "log": proc.stdout + proc.stderr,
        "outputs": outputs,
        "summary": summary.read_text() if summary.exists() else "",
    }


PR = {"pull_request": {"number": 7}}


def test_regression_comments_and_fails(tmp_path, github):
    r = run_step(
        tmp_path, github, PR,
        baseline=str(DIFF / "base"), new=str(DIFF / "python_udf"), profile="laptop",
    )  # fmt: skip
    assert r["code"] == 1, r["log"]
    assert r["outputs"]["result"] == "fail"
    assert r["outputs"]["confirmed-slower"] == "1"
    assert r["outputs"]["new-problems"] == "1"
    assert r["outputs"]["comment"] == "created"
    assert Path(r["outputs"]["report"]).read_text().startswith("## Spark Sentinel diff")
    (posted,) = github.comments
    assert posted["body"].startswith("<!-- spark-sentinel-diff:default -->\n## Spark Sentinel diff")
    assert "row-by-row Python UDF" in posted["body"]
    assert "row-by-row Python UDF" in r["summary"]
    assert "::error title=Spark Sentinel::Performance check failed" in r["log"]


def test_second_push_updates_the_same_comment(tmp_path, github):
    github.comments.append({"id": 5, "body": "LGTM"})  # someone else's comment stays
    for new in ("python_udf", "rerun"):
        r = run_step(
            tmp_path, github, PR,
            baseline=str(DIFF / "base"), new=str(DIFF / new), profile="laptop",
        )  # fmt: skip
    assert r["code"] == 0, r["log"]
    assert r["outputs"]["comment"] == "updated"
    assert [c["id"] for c in github.comments] == [5, 101]
    assert "No performance regression" in github.comments[1]["body"]
    assert github.comments[0]["body"] == "LGTM"
    assert [m for m, _, _ in github.requests].count("POST") == 1


def test_comment_keys_keep_separate_comments(tmp_path, github):
    for key in ("etl", "report"):
        run_step(
            tmp_path, github, PR, comment_key=key, fail_on="never",
            baseline=str(DIFF / "base"), new=str(DIFF / "rerun"), profile="laptop",
        )  # fmt: skip
    markers = [c["body"].splitlines()[0] for c in github.comments]
    assert markers == ["<!-- spark-sentinel-diff:etl -->", "<!-- spark-sentinel-diff:report -->"]


def test_fork_pull_request_warns_but_keeps_the_verdict(tmp_path, github):
    github.status = 403
    r = run_step(
        tmp_path, github, PR,
        baseline=str(DIFF / "base"), new=str(DIFF / "python_udf"), profile="laptop",
    )  # fmt: skip
    assert r["code"] == 1  # the slowdown still fails the check
    assert r["outputs"]["comment"] == "forbidden"
    assert (
        "::warning title=Spark Sentinel::Couldn't comment on pull request #7 (HTTP 403)" in r["log"]
    )
    assert "row-by-row Python UDF" in r["summary"]


def test_api_outage_is_only_a_warning(tmp_path):
    r = run_step(
        tmp_path, None, PR, fail_on="never",
        baseline=str(DIFF / "base"), new=str(DIFF / "python_udf"), profile="laptop",
    )  # fmt: skip
    assert r["code"] == 0, r["log"]
    assert r["outputs"]["comment"] == "failed"
    assert "Couldn't reach the GitHub API" in r["log"]


def test_push_event_writes_the_summary_only(tmp_path, github):
    r = run_step(
        tmp_path, github, {"ref": "refs/heads/main"}, fail_on="never",
        baseline=str(DIFF / "base"), new=str(DIFF / "no_broadcast"), profile="laptop",
    )  # fmt: skip
    assert r["code"] == 0
    assert r["outputs"]["comment"] == "not-a-pr"
    assert github.requests == []
    assert "broadcast join became a sort-merge join" in r["summary"]


def test_comment_off_and_fail_on_never(tmp_path, github):
    r = run_step(
        tmp_path, github, PR, comment="false", fail_on="never",
        baseline=str(DIFF / "base"), new=str(DIFF / "python_udf"), profile="laptop",
    )  # fmt: skip
    assert r["code"] == 0
    assert r["outputs"]["result"] == "pass" and r["outputs"]["confirmed-slower"] == "1"
    assert github.requests == []


def test_fail_on_severity_and_thresholds(tmp_path, github):
    common = {"baseline": str(DIFF / "base"), "new": str(DIFF / "no_broadcast"), "comment": "false"}
    # The new problems are MED and INFO: fail on medium, not on high.
    assert run_step(tmp_path, github, PR, fail_on="medium", **common)["code"] == 1
    assert run_step(tmp_path, github, PR, fail_on="high", **common)["code"] == 0
    # With the laptop profile the 2 s slowdown counts; raising min_time_ms hides it.
    laptop = {**common, "profile": "laptop", "fail_on": "slower"}
    assert run_step(tmp_path, github, PR, **laptop)["code"] == 1
    assert run_step(tmp_path, github, PR, thresholds="min_time_ms=60000\n", **laptop)["code"] == 0


def test_profile_comes_from_the_config_unless_given(tmp_path, github):
    config = tmp_path / "sentinel.toml"
    config.write_text('profile = "laptop"\n')
    common = {"baseline": str(DIFF / "base"), "new": str(DIFF / "no_broadcast"), "comment": "false"}
    from_config = run_step(tmp_path, github, PR, config=str(config), **common)
    assert "(profile: laptop)" in from_config["summary"]
    assert from_config["code"] == 1  # the 2 s slowdown counts on the laptop profile
    given = run_step(tmp_path, github, PR, config=str(config), profile="default", **common)
    assert given["summary"].count("(profile: default)") == 1
    assert given["code"] == 0


def test_bad_input_is_an_error_not_a_verdict(tmp_path, github):
    r = run_step(tmp_path, github, PR, baseline=str(DIFF / "base"), new="no/such/run")
    assert r["code"] == 2
    assert r["outputs"]["result"] == "error"
    assert "is not an event log, a folder, or an app ID" in r["summary"]
    assert "::error title=Spark Sentinel::" in r["log"]
    assert github.requests == []

    missing = run_step(tmp_path, github, PR, baseline="", new=str(DIFF / "base"))
    assert missing["code"] == 2
    assert "Both `baseline` and `new` inputs are required." in missing["summary"]


# --- pieces


def test_truncate_closes_open_blocks():
    report = "## Diff\n\n<details><summary>Plan</summary>\n\n```diff\n" + "+ line\n" * 5000
    cut = run_action.truncate(report, limit=2000)
    assert len(cut) < 2300
    assert cut.count("```") % 2 == 0
    assert cut.count("<details") == cut.count("</details>")
    assert cut.rstrip().endswith("the full report is in the job summary._")
    assert run_action.truncate("short") == "short"


def test_pull_request_number_from_events(tmp_path, monkeypatch):
    path = tmp_path / "e.json"
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(path))
    for event, number in [
        ({"pull_request": {"number": 3}}, 3),
        ({"issue": {"number": 4, "pull_request": {}}}, 4),  # issue_comment on a PR
        ({"issue": {"number": 5}}, None),  # a plain issue
        ({}, None),
    ]:
        path.write_text(json.dumps(event))
        assert run_action.pull_request_number() == number


def test_find_comment_pages_through(tmp_path, github, monkeypatch):
    github.comments = [{"id": i, "body": f"comment {i}"} for i in range(1, 251)]
    github.comments.append({"id": 999, "body": "<!-- spark-sentinel-diff:default -->\nold"})
    api = run_action.GitHub("test-token", "o/r", github.url)
    assert api.find_comment(7, "<!-- spark-sentinel-diff:default -->") == 999
    assert [p for _, p, _ in github.requests][-1].endswith("page=3")


# --- action.yml itself


def test_action_inputs_reach_scripts_only_through_env():
    """${{ inputs.x }} inside a run: script would let an input inject shell code."""
    for step in ACTION["runs"]["steps"]:
        assert "${{" not in step["run"], step["name"]
    used = {
        m
        for step in ACTION["runs"]["steps"]
        for value in step.get("env", {}).values()
        for m in re.findall(r"inputs\.([\w-]+)", str(value))
    }
    assert used == set(ACTION["inputs"])  # every input is used, none is misspelled


def test_action_outputs_come_from_the_runner():
    text = (ROOT / "action" / "run_action.py").read_text()
    for name, spec in ACTION["outputs"].items():
        assert spec["value"] == f"${{{{ steps.diff.outputs.{name} }}}}"
        assert f'"{name}"' in text, name
