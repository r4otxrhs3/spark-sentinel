"""The Spark Sentinel GitHub Action: run `sentinel diff`, report it, and decide pass or fail.

action.yml installs Sentinel into its own virtual environment and runs this script with that
environment's Python. Everything comes in through environment variables (never through
${{ }} in the script, so input values can't inject code):

    SENTINEL_BASELINE, SENTINEL_NEW      event log, folder with one run's log, or app ID
    SENTINEL_PROFILE                     default | laptop
    SENTINEL_FAIL_ON                     comma- or space-separated: slower, high, medium, ...
    SENTINEL_THRESHOLDS                  lines of name=value (min_change_pct, min_time_ms, ...)
    SENTINEL_CONFIG                      path to sentinel.toml (optional)
    SENTINEL_COMMENT                     true | false
    SENTINEL_COMMENT_KEY                 tells apart several diffs commenting on one PR
    GITHUB_TOKEN, GITHUB_*, RUNNER_TEMP  set by GitHub (or the action)

It writes the report to the job summary, posts or updates one PR comment (found again by a
hidden marker), sets the step's outputs, and exits 1 only when --fail-on matched. A comment
that can't be posted (a pull request from a fork gets a read-only token) is a warning, not a
failure: the report is still in the job summary.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

COMMENT_LIMIT = 60_000  # GitHub allows 65,536 characters; leave room for the footer
MARKER = "<!-- spark-sentinel-diff:{key} -->"
TRUTHY = {"1", "true", "yes", "on"}


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def log(message: str) -> None:
    print(message, flush=True)


def annotate(level: str, message: str) -> None:
    """A workflow command: shows as an annotation on the run's summary page."""
    print(f"::{level} title=Spark Sentinel::{message}", flush=True)


# --- running sentinel diff


def diff_command(fmt: str, output: Path, workdir: Path) -> list[str]:
    command = [
        sys.executable, "-m", "spark_sentinel", "diff",
        env("SENTINEL_BASELINE"), env("SENTINEL_NEW"),
        "-w", str(workdir / "warehouse"),
        "-f", fmt, "-o", str(output),
    ]  # fmt: skip
    if env("SENTINEL_PROFILE"):  # else sentinel.toml's profile, else "default"
        command += ["--profile", env("SENTINEL_PROFILE")]
    for condition in env("SENTINEL_FAIL_ON").replace(",", " ").split():
        command += ["--fail-on", condition]
    for line in env("SENTINEL_THRESHOLDS").splitlines():
        if line.strip():
            command += ["--threshold", line.strip()]
    if env("SENTINEL_CONFIG"):
        command += ["--config", env("SENTINEL_CONFIG")]
    return command


def run_diff(workdir: Path) -> tuple[int, str, dict, str]:
    """Returns (exit code, Markdown report, JSON summary, error text)."""
    if not env("SENTINEL_BASELINE") or not env("SENTINEL_NEW"):
        return 2, "", {}, "Both `baseline` and `new` inputs are required."
    md, js = workdir / "report.md", workdir / "report.json"
    result = subprocess.run(
        diff_command("md", md, workdir), capture_output=True, text=True, check=False
    )
    log(result.stdout.strip())
    if result.returncode not in (0, 1):
        return result.returncode, "", {}, (result.stderr or result.stdout).strip()
    # Same comparison as JSON, for the step's outputs. Logs are ingested already, so it's quick.
    data_run = subprocess.run(
        diff_command("json", js, workdir), capture_output=True, text=True, check=False
    )
    data = json.loads(js.read_text()) if data_run.returncode in (0, 1) and js.exists() else {}
    return result.returncode, md.read_text(encoding="utf-8"), data, ""


# --- the report in a comment


def truncate(report: str, limit: int = COMMENT_LIMIT) -> str:
    """Cut a long report at a line, closing any open code block or <details>."""
    if len(report) <= limit:
        return report
    kept: list[str] = []
    size = 0
    fence = details = 0
    for line in report.splitlines():
        if size + len(line) + 1 > limit:
            break
        kept.append(line)
        size += len(line) + 1
        if line.startswith("```"):
            fence ^= 1
        details += line.count("<details") - line.count("</details>")
    if fence:
        kept.append("```")
    kept += ["</details>"] * max(details, 0)
    kept += ["", "_Report cut to fit a comment: the full report is in the job summary._"]
    return "\n".join(kept) + "\n"


def pull_request_number() -> int | None:
    path = env("GITHUB_EVENT_PATH")
    if not path or not Path(path).exists():
        return None
    event = json.loads(Path(path).read_text(encoding="utf-8"))
    number = (event.get("pull_request") or {}).get("number")
    if number is None and "pull_request" in (event.get("issue") or {}):  # a comment on a PR
        number = event["issue"]["number"]
    return int(number) if number is not None else None


class GitHub:
    def __init__(self, token: str, repo: str, api: str) -> None:
        self.token, self.repo, self.api = token, repo, api.rstrip("/")

    def call(self, method: str, path: str, body: dict | None = None) -> object:
        request = urllib.request.Request(
            f"{self.api}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
                "User-Agent": "spark-sentinel-action",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read().decode()
        return json.loads(text) if text else None

    def find_comment(self, number: int, marker: str) -> int | None:
        page = 1
        while True:
            comments = self.call(
                "GET", f"/repos/{self.repo}/issues/{number}/comments?per_page=100&page={page}"
            )
            for comment in comments or []:
                if (comment.get("body") or "").startswith(marker):
                    return comment["id"]
            if not comments or len(comments) < 100:
                return None
            page += 1

    def post_or_update(self, number: int, marker: str, body: str) -> str:
        existing = self.find_comment(number, marker)
        if existing is not None:
            self.call("PATCH", f"/repos/{self.repo}/issues/comments/{existing}", {"body": body})
            return "updated"
        self.call("POST", f"/repos/{self.repo}/issues/{number}/comments", {"body": body})
        return "created"


def comment(report: str) -> str:
    """Post the report on the pull request. Returns what happened, for the log and outputs."""
    if env("SENTINEL_COMMENT", "true").lower() not in TRUTHY:
        return "off"
    number = pull_request_number()
    if number is None:
        log("Not a pull request: the report is in the job summary only.")
        return "not-a-pr"
    token, repo = env("GITHUB_TOKEN"), env("GITHUB_REPOSITORY")
    if not token or not repo:
        annotate("warning", "No GitHub token: can't comment. The report is in the job summary.")
        return "no-token"
    marker = MARKER.format(key=env("SENTINEL_COMMENT_KEY", "default") or "default")
    body = f"{marker}\n{truncate(report)}"
    github = GitHub(token, repo, env("GITHUB_API_URL", "https://api.github.com"))
    try:
        what = github.post_or_update(number, marker, body)
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            annotate(
                "warning",
                f"Couldn't comment on pull request #{number} (HTTP {exc.code}). The job needs "
                "`permissions: pull-requests: write`; pull requests from forks get a read-only "
                "token. The report is in the job summary.",
            )
            return "forbidden"
        annotate(
            "warning", f"Couldn't comment (HTTP {exc.code}). The report is in the job summary."
        )
        return "failed"
    except (urllib.error.URLError, TimeoutError) as exc:
        # The performance check shouldn't fail because GitHub's API had a bad moment.
        annotate(
            "warning", f"Couldn't reach the GitHub API ({exc}). The report is in the job summary."
        )
        return "failed"
    log(f"Comment {what} on pull request #{number}.")
    return what


# --- outputs


def write_summary(report: str) -> None:
    path = env("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as summary:
            summary.write(report + "\n")


def set_outputs(values: dict[str, object]) -> None:
    path = env("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as out:
        for key, value in values.items():
            out.write(f"{key}={value}\n")


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="spark-sentinel-", dir=env("RUNNER_TEMP") or None))
    code, report, data, error = run_diff(workdir)
    if error:
        annotate("error", error.splitlines()[-1] if error else f"sentinel diff exited {code}")
        write_summary(f"## Spark Sentinel diff\n\n❌ The diff couldn't run:\n\n```\n{error}\n```")
        set_outputs({"result": "error"})
        return code or 2

    write_summary(report)
    commented = comment(report)
    summary = data.get("summary", {})
    result = "fail" if code == 1 else "pass"
    set_outputs(
        {
            "result": result,
            "confirmed-slower": summary.get("confirmed_slower", 0),
            "slower": summary.get("slower", 0),
            "new-problems": summary.get("new_problems", 0),
            "report": workdir / "report.md",
            "comment": commented,
        }
    )
    if code == 1:
        annotate("error", "Performance check failed: see the report in the job summary.")
    return code


if __name__ == "__main__":
    sys.exit(main())
