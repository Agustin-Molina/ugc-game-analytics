"""Tests de scripts/ran_today.sh con un `gh` falso (necesita bash y jq)."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ran_today.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("jq") is None, reason="necesita bash y jq"
)

FAKE_GH = """#!/usr/bin/env bash
# gh falso: guarda los argumentos y aplica -q (jq) sobre $FAKE_RUNS
printf '%s\\n' "$@" > "$FAKE_ARGS"
[ -n "${FAKE_FAIL:-}" ] && exit 1
while [ $# -gt 0 ]; do
  if [ "$1" = "-q" ]; then q="$2"; fi
  shift
done
printf '%s' "$FAKE_RUNS" | jq -r "$q"
"""


def run_guard(tmp_path: Path, event: str, runs: list[dict], fail: bool = False):
    gh = tmp_path / "gh"
    gh.write_text(FAKE_GH, newline="\n")
    gh.chmod(0o755)
    out = tmp_path / "out"
    env = {
        **os.environ,
        "GH": str(gh),
        "FAKE_RUNS": json.dumps(runs),
        "FAKE_ARGS": str(tmp_path / "args"),
        "GITHUB_EVENT_NAME": event,
        "GITHUB_WORKFLOW_REF": "o/r/.github/workflows/fortnite_daily.yml@refs/heads/main",
        "GITHUB_RUN_ID": "100",
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_OUTPUT": str(out),
    }
    if fail:
        env["FAKE_FAIL"] = "1"
    proc = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                          check=True)
    return out.read_text().strip(), proc.stdout, tmp_path / "args"


def test_schedule_skips_when_another_run_succeeded_today(tmp_path):
    runs = [{"databaseId": 100, "status": "in_progress", "conclusion": ""},
            {"databaseId": 7, "status": "completed", "conclusion": "success"}]
    out, stdout, args = run_guard(tmp_path, "schedule", runs)
    assert out == "skip=true"
    assert "run 7" in stdout
    a = args.read_text().splitlines()
    assert a[a.index("--workflow") + 1] == "fortnite_daily.yml"
    assert a[a.index("--created") + 1].startswith(">=")


def test_schedule_skips_when_another_run_in_progress(tmp_path):
    runs = [{"databaseId": 100, "status": "in_progress", "conclusion": ""},
            {"databaseId": 8, "status": "in_progress", "conclusion": ""}]
    assert run_guard(tmp_path, "schedule", runs)[0] == "skip=true"


def test_schedule_runs_when_only_itself_or_failures(tmp_path):
    runs = [{"databaseId": 100, "status": "in_progress", "conclusion": ""},
            {"databaseId": 9, "status": "completed", "conclusion": "failure"},
            {"databaseId": 10, "status": "completed", "conclusion": "cancelled"}]
    assert run_guard(tmp_path, "schedule", runs)[0] == "skip=false"


def test_dispatch_always_runs(tmp_path):
    runs = [{"databaseId": 7, "status": "completed", "conclusion": "success"}]
    out, _, args = run_guard(tmp_path, "workflow_dispatch", runs)
    assert out == "skip=false"
    assert not args.exists()  # ni consulta la API


def test_api_failure_runs_anyway(tmp_path):
    out, stdout, _ = run_guard(tmp_path, "schedule", [], fail=True)
    assert out == "skip=false"
    assert "corro igual" in stdout
