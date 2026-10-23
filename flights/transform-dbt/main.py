"""MotherDuck Flight: run the f1 dbt project (`dbt build`) against md:f1.

Fetches the dbt/ project directly from the GitHub repo at build time (as a
tarball of GITHUB_REF, default "main") instead of running against a snapshot
embedded in this file, so the Flight always builds whatever is on GitHub.
"""
# Run-visualization chart; node ids join to the @flight-run lines below. The
# convention is MotherDuck's flight viz guide (get_flight_viz_guide).
#
# @flight
# flowchart TD
#   fetch[Fetch dbt project from GitHub]:::extract --> build[dbt build]:::query
#   class build fanout
# @end-flight
import io
import json
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import urllib.request
from datetime import datetime, timezone

GITHUB_REPO = "colin-k-rogers/formula-1-data-analysis"
GITHUB_REF = os.environ.get("GITHUB_REF", "main")
# Appended to the staging/marts schema names (see dbt/dbt_project.yml) so a
# preview run lands in its own branch-scoped schemas instead of prod's.
SCHEMA_SUFFIX = os.environ.get("SCHEMA_SUFFIX", "")
FETCH_TIMEOUT_SEC = 30

PROJECT_DIR = pathlib.Path("/tmp/dbt_project")

SKIP_DIRS = {"target", "dbt_packages", "logs"}

# dbt's per-node progress line, e.g. "1 of 40 OK created sql view model
# main.stg_x ..... [OK in 0.12s]" or "3 of 40 FAIL 2 unique_x ..... [FAIL 2 in
# 0.1s]". The node's name is the last word before the dot leader.
DBT_PROGRESS = re.compile(
    r"\b\d+ of \d+ (START|OK|PASS|WARN|ERROR|FAIL|SKIP)\b(.*?)\s+\.*\s*\["
)
DBT_STATUS = {
    "START": "running",
    "OK": "ok",
    "PASS": "ok",
    "WARN": "ok",
    "ERROR": "failed",
    "FAIL": "failed",
    "SKIP": "skipped",
}

# Node last marked running, so a crash can be pinned on the step it hit.
_current = None


def emit(node, status, **fields):
    """Print one @flight-run progress line for the run-visualization chart."""
    global _current
    _current = (node, fields.get("key")) if status == "running" else None
    fields["ts"] = datetime.now(timezone.utc).isoformat()
    parts = [f"@flight-run {node} {status}"]
    for name, value in fields.items():
        value = " ".join(str(value).split())  # one event per line
        if " " in value or '"' in value or "=" in value:
            value = '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        parts.append(f"{name}={value}")
    print(" ".join(parts), flush=True)


def run_dbt(args, node):
    """Run dbt, passing its output through and reporting each model, seed and
    test it builds as one instance of `node`. Raises CalledProcessError on
    failure."""
    proc = subprocess.Popen(
        ["dbt", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    reported = 0
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        m = DBT_PROGRESS.search(line)
        if not m:
            continue
        verb, rest = m.groups()
        name = rest.split()[-1]
        fields = {"key": name}
        # "OK created ... model main.x" / "OK loaded seed file main.x" persist
        # a relation; a test's PASS doesn't.
        if verb == "OK":
            fields["effect"] = f"table:f1.{name}"
        emit(node, DBT_STATUS[verb], **fields)
        reported += 1
    if proc.wait() != 0:
        # Per-node failures are already on their own rows; this covers dbt
        # dying before it built anything (bad profile, parse error).
        if not reported:
            emit(node, "failed", error=f"dbt exited {proc.returncode}")
        raise subprocess.CalledProcessError(proc.returncode, ["dbt", *args])


def fetch_dbt_project():
    """Extract the repo's dbt/ directory into PROJECT_DIR; returns the number
    of files written."""
    url = f"https://codeload.github.com/{GITHUB_REPO}/tar.gz/{GITHUB_REF}"
    written = 0
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SEC) as resp:
        archive = resp.read()

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            # Strip the leading "<repo>-<ref>/" component GitHub adds, then
            # the "dbt/" prefix — we only want that subdirectory.
            parts = pathlib.PurePosixPath(member.name).parts[1:]
            if len(parts) < 2 or parts[0] != "dbt":
                continue
            rel_parts = parts[1:]
            if any(part in SKIP_DIRS for part in rel_parts):
                continue

            dest = PROJECT_DIR / pathlib.PurePosixPath(*rel_parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(tar.extractfile(member).read())
            written += 1
    return written


def main():
    emit("fetch", "running")
    files = fetch_dbt_project()
    emit("fetch", "ok", rows=files, effect="net:codeload.github.com")

    run_dbt(
        [
            "build",
            "--project-dir", str(PROJECT_DIR),
            "--profiles-dir", str(PROJECT_DIR),
            "--vars", json.dumps({"schema_suffix": SCHEMA_SUFFIX}),
        ],
        "build",
    )


if __name__ == "__main__":
    try:
        main()
        emit("@flight", "ok", exit=0)
    except Exception as err:
        if _current:
            node, key = _current
            extra = {"key": key} if key else {}
            emit(node, "failed", error=f"{type(err).__name__}: {err}", **extra)
        emit("@flight", "failed", exit=1)
        raise
