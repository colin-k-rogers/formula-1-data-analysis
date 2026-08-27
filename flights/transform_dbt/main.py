"""MotherDuck Flight: run the f1 dbt project (`dbt build`) against md:f1.

Fetches the dbt/ project directly from the GitHub repo at build time (as a
tarball of GITHUB_REF, default "main") instead of running against a snapshot
embedded in this file, so the Flight always builds whatever is on GitHub.
"""
import io
import os
import pathlib
import subprocess
import tarfile
import urllib.request

GITHUB_REPO = "colin-k-rogers/formula-1-data-analysis"
GITHUB_REF = os.environ.get("GITHUB_REF", "main")
FETCH_TIMEOUT_SEC = 30

PROJECT_DIR = pathlib.Path("/tmp/dbt_project")

SKIP_DIRS = {"target", "dbt_packages", "logs"}


def fetch_dbt_project():
    url = f"https://codeload.github.com/{GITHUB_REPO}/tar.gz/{GITHUB_REF}"
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


def main():
    fetch_dbt_project()

    subprocess.run(
        ["dbt", "build", "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)],
        check=True,
    )


if __name__ == "__main__":
    main()
