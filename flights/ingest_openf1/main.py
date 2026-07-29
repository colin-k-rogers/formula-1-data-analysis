"""MotherDuck Flight: ingest OpenF1 Race-session data for a season into f1.raw.*

Idempotent: re-running (whether on schedule or on demand) deletes and
re-inserts rows for every session fetched this run, so corrections published
upstream by OpenF1 are picked up and no duplicates accumulate.
"""
import json
import os
import time

import duckdb
import requests

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 2
INTER_REQUEST_SLEEP_SEC = 0.2


def fetch(endpoint, params):
    url = f"{BASE_URL}/{endpoint}"
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            last_err = err
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    raise RuntimeError(f"GET {url} params={params} failed after {MAX_RETRIES} attempts") from last_err


def load_table(con, tmp_path, records, table, key_columns, key_values):
    """Bulk-load `records` (a list of dicts) into f1.raw.<table>, replacing any
    existing rows matching key_values on key_columns (delete+insert upsert)."""
    if key_values:
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = 'f1' AND table_schema = 'raw' AND table_name = ?",
            [table],
        ).fetchone()[0] > 0
        # Delete stale rows for these keys even if this run fetched zero
        # records for them, so a session that now returns nothing doesn't
        # leave old rows behind.
        if table_exists:
            placeholders = ", ".join("?" for _ in key_values)
            key_expr = key_columns[0] if len(key_columns) == 1 else f"({', '.join(key_columns)})"
            con.execute(
                f"DELETE FROM f1.raw.{table} WHERE {key_expr} IN ({placeholders})",
                key_values,
            )

    if not records:
        return 0

    with open(tmp_path, "w") as f:
        json.dump(records, f)

    con.execute(
        f"CREATE TABLE IF NOT EXISTS f1.raw.{table} AS "
        f"SELECT * FROM read_json_auto('{tmp_path}') WHERE false"
    )

    con.execute(f"INSERT INTO f1.raw.{table} SELECT * FROM read_json_auto('{tmp_path}')")
    return len(records)


def main():
    season_year = os.environ.get("SEASON_YEAR", "2025")

    all_race_type_sessions = fetch("sessions", {"year": season_year, "session_type": "Race"})
    # OpenF1 tags Sprint sessions with session_type=Race too; keep full-length
    # Race sessions only, per the "Race only" pace-comparison scope.
    sessions = [s for s in all_race_type_sessions if s.get("session_name") == "Race"]

    if not sessions:
        print(f"No Race sessions found for season {season_year}; nothing to do.")
        return

    session_keys = [s["session_key"] for s in sessions]
    meeting_keys = sorted({s["meeting_key"] for s in sessions})

    all_meetings = fetch("meetings", {"year": season_year})
    meetings = [m for m in all_meetings if m["meeting_key"] in set(meeting_keys)]

    drivers = []
    laps = []
    for session_key in session_keys:
        drivers.extend(fetch("drivers", {"session_key": session_key}))
        time.sleep(INTER_REQUEST_SLEEP_SEC)
        laps.extend(fetch("laps", {"session_key": session_key}))
        time.sleep(INTER_REQUEST_SLEEP_SEC)

    con = duckdb.connect("md:")

    n_meetings = load_table(
        con, "/tmp/meetings.json", meetings, "meetings", ["meeting_key"], meeting_keys
    )
    n_sessions = load_table(
        con, "/tmp/sessions.json", sessions, "sessions", ["session_key"], session_keys
    )
    n_drivers = load_table(
        con, "/tmp/drivers.json", drivers, "drivers", ["session_key"], session_keys
    )
    n_laps = load_table(
        con, "/tmp/laps.json", laps, "laps", ["session_key"], session_keys
    )

    print(
        f"season={season_year} sessions={n_sessions} meetings={n_meetings} "
        f"drivers={n_drivers} laps={n_laps}"
    )


if __name__ == "__main__":
    main()
