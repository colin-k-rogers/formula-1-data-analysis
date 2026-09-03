"""MotherDuck Flight: ingest OpenF1 session/meeting/driver/lap data for a
season into f1.raw.*

Idempotent: re-running (whether on schedule or on demand) deletes and
re-inserts rows for every session refreshed this run, so corrections
published upstream by OpenF1 are picked up and no duplicates accumulate.

Drivers/laps fetching is incremental (see `needs_refresh`): a session that
already has the data stored and finished more than RECENCY_WINDOW ago is
treated as final and skipped, so a run doesn't re-pull the whole season's
data every time — only new, upcoming, or recently-finished sessions.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import duckdb
import requests

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 3
MAX_RATE_LIMIT_RETRIES = 6
RETRY_BACKOFF_SEC = 2
RATE_LIMIT_BACKOFF_SEC = 5
INTER_REQUEST_SLEEP_SEC = 0.5
# How long after a session finishes (or before it starts) to keep re-fetching
# its laps, to catch upstream corrections / lineup changes. Comfortably wider
# than the weekly schedule so every session gets re-checked at least once
# after it actually happens before being considered final.
RECENCY_WINDOW = timedelta(days=7)
# Kept in sync with spark_jobs/radio_topic_modeling/job.py's
# TARGET_SESSION_NAMES: that job transcribes radio for these session types,
# and needs dim_sessions/dim_drivers (sourced from here) to actually have
# session/meeting/driver metadata for them, not just for Race.
TARGET_SESSION_NAMES = {"Race", "Qualifying", "Sprint"}


def fetch(endpoint, params):
    """GET with retries. 429s get their own longer, more patient backoff
    budget (honoring Retry-After when OpenF1 sends it) since they reflect
    rate limiting rather than a transient failure. A 404 means OpenF1 has no
    rows for these params (e.g. laps for a session that hasn't run yet) —
    treat that as an empty result rather than an error."""
    url = f"{BASE_URL}/{endpoint}"
    last_err = None
    retries_used = 0
    rate_limit_retries_used = 0
    while retries_used < MAX_RETRIES and rate_limit_retries_used < MAX_RATE_LIMIT_RETRIES:
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            last_err = err
            status = err.response.status_code if err.response is not None else None
            if status == 429:
                retry_after = err.response.headers.get("Retry-After")
                sleep_sec = float(retry_after) if retry_after else RATE_LIMIT_BACKOFF_SEC * (2 ** rate_limit_retries_used)
                rate_limit_retries_used += 1
            else:
                retries_used += 1
                sleep_sec = RETRY_BACKOFF_SEC * retries_used
            time.sleep(sleep_sec)
    total_attempts = 1 + retries_used + rate_limit_retries_used
    raise RuntimeError(f"GET {url} params={params} failed after {total_attempts} attempts") from last_err


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

    # A column that's NULL in every row of the batch that first creates the
    # table gets inferred as JSON (DuckDB's fallback type for an all-null
    # sample) -- widen it to VARCHAR so a later batch with real string
    # values for that column (e.g. country_code) doesn't fail to cast.
    json_columns = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = 'f1' AND table_schema = 'raw' AND table_name = ? "
        "AND data_type = 'JSON'",
        [table],
    ).fetchall()
    for (column_name,) in json_columns:
        con.execute(f"ALTER TABLE f1.raw.{table} ALTER COLUMN {column_name} TYPE VARCHAR")

    con.execute(f"INSERT INTO f1.raw.{table} SELECT * FROM read_json_auto('{tmp_path}')")
    return len(records)


def sessions_with_rows(con, table):
    """session_keys that already have at least one row in f1.raw.<table>."""
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = 'f1' AND table_schema = 'raw' AND table_name = ?",
        [table],
    ).fetchone()[0] > 0
    if not table_exists:
        return set()
    return {
        row[0]
        for row in con.execute(f"SELECT DISTINCT session_key FROM f1.raw.{table}").fetchall()
    }


def needs_refresh(session, already_have, now):
    """Worth (re-)fetching drivers/laps for this session if we don't have any
    rows for it yet, or it's within RECENCY_WINDOW of now (upcoming/in
    progress, or recently finished and might still get corrections)."""
    if session["session_key"] not in already_have:
        return True
    date_end = session.get("date_end")
    if not date_end:
        return True
    finished_at = datetime.fromisoformat(date_end)
    return abs(now - finished_at) <= RECENCY_WINDOW


def main():
    season_year = os.environ.get("SEASON_YEAR", "2026")

    # No server-side session_type filter: Sprint shares session_type=Race
    # with the Race session itself, and Qualifying is its own session_type,
    # so session_name is the only field that actually distinguishes the
    # sessions we want from practice/testing/Sprint Qualifying noise.
    all_sessions = fetch("sessions", {"year": season_year})
    # Cancelled sessions have no laps/drivers data at all (OpenF1 404s those
    # endpoints for them), so skip them too.
    sessions = [
        s
        for s in all_sessions
        if s.get("session_name") in TARGET_SESSION_NAMES and not s.get("is_cancelled")
    ]

    if not sessions:
        print(f"No target sessions found for season {season_year}; nothing to do.")
        return

    session_keys = [s["session_key"] for s in sessions]
    meeting_keys = sorted({s["meeting_key"] for s in sessions})

    all_meetings = fetch("meetings", {"year": season_year})
    meetings = [m for m in all_meetings if m["meeting_key"] in set(meeting_keys)]

    con = duckdb.connect("md:")
    now = datetime.now(timezone.utc)

    # Driver rosters matter for every target session type -- a Qualifying or
    # Sprint radio message needs driver_number -> name/team just as much as
    # a Race one does -- so refresh them incrementally across all of
    # `sessions`, not just Race.
    already_have_drivers = sessions_with_rows(con, "drivers")
    driver_refresh_sessions = [s for s in sessions if needs_refresh(s, already_have_drivers, now)]
    driver_refresh_keys = [s["session_key"] for s in driver_refresh_sessions]

    # Lap-by-lap pace comparison (fct_lap_pace / the Relative Lap Pace dive)
    # is deliberately Race-only: Qualifying is flying laps with no stable
    # "gap to the field" and Sprint is a shorter, differently-fueled race
    # distance, neither comparable lap-for-lap to Race pace. Keep laps
    # ingestion scoped to Race sessions only so that dive's scope doesn't
    # silently change underneath it.
    race_sessions = [s for s in sessions if s.get("session_name") == "Race"]
    already_have_laps = sessions_with_rows(con, "laps")
    lap_refresh_sessions = [s for s in race_sessions if needs_refresh(s, already_have_laps, now)]
    lap_refresh_keys = [s["session_key"] for s in lap_refresh_sessions]

    drivers = []
    for session_key in driver_refresh_keys:
        drivers.extend(fetch("drivers", {"session_key": session_key}))
        time.sleep(INTER_REQUEST_SLEEP_SEC)

    laps = []
    for session_key in lap_refresh_keys:
        laps.extend(fetch("laps", {"session_key": session_key}))
        time.sleep(INTER_REQUEST_SLEEP_SEC)

    n_meetings = load_table(
        con, "/tmp/meetings.json", meetings, "meetings", ["meeting_key"], meeting_keys
    )
    n_sessions = load_table(
        con, "/tmp/sessions.json", sessions, "sessions", ["session_key"], session_keys
    )
    n_drivers = load_table(
        con, "/tmp/drivers.json", drivers, "drivers", ["session_key"], driver_refresh_keys
    )
    n_laps = load_table(
        con, "/tmp/laps.json", laps, "laps", ["session_key"], lap_refresh_keys
    )

    print(
        f"season={season_year} sessions={n_sessions} meetings={n_meetings} "
        f"drivers={n_drivers} laps={n_laps} "
        f"drivers_refreshed={len(driver_refresh_keys)}/{len(session_keys)} "
        f"laps_refreshed={len(lap_refresh_keys)}/{len(race_sessions)} sessions"
    )


if __name__ == "__main__":
    main()
