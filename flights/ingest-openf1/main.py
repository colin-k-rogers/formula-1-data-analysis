"""MotherDuck Flight: ingest OpenF1 session/meeting/driver/lap data for a
season into f1.raw.* (a branch-scoped raw_<suffix> schema in preview, so a
preview run never writes into prod's raw data).

Idempotent: re-running (whether on schedule or on demand) deletes and
re-inserts rows for every session refreshed this run, so corrections
published upstream by OpenF1 are picked up and no duplicates accumulate.

Drivers/laps fetching is incremental (see `needs_refresh`): a session that
already has the data stored and finished more than RECENCY_WINDOW ago is
treated as final and skipped, so a run doesn't re-pull the whole season's
data every time — only new, upcoming, or recently-finished sessions.
"""
# Run-visualization chart; node ids join to the @flight-run lines below. The
# convention is MotherDuck's flight viz guide (get_flight_viz_guide).
#
# DRIFT TEST: plan is really :::query; declared :::transform on purpose.
#
# @flight
# flowchart TD
#   sessions[Fetch sessions]:::extract --> any{Target sessions?}:::check
#   any -->|none| nothing(Nothing to do)
#   any -->|found| meetings[Fetch meetings]:::extract
#   meetings --> plan[Pick sessions to refresh]:::transform
#   plan --> drivers[Fetch drivers]:::extract
#   drivers --> laps[Fetch laps]:::extract
#   laps --> load[Load raw tables]:::load
#   class drivers,laps fraction
#   class load fanout
# @end-flight
import json
import os
import time
from datetime import datetime, timedelta, timezone

import duckdb
import requests

BASE_URL = "https://api.openf1.org/v1"
# Branch-scoped in preview (see motherduck.yml's preview_suffix), "" in prod.
RAW_SCHEMA = f"raw{os.environ.get('SCHEMA_SUFFIX', '')}"
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
    """Bulk-load `records` (a list of dicts) into f1.<RAW_SCHEMA>.<table>,
    replacing any existing rows matching key_values on key_columns
    (delete+insert upsert)."""
    if key_values:
        table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = 'f1' AND table_schema = ? AND table_name = ?",
            [RAW_SCHEMA, table],
        ).fetchone()[0] > 0
        # Delete stale rows for these keys even if this run fetched zero
        # records for them, so a session that now returns nothing doesn't
        # leave old rows behind.
        if table_exists:
            placeholders = ", ".join("?" for _ in key_values)
            key_expr = key_columns[0] if len(key_columns) == 1 else f"({', '.join(key_columns)})"
            con.execute(
                f'DELETE FROM f1."{RAW_SCHEMA}".{table} WHERE {key_expr} IN ({placeholders})',
                key_values,
            )

    if not records:
        return 0

    with open(tmp_path, "w") as f:
        json.dump(records, f)

    con.execute(
        f'CREATE TABLE IF NOT EXISTS f1."{RAW_SCHEMA}".{table} AS '
        f"SELECT * FROM read_json_auto('{tmp_path}') WHERE false"
    )

    # A column that's NULL in every row of the batch that first creates the
    # table gets inferred as JSON (DuckDB's fallback type for an all-null
    # sample) -- widen it to VARCHAR so a later batch with real string
    # values for that column (e.g. country_code) doesn't fail to cast.
    json_columns = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = 'f1' AND table_schema = ? AND table_name = ? "
        "AND data_type = 'JSON'",
        [RAW_SCHEMA, table],
    ).fetchall()
    for (column_name,) in json_columns:
        con.execute(f'ALTER TABLE f1."{RAW_SCHEMA}".{table} ALTER COLUMN {column_name} TYPE VARCHAR')

    con.execute(f'INSERT INTO f1."{RAW_SCHEMA}".{table} SELECT * FROM read_json_auto(\'{tmp_path}\')')
    return len(records)


def sessions_with_rows(con, table):
    """session_keys that already have at least one row in f1.<RAW_SCHEMA>.<table>."""
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = 'f1' AND table_schema = ? AND table_name = ?",
        [RAW_SCHEMA, table],
    ).fetchone()[0] > 0
    if not table_exists:
        return set()
    return {
        row[0]
        for row in con.execute(
            f'SELECT DISTINCT session_key FROM f1."{RAW_SCHEMA}".{table}'
        ).fetchall()
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


def refresh_keys_for(con, table, candidate_sessions, now):
    """session_keys among `candidate_sessions` worth (re-)fetching fresh
    f1.<RAW_SCHEMA>.<table> data for."""
    already_have = sessions_with_rows(con, table)
    return [s["session_key"] for s in candidate_sessions if needs_refresh(s, already_have, now)]


def main():
    season_year = os.environ.get("SEASON_YEAR", "2026")

    # No server-side session_type filter: Sprint shares session_type=Race
    # with the Race session itself, and Qualifying is its own session_type,
    # so session_name is the only field that actually distinguishes the
    # sessions we want from practice/testing/Sprint Qualifying noise.
    emit("sessions", "running")
    all_sessions = fetch("sessions", {"year": season_year})
    emit("sessions", "ok", rows=len(all_sessions), effect="net:api.openf1.org/v1/sessions")
    # Cancelled sessions have no laps/drivers data at all (OpenF1 404s those
    # endpoints for them), so skip them too.
    emit("any", "running")
    sessions = [
        s
        for s in all_sessions
        if s.get("session_name") in TARGET_SESSION_NAMES and not s.get("is_cancelled")
    ]
    emit("any", "ok", rows=len(sessions), effect="file:/tmp/target_sessions.json")  # DRIFT TEST

    if not sessions:
        emit("nothing", "running")
        print(f"No target sessions found for season {season_year}; nothing to do.")
        emit("nothing", "ok")
        return

    session_keys = [s["session_key"] for s in sessions]
    meeting_keys = sorted({s["meeting_key"] for s in sessions})

    emit("meetings", "running")
    all_meetings = fetch("meetings", {"year": season_year})
    meetings = [m for m in all_meetings if m["meeting_key"] in set(meeting_keys)]
    emit("meetings", "ok", rows=len(meetings), effect="net:api.openf1.org/v1/meetings")

    emit("plan", "running")
    con = duckdb.connect("md:")
    # Preview's RAW_SCHEMA (e.g. raw_preview_<branch>) won't exist yet the
    # first time a branch runs this Flight -- CREATE TABLE doesn't implicitly
    # create a missing schema. Prod's "raw" already exists, so this is a no-op
    # there.
    con.execute(f'CREATE SCHEMA IF NOT EXISTS f1."{RAW_SCHEMA}"')
    now = datetime.now(timezone.utc)

    # Drivers and laps both matter for every target session type now -- a
    # Qualifying or Sprint radio message needs driver_number -> name/team
    # just as much as a Race one does, and fct_radio_messages needs a lap
    # number for those sessions too. Each table still gets its own
    # incremental refresh (a session can need one refreshed without the
    # other), but both draw from the same candidate pool. Race-only scoping
    # for lap-pace comparison (fct_lap_pace / the Relative Lap Pace dive) is
    # enforced in that mart itself, not here -- see fct_lap_pace.sql.
    driver_refresh_keys = refresh_keys_for(con, "drivers", sessions, now)
    lap_refresh_keys = refresh_keys_for(con, "laps", sessions, now)
    emit(
        "plan", "ok",
        effect=f"table:f1.{RAW_SCHEMA}.drivers",  # DRIFT TEST
        drivers_refresh=f"{len(driver_refresh_keys)}/{len(session_keys)}",
        laps_refresh=f"{len(lap_refresh_keys)}/{len(session_keys)}",
    )

    drivers = fetch_per_session("drivers", driver_refresh_keys)
    laps = fetch_per_session("laps", lap_refresh_keys)

    loads = [
        ("meetings", meetings, ["meeting_key"], meeting_keys),
        ("sessions", sessions, ["session_key"], session_keys),
        ("drivers", drivers, ["session_key"], driver_refresh_keys),
        ("laps", laps, ["session_key"], lap_refresh_keys),
    ]
    counts = {}
    for table, records, key_columns, key_values in loads:
        emit("load", "running", key=table)
        counts[table] = load_table(
            con, f"/tmp/{table}.json", records, table, key_columns, key_values
        )
        emit("load", "ok", key=table, rows=counts[table], effect=f"table:f1.{RAW_SCHEMA}.{table}")

    print(
        f"season={season_year} sessions={counts['sessions']} meetings={counts['meetings']} "
        f"drivers={counts['drivers']} laps={counts['laps']} "
        f"drivers_refreshed={len(driver_refresh_keys)}/{len(session_keys)} "
        f"laps_refreshed={len(lap_refresh_keys)}/{len(session_keys)} sessions"
    )


def fetch_per_session(endpoint, session_keys):
    """Fetch `endpoint` for each session, reporting progress on the chart
    node of the same name."""
    total = len(session_keys)
    emit(endpoint, "running", done=0, total=total)
    records = []
    for done, session_key in enumerate(session_keys, start=1):
        records.extend(fetch(endpoint, {"session_key": session_key}))
        emit(endpoint, "running", done=done, total=total)
        time.sleep(INTER_REQUEST_SLEEP_SEC)
    emit(endpoint, "ok", done=total, total=total, rows=len(records),
         effect=f"net:api.openf1.org/v1/{endpoint}")
    return records


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
